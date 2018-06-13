/**
 *    Copyright (C) 2016 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/db/s/balancer/balancer.h"

#include <algorithm>
#include <string>
#include "mongo/db/modules/rocks/src/GlobalConfig.h"

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/client/read_preference.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/s/balancer/balancer_chunk_selection_policy_impl.h"
#include "mongo/db/s/balancer/cluster_statistics_impl.h"
#include "mongo/db/s/balancer/state_machine_assign_event.h"
#include "mongo/db/s/balancer/state_machine_offload_event.h"
#include "mongo/db/s/balancer/state_machine_move_event.h"
#include "mongo/db/s/balancer/state_machine_split_event.h"
#include "mongo/s/balancer_configuration.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/cluster_identity_loader.h"
#include "mongo/s/grid.h"
#include "mongo/s/shard_util.h"
#include "mongo/s/sharding_raii.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/timer.h"
#include "mongo/util/version.h"
#include "mongo/s/offload_chunk_request.h"
#include "mongo/s/assign_chunk_request.h"
#include "mongo/s/split_chunk_request.h"
#include "mongo/s/catalog/sharding_catalog_manager.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/util_extend/default_parameters.h"

namespace mongo {

using std::map;
using std::string;
using std::vector;
using str::stream;

namespace {

const ReadPreferenceSetting kPrimaryOnlyReadPreference{ReadPreference::PrimaryOnly};

const auto getBalancer = ServiceContext::declareDecoration<std::unique_ptr<Balancer>>();

/**
 * Utility class to generate timing and statistics for a single balancer round.
 */
class BalanceRoundDetails {
public:
    BalanceRoundDetails() : _executionTimer() {}

    void setSucceeded(int candidateChunks, int chunksMoved) {
        invariant(!_errMsg);
        _candidateChunks = candidateChunks;
        _chunksMoved = chunksMoved;
    }

    void setFailed(const string& errMsg) {
        _errMsg = errMsg;
    }

    BSONObj toBSON() const {
        BSONObjBuilder builder;
        builder.append("executionTimeMillis", _executionTimer.millis());
        builder.append("errorOccured", _errMsg.is_initialized());

        if (_errMsg) {
            builder.append("errmsg", *_errMsg);
        } else {
            builder.append("candidateChunks", _candidateChunks);
            builder.append("chunksMoved", _chunksMoved);
        }

        return builder.obj();
    }

private:
    const Timer _executionTimer;

    // Set only on success
    int _candidateChunks{0};
    int _chunksMoved{0};

    // Set only on failure
    boost::optional<string> _errMsg;
};

/**
 * Occasionally prints a log message with shard versions if the versions are not the same
 * in the cluster.
 */
void warnOnMultiVersion(const vector<ClusterStatistics::ShardStatistics>& clusterStats) {
    auto&& vii = VersionInfoInterface::instance();

    bool isMultiVersion = false;
    for (const auto& stat : clusterStats) {
        if (!vii.isSameMajorVersion(stat.mongoVersion.c_str())) {
            isMultiVersion = true;
            break;
        }
    }

    // If we're all the same version, don't message
    if (!isMultiVersion)
        return;

    StringBuilder sb;
    sb << "Multi version cluster detected. Local version: " << vii.version()
       << ", shard versions: ";

    for (const auto& stat : clusterStats) {
        sb << stat.shardId << " is at " << stat.mongoVersion << "; ";
    }

    warning() << sb.str();
}

}  // namespace

Balancer::Balancer(ServiceContext* serviceContext)
    : _balancedLastTime(0),
      _clusterStats(stdx::make_unique<ClusterStatisticsImpl>()),
      _chunkSelectionPolicy(
          stdx::make_unique<BalancerChunkSelectionPolicyImpl>(_clusterStats.get())),
      _migrationManager(serviceContext) {}

Balancer::~Balancer() {
    // The balancer thread must have been stopped
    stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
    invariant(_state == kStopped);
}

void Balancer::create(ServiceContext* serviceContext) {
    invariant(!getBalancer(serviceContext));
    getBalancer(serviceContext) = stdx::make_unique<Balancer>(serviceContext);
}

Balancer* Balancer::get(ServiceContext* serviceContext) {
    return getBalancer(serviceContext).get();
}

Balancer* Balancer::get(OperationContext* operationContext) {
    return get(operationContext->getServiceContext());
}

void Balancer::onTransitionToPrimary(OperationContext* txn) {
    stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
    invariant(_state == kStopped);
    _state = kRunning;

    _migrationManager.startRecoveryAndAcquireDistLocks(txn);

    invariant(!_thread.joinable());
    invariant(!_threadOperationContext);
    _thread = stdx::thread([this] { _mainThread(); });
}

void Balancer::onStepDownFromPrimary() {
    stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
    if (_state != kRunning)
        return;

    _state = kStopping;

    // Interrupt the balancer thread if it has been started. We are guaranteed that the operation
    // context of that thread is still alive, because we hold the balancer mutex.
    if (_threadOperationContext) {
        stdx::lock_guard<Client> scopedClientLock(*_threadOperationContext->getClient());
        _threadOperationContext->markKilled(ErrorCodes::InterruptedDueToReplStateChange);
    }

    // Schedule a separate thread to shutdown the migration manager in order to avoid deadlock with
    // replication step down
    invariant(!_migrationManagerInterruptThread.joinable());
    _migrationManagerInterruptThread =
        stdx::thread([this] { _migrationManager.interruptAndDisableMigrations(); });

    _condVar.notify_all();
}

void Balancer::onDrainComplete(OperationContext* txn) {
    invariant(!txn->lockState()->isLocked());

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        if (_state == kStopped)
            return;

        invariant(_state == kStopping);
        invariant(_thread.joinable());
    }

    _thread.join();

    stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
    _state = kStopped;
    _thread = {};

    LOG(1) << "Balancer thread terminated";
}

void Balancer::joinCurrentRound(OperationContext* txn) {
    stdx::unique_lock<stdx::mutex> scopedLock(_mutex);
    const auto numRoundsAtStart = _numBalancerRounds;
    _condVar.wait(scopedLock,
                  [&] { return !_inBalancerRound || _numBalancerRounds != numRoundsAtStart; });
}

Status Balancer::rebalanceSingleChunk(OperationContext* txn, const ChunkType& chunk) {
    auto migrateStatus = _chunkSelectionPolicy->selectSpecificChunkToMove(txn, chunk);
    if (!migrateStatus.isOK()) {
        return migrateStatus.getStatus();
    }

    auto migrateInfo = std::move(migrateStatus.getValue());
    if (!migrateInfo) {
        LOG(1) << "Unable to find more appropriate location for chunk " << redact(chunk.toString());
        return Status::OK();
    }

    auto balancerConfig = Grid::get(txn)->getBalancerConfiguration();
    Status refreshStatus = balancerConfig->refreshAndCheck(txn);
    if (!refreshStatus.isOK()) {
        return refreshStatus;
    }

    return _migrationManager.executeManualMigration(txn,
                                                    *migrateInfo,
                                                    balancerConfig->getMaxChunkSizeBytes(),
                                                    balancerConfig->getSecondaryThrottle(),
                                                    balancerConfig->waitForDelete());
}

Status Balancer::moveSingleChunk(OperationContext* txn,
                                 const ChunkType& chunk,
                                 const ShardId& newShardId,
                                 uint64_t maxChunkSizeBytes,
                                 const MigrationSecondaryThrottleOptions& secondaryThrottle,
                                 bool waitForDelete,
                                 bool userCommand) {
    auto processedChunk = chunk;
    auto moveAllowedStatus = _chunkSelectionPolicy->checkMoveAllowed(txn, processedChunk, newShardId);
    if (!moveAllowedStatus.isOK()) {
        return moveAllowedStatus;
    }

    if (processedChunk.getProcessIdentity() == "noidentiy") {
        auto getShardTypeStatus =
            Grid::get(txn)->catalogManager()->getShardServerManager()->getShardTypeByShardId(processedChunk.getShard());
        if (!getShardTypeStatus.isOK()) {
            error() << "[moveSingleChunk] cannot get ShardType for shard: "
                    << processedChunk.getShard().toString() + "when moving chunk("
                    << processedChunk.getID() + ")";
            return getShardTypeStatus.getStatus();
        }
        auto processIdent = getShardTypeStatus.getValue().getProcessIdentity();
        processedChunk.setProcessIdentity(processIdent);
    }

    // Create a Move event and execute in state machine
    auto resultNotification = std::make_shared<Notification<ErrorCodes::Error>>();
    auto eventResultInfo = new IRebalanceEvent::EventResultInfo(resultNotification,
                                                                StateMachine::EventType::kMoveEvent);
    auto getShardTypeStatus = 
        Grid::get(txn)->catalogManager()->getShardServerManager()->getShardTypeByShardId(newShardId);
    if (!getShardTypeStatus.isOK()) {
        error() << "[moveSingleChunk] cannot get ShardType for shard: " << newShardId.toString();
        return getShardTypeStatus.getStatus();
    }

    ShardId shardId(getShardTypeStatus.getValue().getName());
    auto processIdent = getShardTypeStatus.getValue().getProcessIdentity();
    ShardIdent shardIdent(shardId, processIdent);
    log() << "[moveSingleChunk] create MoveEvent and execute the event, chunk("
          << processedChunk.toString()
          << ")";

    IRebalanceEvent* event = new MoveEvent(processedChunk,
                                           shardIdent,
                                           eventResultInfo,
                                           userCommand);
    auto executeEventStatus = StateMachine::executeEvent(txn, event);
    if (!executeEventStatus.isOK()) {
        error() << "[moveSingleChunk] failed to execute moveEvent for chunk("
                << processedChunk.getID() + "), due to "
                << executeEventStatus.reason();
        StateMachine::deleteEvent(txn, event);
        return executeEventStatus;
    }

    if (userCommand) {
        ErrorCodes::Error moveResultCode = eventResultInfo->resultNotification->get();
        if (moveResultCode != ErrorCodes::OK) {
            error() << "[moveSingleChunk] failed to move chunk("
                    << processedChunk.getID() + "), due to"
                    << eventResultInfo->getErrorMsg();
            StateMachine::deleteEvent(txn, event);
            return Status(eventResultInfo->getErrorCode(), eventResultInfo->getErrorMsg());
        }
        StateMachine::deleteEvent(txn, event);
    }

    return Status::OK();
}
void Balancer::deleteAndClearEvent(OperationContext* txn,std::vector<IRebalanceEvent*> &evs,const std::string& ns){
     for( IRebalanceEvent* event : evs ){
         StateMachine::deleteEvent(txn, event);
     }
     {
         stdx::lock_guard<stdx::mutex> lock(_nsToVectorMutex);       
         NsEventVectorMap::iterator it = events.find(ns);
         events.erase(it);
     }
}
Status Balancer::getResults(OperationContext* txn,const std::string& ns){
   std::vector<IRebalanceEvent*> envetVector;
   NsEventVectorMap::iterator it = events.find(ns); 
   if( it != events.end() ){
        envetVector = it->second;
   }else{
        log()<<" ns not found";
        return {ErrorCodes::BadValue,
                str::stream() << "ns [" << ns  << "] not found."};
   }
   LOG(1)<<"events size:"<< envetVector.size();

   Status ret_status = Status::OK();
   for(IRebalanceEvent* event : envetVector){
       auto eventResultInfo = event->getEventResultInfo();  
       ErrorCodes::Error moveResultCode = eventResultInfo->resultNotification->get();
       if (moveResultCode != ErrorCodes::OK) {
           error() << "[assignChunk] failed to assignChunk("
                   << event->getChunk().getID() + "), due to "
                   << eventResultInfo->getErrorMsg();
           ret_status = Status(eventResultInfo->getErrorCode(), eventResultInfo->getErrorMsg());
           //deleteAndClearEvent(txn,envetVector,ns);
           //return st;
       }
       LOG(1)<<"deleteEvent :"<<event->getChunk().getID();
       StateMachine::deleteEvent(txn, event);
   }
   stdx::lock_guard<stdx::mutex> lock(_nsToVectorMutex);
   events.erase(it);
   return ret_status;
}

Status Balancer::assignChunk(OperationContext* txn,
                             const ChunkType& chunk,
                             bool newChunk,
                             bool userCommand,
                             ShardId newShardId,
                             bool flag)
{
    // TODO: do some checking: chunk already on the new shard ? chunk state err ?
    auto processedChunk = chunk;
    ShardIdent newShardIdent;

    // Create a Assign event and execute in state machine
    auto resultNotification = std::make_shared<Notification<ErrorCodes::Error>>();
    auto eventResultInfo = new IRebalanceEvent::EventResultInfo(resultNotification,
                                                                StateMachine::EventType::kAssignEvent);
    getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+chunk.getName())->noteStageStart("getShardType");
    if (!newShardId.isValid()) {
        // donot spicify a ShardId when calling assignChunk
        auto getTakeOverShardStatus = 
            Grid::get(txn)->catalogManager()->getShardServerManager()->getTakeOverShard(txn,
                                                                                        userCommand);
        if (!getTakeOverShardStatus.isOK()) {
            error() << "[assignChunk] failed to assign chunk due to "
                    << getTakeOverShardStatus.getStatus().reason();
            return getTakeOverShardStatus.getStatus();
        }
        auto shardType = getTakeOverShardStatus.getValue();
        ShardId shardId(shardType.getName());
        auto processIdent = shardType.getProcessIdentity();
        newShardIdent.setShardId(shardId);
        newShardIdent.setProcessIdentity(processIdent);
    } else {
        auto getShardTypeStatus =
            Grid::get(txn)->catalogManager()->getShardServerManager()->getShardTypeByShardId(newShardId);
        if (!getShardTypeStatus.isOK()) {
            error() << "[assignChunk] cannot get ShardType for shard: "
                    << processedChunk.getShard().toString() + "when assigning chunk("
                    << processedChunk.getID() + ")";
            return getShardTypeStatus.getStatus();
        }
        auto processIdent = getShardTypeStatus.getValue().getProcessIdentity();
        newShardIdent.setShardId(newShardId);
        newShardIdent.setProcessIdentity(processIdent);
    }

    if (processedChunk.getProcessIdentity() == "noidentity") {
        processedChunk.setProcessIdentity(newShardIdent.getProcessIdentity());
    }
    /*
    log() << "[assignChunk] create AssignEvent and execute the event, chunk("
          << processedChunk.toString()
          << "), newShardId is "
          << newShardIdent.getShardId().toString(); */
    IRebalanceEvent* event = new AssignEvent(processedChunk,
                                             newShardIdent,
                                             eventResultInfo,
                                             newChunk,
                                             userCommand);
    getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+chunk.getName())->noteStageStart(
            "executeEvent:State"+std::to_string((int)event->getState()));
    auto executeEventStatus = StateMachine::executeEvent(txn, event);
    if (!executeEventStatus.isOK()) {
        error() << "[assignChunk] failed to execute assignEvent for chunk("
                << processedChunk.getID() + "), due to "
                << executeEventStatus.reason();
        StateMachine::deleteEvent(txn, event);
        return executeEventStatus;
    }

    // For user command, we need to wait for the event to complete synchronously
    if (userCommand) {
        if( flag ){
        	std::string ns = chunk.getNS();
        	{
        	    stdx::lock_guard<stdx::mutex> lock(_nsToVectorMutex);
        	    NsEventVectorMap::iterator it = events.find(ns);
        	    if( it != events.end()){
        	    //auto eventVector = it->second;
        	        it->second.push_back(event);
        	    }else{
        	        std::vector<IRebalanceEvent*> envetVector;
        	        envetVector.push_back(event);
        	        events[ns] = envetVector;
        	    }
        	 }
       }else{
           ErrorCodes::Error assignResultCode = eventResultInfo->resultNotification->get();
           if (ErrorCodes::OK != assignResultCode) {
                error() << "[assignChunk] failed to assignChunk("
                        << processedChunk.getID() + "), due to "
                        << eventResultInfo->getErrorMsg();
                StateMachine::deleteEvent(txn, event);
                return Status(eventResultInfo->getErrorCode(), eventResultInfo->getErrorMsg());
           }
           StateMachine::deleteEvent(txn, event);
        
       }
    }
    return Status::OK();
}

Status Balancer::offloadChunk(OperationContext* txn,
                              const ChunkType& chunk,
                              bool userCommand)
{
    // TODO: do some checking:   chunk state err ?
    auto processedChunk = chunk;
    if (processedChunk.getProcessIdentity() == "noidentity") {
        auto newShardId = processedChunk.getShard();
        auto getShardTypeStatus =
            Grid::get(txn)->catalogManager()->getShardServerManager()->getShardTypeByShardId(newShardId);
        if (!getShardTypeStatus.isOK()) {
            log() << "[offloadChunk] cannot get ShardType for shard: " << newShardId.toString();
            return Status::OK();
        }
        auto processIdent = getShardTypeStatus.getValue().getProcessIdentity();
        processedChunk.setProcessIdentity(processIdent);
    }
    // 1. Create a Offload event and execute in state machine
    auto resultNotification = std::make_shared<Notification<ErrorCodes::Error>>();
    auto eventResultInfo = new IRebalanceEvent::EventResultInfo(resultNotification,
                                                                StateMachine::EventType::kOffloadEvent);
    /*log() << "[offloadChunk] create OffloadEvent and execute the event, chunk("
              << processedChunk.toString()
              << ")";*/

    IRebalanceEvent* event = new OffloadEvent(processedChunk,
                                              eventResultInfo,
                                              userCommand);

    auto executeEventStatus = StateMachine::executeEvent(txn, event);
    if (!executeEventStatus.isOK()) {
        error() << "[offloadChunk] failed to execute offloadEvent for chunk("
                << processedChunk.getID() + "), due to "
                << executeEventStatus.reason();
        StateMachine::deleteEvent(txn, event);
        return executeEventStatus;
    }
    /*log() << "[offloadChunk] succeeds to execute the event, chunk("
          << processedChunk.getID() + ")";*/

    if (userCommand) {
        ErrorCodes::Error offloadResultCode = eventResultInfo->resultNotification->get();
        if (ErrorCodes::OK != offloadResultCode) {
            error() << "[offloadChunk] failed to offload chunk("
                    << processedChunk.getID() + "), due to "
                    << eventResultInfo->getErrorMsg();
            StateMachine::deleteEvent(txn, event);
            return Status(eventResultInfo->getErrorCode(), eventResultInfo->getErrorMsg());
        }
        /*log() << "[offloadChunk] finish offloading the chunk("
              << processedChunk.getID()
              << ")";*/
        StateMachine::deleteEvent(txn, event);
    }

    return Status::OK();
}

Status Balancer::splitChunk(OperationContext* txn,
                            const ChunkType& chunk,
                            const BSONObj& splitPoint,
                            bool userCommand) {
    LOG(3) << "[splitChunk]chunk:" << chunk.toString()
          << "[splitChunk]splitPoint:" << splitPoint.toString();
    auto processedChunk = chunk;

    if (processedChunk.getProcessIdentity() == "noidentity") {
        auto shardId = processedChunk.getShard();
        auto getShardTypeStatus = 
            Grid::get(txn)->catalogManager()->getShardServerManager()->getShardTypeByShardId(shardId);
        if (!getShardTypeStatus.isOK()) {
            error() << "[splitChunk] cannot get ShardType for shard: " << shardId.toString();
            return getShardTypeStatus.getStatus();
        }
        auto processIdent = getShardTypeStatus.getValue().getProcessIdentity();
        processedChunk.setProcessIdentity(processIdent);
    }

    std::string chunkID;
    auto chunkstatus = grid.catalogClient(txn)->generateNewChunkID(txn, chunkID);
    if (!chunkstatus.isOK()) {
        error() << "[splitChunk] fail to generate new chunk id";
        return chunkstatus;
    }

    ChunkType newChunk;
    newChunk.setName(chunkID);
    // create root folder: ploglist for OBSindex and file for Maas
    std::string chunkRootFolder;
    Status status = grid.catalogManager()->createRootFolder(txn, newChunk.getID(), chunkRootFolder);
    if (!status.isOK()) {
        error() << "[splitChunk] fail to create root folder";
        return status;
    }
    log()<<"[splitChunk] create new root folder "<< chunkRootFolder;

    auto newVersionStatus = grid.catalogManager()->newMaxChunkVersion(txn, processedChunk.getNS());
    if (!newVersionStatus.isOK()) {
        error() << "[splitChunk] failed to create version";
        Status status = grid.catalogManager()->deleteRootFolder(chunkRootFolder);
        if (!status.isOK()) {
            error() << "[splitChunk] failed to delete root folder";
            return status;
        }
        return newVersionStatus.getStatus();
    }
    ChunkVersion chunkVersion(newVersionStatus.getValue(), processedChunk.getVersion().epoch());

    // inherit other fields of chunktype from parent chunk, we fill in all the fields
    // due to the validation on chunk during the recovery process

    newChunk.setNS(processedChunk.getNS());
    newChunk.setMin(processedChunk.getMin());
    newChunk.setMax(processedChunk.getMax());
    newChunk.setShard(processedChunk.getShard());
    newChunk.setProcessIdentity(processedChunk.getProcessIdentity());
    newChunk.setVersion(chunkVersion);
    newChunk.setRootFolder(chunkRootFolder);
    newChunk.setStatus(ChunkType::ChunkStatus::kOffloaded);

    // 1. Create a Split event and execute in state machine
    auto resultNotification = std::make_shared<Notification<ErrorCodes::Error>>();
    auto eventResultInfo = new IRebalanceEvent::EventResultInfo(resultNotification,
                                                                StateMachine::EventType::kSplitEvent);
    log() << "[splitChunk] create SplitEvent and execute the event, chunk("
              << processedChunk.toString()
              << "), childChunk("
              << newChunk.toString()
              << ")";

    IRebalanceEvent* event = new SplitEvent(processedChunk,
                                            newChunk,
                                            splitPoint,
                                            eventResultInfo,
                                            userCommand);

    auto executeEventStatus = StateMachine::executeEvent(txn, event);
    if (!executeEventStatus.isOK()) {
        error() << "[splitChunk] failed to execute splitEvent for chunk("
                << processedChunk.getID() + "), due to "
                << executeEventStatus.reason();
        Status status = grid.catalogManager()->deleteRootFolder(chunkRootFolder);
        if (!status.isOK()) {
            error() << "[splitChunk] failed to delete root folder for chunk("
                    << processedChunk.getID() + ")";
        }
        StateMachine::deleteEvent(txn, event);
        return executeEventStatus;
    }
    log() << "[splitChunk] succeeds to execute the event, chunk("
          << processedChunk.getID()
          << ")";

    if (userCommand) {
        ErrorCodes::Error splitResultCode = eventResultInfo->resultNotification->get();
        if (ErrorCodes::OK != splitResultCode) {
            error() << "[splitChunk] fail to split chunk("
                    << processedChunk.getID() + "), due to "
                    << eventResultInfo->getErrorMsg();
            StateMachine::deleteEvent(txn, event);
            return Status(eventResultInfo->getErrorCode(), eventResultInfo->getErrorMsg());
        }
        log() << "[splitChunk] finish spliting the chunk("
              << processedChunk.getID()
              << ")";
        StateMachine::deleteEvent(txn, event);
    }

    return Status::OK();
}

void Balancer::report(OperationContext* txn, BSONObjBuilder* builder) {
    auto balancerConfig = Grid::get(txn)->getBalancerConfiguration();
    balancerConfig->refreshAndCheck(txn);

    const auto mode = balancerConfig->getBalancerMode();

    stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
    builder->append("mode", BalancerSettingsType::kBalancerModes[mode]);
    builder->append("inBalancerRound", _inBalancerRound);
    builder->append("numBalancerRounds", _numBalancerRounds);
}

Status Balancer::checkGCCollection(OperationContext* txn, bool &existed) {

    auto findGCCollectionStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                txn, 
                kPrimaryOnlyReadPreference,
                repl::ReadConcernLevel::kLocalReadConcern,
                NamespaceString(ChunkType::ConfigNS),
                BSON(ChunkType::ns() << "gc.references"),
                BSONObj(),
                boost::none); // no limit need to check.

    if (!findGCCollectionStatus.isOK()) {
        LOG(1) << "error string is " << findGCCollectionStatus.getStatus().toString();
        return findGCCollectionStatus.getStatus();
    }    

    auto gcCollectionDocs = findGCCollectionStatus.getValue().docs;
    if (gcCollectionDocs.size() == 0) { 
        existed = false;
        return Status::OK();
    }    

    existed = true;
    return Status::OK();
}

void Balancer::findAndRunCommand(OperationContext *txn, std::string command, std::string dbname, BSONObj &cmdObj)
{
    int options;
    std::string errmsg;
    BSONObjBuilder result;
    Command* c = nullptr;

    c = Command::findCommand(command);
    c->run(txn, dbname, cmdObj, options, errmsg, result);
}

void Balancer::createGCCollection(OperationContext *txn)
{
    log() << "Create db gc start!";

    BSONObj cmdObj;
    std::string dbname = "gc";
    auto shardingContext = Grid::get(txn);
    shardingContext->catalogClient(txn)->enableSharding(txn, dbname);
#if 0
    {
        BSONObjBuilder builder;
        builder << "create" << "references";
        cmdObj = builder.obj();
        log() << "run create collection named references" << cmdObj;
        findAndRunCommand(txn, "create", dbname, cmdObj);
    }
#endif

    {
        BSONObjBuilder builder, subBuilder;
        subBuilder << "resourceid" << 1.0;
        builder << "shardCollection" << "gc.references" << "key" << subBuilder.obj();
        cmdObj = builder.obj();
        log() << "run shardCollection" << cmdObj;
        findAndRunCommand(txn, "shardCollection", dbname, cmdObj);
    }


    {
        BSONObjBuilder builder, midBuilder, subBuilder;
        BSONArrayBuilder arraryBuilder;

        subBuilder.append("resourceid", 1.0);
        subBuilder.append("chunkid", 1.0);
        midBuilder << "key" << subBuilder.obj() << "name" << "resourceid_1_chunkid_1" << "unique" << true;
        arraryBuilder << midBuilder.obj();
        builder << "createIndexes" << "references" << "indexes" << arraryBuilder.arr();
        cmdObj = builder.obj();
        log() << "cmdObj is : " << cmdObj;
        findAndRunCommand(txn, "createIndexes", dbname, cmdObj);
    }


    {
        BSONObjBuilder builder, midBuilder, subBuilder;
        BSONArrayBuilder arraryBuilder;

        subBuilder.append("deleted", 1.0);
        midBuilder << "key" << subBuilder.obj() << "name" << "deleted_1" << "sparse" << true;
        arraryBuilder << midBuilder.obj();
        builder << "createIndexes" << "references" << "indexes" << arraryBuilder.arr();
        cmdObj = builder.obj();
        log() << "cmdObj is : " << cmdObj;
        findAndRunCommand(txn, "createIndexes", dbname, cmdObj);
    }


    log() << "Create db gc finished!";
}

void Balancer::_mainThread() {
    Client::initThread(kBalancerJobName.c_str());
    auto txn = cc().makeOperationContext();
    auto shardingContext = Grid::get(txn.get());

    LOG(0) << "CSRS balancer is starting";
    LOG(0) << "Find shard.";
    /*while (1) {
        auto findShardStatus =
            Grid::get(txn.get())->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                    txn.get(), 
                    kPrimaryOnlyReadPreference,
                    repl::ReadConcernLevel::kLocalReadConcern,
                    NamespaceString(ShardType::ConfigNS),
                    BSON(ShardType::state() << 1),
                    BSONObj(),
                    boost::none); // no limit need to check.

        const auto shardDocs = findShardStatus.getValue().docs;
        if (shardDocs.size() > 0) { 
            log() << "Awared shard num is :"  << shardDocs.size();
            break;
        }    
        sleep(10);

    }*/
    LOG(0) << "Find shard done.";

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        _threadOperationContext = txn.get();
    }

    const Milliseconds kInitBackoffInterval(10000);

    // Take the balancer distributed lock and hold it permanently. Do the attempts with single
    // attempts in order to not block the thread and be able to check for interrupt more frequently.
    while (!_stopRequested()) {
        auto status = _migrationManager.tryTakeBalancerLock(txn.get(), "CSRS Balancer");
        if (!status.isOK()) {
            log() << "Balancer distributed lock could not be acquired and will be retried in "
                  << durationCount<Seconds>(kInitBackoffInterval) << " seconds"
                  << causedBy(redact(status));

            _sleepFor(txn.get(), kInitBackoffInterval);
            continue;
        }

        break;
    }

    auto balancerConfig = shardingContext->getBalancerConfiguration();
    while (!_stopRequested()) {
        Status refreshStatus = balancerConfig->refreshAndCheck(txn.get());
        if (!refreshStatus.isOK()) {
            warning() << "Balancer settings could not be loaded and will be retried in "
                      << durationCount<Seconds>(kInitBackoffInterval) << " seconds"
                      << causedBy(refreshStatus);

            _sleepFor(txn.get(), kInitBackoffInterval);
            continue;
        }

        break;
    }

    log() << "CSRS balancer thread is recovering";

    _migrationManager.finishRecovery(
        txn.get(), balancerConfig->getMaxChunkSizeBytes(), balancerConfig->getSecondaryThrottle());

    log() << "CSRS balancer thread is recovered";

    // Main balancer loop
    while (!_stopRequested()) {        

        BalanceRoundDetails roundDetails;
        _beginRound(txn.get());

        try {
            shardingContext->shardRegistry()->reload(txn.get());

            uassert(13258, "oids broken after resetting!", _checkOIDs(txn.get()));

            Status refreshStatus = balancerConfig->refreshAndCheck(txn.get());
            if (!refreshStatus.isOK()) {
                warning() << "Skipping balancing round" << causedBy(refreshStatus);
                _endRound(txn.get(), kBalanceRoundDefaultInterval);
                continue;
            }

            if (!balancerConfig->shouldBalance()) {
                LOG(1) << "Skipping balancing round because balancing is disabled";
                _endRound(txn.get(), kBalanceRoundDefaultInterval);
                continue;
            }

            {
                LOG(1) << "*** start balancing round. "
                       << "waitForDelete: " << balancerConfig->waitForDelete()
                       << ", secondaryThrottle: "
                       << balancerConfig->getSecondaryThrottle().toBSON();

                OCCASIONALLY warnOnMultiVersion(
                    uassertStatusOK(_clusterStats->getStats(txn.get())));

                // TODO: we donnt support splitting right now, after implementation of split, recover this
                // TODO: decide witch chunks need to split acording to TPS, and split them
                Status status = _enforceTagRanges(txn.get());
                if (!status.isOK()) {
                    warning() << "Failed to enforce tag ranges" << causedBy(status);
                } else {
                    LOG(1) << "Done enforcing tag range boundaries.";
                }

                status = _findAndSplitChunks(txn.get());
                if (!status.isOK()) {
                    warning() << "Failed to find and split chunks" << causedBy(status);
                } else {
                    LOG(1) << "Done find and split chunks.";
                }

                const auto candidateChunks = uassertStatusOK(
                    _chunkSelectionPolicy->selectChunksToMove(txn.get(), _balancedLastTime));

                if (candidateChunks.empty()) {
                    LOG(1) << "no need to move any chunk";
                    _balancedLastTime = false;
                } else {
                    _balancedLastTime = _moveChunks(txn.get(), candidateChunks);

                    roundDetails.setSucceeded(static_cast<int>(candidateChunks.size()),
                                              _balancedLastTime);

                    shardingContext->catalogClient(txn.get())->logAction(
                        txn.get(), "balancer.round", "", roundDetails.toBSON());
                }

                LOG(1) << "*** End of balancing round";
            }

            _endRound(txn.get(),
                      _balancedLastTime ? kShortBalanceRoundInterval
                                        : kBalanceRoundDefaultInterval);
        } catch (const std::exception& e) {
            log() << "caught exception while doing balance: " << e.what();

            // Just to match the opening statement if in log level 1
            LOG(1) << "*** End of balancing round";

            // This round failed, tell the world!
            roundDetails.setFailed(e.what());

            shardingContext->catalogClient(txn.get())->logAction(
                txn.get(), "balancer.round", "", roundDetails.toBSON());

            // Sleep a fair amount before retrying because of the error
            _endRound(txn.get(), kBalanceRoundDefaultInterval);
        }
    }

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        invariant(_state == kStopping);
        invariant(_migrationManagerInterruptThread.joinable());
    }

    _migrationManagerInterruptThread.join();
    _migrationManager.drainActiveMigrations();

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        _migrationManagerInterruptThread = {};
        _threadOperationContext = nullptr;
    }

    log() << "CSRS balancer is now stopped";
}

bool Balancer::_stopRequested() {
    stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
    return (_state != kRunning);
}

void Balancer::_beginRound(OperationContext* txn) {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    _inBalancerRound = true;
    _condVar.notify_all();
}

void Balancer::_endRound(OperationContext* txn, Milliseconds waitTimeout) {
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _inBalancerRound = false;
        _numBalancerRounds++;
        _condVar.notify_all();
    }

    _sleepFor(txn, waitTimeout);
}

void Balancer::_sleepFor(OperationContext* txn, Milliseconds waitTimeout) {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    _condVar.wait_for(lock, waitTimeout.toSystemDuration(), [&] { return _state != kRunning; });
}

bool Balancer::_checkOIDs(OperationContext* txn) {
    auto shardingContext = Grid::get(txn);

    vector<ShardId> all;
    shardingContext->shardRegistry()->getAllShardIds(&all);

    // map of OID machine ID => shardId
    map<int, ShardId> oids;

    for (const ShardId& shardId : all) {
        if (_stopRequested()) {
            return false;
        }

        auto shardStatus = shardingContext->shardRegistry()->getShard(txn, shardId);
        if (!shardStatus.isOK()) {
            continue;
        }
        const auto s = shardStatus.getValue();

        auto result = uassertStatusOK(
            s->runCommandWithFixedRetryAttempts(txn,
                                                ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                                                "admin",
                                                BSON("features" << 1),
                                                Shard::RetryPolicy::kIdempotent));
        uassertStatusOK(result.commandStatus);
        BSONObj f = std::move(result.response);

        if (f["oidMachine"].isNumber()) {
            int x = f["oidMachine"].numberInt();
            if (oids.count(x) == 0) {
                oids[x] = shardId;
            } else {
                log() << "error: 2 machines have " << x << " as oid machine piece: " << shardId
                      << " and " << oids[x];

                result = uassertStatusOK(s->runCommandWithFixedRetryAttempts(
                    txn,
                    ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                    "admin",
                    BSON("features" << 1 << "oidReset" << 1),
                    Shard::RetryPolicy::kIdempotent));
                uassertStatusOK(result.commandStatus);

                auto otherShardStatus = shardingContext->shardRegistry()->getShard(txn, oids[x]);
                if (otherShardStatus.isOK()) {
                    result = uassertStatusOK(
                        otherShardStatus.getValue()->runCommandWithFixedRetryAttempts(
                            txn,
                            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                            "admin",
                            BSON("features" << 1 << "oidReset" << 1),
                            Shard::RetryPolicy::kIdempotent));
                    uassertStatusOK(result.commandStatus);
                }

                return false;
            }
        } else {
            log() << "warning: oidMachine not set on: " << s->toString();
        }
    }

    return true;
}

Status Balancer::_enforceTagRanges(OperationContext* txn) {
    auto chunksToSplitStatus = _chunkSelectionPolicy->selectChunksToSplit(txn);
    if (!chunksToSplitStatus.isOK()) {
        return chunksToSplitStatus.getStatus();
    }
    
    for (const auto& splitInfo : chunksToSplitStatus.getValue()) {
        LOG(1) << "[Auto-SplitChunk][CandidateSplitChunks] " << "Ns: " << splitInfo.chunk.getNS()
              << ",Name: " << splitInfo.chunk.getName() << ",Min: " << splitInfo.chunk.getMin().toString()
              << ",Max: " << splitInfo.chunk.getMax().toString() << ",SplitPoint: " << splitInfo.splitPoint.toString();
        auto splitStatus = splitChunk(txn,
                                     splitInfo.chunk,
                                     splitInfo.splitPoint,
                                     true);
        
        if (!splitStatus.isOK()) {
            log() << "[Auto-SplitChunk][splitChunk] failed to split chunk("
                  << splitInfo.chunk.getName()
                  << "), due to "
                  << splitStatus.reason();
        } else {
            LOG(1) << "[Auto-SplitChunk][splitChunk] succeed to split chunk("
                  << splitInfo.chunk.getName()
                  << ")";
        }
    }

    return Status::OK();
}

int Balancer::_moveChunks(OperationContext* txn,
                          const BalancerChunkSelectionPolicy::MigrateInfoVector& candidateChunks) {
    auto balancerConfig = Grid::get(txn)->getBalancerConfiguration();

    // If the balancer was disabled since we started this round, don't start new chunk moves
    if (_stopRequested() || !balancerConfig->shouldBalance()) {
        LOG(1) << "Skipping balancing round because balancer was stopped";
        return 0;
    }

    // TODO: async and parallel, use the state machine interface

    int numChunksProcessed = 0;

    for (const auto& migrateInfo : candidateChunks) {
        std::string chunkId = migrateInfo.getName();
        auto findChunkStatus =
            Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                txn,
                kPrimaryOnlyReadPreference,
                repl::ReadConcernLevel::kLocalReadConcern,
                NamespaceString(ChunkType::ConfigNS),
                BSON(ChunkType::name(chunkId)),
                BSONObj(),
                boost::none);

        if (!findChunkStatus.isOK()) {
            log() << "[_moveChunks] failed to get info of chunk " << chunkId;
            continue;
        }

        auto chunkDocs = findChunkStatus.getValue().docs;
        if (chunkDocs.size() == 0) {
            log() << "[_moveChunks] there is no chunk named " << chunkId << "is found";
            continue;
        }

        auto chunkDocStatus = ChunkType::fromBSON(chunkDocs.front());
        if (!chunkDocStatus.isOK()) {
            log() << "[_moveChunks] failed to get chunktype from BSON for " << chunkId;
            continue;
        }

        auto chunkType = chunkDocStatus.getValue();
        if (!chunkType.isAssigned()) {
            log() << "[_moveChunks] cannot move chunk because its status is not assigned";
            continue;
        }

        auto moveStatus = moveSingleChunk(txn,
                                          chunkType,
                                          migrateInfo.to,
                                          balancerConfig->getMaxChunkSizeBytes(),
                                          balancerConfig->getSecondaryThrottle(),
                                          balancerConfig->waitForDelete(),
                                          true);
        if (!moveStatus.isOK()) {
            error() << "[moveSingleChunk] failed to move chunk("
                    << chunkType.toString()
                    << "), due to "
                    << moveStatus.reason();
        } else {
            log() << "[moveSingleChunk] succeed to move chunk("
                  << chunkType.getID()
                  << "), from " + chunkType.getShard().toString()
                  << " to " + migrateInfo.to.toString();
            numChunksProcessed++;
        }
    }

    return numChunksProcessed;
}

Status Balancer::_findAndSplitChunks(OperationContext* txn) {
    if (!GLOBAL_CONFIG_GET(enableAutoChunkSplit))
        return Status::OK();

    const auto balancerConfig = Grid::get(txn)->getBalancerConfiguration();
    Status refreshStatus = balancerConfig->refreshAndCheck(txn);
    if (!refreshStatus.isOK()) {
        warning() << "Unable to refresh balancer settings" << causedBy(refreshStatus);
    }

    bool shouldAutoSplit = balancerConfig->getShouldAutoSplit();
    if (!shouldAutoSplit) {
        return Status::OK();
    }

    auto chunksToSplitStatus = _chunkSelectionPolicy->indexSelectChunksToSplit(txn);
    if (!chunksToSplitStatus.isOK()) {
        return chunksToSplitStatus.getStatus();
    }

    for (const auto& splitInfo : chunksToSplitStatus.getValue()) {
        LOG(1) << "[Auto-SplitChunk][CandidateSplitChunks] " << "Ns: " << splitInfo.chunk.getNS()
              << ",Name: " << splitInfo.chunk.getName() << ",Min: " << splitInfo.chunk.getMin().toString()
              << ",Max: " << splitInfo.chunk.getMax().toString() << ",SplitPoint: " << splitInfo.splitPoint.toString();

        auto splitStatus = splitChunk(txn,
                                     splitInfo.chunk,
                                     splitInfo.splitPoint,
                                     true);
        
        if (!splitStatus.isOK()) {
            log() << "[Auto-SplitChunk][splitChunk] failed to split chunk("
                  << splitInfo.chunk.getName()
                  << "), due to "
                  << splitStatus.reason();
        } else {
            LOG(1) << "[Auto-SplitChunk][splitChunk] succeed to split chunk("
                  << splitInfo.chunk.getName()
                  << ")";
        }

    }
    
    return Status::OK();
                          
}

void Balancer::_splitOrMarkJumbo(OperationContext* txn,
                                 const NamespaceString& nss,
                                 const BSONObj& minKey) {
    auto scopedChunkManager = uassertStatusOK(ScopedChunkManager::getExisting(txn, nss));
    ChunkManager* const chunkManager = scopedChunkManager.cm();

    auto chunk = chunkManager->findIntersectingChunkWithSimpleCollation(txn, minKey);

    auto splitStatus = chunk->split(txn, Chunk::normal, nullptr);
    if (!splitStatus.isOK()) {
        log() << "Marking chunk " << chunk->toString() << " as jumbo.";
        chunk->markAsJumbo(txn);
    }
}

}  // namespace mongo

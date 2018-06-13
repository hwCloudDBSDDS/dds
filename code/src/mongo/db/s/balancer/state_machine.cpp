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

#include <list>
#include <map>
#include <vector>

#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/remote_command_targeter.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/s/balancer/balancer.h"
#include "mongo/db/s/balancer/state_machine.h"
#include "mongo/db/s/balancer/state_machine_assign_event.h"
#include "mongo/db/s/balancer/state_machine_offload_event.h"
#include "mongo/db/s/balancer/state_machine_move_event.h"
#include "mongo/db/s/balancer/state_machine_split_event.h"
#include "mongo/db/s/balancer/type_rebalance_events.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/sharding_catalog_manager.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/sharding_raii.h"
#include "mongo/s/assign_chunk_request.h"
#include "mongo/s/offload_chunk_request.h"
#include "mongo/s/split_chunk_request.h"
#include "mongo/s/confirm_split_request.h"
#include "mongo/util/log.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/stacktrace.h"
#include "mongo/s/catalog/type_chunk.h"





namespace mongo {

using CallbackHandle = executor::TaskExecutor::CallbackHandle;
using CallbackArgs = executor::TaskExecutor::CallbackArgs;
using RemoteCommandCallbackArgs = executor::TaskExecutor::RemoteCommandCallbackArgs;
using RemoteCommandCallbackFn = executor::TaskExecutor::RemoteCommandCallbackFn;
using RemoteCommandRequest = executor::RemoteCommandRequest;
using RemoteCommandResponse = executor::RemoteCommandResponse;
using std::shared_ptr;
using std::vector;
using str::stream;

namespace {

const WriteConcernOptions kMajorityWriteConcern(WriteConcernOptions::kMajority,
                                                WriteConcernOptions::SyncMode::UNSET,
                                                Seconds(10));
const int kMaxRetries = 3;
const Milliseconds kDefaultEventUpdateRetryInterval(500);

// Returns whether the specified status is an error caused by stepdown of the primary config node
// currently running the statemachine.
bool isErrorDueToConfigStepdown(Status status, bool isStopping) {
    return ((status == ErrorCodes::CallbackCanceled && isStopping) ||
            status == ErrorCodes::BalancerInterrupted ||
            status == ErrorCodes::InterruptedDueToReplStateChange);
}

// util function to print log for debugging
void logStateMachineEvent(const std::string& function, IRebalanceEvent* event) {
    /*log() << "[statemachine] "
          << function
          << ", eventid(" + event->getEventId() + "), "
          << "eventtype("
          << static_cast<int>(event->getEventType())
          << "), "
          << "chunk(" + event->getChunk().toString() + "), "
          << "prevState("
          << static_cast<int>(event->getPrevState())
          << "), "
          << "curState("
          << static_cast<int>(event->getState())
          << ")";
   */
}

} // namespace

// Decode RebalanceEventType BSONObj and create corresponding rebalance event.
StatusWith<IRebalanceEvent*> createRebalanceEventFromBSON(OperationContext* txn,
                                                         std::string& eventId,
                                                         StateMachine::EventType eventType,
                                                         const BSONObj& eventData) {
    IRebalanceEvent* rbEvent = nullptr;
    auto resultNotification = std::make_shared<Notification<ErrorCodes::Error>>();
    switch (eventType) {
        case StateMachine::EventType::kAssignEvent:
        {
            auto eventResultInfo = new IRebalanceEvent::EventResultInfo(resultNotification,
                                                                StateMachine::EventType::kAssignEvent);
            AssignEvent *assignEvent = new AssignEvent(eventId,
                                                       eventResultInfo);
            Status status = assignEvent->dataFromBSON(eventData);
            if (!status.isOK()) {
                delete eventResultInfo;
                delete assignEvent;
                return status;
            }

            rbEvent = assignEvent;
            break;
        }

        case StateMachine::EventType::kOffloadEvent:
        {
            auto eventResultInfo = new IRebalanceEvent::EventResultInfo(resultNotification,
                                                        StateMachine::EventType::kOffloadEvent);
            OffloadEvent *offloadEvent = new OffloadEvent(eventId,
                                                          eventResultInfo);
            Status status = offloadEvent->dataFromBSON(eventData);
            if (!status.isOK()) {
                delete eventResultInfo;
                delete offloadEvent;
                return status;
            }

            rbEvent = offloadEvent;
            break;
        }

        case StateMachine::EventType::kMoveEvent:
        {
            auto eventResultInfo = new IRebalanceEvent::EventResultInfo(resultNotification,
                                                        StateMachine::EventType::kMoveEvent);
            MoveEvent *moveEvent = new MoveEvent(eventId,
                                                 eventResultInfo);

            Status status = moveEvent->dataFromBSON(eventData);
            if (!status.isOK()) {
                delete eventResultInfo;
                delete moveEvent;
                return status;
            }

            rbEvent = moveEvent;
            break;
        }

        case StateMachine::EventType::kSplitEvent:
        {
            auto eventResultInfo = new IRebalanceEvent::EventResultInfo(resultNotification,
                StateMachine::EventType::kSplitEvent);

            SplitEvent *splitEvent = new SplitEvent(eventId,
                                                    eventResultInfo);

            Status status = splitEvent->dataFromBSON(eventData);
            if (!status.isOK()) {
                delete eventResultInfo;
                delete splitEvent;
                return status;
            }

            rbEvent = splitEvent;
            break;
        }

        default:
        {
            log() << "[statemachine] unknown event type";
            invariant(false);
        }
    }

    // refresh chunkType according to config.chunks
    SplitEvent* splitEvent = nullptr;
    auto chunkId = rbEvent->getChunk().getID();
    BSONArrayBuilder orQueryBuilder;
    orQueryBuilder.append(BSON(ChunkType::name() << chunkId));
    if (eventType == StateMachine::EventType::kSplitEvent) {
        splitEvent = dynamic_cast<SplitEvent*>(rbEvent);
        auto childChunkId = splitEvent->getChildChunk().getID();
        orQueryBuilder.append(BSON(ChunkType::name() << childChunkId));
    }
    auto findChunkStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                                                    txn,
                                                    ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                                                    repl::ReadConcernLevel::kLocalReadConcern,
                                                    NamespaceString(ChunkType::ConfigNS),
                                                    BSON("$or" << orQueryBuilder.arr()),
                                                    BSONObj(),
                                                    boost::none);
    if (!findChunkStatus.isOK()) {
        return findChunkStatus.getStatus();
    }

    auto chunkDocs = findChunkStatus.getValue().docs;
    if (chunkDocs.size() == 0) {
        return Status(ErrorCodes::BadValue, "can not read ChunkType from configDB");
    }

    if (chunkDocs.size() > 2) {
        return Status(ErrorCodes::BadValue, "read more than two ChunkType from configDB");
    }

    auto chunkDocStatus = ChunkType::fromBSON(chunkDocs[0]);
    if (!chunkDocStatus.isOK()) {
        return Status(ErrorCodes::BadValue, "failed to convert chunktype from bson");
    }
    auto chunkType = chunkDocStatus.getValue();
    if (chunkDocs.size() == 1) {
        rbEvent->refreshChunk(chunkType);
    } else {
        if (splitEvent) {
            auto childChunkDocStatus = ChunkType::fromBSON(chunkDocs[1]);
            if (!chunkDocStatus.isOK()) {
                return Status(ErrorCodes::BadValue, "failed to convert childchunktype from bson");
            }
            auto childChunkType = childChunkDocStatus.getValue();
            splitEvent->refreshChunk(chunkType, childChunkType);
        } else {
            return Status(ErrorCodes::BadValue, "read more than one chunk for non-split event");
        }
    }

    return rbEvent;
}

StateMachine::StateMachine() {
    ThreadPool::Options options;
    options.poolName = "eventHanldingThreadPool";
    options.threadNamePrefix = "eventHanlding";
    options.onCreateThread = [](const std::string& threadName) {
        Client::initThread(threadName.c_str());
    };
    _eventHandlingThreadPool = new ThreadPool(options);
    _eventHandlingThreadPool->startup();
}

StateMachine::~StateMachine() {
    _eventHandlingThreadPool->shutdown();
    _eventHandlingThreadPool->join();
    delete (_eventHandlingThreadPool);
}

StateMachine::State StateMachine::getStateMachineState() {
    return _state;
}

Status StateMachine::writeEventDocument(OperationContext* txn,
                                        IRebalanceEvent* rbEvent) {
    // Try to write a unique statemachine document to config.statemachine.
    const RebalanceEventType rebalanceEventType(rbEvent);
    std::string eventid = rbEvent->getEventId();

    // this is a new event, write a document into config.rebalanceevents
    Status result = grid.catalogClient(txn)->insertConfigDocument(txn,
                                               RebalanceEventType::ConfigNS,
                                               rebalanceEventType.toBSON(),
                                               kMajorityWriteConcern);
    if (!result.isOK()) {
        if (result == ErrorCodes::DuplicateKey) {
            auto statusWithEventQueryResult =
                grid.shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                    txn,
                    ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                    repl::ReadConcernLevel::kLocalReadConcern,
                    NamespaceString(RebalanceEventType::ConfigNS),
                    BSON(RebalanceEventType::eventId(eventid)),
                    BSONObj(),
                    boost::none);
        
            if (!statusWithEventQueryResult.isOK()) {
                log() << "[statemachine] failed to read document due to "
                      << statusWithEventQueryResult.getStatus().reason();
                return Status(ErrorCodes::DuplicateKey,
                       "failed to read document from config.rebalanceevents");
            }

            auto eventDocs = statusWithEventQueryResult.getValue().docs;
            if (eventDocs.size() == 0) {
                log() << "[statemachine] event(" + eventid + ") "
                      << "document has been deleted after executing exhaustiveFindOnConfig";
                return Status(ErrorCodes::NoSuchKey,
                       "failed to read document from config.rebalanceevents");
            } else if (eventDocs.size() > 1) {
                error() << "[statemachine] event(" + eventid + ") "
                        << "document has more than one record";
                invariant(eventDocs.size() > 1);
            }
            auto statusWithRebalanceEventType = RebalanceEventType::fromBSON(eventDocs[0]);
            auto eventType = statusWithRebalanceEventType.getValue();
            log() << "[statemachine] existing event document "
                  << eventType.toString();
        }

        log() << "[statemachine] failed to write document into config.rebalanceevents";
        return result;
    }

    return Status::OK();
}

Status StateMachine::updateEventDocument(OperationContext* txn,
                                          const std::string& eventId,
                                          const BSONObj& eventData) {
    auto updateStatus = grid.catalogClient(txn)->updateConfigDocument(
                            txn,
                            RebalanceEventType::ConfigNS,
                            BSON(RebalanceEventType::eventId(eventId)),
                            eventData,
                            false,
                            kMajorityWriteConcern);

    if (!updateStatus.isOK()) {
        log() << "Failed to update event document: " << eventId
              << " err: " << updateStatus.getStatus().reason();
        return updateStatus.getStatus();
    }

    return Status::OK();
}

Status StateMachine::removeEventDocument(OperationContext* txn,
                                          const std::string& eventId) {
    BSONObj rebalaneEventDocumentIdentifier =
        BSON(RebalanceEventType::eventId(eventId));
    Status status = grid.catalogClient(txn)->removeConfigDocuments(
        txn, RebalanceEventType::ConfigNS, rebalaneEventDocumentIdentifier, kMajorityWriteConcern);
    return status;
}

void StateMachine::scheduleRebalanceEvent() {
    _eventHandlingThreadPool->schedule(StateMachine::pickAndProcessEvent);
}

bool StateMachine::canExecuteEvent() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    if (_state != State::kRunning) {
        log() << "Event cannot be processed because state machine is not running";
        return false;
    }
    return true;
}

void StateMachine::_recoveryThread() {
    log() << "[statemachine] start recovery thread";
    Client::initThread("Statemachine");
    auto txn = cc().makeOperationContext();

    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _threadOperationContext = txn.get();
    }

    Status status = startRecovery(txn.get());    
    if (!status.isOK()) {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        if (_state != State::kRunning) {
            log() << "Statemachine recovery thread is now stopped, with failed "
                  << "startRecovery" << causedBy(status);
            _threadOperationContext = nullptr;

            return;
        }

        fassertFailedWithStatus(40400,
            Status(status.code(), 
                stream() << "Failed to start recovery" << causedBy(status)));
    }
    
    finishRecovery(txn.get());
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _threadOperationContext = nullptr;
        _condVar.notify_all();
    }

    log() << "Statemachine recovery thread is now stopped";

    return;
}

void StateMachine::onTransitionToPrimary(OperationContext* txn) {
    {
        LOG(2) << "[statemachine][onTransitionToPrimary] try to hold _mutex";
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        LOG(2) << "[statemachine][onTransitionToPrimary] hold _mutex";
        LOG(2) << "[statemachine][onTransitionToPrimary] state is: " 
            << static_cast<std::underlying_type<StateMachine::State>::type>(_state);
        invariant(_state == State::kStopped);
        invariant(_rebalanceEventRecoveryList.empty());
        _state = State::kRunning;
    }

    invariant(!_thread.joinable());
    invariant(!_threadOperationContext);
    _thread = stdx::thread([this] { _recoveryThread(); });
}

void StateMachine::onStepDownFromPrimary() {
    LOG(2) << "[statemachine][onStepDownFromPrimary] try to hold _mutex";
    stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
    LOG(2) << "[statemachine][onStepDownFromPrimary] hold _mutex";
    LOG(2) << "[statemachine][onStepDownFromPrimary] state is: "
        << static_cast<std::underlying_type<StateMachine::State>::type>(_state);
    if (_state != State::kRunning)
        return;

    _state = State::kStopping;

    if (_threadOperationContext) {
        LOG(2) << "[statemachine][onStepDownFromPrimary] try to hold client lock";
        stdx::lock_guard<Client> scopedClientLock(*_threadOperationContext->getClient());
        LOG(2) << "[statemachine][onStepDownFromPrimary] hold client lock";
        _threadOperationContext->markKilled(ErrorCodes::InterruptedDueToReplStateChange);
        LOG(2) << "[statemachine][onStepDownFromPrimary] killed recovery thread";
    }
}

void StateMachine::onDrainComplete(OperationContext* txn) {
    {
        LOG(2) << "[statemachine][onDrainComplete] try to hold _mutex";
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        LOG(2) << "[statemachine][onDrainComplete] hold _mutex";
        LOG(2) << "[statemachine][onDrainComplete] state is: " 
            << static_cast<std::underlying_type<StateMachine::State>::type>(_state);
        if (_state == State::kStopped)
            return;
        invariant(_state == State::kStopping);
    }

    drainActiveEvents();

    _thread.join();

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        _state = State::kStopped;
        _thread = {};
    }

    LOG(1) << "State machine is terminated";
}

void StateMachine::drainActiveEvents() {
    LOG(2) << "[statemachine][drainActiveEvents] try to hold mutex";
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    LOG(2) << "[statemachine][drainActiveEvents] hold mutex";
    if (_state == State::kStopped)
        return;
    invariant(_state == State::kStopping);

    LOG(2) << "[statemachine][drainActiveEvents] waitting Events size is : " << _waitingEvents.size();
    
    _condVar.wait(lock, [this] {
        return _waitingEvents.empty();
    });
}

Status StateMachine::_validateEventChunk(OperationContext* txn, IRebalanceEvent* event,
                                         ChunkType& chunkType) {
    auto chunk = event->getChunk();
    auto chunkId = chunk.getID();
    auto findChunkStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                                                    txn,
                                                    ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                                                    repl::ReadConcernLevel::kLocalReadConcern,
                                                    NamespaceString(ChunkType::ConfigNS),
                                                    BSON(ChunkType::name(chunkId)),
                                                    BSONObj(),
                                                    boost::none);
    if (!findChunkStatus.isOK()) {
        return findChunkStatus.getStatus();
    }

    auto chunkDocs = findChunkStatus.getValue().docs;
    invariant(chunkDocs.size() == 1);

    auto chunkDocStatus = ChunkType::fromBSON(chunkDocs.front());
    invariant(chunkDocStatus.isOK());
    chunkType = chunkDocStatus.getValue();

    if (chunk.getNS() != chunkType.getNS()) {
        return Status(ErrorCodes::ChunkChanged, "chunk ns has changed");
    }

    auto status = _isStaleChunk(event, chunkType);

    return status; 
}

Status StateMachine::_isStaleChunk(IRebalanceEvent* event, const ChunkType& chunkType) {
    auto chunk = event->getChunk();

    ErrorCodes::Error errorCode = ErrorCodes::OK;
    std::string errorMessage = "";

    do {
        if (chunk.getShard() != chunkType.getShard()) {
            errorCode = ErrorCodes::ChunkChanged,
            errorMessage = "chunk shard has changed";
            break;    
        }

        if (!chunk.getVersion().equals(chunkType.getVersion())) {
            errorCode = ErrorCodes::ChunkChanged;
            errorMessage = "chunk version has changed";
            break;
        }

        if (chunk.getMin().woCompare(chunkType.getMin()) != 0 || chunk.getMax().woCompare(chunkType.getMax()) != 0) {
            errorCode = ErrorCodes::ChunkChanged;
            errorMessage = "chunk key range has changed";
            break;
        }
    } while (0);

    if ((errorCode == ErrorCodes::ChunkChanged) && 
        (event->getEventType() == StateMachine::EventType::kAssignEvent) &&
        !(event->isUserRequest())) {
        ShardIdent shardIdent;
        auto shardId = chunkType.getShard();
        auto processIdent = chunkType.getProcessIdentity();
        shardIdent.setShardId(shardId);
        shardIdent.setProcessIdentity(processIdent);
        bool alive = grid.catalogManager()->getShardServerManager()->isShardServerAlive(shardIdent);
        if (!alive) {
            return Status::OK();
        } 
    } 

    return Status(errorCode, errorMessage); 
}

bool StateMachine::_canExecuteDuplicateEvent(IRebalanceEvent* existingEvent,
                                             IRebalanceEvent* newEvent) {
    logStateMachineEvent("_canExecuteDuplicateEvent", newEvent);
    if ((newEvent->getEventType() != StateMachine::EventType::kAssignEvent) ||
        newEvent->isUserRequest()) {
        return false;
    }

    ShardIdent existingTargetIdent;
    switch (existingEvent->getEventType()) {
        case StateMachine::EventType::kAssignEvent:
        {
            AssignEvent* existingAssignEvent = dynamic_cast<AssignEvent*>(existingEvent);
            existingTargetIdent = existingAssignEvent->getTarget();
            break;
        }

        case StateMachine::EventType::kOffloadEvent:
        {
            OffloadEvent* existingOffloadEvent = dynamic_cast<OffloadEvent*>(existingEvent);
            existingTargetIdent = existingOffloadEvent->getSource();
            break;
        }

        case StateMachine::EventType::kMoveEvent:
        {
            MoveEvent* existingMoveEvent = dynamic_cast<MoveEvent*>(existingEvent);
            existingTargetIdent = existingMoveEvent->getTarget();
            break;
        }

        case StateMachine::EventType::kSplitEvent:
        {
            auto existingShardId = existingEvent->getChunk().getShard();
            auto existingProcessIdent = existingEvent->getChunk().getProcessIdentity();
            existingTargetIdent.setShardId(existingShardId);
            existingTargetIdent.setProcessIdentity(existingProcessIdent);
            break;
        }

        default:
        {
            //should never be here
            log() << "[statemachine] unknow event type";
            invariant(false);
        }
    }

    // determine target is dead or not
    bool alive = grid.catalogManager()->getShardServerManager()->isShardServerAlive(existingTargetIdent);
    if (alive) {
        return false;
    }

    log() << "[statemachine] interrupte duplicate event(eventId:"
          << existingEvent->getEventId()
          << ", eventType:"
          << static_cast<int>(existingEvent->getEventType())
          << ", chunk:" << existingEvent->getChunk();
    existingEvent->setInterrupt(true);
    return true;
}

Status StateMachine::addEventToBalancedChunkMap(OperationContext* txn, IRebalanceEvent* rbEvent) {
    auto chunkId = rbEvent->getChunk().getID();
    LOG(2) << "[statemachine] addEventToBalancedChunkMap: chunkId " << chunkId;
    bool scheduleEvent = false;
    ChunkBalanceInfo* balanceInfo = new ChunkBalanceInfo(chunkId);
    if (balanceInfo == nullptr) {
        return Status(ErrorCodes::ExceededMemoryLimit, "fail to allocate ChunkBalanceInfo");
    }

    {
        stdx::lock_guard<stdx::mutex> lock(_chunkMapMutex);
        BalancedChunkMap::const_iterator it = _balancedChunkMap.find(chunkId);
        if (it == _balancedChunkMap.end()) {
            LOG(2) << "[statemachine] addEventToBalancedChunkMap: chunk not found in map " << chunkId;
            _balancedChunkMap[chunkId] = balanceInfo;
        } else {
            LOG(2) << "[statemachine] addEventToBalancedChunkMap: chunk found in map " << chunkId;
            delete balanceInfo;
            balanceInfo = _balancedChunkMap[chunkId];
        }
    
        if (balanceInfo->activeEvent == nullptr) {
            LOG(2) << "[statemachine] addEventToBalancedChunkMap: activeEvent is null " << chunkId;
            balanceInfo->activeEvent = rbEvent;
            scheduleEvent = true;
        } else {
            log() << "[statemachine] found an existing event, chunk("
                  << balanceInfo->activeEvent->getChunk()
                  << "), eventtype("
                  << static_cast<int>(balanceInfo->activeEvent->getEventType())
                  << "), curstate("
                  << static_cast<int>(balanceInfo->activeEvent->getState())
                  << "), prevstate("
                  << static_cast<int>(balanceInfo->activeEvent->getPrevState())
                  << ")";

            auto canExecute = _canExecuteDuplicateEvent(balanceInfo->activeEvent, rbEvent);
            if (canExecute) {
                rbEvent->getEventResultInfo()->setErrorInfo(ErrorCodes::ChunkBusy,
                                                            "Chunk Busy");
            }    
            return Status(ErrorCodes::ChunkBusy,
                          "A rebalance event for this chunk already exists");
        }

        if (scheduleEvent) {
            if ((rbEvent->getEventType() == StateMachine::EventType::kAssignEvent) &&
                (!rbEvent->isInRecovery())) {
                getGlobalServiceContext()->getProcessStageTime(
                    "assignChunk:"+rbEvent->getChunk().getName())->noteStageStart(
                    "addEventToBalancedChunkMap:addEvent:State"+std::to_string((int)rbEvent->getState()));
            }
            addEvent(rbEvent);
            if ((rbEvent->getEventType() == StateMachine::EventType::kAssignEvent) &&
                (!rbEvent->isInRecovery())) {
                getGlobalServiceContext()->getProcessStageTime(
                    "assignChunk:"+rbEvent->getChunk().getName())->noteStageStart(
                    "addEventToBalancedChunkMap:scheduleRebalanceEvent:State"+std::to_string((int)rbEvent->getState()));
            }
            scheduleRebalanceEvent();
        } 
    }

    return Status::OK();
}

void StateMachine::removeEventFromBalancedChunkMap(IRebalanceEvent* rbEvent) {
    logStateMachineEvent("removeEventFromBalancedChunkMap", rbEvent);
    auto chunkId = rbEvent->getChunk().getID();
    ChunkBalanceInfo* balanceInfo = nullptr;
    {
        stdx::lock_guard<stdx::mutex> lock(_chunkMapMutex);
        BalancedChunkMap::iterator it = _balancedChunkMap.find(chunkId);
        if (it != _balancedChunkMap.end()) {
            balanceInfo = it->second;
        } else {
            log() << "[statemachine] can not find balanceInfo for " << chunkId << " in _balancedChunkMap";
            return;
        }

        if (!rbEvent->isProcessed() && (balanceInfo->activeEvent != rbEvent)) {
            log() << "[statemachine] event is not processed and not equal to balanceInfo->activeEvent";
            return;
        }
        _balancedChunkMap.erase(it);
    }

    delete(balanceInfo);
}

Status StateMachine::addEvent(IRebalanceEvent* rbEvent) {
    LOG(2) << "[statemachine][addEvent] try to hold _mutex";
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    LOG(2) << "[statemachine][addEvent] hold _mutex";
    
        //rbEvent->startTraceForAsyncStats(E_TPOINT_CONFIG_STATEMACHINE_INQUEUE);
    logStateMachineEvent("addEvent waitingEvents list add event", rbEvent);
    _waitingEvents.push_back(rbEvent);

    return Status::OK();
}

Status StateMachine::pickOneActiveEvent(IRebalanceEvent*& rbEvent) {
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        if (!_waitingEvents.empty()) {
            rbEvent = _waitingEvents.front();
            _waitingEvents.pop_front();
            //rbEvent->endTraceForAsyncStats(true);
        } else {
            if (_state == State::kStopping) {
                LOG(0) << "[statemachine] statemachine is interrupted, "
                      << "_waitingEvents is empty notify _condVar";
                _condVar.notify_all();
            }
            log() << "[statemachine] pickOneActiveEvent error, no rebalance event";
            return Status(ErrorCodes::BadValue, "No rebalance event");
        }
    }

    logStateMachineEvent("pickOneActiveEvent", rbEvent);
    return Status::OK();
}

void StateMachine::deleteEvent(OperationContext* txn, IRebalanceEvent* rbEvent) {
    if (!rbEvent) {
        log() << "no need to delete rbEvent, it is nullptr";
        return;
    }

    logStateMachineEvent("deleteEvent", rbEvent);
    auto client = txn->getClient();
    StateMachine* stateMachine = Grid::get(txn)->catalogManager()->getStateMachine();
    if (rbEvent->isProcessed()) {
        int removeEventRound = 0;
        {
            stdx::lock_guard<Client> lk(*client);
            client->resetOperationContext();
        }

        {
            auto opTxn = client->makeOperationContext();
            while (true) {
                if (!stateMachine->canExecuteEvent()) {
                    log() << "[statemachine] can delete event " << rbEvent->getEventId()
                          << "due to state machine is not running";
                    break;
                }
    
                log() << std::to_string(removeEventRound) << "-th removeEvent round starts...";
                Status status = removeEventDocument(opTxn.get(), rbEvent->getEventId());
                if (status.isOK()) {
                    log() << "[statemachine] Finish to delete event " << rbEvent->getEventId();
                    break;
                }
    
                log() << "[statemachine] Failed to delete event from configDB";
                stdx::this_thread::sleep_for(kDefaultEventUpdateRetryInterval.toSystemDuration());
                removeEventRound++;
            }
        }

        {
            stdx::lock_guard<Client> lk(*client);
            client->setOperationContext(txn);
        }
    
    }

    // delete chunkRebalanceInfo if needed
    stateMachine->removeEventFromBalancedChunkMap(rbEvent);

    // put chunk into failed chunk list if needed
    auto eventResultInfo = rbEvent->getEventResultInfo();
    if (eventResultInfo) {
        auto curState = eventResultInfo->getCurrentState();
        auto errorCode = eventResultInfo->getErrorCode();
        log() << "[statemachine] determine whether this event needs to be put into failed chunk list,"
              << "curState(" << static_cast<int>(curState) << "),"
              << "errorCode(" << static_cast<int>(errorCode) << "),"
              << "eventType(" << static_cast<int>(rbEvent->getEventType()) << "),"
              << "interrupted(" << static_cast<int>(rbEvent->isInterrupted()) << "),"
              << "chunkChanged(" << static_cast<int>(rbEvent->getChunkChangedFlag()) << ")";
        if (errorCode != ErrorCodes::OK &&
            curState > StateMachine::RebalanceEventState::kOffloaded &&
            curState <= StateMachine::RebalanceEventState::kAssignFailed) {
            if ((!(rbEvent->getEventType() == StateMachine::EventType::kAssignEvent &&
                rbEvent->isUserRequest())) &&
                (!rbEvent->isInterrupted()) &&
                (!rbEvent->getChunkChangedFlag()) &&
                (stateMachine->canExecuteEvent())) {
                auto chunk = rbEvent->getChunk();
                   Grid::get(txn)->catalogManager()->getShardServerManager()->processFailedChunk(chunk);
            }             
        }
        delete eventResultInfo;
    }

    delete rbEvent;
}

void StateMachine::processEvent(OperationContext* txn, IRebalanceEvent* rbEvent) {
    logStateMachineEvent("processEvent", rbEvent);
    StateMachine* stateMachine = Grid::get(txn)->catalogManager()->getStateMachine();
    // notify upper layer to complete the task
    auto resultPtr = rbEvent->getEventResultInfo();
    bool internalError = false;
    // Execute the task according to the statemachine map
    StatusWith<StateMachine::NextAction> actionState(StateMachine::NextAction::kActionContinue);
    do {
        if (!stateMachine->canExecuteEvent()) {
            rbEvent->getEventResultInfo()->setErrorInfo(ErrorCodes::ShutdownInProgress,
                                                        "state machine is not running");
            internalError = true;
            break;
        }

        if (rbEvent->isInterrupted()) {
            rbEvent->getEventResultInfo()->setErrorInfo(ErrorCodes::Interrupted,
                                                        "event is interrupted");
            internalError = true;
            break;
        }

        StateMachine::RebalanceEventState curState = rbEvent->getState();
        log() << "Event[" + rbEvent->getEventId() + "] is being processed, "
              << "before transition state is "
              << static_cast<int>(curState);

        if (rbEvent->needPersist()) {
            if ((rbEvent->getEventType() == StateMachine::EventType::kAssignEvent) &&
                (!rbEvent->isInRecovery())) {
                getGlobalServiceContext()->getProcessStageTime(
                    "assignChunk:"+rbEvent->getChunk().getName())->noteStageStart(
                    "persistStateTransition:State"+std::to_string(static_cast<int>(rbEvent->getState())));
            }
            Status status = stateMachine->persistStateTransition(txn, rbEvent);
            if (!status.isOK()) {
                log() << status.reason();
                rbEvent->rollbackLastTransition();
                rbEvent->getEventResultInfo()->setErrorInfo(status.code(),
                                                            status.reason());
                internalError = true;
                break;
            }
        }

        // Get the event handler
        RebalanceEventHandler eventHandler = StateMachine::stateHandlerMap[curState];
        if ((rbEvent->getEventType() == StateMachine::EventType::kAssignEvent) &&
            (!rbEvent->isInRecovery())) {
            getGlobalServiceContext()->getProcessStageTime(
                "assignChunk:"+rbEvent->getChunk().getName())->noteStageStart(
                "eventHandler:State"+std::to_string(static_cast<int>(rbEvent->getState())));
        }
        actionState = eventHandler(rbEvent);
        log() << "Event[" + rbEvent->getEventId() + "] is being processed, "
              << "after transition stat is "
              << static_cast<int>(rbEvent->getState());
    } while (actionState.getStatus().isOK() &&
             StateMachine::NextAction::kActionContinue == actionState.getValue());

    resultPtr->setCurrentState(rbEvent->getState());
    if (!actionState.getStatus().isOK()) {
        resultPtr->setErrorInfo(actionState.getStatus().code(), actionState.getStatus().reason());
    }

    if (!actionState.getStatus().isOK() || internalError) {
        log() << "Event[" + rbEvent->getEventId() + "] " + resultPtr->getErrorMsg();
       
        if (!rbEvent->isUserRequest()) {
            deleteEvent(txn, rbEvent);
        } else {
            resultPtr->resultNotification->set(resultPtr->getErrorCode());
        }
    } else if (StateMachine::NextAction::kActionWait != actionState.getValue()) {
        ErrorCodes::Error statusCode = ErrorCodes::OK;
        if (StateMachine::NextAction::kActionError == actionState.getValue()) {
            statusCode = ErrorCodes::RebalanceEventFailed;
            resultPtr->setErrorInfo(ErrorCodes::RebalanceEventFailed, "Rebalance event execution failed");
        } else if (StateMachine::NextAction::kActionFinish == actionState.getValue()) {
            statusCode = ErrorCodes::OK;
        }

        if (!rbEvent->isUserRequest()) {
            deleteEvent(txn, rbEvent);
        } else {
            resultPtr->resultNotification->set(statusCode);
        }
    }

    // for StateMachine::NextAction::kActionWait, just return
}

void StateMachine::pickAndProcessEvent() {
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    IRebalanceEvent* rbEvent = nullptr;
    Status getActiveEventStatus =
        Grid::get(txn)->catalogManager()->getStateMachine()->pickOneActiveEvent(rbEvent);

    if (getActiveEventStatus.isOK()) {
        // Execute the task according to the statemachine map
        if ((rbEvent->getEventType() == StateMachine::EventType::kAssignEvent) &&
            (!rbEvent->isInRecovery())) {
            getGlobalServiceContext()->getProcessStageTime(
                "assignChunk:"+rbEvent->getChunk().getName())->noteStageStart(
                "processEvent:State"+std::to_string((int)rbEvent->getState()));
        }
        processEvent(txn, rbEvent);
    } else {
        log() << "[statemachine] pickAndProcessEvent failed, due to "
              << getActiveEventStatus.reason();
    }
}

Status StateMachine::executeEvent(OperationContext* txn, IRebalanceEvent* event, bool recovery) {
    logStateMachineEvent("executeEvent", event);
    StateMachine* statemachine = Grid::get(txn)->catalogManager()->getStateMachine();
    if (!statemachine->canExecuteEvent()) {
        return Status(ErrorCodes::ShutdownInProgress,
                     "Event cannot be executed because state machine is not running");
    }

    if (recovery) {
        event->setRecoveryFlag(true);
    }

    // Add the event into the _balancedChunkMap list
    if ((event->getEventType() == StateMachine::EventType::kAssignEvent) &&
        (!event->isInRecovery())) {
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+event->getChunk().getName())->noteStageStart(
            "addEventToBalancedChunkMap:State"+std::to_string((int)event->getState()));
    }
    auto status = statemachine->addEventToBalancedChunkMap(txn, event);
    if (!status.isOK()) {
        log() << "[statemachine] failed to add event " << causedBy(status);
        return status;
    }
    return Status::OK();
}

Status StateMachine::startRecovery(OperationContext* txn) {
    LOG(0) << "[statemachine] Begin to load rebalance events";
    // Load all the active events from config.rebalanceevents collection
    auto statusWithEventsQueryResponse =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            repl::ReadConcernLevel::kMajorityReadConcern,
            NamespaceString(RebalanceEventType::ConfigNS),
            BSONObj(),
            BSONObj(),
            boost::none);

    if (!statusWithEventsQueryResponse.isOK()) {
        return Status(statusWithEventsQueryResponse.getStatus().code(),
            stream() << "Failed to read " << RebalanceEventType::ConfigNS
                     << " collection documents for rebalance events recovery"
                     << causedBy(statusWithEventsQueryResponse.getStatus()));
    }

    for (const BSONObj& event : statusWithEventsQueryResponse.getValue().docs) {
        auto statusWithRebalanceEventType = RebalanceEventType::fromBSON(event);
        if (!statusWithRebalanceEventType.isOK()) {
            // The format of this statemachine document is incorrect.
            return Status(statusWithRebalanceEventType.getStatus().code(),
                stream() << "Failed to parse config.rebalanceevents doc '" << redact(event.toString())
                          << "' for rebalance events recovery, "
                          << causedBy(redact(statusWithRebalanceEventType.getStatus())));
        }
        _rebalanceEventRecoveryList.push_back(std::move(statusWithRebalanceEventType.getValue()));
    }

    LOG(0) << "[statemachine] Finish loading rebalance events";
    return Status::OK();
}

void StateMachine::finishRecovery(OperationContext* txn) {
    log() << "Start finishRecovery";
    
    // Schedule recovered rebalance events.
    std::deque<RebalanceEventType>::iterator it = _rebalanceEventRecoveryList.begin();
    while (it != _rebalanceEventRecoveryList.end()) {
        // create statemachine based on the statemachineType
        auto eventId = it->getEventId();
        auto eventType = static_cast<StateMachine::EventType>(it->getEventType());
        auto eventData = it->getEventData();
        auto createStatus = createRebalanceEventFromBSON(txn,
                                                       eventId,
                                                       eventType,
                                                       eventData);
        if (!createStatus.isOK()) {
            error() << "failed to recover event " << it->toString()
                  << ", due to " << createStatus.getStatus().reason();
            it = _rebalanceEventRecoveryList.erase(it);
            int removeEventRound = 0;
            while (true) {
                if (!canExecuteEvent()) {
                    log() << "[statemachine] cannot delete event " << eventId
                          << "due to state machine is not running";
                    break;
                }

                log() << std::to_string(removeEventRound) << "-th removeEvent round starts...";
                Status status = removeEventDocument(txn, eventId);
                if (status.isOK()) {
                    log() << "[statemachine] Finish to delete event " << eventId;
                    break;
                }

                log() << "[statemachine] Failed to delete event " << eventId;
                stdx::this_thread::sleep_for(kDefaultEventUpdateRetryInterval.toSystemDuration());
                removeEventRound++;
            }
            continue;
        }

        IRebalanceEvent* rbEvent = createStatus.getValue();
        rbEvent->setProcessed(true);
        auto executeEventStatus = executeEvent(txn, rbEvent, true);
        if (!executeEventStatus.isOK()) {
            log() << "failed to execute recover event " << it->toString()
                  << "due to " + executeEventStatus.reason();

            StateMachine::deleteEvent(txn, rbEvent);
        }

        it = _rebalanceEventRecoveryList.erase(it);
    }

    if (it != _rebalanceEventRecoveryList.end()) {
        log() << "finishRecovery is interrupted";
        _rebalanceEventRecoveryList.clear();

        return;
    }

    log() << "Finish finishRecovery";

    return;
}

void StateMachine::waitForRecovery() {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    _condVar.wait(lock, [this]{return (_state == State::kRunning);});
}

// TODO, will implement this later
void StateMachine::interruptAndDisableRebalance() {
}

// TODO, will implement this later
void _abandonActiveRebalances(OperationContext* txn) {
}

//////////////////////////////////////////////////////////////////////////
// Start to define state transition of the state machine
StateMachine::StateHandlerMapType StateMachine::stateHandlerMap = {
    {StateMachine::RebalanceEventState::kStartOffload, _startOffloadHandler},
    {StateMachine::RebalanceEventState::kKillSourceSS, _killSourceSSHandler},
    {StateMachine::RebalanceEventState::kOffloaded, _offloadedHandler},
    {StateMachine::RebalanceEventState::kStartAssign, _startAssignHandler},
    {StateMachine::RebalanceEventState::kKillTargetSS, _killTargetSSHandler},
    {StateMachine::RebalanceEventState::kAssignFailed, _assignFailedHandler},
    {StateMachine::RebalanceEventState::kAssigned, _assignedHandler},
    {StateMachine::RebalanceEventState::kStartSplit, _startSplitHandler},
    {StateMachine::RebalanceEventState::kSplitFailed, _splitFailedHandler},
    {StateMachine::RebalanceEventState::kCommitSplit, _commitSplitHandler},
    {StateMachine::RebalanceEventState::kAcknowledgeSplit, _acknowledgeSplitHandler},
    {StateMachine::RebalanceEventState::kSplitted, _splittedHandler}
};

Status StateMachine::persistStateTransition(OperationContext* txn,
                                            IRebalanceEvent* rbEvent) {

    logStateMachineEvent("persistStateTransition", rbEvent);

    BSONObjBuilder updateBuilder{};
    updateBuilder.append(kCurStateInEventData,
        static_cast<long long>(rbEvent->getState()));
    updateBuilder.append(kPrevStateInEventData,
        static_cast<long long>(rbEvent->getPrevState()));
    updateBuilder.append(kChunkInEventData, rbEvent->getChunk().toBSON());
    //TODO need to retry a few times once we got certain failure from updateEventDocument()
    if (rbEvent->getEventType() == StateMachine::EventType::kSplitEvent) {
        SplitEvent* splitEvent = dynamic_cast<SplitEvent*>(rbEvent);
        if ((rbEvent->getState() == StateMachine::RebalanceEventState::kCommitSplit) &&
           (rbEvent->getPrevState() == StateMachine::RebalanceEventState::kStartSplit)) {
            updateBuilder.append(kSplitPointInEventData, splitEvent->getSplitPoint());
        }
        updateBuilder.append(kChildChunkInEventData, splitEvent->getChildChunk().toBSON());
    }

    auto status = updateEventDocument(txn,
                                      rbEvent->getEventId(),
                                      BSON("$set" << updateBuilder.obj()));
    if (status.isOK()) {
        rbEvent->setNeedPersist(false);
    }

    return status;
}

bool StateMachine::isEventDone(IRebalanceEvent* rbEvent) {
    StateMachine::RebalanceEventState curState = rbEvent->getState();
    if ((curState == StateMachine::RebalanceEventState::kAssignFailed) ||
        (curState == StateMachine::RebalanceEventState::kAssigned)) {
        return true;
    }

    if ((curState == StateMachine::RebalanceEventState::kOffloaded) &&
        (rbEvent->getEventType() == StateMachine::EventType::kOffloadEvent)) {
        return true;
    }

    return false;
}

StateMachine::RebalanceEventState StateMachine::transitionState(IRebalanceEvent* rbEvent,
                                                                ErrorCodes::Error statusCode) {
    StateMachine::RebalanceEventState curState = rbEvent->getState();
    StateMachine::RebalanceEventState nextState;

    // State transition logic
    switch (curState) {
        case StateMachine::RebalanceEventState::kStartAssign:
        {
            if (statusCode == ErrorCodes::OK) {
                nextState = StateMachine::RebalanceEventState::kAssigned;
            } else {
                //if (statusCode ==  ErrorCodes::CannotServe) {
                if (statusCode != ErrorCodes::ExceededTimeLimit) {
                    nextState = StateMachine::RebalanceEventState::kAssignFailed;
                } else if (rbEvent->exhaustRetries()) {
                    nextState = StateMachine::RebalanceEventState::kKillTargetSS;
                }
            }
            break;
        }

        case StateMachine::RebalanceEventState::kKillTargetSS:
        {
            nextState = StateMachine::RebalanceEventState::kAssignFailed;
            break;
        }

        case StateMachine::RebalanceEventState::kAssignFailed:
        {
            nextState = StateMachine::RebalanceEventState::kAssignFailed;
            break;
        }

        case StateMachine::RebalanceEventState::kAssigned:
        {
            nextState = StateMachine::RebalanceEventState::kAssigned;
            break;
        }

        case StateMachine::RebalanceEventState::kStartOffload:
        {
            if (statusCode == ErrorCodes::OK) {
                nextState = StateMachine::RebalanceEventState::kOffloaded;
            } else {
                if (statusCode != ErrorCodes::ExceededTimeLimit) {
                    nextState = StateMachine::RebalanceEventState::kOffloaded;
                } else if (rbEvent->exhaustRetries()) {
                    nextState = StateMachine::RebalanceEventState::kKillSourceSS;
                }
            }
            break;
        }

        case StateMachine::RebalanceEventState::kKillSourceSS:
        {
            nextState = StateMachine::RebalanceEventState::kOffloaded;
            break;
        }

        case StateMachine::RebalanceEventState::kOffloaded:
        {
            if (StateMachine::EventType::kMoveEvent == rbEvent->getEventType()) {
                nextState = StateMachine::RebalanceEventState::kStartAssign;
                if (!rbEvent->isInRecovery()) {
                    getGlobalServiceContext()->registerProcessStageTime(
                        "assignChunk:"+rbEvent->getChunk().getName());
                    getGlobalServiceContext()->getProcessStageTime(
                        "assignChunk:"+rbEvent->getChunk().getName())->noteStageStart(
                        "assignChunkByMove");
                }
            } else if (StateMachine::EventType::kOffloadEvent == rbEvent->getEventType()) {
                nextState = StateMachine::RebalanceEventState::kOffloaded;
            }
            break;
        }

        case StateMachine::RebalanceEventState::kStartSplit:
        {
            if (statusCode == ErrorCodes::OK) {
                nextState = StateMachine::RebalanceEventState::kCommitSplit;
            } else if ((statusCode != ErrorCodes::ExceededTimeLimit) ||
                       (rbEvent->exhaustRetries())) {
                nextState = StateMachine::RebalanceEventState::kSplitFailed;
            }
            break;
        }

        case StateMachine::RebalanceEventState::kCommitSplit:
        {
            if (statusCode == ErrorCodes::OK) {
                nextState = StateMachine::RebalanceEventState::kAcknowledgeSplit;
            } else {
                nextState = StateMachine::RebalanceEventState::kSplitFailed;
            }
            break;
        }

        case StateMachine::RebalanceEventState::kAcknowledgeSplit:
        {
            if (statusCode == ErrorCodes::OK) {
                nextState = StateMachine::RebalanceEventState::kSplitted;
            } else if ((statusCode != ErrorCodes::ExceededTimeLimit) ||
                       (rbEvent->exhaustRetries())) {
                nextState = StateMachine::RebalanceEventState::kSplitted;
            }
            break;
        }

        case StateMachine::RebalanceEventState::kSplitted:
        {
            nextState = StateMachine::RebalanceEventState::kSplitted;
            break;
        }

        case StateMachine::RebalanceEventState::kSplitFailed:
        {
            nextState = StateMachine::RebalanceEventState::kSplitFailed;
            break;
        }

        default:
        {
            // should never happen, still add a log here
            log() << "Failed to transition the state, because state("
                  << static_cast<int>(curState)
                  << ") is invalid";
            break;
        }
    }

    rbEvent->transitState(nextState);
    if (nextState != curState) {
        rbEvent->setNeedPersist(true);
    }
    logStateMachineEvent("transtionState", rbEvent);

    return nextState;
}

Status StateMachine::extractDataFromCommandResponse(IRebalanceEvent* rbEvent,
                                                    const BSONObj& commandResponse) {

    if ((rbEvent->getEventType() == StateMachine::EventType::kSplitEvent) &&
        (rbEvent->getState() == StateMachine::RebalanceEventState::kStartSplit)) {
        // need to extract the split point from the command Response
        BSONElement splitPoint;
        auto status = bsonExtractTypedField(commandResponse, kSplitPointFieldName,
                                       Object, &splitPoint);
        if (!status.isOK()) {
            return {status.code(),
                    str::stream() << "Invalid splitPoint data " << status.reason()};
        }

        if (splitPoint.Obj().isEmpty()) {
            return {ErrorCodes::BadValue, "The splitPoint data cannot be empty"};
        }
        SplitEvent* splitEvent = dynamic_cast<SplitEvent*>(rbEvent);
        splitEvent->setSplitPoint(splitPoint.Obj().getOwned());
    } else if ((rbEvent->getEventType() == StateMachine::EventType::kAssignEvent) && 
        (rbEvent->getState() == StateMachine::RebalanceEventState::kStartAssign)) {
        std::string processIdentity;
        auto status = bsonExtractStringField(commandResponse, kAssignProcessIdentityData, &processIdentity);
        if (!status.isOK()) {
            return {status.code(),
                    str::stream() << "Invalid process identity " << status.reason()};
        }

        std::string shardName;
        status = bsonExtractStringField(commandResponse, kAssignShardNameData, &shardName);
        if (!status.isOK()) {
            return {status.code(),
                    str::stream() << "Invalid shardName " << status.reason()};
        }

        AssignEvent* assignEvent = dynamic_cast<AssignEvent*>(rbEvent);
        ChunkType chunk = assignEvent->getChunk();
        chunk.setShard(shardName);
        chunk.setProcessIdentity(processIdentity);
        assignEvent->refreshChunk(chunk);  
    }

    return Status::OK();
}

void StateMachine::handleRequestResponse(const RemoteCommandCallbackArgs& cbArgs,
                                         IRebalanceEvent* rbEvent) {
    if ((rbEvent->getEventType() == StateMachine::EventType::kAssignEvent) &&
        (!rbEvent->isInRecovery())) {
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+rbEvent->getChunk().getName())->noteStageStart(
            "handleRequestResponse:State"+std::to_string((int)rbEvent->getState()));
    }
    logStateMachineEvent("handleRequestResponse", rbEvent);

    StateMachine* stateMachine = grid.catalogManager()->getStateMachine();
    auto response = cbArgs.response;
    //rbEvent->endTraceForAsyncStats(response.isOK());
    if (!response.isOK()) {
        if (response.status.code() == ErrorCodes::ExceededTimeLimit) {
            //retry
            if (rbEvent->exhaustRetries()) {
                log() << "[statemachine], rbEvent->exhaustRetries(), chunk("
                      << rbEvent->getChunk().toString()
                      << ")";
                (void)transitionState(rbEvent, response.status.code());
            } else {
                log() << "[statemachine], !rbEvent->exhaustRetries(), chunk("
                      << rbEvent->getChunk().toString()
                      << ")";
                rbEvent->increaseRetryCnt();
            }
        } else {
            // transition to Failed and return
            // the upper layer will handle the result
            log() << "[statemachine], errorcode("
                  << static_cast<int>(response.status.code())
                  << "), chunk(" + rbEvent->getChunk().toString() + ")";
            (void)transitionState(rbEvent, response.status.code());
        }
    } else {
        auto status = getStatusFromCommandResult(response.data);
        if (!status.isOK()) {
            log() << "[statemachine], errorcode("
                  << static_cast<int>(status.code())
                  << "), chunk(" + rbEvent->getChunk().toString() + ")";
            transitionState(rbEvent, status.code());
        } else {
            log() << "[statemachine], response.isOK(), chunk("
                  << rbEvent->getChunk().toString()
                  << ")";
            auto status = extractDataFromCommandResponse(rbEvent, response.data);
            if (!status.isOK()) {
                log() << "[statemachine], extractDataFromCommandResponse failed due to "
                    << status.reason() << ", chunk("
                    << rbEvent->getChunk().toString()
                    << ")";
            }
            transitionState(rbEvent, status.code());
        }
    }
    if ((rbEvent->getEventType() == StateMachine::EventType::kAssignEvent) &&
        (!rbEvent->isInRecovery())) {
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+rbEvent->getChunk().getName())->noteStageStart(
            "handleRequestResponse:addEvent:State"+std::to_string((int)rbEvent->getState()));
    }
    stateMachine->addEvent(rbEvent);
    if ((rbEvent->getEventType() == StateMachine::EventType::kAssignEvent) &&
        (!rbEvent->isInRecovery())) {
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+rbEvent->getChunk().getName())->noteStageStart(
            "handleRequestResponse:scheduleRebalanceEvent:State"+std::to_string((int)rbEvent->getState()));
    }
    stateMachine->scheduleRebalanceEvent();
}

StatusWith<StateMachine::NextAction> StateMachine::_asyncCommand(OperationContext* txn,
    IRebalanceEvent* event,
    const HostAndPort& theTarget,
    BSONObjBuilder& builder) {

    logStateMachineEvent("_asyncCommand", event);
    //event->startTraceForAsyncStats(eType);

    executor::TaskExecutor* const executor = Grid::get(txn)->getExecutorPool()->getFixedExecutor();

    const RemoteCommandRequest request(theTarget,
                                       "admin",
                                       builder.obj(),
                                       txn);

    const RemoteCommandCallbackFn callback = stdx::bind(&StateMachine::handleRequestResponse,
                                                        stdx::placeholders::_1,
                                                        event);
    StatusWith<CallbackHandle> callbackHandleWithStatus =
        executor->scheduleRemoteCommand(request, callback);

    if (callbackHandleWithStatus.isOK()) {
        log() << "[statemachine] succeed to send out async command to " << theTarget.toString();
        return StateMachine::NextAction::kActionWait;
    }

    warning() << "[statemachine] failed to schedule command to " << theTarget.toString()
              << redact(callbackHandleWithStatus.getStatus());

    return callbackHandleWithStatus.getStatus();
}

StatusWith<StateMachine::NextAction> StateMachine::_startAssignHandler(IRebalanceEvent* event) {
    if (!event->isInRecovery()) {
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+event->getChunk().getName())->noteStageStart("startAssignHandler");
    }
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    ChunkType chunkType;
    if (!event->getNewChunkFlag()) {
        auto validateStatus = _validateEventChunk(txn, event, chunkType);
        if (!validateStatus.isOK()) {
            error() << "[statemachine] failed to validate event chunk due to " << validateStatus.reason();
            event->setChunkChangedFlag(true);
            return validateStatus;
        }

        if (chunkType.getStatus() == ChunkType::ChunkStatus::kAssigned) {
            auto sourceShardName = chunkType.getShard();
            auto sourceIdentity = chunkType.getProcessIdentity();
            ShardIdent sourceShardIdent(sourceShardName, sourceIdentity);

            bool alive = grid.catalogManager()->getShardServerManager()->isShardServerAlive(sourceShardIdent);
            if (alive) {
                return StateMachine::NextAction::kActionFinish;
            } 
        }

        event->refreshChunk(chunkType);
    } else {
        chunkType = event->getChunk();
    }

    if (!event->isInRecovery() && event->getEventType() != StateMachine::EventType::kMoveEvent) {
        auto status = writeEventDocument(txn, event);
        if (!status.isOK()) {
            event->getEventResultInfo()->setErrorInfo(status.code(),
                                                      status.reason());
            error() << "[statemachine] failed to write event document due to " << status.reason();
            return status;
        }
        event->setProcessed(true);
    }

    // get the collection info
    if (!event->isInRecovery()) {
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+event->getChunk().getName())->noteStageStart("getCollection");
    }
    auto collstatus = grid.catalogClient(txn)->getCollection(txn, chunkType.getNS());
    if (!collstatus.isOK()) {
        log() << "[statemachine] failed to get collection: " << chunkType.getNS();
        return collstatus.getStatus();
    }
    const auto collOpTimePair = collstatus.getValue();
    CollectionType coll       = collOpTimePair.value;

    ShardId newShardId;
    std::string newShardIdent;
    if (StateMachine::EventType::kMoveEvent == event->getEventType()) {
        MoveEvent* moveEvent = dynamic_cast<MoveEvent*>(event);
        newShardId = moveEvent->getTarget().getShardId();
        newShardIdent = moveEvent->getTarget().getProcessIdentity();
    } else if (StateMachine::EventType::kAssignEvent == event->getEventType()) {
        AssignEvent* assignEvent = dynamic_cast<AssignEvent*>(event);
        newShardId = assignEvent->getTarget().getShardId();
        newShardIdent = assignEvent->getTarget().getProcessIdentity();
    }
    chunkType.setShard(newShardId);
    chunkType.setProcessIdentity(newShardIdent);

    // get shard host
    const auto ShardStatus = grid.shardRegistry()->getShard(txn, newShardId);
    if (!ShardStatus.isOK()) {
        log()<<"[statemachine] failed to get shard: " << newShardId;
        return ShardStatus.getStatus();
    }
    const auto shard = ShardStatus.getValue();
    const auto hostCS = shard->getConnString();
    invariant(hostCS.getServers().size() == 1);
    auto host = hostCS.getServers().front();

    log() << "[statemachine] _startAssignHandler for chunk("
          << chunkType.getID()
          << "), targetShard is " + host.toString()
          << ", processIdentity is " + newShardIdent;

    // create root folder: ploglist for OBSindex and file for Maas
    if (!event->getNewChunkFlag()) {
        log() << "[statemachine] _startAssignHandler for chunk("
              << chunkType.getID()
              << ") before create, root folder is "
              << chunkType.getRootFolder();
    }

    if (event->getNewChunkFlag()) {

        chunkType.setStatus(ChunkType::ChunkStatus::kDisabled);
    } else {
        chunkType.setStatus(ChunkType::ChunkStatus::kOffloaded);
    }

    if (!event->isInRecovery()) {
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+event->getChunk().getName())->noteStageStart("newMaxChunkVersion");
    }
    auto maxVersionStatus = grid.catalogManager()->newMaxChunkVersion(txn, chunkType.getNS());
    if (!maxVersionStatus.isOK()) {
        LOG(0) << "[statemachine] _startAssignHandler failed to newMaxChunkVersion";
        return maxVersionStatus.getStatus();
    }
    ChunkVersion currentMaxVersion(maxVersionStatus.getValue(), chunkType.getVersion().epoch());
    chunkType.setVersion(currentMaxVersion);

    auto updateChunkStatus = grid.catalogClient(txn)->updateConfigDocument(txn,
                                            ChunkType::ConfigNS,
                                            BSON(ChunkType::name(chunkType.getID())),
                                            chunkType.toBSON(),
                                            true,
                                            ShardingCatalogClient::kMajorityWriteConcern);
    if (!updateChunkStatus.isOK()) {
        log() << "[statemachine] failed to update chunk("
              << chunkType.getID()
              << ")";
        return updateChunkStatus.getStatus();
    }

    event->refreshChunk(chunkType);
    BSONObjBuilder builder;
    AssignChunkRequest::appendAsCommand(&builder, chunkType, coll, event->getNewChunkFlag(), 
                                        newShardId.toString(), newShardIdent);
    if (!event->isInRecovery()) {
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+event->getChunk().getName())->noteStageStart(
            "startAssignHandler:asyncCommand:newShard:"+event->getChunk().getShard().toString());
    }
    return _asyncCommand(txn, event, host, builder/*, E_TPOINT_CONFIG_STATEMACHINE_ASSIGN_CMD*/);
}


// TODO, will implement in the near future
StatusWith<StateMachine::NextAction> StateMachine::_killTargetSSHandler(IRebalanceEvent* event) {
    logStateMachineEvent("_killTargetSSHandler", event);
    transitionState(event, ErrorCodes::OK);
    return StateMachine::NextAction::kActionContinue;
}

StatusWith<StateMachine::NextAction> StateMachine::_assignFailedHandler(IRebalanceEvent* event) {
    if ((event->getEventType() == StateMachine::EventType::kAssignEvent) &&
        (!event->isInRecovery())) {
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+event->getChunk().getName())->noteStageStart("assignFailedHandler");
    }
    logStateMachineEvent("_assignFailedHandler", event);
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    Grid::get(txn)->catalogClient(txn)->logChange(
        txn,
        "assignFailed",
        "",
        BSON(RebalanceEventType::eventData() << event->dataToBSON()),
        kMajorityWriteConcern);

    error() << "[statemachine] _assignFailedHandler, failed to assign chunk(" << event->getChunk()
            << ")";

    return StateMachine::NextAction::kActionError;
}

StatusWith<StateMachine::NextAction> StateMachine::_assignedHandler(IRebalanceEvent* event) {
    if (!event->isInRecovery()) {
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+event->getChunk().getName())->noteStageStart("assignFailedHandler");
    }
    logStateMachineEvent("_assignedHandler", event);
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
            txn = txnPtr.get();
    }

    auto chunk = event->getChunk();

    // mark the chunk state as kAssigned, and modify the "shard"
    BSONObjBuilder chunkstatusafterbuilder;
    auto shardName = event->getChunk().getShard();
    auto processIdentity = event->getChunk().getProcessIdentity();
    chunkstatusafterbuilder.append(ChunkType::shard(), shardName);
    chunkstatusafterbuilder.append(ChunkType::processIdentity(), processIdentity);
    chunkstatusafterbuilder.append(ChunkType::status(),
        static_cast<std::underlying_type<ChunkType::ChunkStatus>::type>(ChunkType::ChunkStatus::kAssigned));
    if (!event->isInterrupted()) {
        auto updatestatus = grid.catalogClient(txn)->updateConfigDocument(txn,
                                ChunkType::ConfigNS,
                                BSON(ChunkType::name(chunk.getID())),
                                BSON("$set" << chunkstatusafterbuilder.obj()),
                                false,
                                ShardingCatalogClient::kMajorityWriteConcern);
        // TODO: if fail, retry
        if (!updatestatus.isOK()) {
            log()<<"[statemachine] update chunk status fail";
            return updatestatus.getStatus();
        }

        ChunkType chunkType;
        chunkType.setNS(chunk.getNS());
        chunkType.setMin(chunk.getMin());
        chunkType.setMax(chunk.getMax());
        chunkType.setShard(chunk.getShard());
        chunkType.setProcessIdentity(chunk.getProcessIdentity());
        chunkType.setVersion(chunk.getVersion());
        chunkType.setRootFolder(chunk.getRootFolder());
        chunkType.setName(chunk.getID());
        chunkType.setStatus(ChunkType::ChunkStatus::kAssigned);

        event->refreshChunk(chunkType);
    }

    log() << "[statemachine] finish assigning chunk: " << event->getChunk();
    if (!event->isInRecovery()) {
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+event->getChunk().getName())->noteProcessEnd();
        log() << "Time of " << "assignChunk:"+event->getChunk().getName() << ": "
            << getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+event->getChunk().getName())->toString();
        getGlobalServiceContext()->cancelProcessStageTime(
            "assignChunk:"+event->getChunk().getName());
    }
    return StateMachine::NextAction::kActionFinish;
}

StatusWith<StateMachine::NextAction> StateMachine::_startOffloadHandler(IRebalanceEvent* event) {
    OperationContext* txn = cc().getOperationContext();

    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    ChunkType chunkType;
    auto validateStatus = _validateEventChunk(txn, event, chunkType);
    if (!validateStatus.isOK()) {
        log() << "[statemachine] failed to validate event chunk due to " << validateStatus.reason();
        event->setChunkChangedFlag(true);
        return validateStatus;
    }

    if (chunkType.getStatus() == ChunkType::ChunkStatus::kOffloaded) {
        if (event->getEventType() == StateMachine::EventType::kOffloadEvent) {
            return StateMachine::NextAction::kActionFinish;
        } else if (event->getEventType() == StateMachine::EventType::kMoveEvent) {
            transitionState(event, ErrorCodes::OK);
            return StateMachine::NextAction::kActionContinue;
        } else {
            error() << "[statemachine] invalid event type";
            return Status(ErrorCodes::BadValue, "invalid event type");
        }
    }
    event->refreshChunk(chunkType);

    if (!event->isInRecovery()) {
        auto status = writeEventDocument(txn, event);
        if (!status.isOK()) {
            event->getEventResultInfo()->setErrorInfo(status.code(),
                                                      status.reason());
            error() << "[statemachine] failed to write event document due to " << status.reason();
            return status;
        }
        event->setProcessed(true);
    }

    const auto ShardStatus = grid.shardRegistry()->getShard(txn, chunkType.getShard());
    if (!ShardStatus.isOK()) {
        log()<<"[offloadChunk] fail to get shard";
        return ShardStatus.getStatus();
    }
    const auto shard = ShardStatus.getValue();
    const auto hostCS = shard->getConnString();
    invariant(hostCS.getServers().size() == 1);
    auto host = hostCS.getServers().front();

    // Send out Offload command
    BSONObjBuilder builder;
    OffloadChunkRequest::appendAsCommand(&builder, chunkType);

    return _asyncCommand(txn, event, host, builder/*, E_TPOINT_CONFIG_STATEMACHINE_OFFLOAD_CMD*/);
}

// TODO, will implement in the near future
StatusWith<StateMachine::NextAction> StateMachine::_killSourceSSHandler(IRebalanceEvent* event) {
    logStateMachineEvent("_killSourceSSHandler", event);
    (void)transitionState(event, ErrorCodes::OK);
    return StateMachine::NextAction::kActionContinue;
}

StatusWith<StateMachine::NextAction> StateMachine::_offloadedHandler(IRebalanceEvent* event) {
    logStateMachineEvent("_offloadedHandler", event);
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
            txn = txnPtr.get();
    }

    auto chunk = event->getChunk();
    BSONObjBuilder chunkstatusafterbuilder;
    chunkstatusafterbuilder.append(ChunkType::status(),
        static_cast<std::underlying_type<ChunkType::ChunkStatus>::type>(ChunkType::ChunkStatus::kOffloaded));
    auto status = grid.catalogClient(txn)->updateConfigDocument(txn,
                                            ChunkType::ConfigNS,
                                            BSON(ChunkType::name(chunk.getID())),
                                            BSON("$set" << chunkstatusafterbuilder.obj()),
                                            false,
                                            ShardingCatalogClient::kMajorityWriteConcern);
    if (!status.isOK()) {
        log() << "[_offloadedHandler] fail to update state of chunk("
              << chunk.getID()
              << ")";
        return status.getStatus();
    }

    ChunkType chunkType;
    chunkType.setNS(chunk.getNS());
    chunkType.setMin(chunk.getMin());
    chunkType.setMax(chunk.getMax());
    chunkType.setShard(chunk.getShard());
    chunkType.setProcessIdentity(chunk.getProcessIdentity());
    chunkType.setVersion(chunk.getVersion());
    chunkType.setRootFolder(chunk.getRootFolder());
    chunkType.setName(chunk.getID());
    chunkType.setStatus(ChunkType::ChunkStatus::kOffloaded);
    event->refreshChunk(chunkType);

    log() << "[_offloadedHandler] finish offload chunk " << chunkType;
    if (StateMachine::EventType::kMoveEvent == event->getEventType()) {
        (void)transitionState(event, ErrorCodes::OK);
        return StateMachine::NextAction::kActionContinue;
    }

    return StateMachine::NextAction::kActionFinish;
}

StatusWith<StateMachine::NextAction> StateMachine::_startSplitHandler(IRebalanceEvent* event) {
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    ChunkType chunkType;
    auto validateStatus = _validateEventChunk(txn, event, chunkType);
    if (!validateStatus.isOK()) {
        log() << "[statemachine] failed to validate event chunk due to " << validateStatus.reason();
        event->setChunkChangedFlag(true);
        return validateStatus;
    }
    
    if (!chunkType.isAssigned()) {
        log() << "[statemachine] cannot split chunk because its status is not assigned";
        return Status(ErrorCodes::ChunkNotAssigned, "chunk is not in assigned status");
    }
    event->refreshChunk(chunkType);

    SplitEvent* splitEvent = dynamic_cast<SplitEvent*>(event);
    auto splitPoint = splitEvent->getSplitPoint();
    if (!splitPoint.isEmpty()) {
        /*const NamespaceString nss(chunkType.getNS());
        auto scopedChunkManager = uassertStatusOK(ScopedChunkManager::getExisting(txn, nss));
        ChunkManager* const chunkManager = scopedChunkManager.cm();

        BSONObj shardkey = chunkManager->getShardKeyPattern().extractShardKeyFromDoc(splitPoint);
        if (shardkey.isEmpty()) {
            log() << "[statemachine] splitPoint is not a shard key";
            return Status(ErrorCodes::BadValue, "splitPoint is not a shard key");
        }
        */
        if (SimpleBSONObjComparator::kInstance.evaluate(splitPoint < chunkType.getMin()) ||
            SimpleBSONObjComparator::kInstance.evaluate(splitPoint >= chunkType.getMax())) {
            log() << "[statemachine] splitPoint is invalid";
            return Status(ErrorCodes::BadValue, "splitPoint is invalid");
        }

        if (SimpleBSONObjComparator::kInstance.evaluate(splitPoint == chunkType.getMin())) {
            return StateMachine::NextAction::kActionFinish;
        }
    }

    if (!event->isInRecovery()) {
        auto status = writeEventDocument(txn, event);
        if (!status.isOK()) {
            event->getEventResultInfo()->setErrorInfo(status.code(),
                                                      status.reason());
            error() << "[statemachine] failed to write event document due to " << status.reason();
            return status;
        }
        event->setProcessed(true);
    }

    executor::TaskExecutor* const executor = Grid::get(txn)->getExecutorPool()->getFixedExecutor();
    // get the collection info
    auto collstatus = grid.catalogClient(txn)->getCollection(txn, chunkType.getNS());
    if (!collstatus.isOK()) {
        log() << "failed to get collection: " << chunkType.getNS();
        return collstatus.getStatus();
    }
    const auto collOpTimePair = collstatus.getValue();
    CollectionType coll       = collOpTimePair.value;

    ShardId shardId = chunkType.getShard();
    // get shard host
    const auto ShardStatus = grid.shardRegistry()->getShard(txn, shardId);
    if (!ShardStatus.isOK()) {
        log()<<"failed to get shard:" << shardId;
        return ShardStatus.getStatus();
    }
    const auto shard = ShardStatus.getValue();
    const auto hostCS = shard->getConnString();
    invariant(hostCS.getServers().size() == 1);
    auto host = hostCS.getServers().front();

    auto childChunk = splitEvent->getChildChunk();

    BSONObjBuilder builder;
    SplitChunkReq::appendAsCommand(&builder,
                                   chunkType,
                                   coll,
                                   childChunk.getRootFolder(),
                                   childChunk.getID(),
                                   splitEvent->getSplitPoint());
    const RemoteCommandRequest request(host,
                                       "admin",
                                       builder.obj(),
                                       txn);

    const RemoteCommandCallbackFn callback = stdx::bind(&StateMachine::handleRequestResponse,
                                                        stdx::placeholders::_1,
                                                        event);

    StatusWith<CallbackHandle> callbackHandleWithStatus =
        executor->scheduleRemoteCommand(request, callback);

    if (callbackHandleWithStatus.isOK()) {
        logStateMachineEvent("_startSplitHandler", event);
        return StateMachine::NextAction::kActionWait;
    }

    warning() << "failed to schedule split command: "
              << redact(callbackHandleWithStatus.getStatus());
    return callbackHandleWithStatus.getStatus();
}

Status StateMachine::_sendAckCommand(IRebalanceEvent* event, bool confirm) {
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    executor::TaskExecutor* const executor = Grid::get(txn)->getExecutorPool()->getFixedExecutor();

    auto chunk = event->getChunk();
    // get the collection info
    auto collstatus = grid.catalogClient(txn)->getCollection(txn, chunk.getNS());
    if (!collstatus.isOK()) {
        log() << "failed to get collection: " << chunk.getNS();
        return collstatus.getStatus();
    }
    const auto collOpTimePair = collstatus.getValue();
    CollectionType coll       = collOpTimePair.value;

    ShardId shardId = chunk.getShard();
    // get shard host
    const auto ShardStatus = grid.shardRegistry()->getShard(txn, shardId);
    if (!ShardStatus.isOK()) {
        log()<<"failed to get shard:" << shardId;
        return ShardStatus.getStatus();
    }
    const auto shard = ShardStatus.getValue();
    const auto hostCS = shard->getConnString();
    invariant(hostCS.getServers().size() == 1);
    auto host = hostCS.getServers().front();

    BSONObjBuilder builder;
    ConfirmSplitRequest::appendAsCommand(&builder, chunk, coll, confirm);
    const RemoteCommandRequest request(host,
                                       "admin",
                                       builder.obj(),
                                       txn);

    const RemoteCommandCallbackFn callback = stdx::bind(&StateMachine::handleRequestResponse,
                                                        stdx::placeholders::_1,
                                                        event);

    StatusWith<CallbackHandle> callbackHandleWithStatus =
        executor->scheduleRemoteCommand(request, callback);

    if (callbackHandleWithStatus.isOK()) {
        logStateMachineEvent("_sendAckCommand", event);
        return Status::OK();
    }

    std::string errMsg = (confirm ? "failed to schedule ack split command: " :
                                    "failed to schedule rollback split command: ");
    warning() << errMsg + callbackHandleWithStatus.getStatus().reason();
    return callbackHandleWithStatus.getStatus();
}

StatusWith<StateMachine::NextAction> StateMachine::_splitFailedHandler(IRebalanceEvent* event) {
    logStateMachineEvent("_splitFailedHandler", event);
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
            txn = txnPtr.get();
    }

    if (event->getPrevState() != StateMachine::RebalanceEventState::kSplitFailed) {
        Grid::get(txn)->catalogClient(txn)->logChange(
            txn,
            "splitFailed",
            "",
            BSON(RebalanceEventType::eventData() << event->dataToBSON()),
            kMajorityWriteConcern);

        error() << "[statemachine] _splitFailedHandler, failed to split chunk(" << event->getChunk()
                << ")";
    }

    if (event->getPrevState() == StateMachine::RebalanceEventState::kCommitSplit) {
        auto sendAckStatus = _sendAckCommand(event, false);
        if (!sendAckStatus.isOK()) {
            // TODO need to retry ack command
            warning() << "failed to send ack command of split";
        } else {
            return StateMachine::NextAction::kActionWait;
        }
    }

    return StateMachine::NextAction::kActionError;
}

void StateMachine::_addUpdateChunkObject(OperationContext* txn,
                                         BSONArrayBuilder& updates,
                                         ChunkType& chunk,
                                         bool parent) {

    log() << "[statemachine] _addUpdateChunkObject chunk" << chunk;
    // build an update operation against the chunks collection of the config database
    // with upsert true
    BSONObjBuilder op;
    op.append("op", "u");
    op.appendBool("b", true);
    op.append("ns", ChunkType::ConfigNS);

    // add the modified (new) chunk information as the update object
    BSONObjBuilder n(op.subobjStart("o"));
    n.append(ChunkType::name(), chunk.getID());
    n.append(ChunkType::ns(), chunk.getNS());
    n.append(ChunkType::min(), chunk.getMin());
    n.append(ChunkType::max(), chunk.getMax());
    n.append(ChunkType::shard(), chunk.getShard().toString());
    n.append(ChunkType::processIdentity(), chunk.getProcessIdentity());
    ChunkVersion currentMaxVersion(chunk.getVersion());
    currentMaxVersion.appendForChunk(&n);
    n.append(ChunkType::rootFolder(), chunk.getRootFolder());
    n.append(ChunkType::status(), (int)(chunk.getStatus()));
    n.done();

    // add the chunk's _id as the query part of the update statement
    BSONObjBuilder q(op.subobjStart("o2"));
    q.append(ChunkType::name(), chunk.getID());
    q.done();

    auto obj = op.obj();
    updates.append(obj);
}

StatusWith<StateMachine::NextAction> StateMachine::_commitSplitHandler(IRebalanceEvent* event) {
    logStateMachineEvent("_commitSplitHandler", event);
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
            txn = txnPtr.get();
    }

    SplitEvent* splitEvent = dynamic_cast<SplitEvent*>(event);
    auto childChunk = splitEvent->getChildChunk();
    auto chunk = event->getChunk();
    auto maxKey = chunk.getMax();
    auto childMinKey = childChunk.getMin();
    if (event->isInRecovery() && 
        SimpleBSONObjComparator::kInstance.evaluate(maxKey == childMinKey)) {
        LOG(2) << "[statemachine] recovered splitevent for chunk("
               << chunk.getID() + "), childchunk("
               << childChunk.getID() + "), has updated chunkmap";
        if (childChunk.getShard() == chunk.getShard() &&
            childChunk.getProcessIdentity() == chunk.getProcessIdentity()) {
            LOG(2) << "[statemachine] recovered splitevent for chunk("
                   << chunk.getID() + "), does not assign child chunk("
                   << childChunk.getID() + ")";
            auto childShardId = childChunk.getShard();
            Balancer::get(txn)->assignChunk(txn,
                                            childChunk,
                                            false,
                                            false,
                                            childShardId);
        }
        
        transitionState(event, ErrorCodes::OK);
        return StateMachine::NextAction::kActionContinue;
    }

    auto maxVersionStatus = grid.catalogManager()->newMaxChunkVersion(txn, chunk.getNS());
    if (!maxVersionStatus.isOK()) {
        return maxVersionStatus.getStatus();
    }
    ChunkVersion currentMaxVersion(maxVersionStatus.getValue(), chunk.getVersion().epoch());

    auto splitPoint = splitEvent->getSplitPoint();
    //TODO move this to handleRequestProcess verify the split point is within the chunk
    ChunkRange range = ChunkRange(chunk.getMin(), chunk.getMax());
    log() << "[statemachine] parent chunk type" << chunk;
    if (splitPoint.woCompare(chunk.getMax()) != 0 && !range.containsKey(splitPoint)) {
        return {ErrorCodes::InvalidOptions,
                str::stream() << "Split key " << splitPoint << " not contained within chunk "
                              << range.toString()};
    }

    BSONArrayBuilder updates;
    // update max key of parent chunk and add it as update object
    chunk.setMax(splitPoint);
    chunk.setVersion(currentMaxVersion);
    _addUpdateChunkObject(txn, updates, chunk, true);
    // update child chunk and add it as update object
    childChunk.setMin(splitPoint);
    childChunk.setMax(maxKey);
    _addUpdateChunkObject(txn, updates, childChunk, false);

    BSONArrayBuilder preCond;
    {
        BSONObjBuilder b;
        b.append("ns", ChunkType::ConfigNS);
        b.append("q",
                 BSON("query" << BSON(ChunkType::ns(chunk.getNS())
                              << ChunkType::min() << range.getMin()
                              << ChunkType::max() << range.getMax())
                              << "orderby"
                              << BSON(ChunkType::DEPRECATED_lastmod() << -1)));
        {
            BSONObjBuilder bb(b.subobjStart("res"));
            bb.append(ChunkType::shard(), chunk.getShard().toString());
        }
        preCond.append(b.obj());
    }

    // apply the batch of updates to remote and local metadata

    Status applyOpsStatus =
        grid.catalogClient(txn)->applyChunkOpsDeprecated(txn,
                                                         updates.arr(),
                                                         preCond.arr(),
                                                         chunk.getNS(),
                                                         currentMaxVersion,
                                                         WriteConcernOptions(),
                                                         repl::ReadConcernLevel::kLocalReadConcern);
    if (!applyOpsStatus.isOK()) {
        error() << "[statemachine] _commitSplitHandler, failed to update chunkmap";
        transitionState(event, applyOpsStatus.code());
    } else {
        splitEvent->refreshChunk(chunk, childChunk);

        // assign the child chunk
        Balancer::get(txn)->assignChunk(txn,
                                        childChunk,
                                        false,
                                        true);
        transitionState(event, ErrorCodes::OK);
    }

    log() << "[statemachine] _commitSplitHandler, succeed to update chunkmap";
    return StateMachine::NextAction::kActionContinue;
}

StatusWith<StateMachine::NextAction> StateMachine::_acknowledgeSplitHandler(IRebalanceEvent* event) {
    logStateMachineEvent("_acknowledgeSplitHandler", event);
    auto sendAckStatus = _sendAckCommand(event, true);
    if (!sendAckStatus.isOK()) {
        warning() << "failed to send ack command of split";
        // TODO retry a few times, now we just complete the split as success
        transitionState(event, ErrorCodes::OK);
        return StateMachine::NextAction::kActionContinue;
    }

    return StateMachine::NextAction::kActionWait;
}

StatusWith<StateMachine::NextAction> StateMachine::_splittedHandler(IRebalanceEvent* event) {
    logStateMachineEvent("_splittedHandler", event);
    return StateMachine::NextAction::kActionFinish;
}


}  // namespace mongo

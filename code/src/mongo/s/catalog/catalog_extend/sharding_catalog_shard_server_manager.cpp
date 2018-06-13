
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"
#include "mongo/s/catalog/catalog_extend/sharding_catalog_shard_server_manager.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/executor/task_executor.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/executor/thread_pool_task_executor.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"
#include "mongo/db/namespace_string.h"
#include "mongo/s/catalog/type_locks.h"
#include "mongo/client/connection_string.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/db/server_options.h"
#include "mongo/db/audit.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/repl/replication_executor.h"
#include "mongo/db/s/balancer/balancer.h"
#include "mongo/client/replica_set_monitor.h"
#include "mongo/s/client/shard_connection.h"
#include "mongo/util/util_extend/default_parameters.h"
#include "mongo/db/modules/rocks/src/GlobalConfig.h"

#include <iomanip>
#include <climits>
#include <time.h>
#include <limits.h> 
//add
#include "mongo/s/config.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/db/namespace_string.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/s/offload_chunk_request.h"


namespace mongo {

using std::unordered_map;
using std::list;
using str::stream;
using std::string;
using std::cout;
using std::vector;
using executor::TaskExecutor;
using executor::RemoteCommandRequest;
using executor::RemoteCommandResponse;
using RemoteCommandCallbackArgs = executor::TaskExecutor::RemoteCommandCallbackArgs;
using executor::TaskExecutorPool;


typedef std::map<string, int>  FailedChunksMap;
std::mutex   g_chun_alarm_mutex;
std::mutex g_non_shard_lock;


namespace {

const ReadPreferenceSetting kConfigReadSelector(ReadPreference::PrimaryOnly, TagSet{});
const ReadPreferenceSetting kConfigPrimarySelector(ReadPreference::PrimaryOnly);



const Milliseconds kDefaultTakeOverShardRetryInterval(500);
const Milliseconds kDefaultCPUSleepTime(10);

const uint64_t kShardNameDigitLimt(UINT64_MAX);
const int kShardNameDigitWidth(4);

const long long kMaxSizeMBDefault = 0;

} // namespace

ShardServerManager::ShardServerManager() {
    _pSecureRandom = SecureRandom::create();

    ThreadPool::Options options;
    options.poolName = "shardServerFailureHanldingThreadPool";
    options.threadNamePrefix = "shardServerFailureHanlding";
    options.onCreateThread = [](const std::string& threadName) {
        Client::initThread(threadName.c_str());
    };
    _failureHandlingThreadPool = new ThreadPool(options);
    _failureHandlingThreadPool->startup();

    options.poolName = "failedChunksHandlingThreadPool";
    options.threadNamePrefix = "failedChunksHandling";
    options.onCreateThread = [](const std::string& threadName) {
        Client::initThread(threadName.c_str());
    };
    _failedChunksThreadPool = new ThreadPool(options);
    _failedChunksThreadPool->startup();
    options.poolName = "takeOverNonShardThreadPool";
    options.threadNamePrefix = "takeOverNonShardHandling";
    options.onCreateThread = [](const std::string& threadName) {
        Client::initThread(threadName.c_str());
    };
    _takeOverNonShardThreadPool = new ThreadPool(options);
    _takeOverNonShardThreadPool->startup();
}

ShardServerManager::~ShardServerManager() {
    _failureHandlingThreadPool->shutdown();
    _failureHandlingThreadPool->join();
    delete(_failureHandlingThreadPool);

    _failedChunksThreadPool->shutdown();
    _failedChunksThreadPool->join();
    delete(_failedChunksThreadPool);
    
    _takeOverNonShardThreadPool->shutdown();
    _takeOverNonShardThreadPool->join();
    delete(_takeOverNonShardThreadPool);
    
    delete(_pSecureRandom);
    _pSecureRandom = NULL;
}

void ShardServerManager::onTransitionToPrimary(OperationContext* txn) {
    stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
    invariant(_failureDetectionThreadState == kStopped);
    _failureDetectionThreadState = kRunning;

    invariant(!_failureDetectionThread.joinable());
    invariant(!_threadOperationContext);
    _failureDetectionThread = stdx::thread([this] { _failureDetectionMainThread(); });
}

void ShardServerManager::onStepDownFromPrimary() {
    stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
    if (_failureDetectionThreadState != kRunning)
        return;

    _failureDetectionThreadState = kStopping;

    // Interrupt the failureDetectionThread if it has been started. We are guaranteed that the operation
    // context of that thread is still alive, because we hold the failureDetectionThread mutex.
    if (_threadOperationContext) {
        stdx::lock_guard<Client> scopedClientLock(*_threadOperationContext->getClient());
        _threadOperationContext->markKilled(ErrorCodes::InterruptedDueToReplStateChange);
    }

    _failureDetectionThreadCondVar.notify_all();
}

void ShardServerManager::onDrainComplete(OperationContext* txn) {
    invariant(!txn->lockState()->isLocked());

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
        if (_failureDetectionThreadState == kStopped)
            return;

        invariant(_failureDetectionThreadState == kStopping);
        invariant(_failureDetectionThread.joinable());
    }

    _failureDetectionThread.join();

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
        _failureDetectionThreadState = kStopped;
        _failureDetectionThread = {};
    }


    {
        stdx::lock_guard<stdx::mutex> initLock(_initMutex);
        _hasInited = false;
    }

    LOG(1) << "Failure detection thread terminated";
}

Status ShardServerManager::_init(OperationContext* txn) {
    vector<string> shardNames;
    isProcessMap.clear();
    
    auto status = _loadShardServerView(txn, shardNames);
    if (!status.isOK()) {
        return Status(status.code(),
            stream() << "Failed to load shard server view"
                     << causedBy(status));
    } 
    
    status = _loadFailedDataBases(txn);
    if (!status.isOK()) {
        return Status(status.code(),
            stream() << "Failed to load databases which primary is failed"
                 << causedBy(status));
    }

    // wait statemachine to finish recovering rebalance events
    Grid::get(txn)->catalogManager()->getStateMachine()->waitForRecovery();
    
    status = _loadFailedChunks(txn, shardNames);
    if (!status.isOK()) {
        return Status(status.code(),
            stream() << "Failed to load failed chunks"
                     << causedBy(status));
    }

    {
        stdx::lock_guard<stdx::mutex> initLock(_initMutex);
        _hasInited = true;
    }

    return Status::OK();
}


Status ShardServerManager::_loadShardServerView(OperationContext* txn, vector<string> &shardNames) {
    LOG(1) << "Begin to load shard server view from configDB";
    
    auto findStatus = 
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                        txn,
                        kConfigReadSelector,
                        repl::ReadConcernLevel::kMajorityReadConcern,
                        NamespaceString(ShardType::ConfigNS),
                        BSONObj(),  // no query filter
                        BSONObj(),  // no sort
                        boost::none);

    if (!findStatus.isOK()) {
        return Status(findStatus.getStatus().code(),
            stream() << "Failed to read " << ShardType::ConfigNS << " collection documents"
                     << causedBy(findStatus.getStatus()));
    }

    auto shards = std::move(findStatus.getValue().docs);
    log() << "found " << shards.size() << " shard servers when loading shard server view";

    {
        stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        stdx::lock_guard<stdx::mutex> failedShardServerLock(_failedShardServerListMutex);
        stdx::lock_guard<stdx::mutex> shardNameLock(_shardNameMutex);

        _shardServerNodeMap.clear();
        _failedShardServerList.clear();
        _maxShardNameDigit = 0;

        for (const BSONObj& doc : shards) {
            auto shardRes = ShardType::fromBSON(doc);
            if (!shardRes.isOK()) {
                return Status(shardRes.getStatus().code(),
                          stream() << "Failed to parse shard document " << doc
                                   << " due to " << shardRes.getStatus().reason());
            }

            ShardType shardType = shardRes.getValue();

            auto conn_status =
                _validateSingleConnectionString(ConnectionString(shardType.getHost(),
                    ConnectionString::ConnectionType::MASTER));
            if (!conn_status.isOK()) {
                return Status(ErrorCodes::BadValue, "invalid shard host");
            }

            shardNames.push_back(shardType.getName());
            string shardServerNodeIP = conn_status.getValue().host();

            // add to failed list and start fault handling immediately
            if (shardType.getState() == ShardType::ShardState::kShardFault) {
                FailedShardType failedShardType;
                failedShardType.shard = shardType;
                failedShardType.isProcessing = false;

                _failedShardServerList.push_front(failedShardType);
                _failureHandlingThreadPool->schedule(ShardServerManager::handleFailedShardServer);

                continue;
            }

            ShardInfo shardInfo;
            shardInfo.shard = shardType;
            shardInfo.heartbeatTime = Date_t::now();



            _shardServerNodeMap[shardServerNodeIP].push_front(std::move(shardInfo));
        }
    }

    log() << "Finish shard server view loading successfully";

    return Status::OK();
}

Status ShardServerManager::_loadFailedChunks(OperationContext* txn, const vector<string> &shardNames) {
    log() << "Begin to load failed chunks from configDB";

    vector<int> failedStates;
    failedStates.push_back(static_cast<int>(ChunkType::ChunkStatus::kOffloaded));
    failedStates.push_back(static_cast<int>(ChunkType::ChunkStatus::kAssigned));

    BSONObjBuilder builder;
    builder.append(ChunkType::shard(), BSON("$nin" << shardNames));
    builder.append(ChunkType::status(), BSON("$in" << failedStates));
    builder.append(ChunkType::rootFolder(), BSON("$ne" << "stalerootfolder"));

    auto findStatus = Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                        txn,
                        kConfigReadSelector,
                        repl::ReadConcernLevel::kMajorityReadConcern,
                        NamespaceString(ChunkType::ConfigNS),
                        builder.obj(),
                        BSONObj(),
                        boost::none);

    if (!findStatus.isOK()) {
        return Status(findStatus.getStatus().code(),
            stream() << "Failed to read " << ChunkType::ConfigNS << " collection documents"
                     << causedBy(findStatus.getStatus()));
    }

    auto failedChunks = std::move(findStatus.getValue().docs);
    log() << "found " << failedChunks.size() << " failed chunks";

    {
        stdx::lock_guard<stdx::mutex> dataLock(_failedChunksMutex);
        _failedChunks.clear();
    }

    for (const BSONObj& doc : failedChunks) {
        auto chunkRes = ChunkType::fromBSON(doc);
        if (!chunkRes.isOK()) {
            return Status(chunkRes.getStatus().code(),
                          stream() << "Failed to parse chunk document " << doc
                                   << " due to " << chunkRes.getStatus().reason());
        }

        ChunkType chunk = chunkRes.getValue();
        std::string coll = chunk.getNS();
        auto findCollectionStatus = 
                    Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                        txn,
                        kConfigReadSelector,
                        repl::ReadConcernLevel::kLocalReadConcern,
                        NamespaceString(CollectionType::ConfigNS),
                        BSON(CollectionType::fullNs() << coll),
                        BSONObj(),
                        boost::none);
        if (!findCollectionStatus.isOK()){
           LOG(2) << "fail to find " << coll << " in config.collections";
           continue;
        }
        auto parseCollStatus = CollectionType::fromBSON(findCollectionStatus.getValue().docs.front());
        if (!parseCollStatus.isOK()){
           LOG(2) << "fail to parse " << coll << " cause by " << parseCollStatus.getStatus();
           continue;
        }
        CollectionType collType = parseCollStatus.getValue();
        if (collType.getTabType() == CollectionType::TableType::kSharded){
           processFailedChunk(chunkRes.getValue());
        } 
    }    
    
    log() << "Finish loading failed chunks from configDB";
    return Status::OK();
}


StatusWith<stdx::unordered_map<string, list<ShardServerManager::ShardInfo>>>
ShardServerManager::getShardServerNodeMap(OperationContext* txn) {
    {
        stdx::lock_guard<stdx::mutex> initLock(_initMutex);
        if (!_hasInited) {
            return Status(ErrorCodes::NotYetInitialized, "not yet initialized to primary");
        }
    }

    return _shardServerNodeMap;
}

StatusWith<std::string> ShardServerManager::generateNewShardName(OperationContext* txn) {
    {
        stdx::lock_guard<stdx::mutex> initLock(_initMutex);
        if (!_hasInited) {
            return Status(ErrorCodes::NotYetInitialized, "not yet initialized to primary");
        }
    }

    BSONObjBuilder shardNameRegex;
    shardNameRegex.appendRegex(ShardType::name(), "^shard");

    auto findStatus = Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
        txn,
        kConfigReadSelector,
        repl::ReadConcernLevel::kMajorityReadConcern,
        NamespaceString(ShardType::ConfigNS),
        shardNameRegex.obj(),
        BSON(ShardType::name() << -1),
        1);
    if (!findStatus.isOK()) {
        return findStatus.getStatus();
    }

    const auto& docs = findStatus.getValue().docs;

    int count = 0;
    if (!docs.empty()) {
        const auto shardStatus = ShardType::fromBSON(docs.front());
        if (!shardStatus.isOK()) {
            return shardStatus.getStatus();
        }

        std::istringstream is(shardStatus.getValue().getName().substr(5));
        is >> count;
        count++;
    }

    // TODO fix so that we can have more than 10000 automatically generated shard names
    if (count < 9999) {
        std::stringstream ss;
        ss << "shard" << std::setfill('0') << std::setw(4) << count;
        return ss.str();
    }

    return Status(ErrorCodes::OperationFailed, "unable to generate new shard name");
}

Status ShardServerManager::activateShard(
    OperationContext* txn,
    const std::string& shardName,
    const ConnectionString& conn,
    const std::string& processIdentity) {
    {
        stdx::lock_guard<stdx::mutex> initLock(_initMutex);
        if (!_hasInited) {
            return Status(ErrorCodes::NotYetInitialized, "not yet initialized to primary");
        }
    }
    
    const auto findShardStatus = 
                Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                    txn,
                    kConfigPrimarySelector,
                    repl::ReadConcernLevel::kLocalReadConcern,
                    NamespaceString(ShardType::ConfigNS),
                    BSON(ShardType::name() << shardName << 
                        ShardType::processIdentity() << processIdentity),
                    BSONObj(),
                    boost::none); 
    if (!findShardStatus.isOK())
    {
        return findShardStatus.getStatus();
    }
    const auto shardDocs = findShardStatus.getValue().docs;
    if (shardDocs.size() > 1) {
        return Status(ErrorCodes::TooManyMatchingDocuments,
            str::stream() << "More than one document for shard " <<
                shardName << " in config databases");
    }

    if (shardDocs.size() == 0) {
        return Status(ErrorCodes::ShardNotFound,
                str::stream() << "Shard " << shardName << " does not exist");
    }

    auto existingShardParseStatus = ShardType::fromBSON(shardDocs.front());
    if (!existingShardParseStatus.isOK()) {
        return existingShardParseStatus.getStatus();
    }

    ShardType existingShard = existingShardParseStatus.getValue();
    if (existingShard.getState() != ShardType::ShardState::kShardRestarting){
       log() << "Finish activationShard for shard server ";

       return Status::OK();    
    }
    auto conn_status = _validateSingleConnectionString(conn);
    if (!conn_status.isOK()) {
        return conn_status.getStatus();
    }
    string shardServerNodeIP = conn_status.getValue().host();


    // Make a fake ConnectionString with setName = shardName
    /*ConnectionString fakeConnectionString(
        ConnectionString::SET,
        conn.getServers(),
        shardName);
    */
    auto addShardResult = Grid::get(txn)->catalogManager()->addShard(
                                    txn,
                                    shardName,
                                    conn,
                                    kMaxSizeMBDefault,
                                    processIdentity);

       if (!addShardResult.isOK()) {
        log() << "Failed addShard for shard: " << conn
            << causedBy(addShardResult.getStatus());
        return addShardResult.getStatus();
    }

    return Status::OK(); 


}


StatusWith<int> ShardServerManager::_checkHeartbeatTimeOut(OperationContext* txn) {
    {
        stdx::lock_guard<stdx::mutex> initLock(_initMutex);
        if (!_hasInited) {
            return Status(ErrorCodes::NotYetInitialized, "not yet initialized to primary");
        }
    }

    stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);

    Date_t now = Date_t::now();
    Date_t expireTime = now - kDefaultShardHeartbeatTimeoutPeriod;

    int newDetectedFailedShardServerNum = 0;
    unordered_map<string, list<ShardInfo>>::iterator itMap = _shardServerNodeMap.begin();
    while (itMap != _shardServerNodeMap.end()) {
        list<ShardInfo>::iterator itList = itMap->second.begin();
        while (itList != itMap->second.end()) {
            if (itList->heartbeatTime <= expireTime) {
                getGlobalServiceContext()->registerProcessStageTime("takeoverShard:"+itList->shard.getName());
                getGlobalServiceContext()->getProcessStageTime(
                    "takeoverShard:"+itList->shard.getName())->noteStageStart("failedShardDetected");
                FailedShardType failedShardType;
                failedShardType.shard = itList->shard;
                failedShardType.isProcessing = false;

                stdx::lock_guard<stdx::mutex> failedShardServerLock(_failedShardServerListMutex);
                // Move the shard server with heartbeat timeout to the failed shard server set
                _failedShardServerList.push_front(failedShardType);
                itList = itMap->second.erase(itList);

                newDetectedFailedShardServerNum++;
            }
            else {
                itList++;
            }
        }

        if (itMap->second.size() == 0) {
            itMap = _shardServerNodeMap.erase(itMap);
        }
        else {
            itMap++;
        }
    }

    return newDetectedFailedShardServerNum;
}

void ShardServerManager::_failureDetectionMainThread() {
    Client::initThread(kShardServerFailureDetectionJobName.c_str());
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    log() << "Shard server failure detector is starting";

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
        _threadOperationContext = txn;
    }

    {
        stdx::lock_guard<stdx::mutex> initLock(_initMutex);
        invariant(!_hasInited);
    }

    auto status = _init(txn);
    if (!status.isOK()) {
        if (_stopRequested()) {
            {
                stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
                invariant(_failureDetectionThreadState == kStopping);
                _threadOperationContext = nullptr;
            }
        
            log() << "Shard server failure detector is now stopped, with failed "
                  << "data initialization before running failure detection main loop"
                  << causedBy(status);
            
            return;
        }
        
        fassertFailedWithStatus(40300,
            Status(status.code(), stream()
                << "Failed to initialize data before running failure detection main loop"
                << causedBy(status)));
    }

    // Main loop
    while (!_stopRequested()) {
        _beginRound(txn);

        if (GLOBAL_CONFIG_GET(SSHeartBeat)) {
            // Find out failed shard servers
            auto statusWithCheckFailedShardServers = _checkHeartbeatTimeOut(txn);
            if (statusWithCheckFailedShardServers.getStatus().isOK()) {
                int newDetectedFailedShardServerNum = statusWithCheckFailedShardServers.getValue();
                for (int i = 0; i < newDetectedFailedShardServerNum; i++) {
                    // Schedule a task for each new failed shard server we found
                    _failureHandlingThreadPool->schedule(ShardServerManager::handleFailedShardServer);
                }
            }
        }

        _endRound(txn, kShardServerFailureDetectionInterval);
    }
    {
        stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
        invariant(_failureDetectionThreadState == kStopping);
        _threadOperationContext = nullptr;
    }

    log() << "Shard server failure detector is now stopped";

    return;
}

Status ShardServerManager::getOneFailedShardServer(ShardType& failedShard) {
    stdx::lock_guard<stdx::mutex> scopedLock(_failedShardServerListMutex);
    if (_failedShardServerList.empty()) {
        return Status(ErrorCodes::BadValue,
                stream() << "Empty list of failed shard servers");
    }

    std::list<FailedShardType>::iterator it = _failedShardServerList.begin();
    while (it != _failedShardServerList.end()) {
        if (it->isProcessing != true) {
            failedShard = it->shard;
            it->isProcessing = true;
            break;
        }
        ++it;
    }

    return Status::OK();
}

void ShardServerManager::removeShardFromFailedList(std::string shardName) {
    stdx::lock_guard<stdx::mutex> scopedLock(_failedShardServerListMutex);

    std::list<FailedShardType>::iterator it = _failedShardServerList.begin();
    while (it != _failedShardServerList.end()) {
        if (it->shard.getName().compare(shardName) == 0) {
            _failedShardServerList.erase(it);
            break;
        }
        ++it;
    }

    return;
}

void ShardServerManager::handleFailedShardServer() {
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    // Get a failed shard
    ShardType failedShard;
    int getOneFailedShardRound = 0;
    while (true) {
        log() << std::to_string(getOneFailedShardRound) << "-th getOneFailedShardServer round starts...";

        if (Grid::get(txn)->catalogManager()->getShardServerManager()->_stopRequested()) {
            log() << "handleFailedShardServer thread exits due to stopRequested";
            return;
        }

        Status getOneFailedShardServerStatus =
        Grid::get(txn)->catalogManager()->getShardServerManager()->getOneFailedShardServer(failedShard);
        if (getOneFailedShardServerStatus.isOK()) {
            log() << "Get a failed shard: " << failedShard.getHost()
                  << ", processIdentity: " << failedShard.getProcessIdentity();
            break;
        }

        log() << "Fail to get a failed shard server " << causedBy(getOneFailedShardServerStatus);

        stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSystemDuration());
        getOneFailedShardRound++;
    }

    // Take over the shard if this shard sever is kShardActive
    if ((failedShard.getState() != ShardType::ShardState::kShardRegistering) &&
        (failedShard.getState() != ShardType::ShardState::kShardRestarting)) {
        getGlobalServiceContext()->getProcessStageTime(
            "takeoverShard:"+failedShard.getName())->noteStageStart("takeOverShard");
        auto takeOverShardStatus = 
            Grid::get(txn)->catalogManager()->getShardServerManager()->takeOverShard(txn, failedShard);
        if (!takeOverShardStatus.isOK()) {
            log() << "failed takeOverShard for shard: " << failedShard.getName() 
                << causedBy(takeOverShardStatus);
            return;
        }
    }
    
    // remove shard
    getGlobalServiceContext()->getProcessStageTime(
            "takeoverShard:"+failedShard.getName())->noteStageStart("removeShardFromConfigDB");
    int removeShardRound = 0;
    while (true) {
        log() << std::to_string(removeShardRound) << "-th removeShard round starts...";

        if (Grid::get(txn)->catalogManager()->getShardServerManager()->_stopRequested()) {
            log() << "handleFailedShardServer thread exits due to stopRequested";
            return;
        }

        BSONObjBuilder filterBuilder;
        filterBuilder.append(ShardType::name(), failedShard.getName());
        filterBuilder.append(ShardType::processIdentity(), failedShard.getProcessIdentity());

        Status removeShardStatus =
            Grid::get(txn)->catalogClient(txn)->removeConfigDocuments(
                                              txn,
                                              ShardType::ConfigNS,
                                              filterBuilder.obj(),
                                              ShardingCatalogClient::kMajorityWriteConcern);
        if (removeShardStatus.isOK()) {
            log() << "Finish remove shard: " << failedShard.getHost();
            break;
        }

        log() << "Fail to remove shard: " << failedShard.getHost()
              << " due to " << causedBy(removeShardStatus);

        std::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSystemDuration());
        removeShardRound++;
    }

    Grid::get(txn)->catalogClient(txn)->logChange(
        txn,
        "removeShard",
        "",
        BSON(ShardType::name() << failedShard.getName()),
        ShardingCatalogClient::kMajorityWriteConcern);

    // delete shard in failedList
    getGlobalServiceContext()->getProcessStageTime(
            "takeoverShard:"+failedShard.getName())->noteStageStart("removeShardFromFailedList");
    Grid::get(txn)->catalogManager()->getShardServerManager()->removeShardFromFailedList(failedShard.getName());

    log() << "Finish shard takeover for: " << failedShard.getName();
    getGlobalServiceContext()->getProcessStageTime(
        "takeoverShard:"+failedShard.getName())->noteProcessEnd();
    log() << "Time of " << "takeoverShard:"+failedShard.getName() << ": " 
        << getGlobalServiceContext()->getProcessStageTime(
        "takeoverShard:"+failedShard.getName())->toString();
    getGlobalServiceContext()->cancelProcessStageTime("takeoverShard:"+failedShard.getName());
    
    return;
}

void ShardServerManager::_addFailedChunk(const ChunkType& chunk) {
    stdx::lock_guard<stdx::mutex> dataLock(_failedChunksMutex);
    _failedChunks.push_back(chunk);
}

Status ShardServerManager::getOneFailedChunk(ChunkType& chunk) {
    stdx::lock_guard<stdx::mutex> dataLock(_failedChunksMutex);
    if (_failedChunks.empty()) {
        return Status(ErrorCodes::ChunkNotFound, "No failed chunks needs to handle");
    }
    chunk = _failedChunks.front();
    _failedChunks.pop_front();
    return Status::OK();
}

void ShardServerManager::processFailedChunk(const ChunkType& chunk) {
    log() << "processFailedChunk: " << chunk;
    _addFailedChunk(chunk);
    getGlobalServiceContext()->registerProcessStageTime("assignChunk:"+chunk.getName());
    getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+chunk.getName())->noteStageStart(
            "scheduleTakeOverChunk:oldShard:"+chunk.getShard().toString());
    _failedChunksThreadPool->schedule(ShardServerManager::takeOverFailedChunk);
}

Status ShardServerManager::markFailedShardDocument(OperationContext* txn, const ShardType& failedShard) {
    // update shard state to be fault
    Status status = Status::OK();
    if ((int)failedShard.getState() != (int)ShardType::ShardState::kShardFault) {
        int markFailedShardDocumentRound = 0;
        while (true) {
            log() << std::to_string(markFailedShardDocumentRound) << "-th markFailedShardDocument round starts...";

            if (_stopRequested()) {
                log() << "markFailedShardDocument exits due to stopRequested";
                status = Status(ErrorCodes::NotMaster, "markFailedShardDocument exits due to stopRequested");
                break;
            }

            auto updateShardStateFaultStatus = 
                Grid::get(txn)->catalogManager()->updateShardStateDuringFailover(
                    txn,
                    failedShard,
                    ShardType::ShardState::kShardFault);

            if (updateShardStateFaultStatus.isOK()) {
                log() << "Finish update shard state be fault: " << failedShard.getName() << " with " << failedShard.getHost();
                break;
            }

            if (ErrorCodes::ShardServerNotFound == updateShardStateFaultStatus.code()) {
                status = updateShardStateFaultStatus;
                break;
            }

            log() << "Fail to update shard: " << failedShard.getHost() 
                  << " state to be Fault due to " << causedBy(updateShardStateFaultStatus);

            stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSystemDuration());
            markFailedShardDocumentRound++;
        }
    }

    return status;
}

Status ShardServerManager::preProcessFailedChunks(OperationContext* txn, const ShardType& failedShard) {
    // mark all chunk state to be fault
    getGlobalServiceContext()->getProcessStageTime(
        "takeoverShard:"+failedShard.getName())->noteStageStart("updateMultiChunkStatePendingOpen");
    int updateChunkStatePendingOpenRound = 0;
    while (true) {
        LOG(2) << std::to_string(updateChunkStatePendingOpenRound) << "-th updateChunkStatePendingOpen round starts...";

        if (_stopRequested()) {
            return Status(ErrorCodes::NotMaster, "preProcessFailedChunks thread exits due to stopRequested");
        }

        auto updateChunkStatePendingOpenStatus =
            Grid::get(txn)->catalogManager()->updateMultiChunkStatePendingOpen(
                txn,
                failedShard.getName());
        if (updateChunkStatePendingOpenStatus.isOK()) {
            LOG(2) << "Finish update chunk state be fault: " << failedShard.getHost();
            break;
        }

        log() << "Fail to update shard: " << failedShard.getHost()
              << " state to be Fault due to " << causedBy(updateChunkStatePendingOpenStatus);

        stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSystemDuration());
        updateChunkStatePendingOpenRound++;
    }

    // get all chunks document
    stdx::lock_guard<stdx::mutex> mapLock(_nonShardedChunksMutex);
    getGlobalServiceContext()->getProcessStageTime(
        "takeoverShard:"+failedShard.getName())->noteStageStart("processFailedChunks");
    int findFailedChunksRound = 0;
    while (true) {
        LOG(0) << std::to_string(findFailedChunksRound) << "-th findFailedChunks round starts...";

        if (_stopRequested()) {
            return Status(ErrorCodes::NotMaster, "preProcessFailedChunks thread exits due to stopRequested");
        }

        BSONObjBuilder failShardChunkFilter;
        failShardChunkFilter.append(ChunkType::shard(), failedShard.getName());
        failShardChunkFilter.append(ChunkType::rootFolder(), BSON("$ne" << "stalerootfolder"));
        failShardChunkFilter.append(ChunkType::status(), BSON("$ne" << static_cast<int>(ChunkType::ChunkStatus::kDisabled)));

        auto findFailedChunksStatus = 
            Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                txn,
                kConfigReadSelector,
                repl::ReadConcernLevel::kLocalReadConcern,
                NamespaceString(ChunkType::ConfigNS),
                failShardChunkFilter.obj(), 
                BSONObj(),  // no sort
                boost::none);

        if (findFailedChunksStatus.isOK()) {
            //add
            
            //std::list<std::string> dbs;
            auto getDbsStatus = getDbsOnFailedShard(txn,failedShard.getName());
            std::vector<string>& dbsOnFailedShard = getDbsStatus.getValue();
            //end
            for (const BSONObj& failedChunk : findFailedChunksStatus.getValue().docs) {
                auto chunkRes = ChunkType::fromBSON(failedChunk);
                if (!chunkRes.isOK()) {
                    log() << "Failed to parse chunk document " << failedChunk
                          << " due to " << chunkRes.getStatus().reason();
                    continue;
                }
                LOG(2) << "[preProcessFailedChunks] chunk:"
                       << chunkRes.getValue().toString()
                       << " eventid: " << GET_EVENTID(chunkRes.getValue());
                std::string coll = chunkRes.getValue().getNS();
                auto findCollectionStatus = 
                    Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                        txn,
                        kConfigReadSelector,
                        repl::ReadConcernLevel::kLocalReadConcern,
                        NamespaceString(CollectionType::ConfigNS),
                        BSON(CollectionType::fullNs() << coll),
                        BSONObj(),
                        boost::none);
                if (!findCollectionStatus.isOK()){
                    LOG(2) << "fail to find " << coll << " in config.collections";
                    continue;
                }
                auto parseCollStatus = CollectionType::fromBSON(findCollectionStatus.getValue().docs.front());
                if (!parseCollStatus.isOK()){
                    LOG(2) << "fail to parse " << coll << " cause by " << parseCollStatus.getStatus();
                    continue;
                }
                CollectionType collType = parseCollStatus.getValue();
                //process sharded chunk       
                if (collType.getTabType() == CollectionType::TableType::kSharded){
                    processFailedChunk(chunkRes.getValue());
                }

            }
 
            for (std::string database : dbsOnFailedShard){
                std::list<ChunkType> chunkList;
                //get all non shard chunks belong to database        
                getAllNonShardChunks(txn,database,chunkList);
                _nonShardedChunksMap[database] = chunkList;
            }
            
            for (stdx::unordered_map<std::string, std::list<ChunkType>>::iterator it = _nonShardedChunksMap.begin();
                 it != _nonShardedChunksMap.end(); ++ it){
                bool isProcess = false;
                std::string db = it->first;
                getIsProcessMap(db,isProcess);
                if (isProcess){
                    continue;
                }
                setIsProcessMap(db,true);
                _takeOverNonShardThreadPool->schedule(ShardServerManager::takeOverNonShardedChunk);
            }

            LOG(0) << "Finish find " << findFailedChunksStatus.getValue().docs.size() << " failed chunks for shard:"
                   << failedShard.getName();
            break;
        }

        log() << "Fail to find failed chunks for shard: " << failedShard.getName()
              << causedBy(findFailedChunksStatus.getStatus());

        stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSystemDuration());
        findFailedChunksRound++;
    }
    return Status::OK();
}


Status ShardServerManager::takeOverShard(OperationContext* txn, const ShardType& failedShard) {
    log() << "Begin to take over shard: " << failedShard.getName(); 
    getGlobalServiceContext()->getProcessStageTime(
        "takeoverShard:"+failedShard.getName())->noteStageStart("markFailedShardDocument");
    auto markFailedShardDocumentStatus = markFailedShardDocument(txn, failedShard);
    if (!markFailedShardDocumentStatus.isOK()) {
        return markFailedShardDocumentStatus;
    }

    getGlobalServiceContext()->getProcessStageTime(
        "takeoverShard:"+failedShard.getName())->noteStageStart("preProcessFailedChunks");
    auto preProcessFailedChunksStatus = preProcessFailedChunks(txn, failedShard);
    if (!preProcessFailedChunksStatus.isOK()) {
        return preProcessFailedChunksStatus;
    }
    
    return Status::OK();
}

// Send chunk abnormal alarm to OAM.
/*void sendChunkAbnormalAlarm(const ChunkType chunk, const ShardId takeOverShard, FailedChunksMap& failedChunksMap) {
    g_chun_alarm_mutex.lock();
    auto mapIter = failedChunksMap.find(chunk.getID());
    if (failedChunksMap.end() != mapIter) {
        mapIter->second += 1;
        if (MAX_CHUNK_ABNORMAL_COUNT == mapIter->second) {
            FML_ALARM_STRU alarmStru = {0};
            FML_INT        ret_alarm      = 0;
            string         locationInfo   = chunk.getID();
            string         additionalInfo = takeOverShard.toString();
            ret_alarm = sendAlarm(ALARM_INDEX_CHUNK_ABNORMAL, ALARM_CRASH, locationInfo, additionalInfo, alarmStru);
            index_log() << ALARM_LOG(alarmStru, ret_alarm);
        }
    } else {
        failedChunksMap.insert(std::pair<string,int>(chunk.getID(),1));//lint !e69
    }
    g_chun_alarm_mutex.unlock();
}*/

 // Recover alarm.
/*void recoverChunkAbnormalAlarm(const ChunkType chunk, const ShardId takeOverShard, FailedChunksMap& failedChunksMap) {
    g_chun_alarm_mutex.lock();
    auto mapIter = failedChunksMap.find(chunk.getID());

    if (failedChunksMap.end() != mapIter) {
        if (MAX_CHUNK_ABNORMAL_COUNT <= mapIter->second) {
            FML_ALARM_STRU alarmStru      = {0};
            FML_INT        ret_alarm      = 0;
            string         locationInfo   = chunk.getID();
            string         additionalInfo = takeOverShard.toString();
            ret_alarm = recoverAlarm(ALARM_INDEX_CHUNK_ABNORMAL, ALARM_CRASH, locationInfo, additionalInfo, alarmStru);
            index_log() << ALARM_LOG(alarmStru, ret_alarm);
        }
        failedChunksMap.erase(mapIter);
    }
    g_chun_alarm_mutex.unlock();
}*/

void ShardServerManager::takeOverFailedChunk() {
    static std::map<string, int>  failedChunksMap;
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    ChunkType chunk;
    Status getOneFailedChunkStatus =
        Grid::get(txn)->catalogManager()->getShardServerManager()->getOneFailedChunk(chunk);
    if (!getOneFailedChunkStatus.isOK()) {
        log() << "Failed to get failed chunk due to "
              << getOneFailedChunkStatus.reason();
        return;
    }

    Status validateStatus = chunk.validate();
    if (!validateStatus.isOK()) {
        // TODO: mark unsolved chunk, need to be handled
        log() << "Failed to validate chunk document " << chunk
              << " due to " << validateStatus.reason();
        return;
    }

    log() << "[failover] start to reassign chunk("
          << chunk.toString()
          << ")"; 
    // re-assign chunks    
    getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+chunk.getName())->noteStageStart("assignChunk");
    auto assignStatus = Balancer::get(txn)->assignChunk(txn,
                                                        chunk,
                                                        false,
                                                        false);
    if (!assignStatus.isOK()) {
        // Send chunk abnormal alarm to OAM.
        //sendChunkAbnormalAlarm(chunk, chunk.getShard(), failedChunksMap);

        // break, re-try re-assign chunk
        log() << "Fail to re-assign chunk: " << chunk.getName();
    } else {
        // Recover alarm.
        //recoverChunkAbnormalAlarm(chunk, chunk.getShard(), failedChunksMap);
    }

    log() << "Finish to execute re-assign chunk event: " << chunk.getName();
}

StatusWith<ShardType> ShardServerManager::getShardTypeByShardId(const ShardId& shardId) {
    // traverse _shardServerNodeMap to get the shardType
    {
        stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        unordered_map<string, list<ShardInfo>>::iterator itMap = _shardServerNodeMap.begin(); //lint !e60
        while (itMap != _shardServerNodeMap.end()) {
            list<ShardInfo>::iterator itList = itMap->second.begin();
            for (itList = itMap->second.begin(); itList != itMap->second.end(); ++itList) {
                if (itList->shard.getName().compare(shardId.toString()) == 0) {
                    return itList->shard;
                }
            }
            itMap++;
        }
    }
    
    return Status(ErrorCodes::ShardServerNotFound, "no matching shard server");
}

StatusWith<ShardType> ShardServerManager::getShardByHostAndPort(const HostAndPort& hostandport)
{
    stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        
    unordered_map<string, list<ShardInfo> >::iterator itMap = 
                                                    _shardServerNodeMap.find(hostandport.host()); 
     
    if (itMap == _shardServerNodeMap.end()) {
        LOG(0) << "ip : " << hostandport.host() << "not found";
        return Status(ErrorCodes::BadValue,
                            "host can only be found in shardServerNodeMap");

    }
        
    list<ShardServerManager::ShardInfo>::iterator itList;    
    for (itList = itMap->second.begin(); itList != itMap->second.end(); ++itList) {
        if (itList->shard.getHost().find(hostandport.toString()) != string::npos) {
           break;
        }
    }

    if (itList == itMap->second.end()) {
       LOG(0) << "port : " << hostandport.port() << "not found !";
       return Status(ErrorCodes::BadValue,
                            "host can only be found in shardServerNodeMap");
    }

    return itList->shard;
}

StatusWith<std::string> ShardServerManager::updateShardInMemory(const HostAndPort& hostandport, const ShardType& shard) { 
     std::string shardServerNodeIP = hostandport.host();
     stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        
     unordered_map<string, list<ShardInfo> >::iterator itMap = 
     _shardServerNodeMap.find(shardServerNodeIP);        
     if (itMap == _shardServerNodeMap.end()) {
        return Status(ErrorCodes::BadValue,
                stream() << "shard (" << shard.getHost()
                         << ")'s IP cannot be found in shardServerNodeMap");
     }
        
     list<ShardInfo>::iterator itList;    
     for (itList = itMap->second.begin(); itList != itMap->second.end(); ++itList) {
          if (itList->shard.getHost().find(hostandport.toString()) != std::string::npos) {
             break;
          }
     }

     if (itList == itMap->second.end()) {
         return Status(ErrorCodes::BadValue,
                stream() << "shard (" << shard.getHost()
                         << ")' port cannot be found in shardServerNodeMap");
     }
     itList->shard.setName(shard.getName());
     itList->shard.setState(ShardType::ShardState::kShardActive); 
     itList->shard.setHost(shard.getHost());
     LOG(0) << "update shard in shardNodeMap to : ";
     _printShardServerNodeView();
     return Status::OK();
}


bool ShardServerManager::isShardServerAlive(const ShardIdent& shardIdent) {
    auto shardId = shardIdent.getShardId();
    auto getShardTypeStatus = getShardTypeByShardId(shardId);
    if (!getShardTypeStatus.isOK()) {
        return false;
    }

    auto processIdent = getShardTypeStatus.getValue().getProcessIdentity();
    if (processIdent != shardIdent.getProcessIdentity()) {
        return false;
    }

    return true;
}

StatusWith<ShardType> ShardServerManager::getAnActiveShard() {
    stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
    if (_shardServerNodeMap.size() > 0) {
        int pick_offset = std::abs(_pSecureRandom->nextInt64()) % (_shardServerNodeMap.size());
        auto targetIt = _shardServerNodeMap.begin();
        std::advance(targetIt, pick_offset);

        Date_t expireTime = repl::getGlobalReplicationCoordinator()->getExecutor()->now() -
            kDefaultShardHeartbeatTimeoutPeriod;

        list<ShardInfo>::iterator itList;
        for (itList = targetIt->second.begin(); itList != targetIt->second.end(); ++itList) {
            if ((int)itList->shard.getState() == (int)ShardType::ShardState::kShardActive
                && (itList->heartbeatTime > expireTime || !GLOBAL_CONFIG_GET(SSHeartBeat))) {
                return itList->shard;
            }
        }
    }

    return Status(ErrorCodes::ShardServerNotFound, "no shard available in cluster");
}

StatusWith<ShardType> ShardServerManager::getTakeOverShard(OperationContext* txn, bool userRequest) {
    int getTakeOverShardRound = 0;
    
    // pick shard for each chunk
    while (true) {
        log() << std::to_string(getTakeOverShardRound) << "-th getTakeOverShard round starts...";
        auto activeShard = getAnActiveShard();
        if (activeShard.isOK()) {
            return activeShard.getValue();
        }

        if (userRequest) {
            return activeShard.getStatus();
        }

        stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSystemDuration());
        getTakeOverShardRound++;
    }
} 

bool ShardServerManager::_stopRequested() {
    stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
    return (_failureDetectionThreadState != kRunning);
}

void ShardServerManager::_beginRound(OperationContext* txn) {
    stdx::unique_lock<stdx::mutex> lock(_failureDetectionThreadStateMutex);
    _inFailureDetectionRound = true;
    _failureDetectionThreadCondVar.notify_all();
}

void ShardServerManager::_endRound(OperationContext* txn, Milliseconds waitTimeout) {
    {
        stdx::lock_guard<stdx::mutex> lock(_failureDetectionThreadStateMutex);
        _inFailureDetectionRound = false;
        _failureDetectionThreadCondVar.notify_all();
    }

    _sleepFor(txn, waitTimeout);
}

void ShardServerManager::_sleepFor(OperationContext* txn, Milliseconds waitTimeout) {
    stdx::unique_lock<stdx::mutex> lock(_failureDetectionThreadStateMutex);
    _failureDetectionThreadCondVar.wait_for(lock, waitTimeout.toSystemDuration(), 
        [&] { return _failureDetectionThreadState != kRunning; });
}

void ShardServerManager::_printShardServerNodeView() {
    unordered_map<string, list<ShardInfo> >::iterator itMap;
    list<ShardInfo>::iterator itList;
    for (itMap = _shardServerNodeMap.begin(); itMap != _shardServerNodeMap.end(); ++itMap) {
        log() << itMap->first << " { ";
        for (itList = itMap->second.begin(); itList != itMap->second.end(); ++itList) {
            log() << itList->shard.toString() << " ";
        }
        log() << "} ";
    }
}

StatusWith<HostAndPort> ShardServerManager::_validateSingleConnectionString(
    const ConnectionString& conn) {
    if (conn.type() == ConnectionString::INVALID) {
        return Status(ErrorCodes::BadValue, "Invalid connection string");
    }

    vector<HostAndPort> hosts = conn.getServers();
    if (hosts.size() == 0) {
        return Status(ErrorCodes::BadValue, "empty connection");
    }

    if (hosts.size() > 1) {
        return Status(ErrorCodes::BadValue, "more than one connections for a shard server");
    }

    if (hosts[0].empty() || (hosts[0].port() == -1) || hosts[0].host().empty()) {
        return Status(ErrorCodes::BadValue, "empty host or port");
    }

    return hosts[0];
}

Status ShardServerManager::determineShardState(OperationContext* txn,
                           const ConnectionString& shardServerConn, //lint !e839
                           const std::string& processIdentity,
                           bool& retryRequest,
                           ShardType::ShardState& currentState,
                           ShardType& existingShard) {
    HostAndPort hostAndPort = shardServerConn.getServers()[0];
    LOG(2) << "[determineShardState]: " << hostAndPort.toString();

    // Get shard doc from configDB
    auto findShardStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            kConfigReadSelector,
            repl::ReadConcernLevel::kLocalReadConcern,
            NamespaceString(ShardType::ConfigNS),
            BSON(ShardType::host() << BSON("$regex" << hostAndPort.toString())),
            BSONObj(),
            boost::none); // no limit

    if (!findShardStatus.isOK()) {
        return findShardStatus.getStatus();
    }

    const auto shardDocs = findShardStatus.getValue().docs;

    LOG(2) << "[determineShardState]: " << hostAndPort.toString() << ", document num: " << shardDocs.size();

    if (shardDocs.size() == 0) {
        retryRequest = false;
        currentState = ShardType::ShardState::kShardRegistering;
        
        return Status::OK();
    }
    
    if (shardDocs.size() > 1) {
        return Status(ErrorCodes::TooManyMatchingDocuments,
                      str::stream() << "more than one shard document found for host "
                                    << hostAndPort.toString() << " in config databases");
    }

    auto shardDocStatus = ShardType::fromBSON(shardDocs.front());
    if (!shardDocStatus.isOK()) {
        return shardDocStatus.getStatus();
    }

    existingShard = shardDocStatus.getValue();

    if (processIdentity.compare(existingShard.getProcessIdentity()) == 0) {
        if ((existingShard.getState() == ShardType::ShardState::kShardRegistering) ||
            (existingShard.getState() == ShardType::ShardState::kShardRestarting)) {
            // retry register due to last response missed
            retryRequest = true;
            currentState = existingShard.getState();
        } 
        else {
            return Status(ErrorCodes::IllegalOperation,
                stream() << "not allowed to re-register a shard with process identity: " 
                         << existingShard.getProcessIdentity()
                         << ", and state: " << (int)existingShard.getState());
        }
    } 
    else {
        // re-launch a shard due to the old one failed
        retryRequest = false;
        currentState = ShardType::ShardState::kShardRestarting;
    }

    return Status::OK();
}

Status ShardServerManager::processFailedPreviousShardServer(const HostAndPort& hostAndPort) {
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    bool foundFlag = false;
    std::string hostAndPortStr = hostAndPort.toString();
    ShardType failShard;
    {
        stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        unordered_map<string, list<ShardInfo> >::iterator itMap = 
            _shardServerNodeMap.find(hostAndPort.host());

        if (itMap == _shardServerNodeMap.end()) {
            return Status(ErrorCodes::ShardServerNotFound, "no matching shard server");    
        }
        for (list<ShardInfo>::iterator itList = itMap->second.begin(); 
        itList != itMap->second.end(); ++itList) {
            if (itList->shard.getHost().find(hostAndPortStr) != std::string::npos) { //lint !e1015
                foundFlag = true;

                getGlobalServiceContext()->registerProcessStageTime(
                    "takeoverShard:"+itList->shard.getName());
                getGlobalServiceContext()->getProcessStageTime(
                    "takeoverShard:"+itList->shard.getName())->noteStageStart(
                    "shardServerRestart");
                //FailedShardType failedShardType;
                //failedShardType.shard = itList->shard;
                //failedShardType.isProcessing = false;

                //stdx::lock_guard<stdx::mutex> failedShardServerLock(_failedShardServerListMutex);
                // Move the shard server to the failed shard server set
                //_failedShardServerList.push_front(failedShardType);

                // handle failShard before erase it
                failShard = itList->shard;
                (void)preProcessFailedChunks(txn, failShard);                
                itMap->second.erase(itList);
                break;
            }

            if (itMap->second.size() == 0) {
                _shardServerNodeMap.erase(itMap);
            }
        } 
    }

    if (!foundFlag) {
        return Status(ErrorCodes::ShardServerNotFound, "no matching shard server");
    }

   // _failureHandlingThreadPool->schedule(ShardServerManager::handleFailedShardServer);

    return Status::OK();
}
StatusWith<BSONObj> ShardServerManager::addShardDocument(OperationContext* txn, 
                                                         const ConnectionString& conn, //lint !e839
                                                         const std::string& extendIPs,
                                                         const std::string& processIdentity) {
    {
        stdx::lock_guard<stdx::mutex> initLock(_initMutex);
        if (!_hasInited) {
            return Status(ErrorCodes::NotYetInitialized, "not yet initialized to primary");
        }
    }

    auto conn_status = _validateSingleConnectionString(conn);
    if (!conn_status.isOK()) {
        return conn_status.getStatus();
    }
    HostAndPort hostAndPort = conn_status.getValue();
    string shardServerNodeIP = hostAndPort.host();

    bool retryRequest;
    ShardType::ShardState state;
    ShardType shard;

    auto determineShardStateStatus =
        determineShardState(txn, conn, processIdentity, retryRequest, state, shard);
    if (!determineShardStateStatus.isOK()) {
        return determineShardStateStatus;
    }
    
    if (retryRequest) {
        BSONObjBuilder builder;
        builder.append("shardName", shard.getName());
        return builder.obj();
    }

    // take over former failed shard in this host:port
    if (state == ShardType::ShardState::kShardRestarting) {
        Status processStatus = processFailedPreviousShardServer(hostAndPort);
        if (processStatus.isOK()) {
            log() << "The failed previous shard server (" << shard 
                << ") is processed in advance during restarting";
        }
        else {
            log() << "The failed previous shard server (" << shard
                << ") has been processed by failure detector";
            return Status(ErrorCodes::IllegalOperation,
                          stream() << "re-start request will be handled after former failover finished");            
        }
    }    

    shard.setState(state);
    shard.setProcessIdentity(processIdentity);
    shard.setExtendIPs(extendIPs);
    
    // Insert the shard into configDB
    auto insOrUpdDocumentResult =
        Grid::get(txn)->catalogManager()->insertOrUpdateShardDocument(txn, shard, conn);

    if (!insOrUpdDocumentResult.isOK()) {
        return insOrUpdDocumentResult.getStatus();
    }

    ShardInfo shardInfo;
    shardInfo.shard = shard;
    shardInfo.heartbeatTime = repl::getGlobalReplicationCoordinator()->getExecutor()->now() +
        kDefaultRegisterReservedTime;

    // Insert the shard into in-memory shard server map 
    {
        stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        _shardServerNodeMap[shardServerNodeIP].push_front(std::move(shardInfo));
    }

    LOG(2) << "Finish addShardDocument for shard server " << conn;

    BSONObjBuilder builder;
    builder.append("shardName", shard.getName());
    return builder.obj();
}

Status ShardServerManager::setLastHeartbeatTime(
    OperationContext* txn, const HostAndPort& hostAndPort, const Date_t& time) {

    if (hostAndPort.empty() || (hostAndPort.port() == -1) ||
        hostAndPort.host().empty()) {
        return Status(ErrorCodes::BadValue, "empty host or port");
    }

    {
        stdx::lock_guard<stdx::mutex> initLock(_initMutex);
        if (!_hasInited) {
            return Status(ErrorCodes::NotYetInitialized, "not yet initialized to primary");
        }
    }

    string host = hostAndPort.toString();
    string shardServerNodeIP = hostAndPort.host();

    {
        stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        unordered_map<string, list<ShardInfo> >::iterator itMap =
            _shardServerNodeMap.find(shardServerNodeIP);

        if (itMap == _shardServerNodeMap.end()) {
            return Status(ErrorCodes::ShardServerNotFound, "no matching shard server");
        }
        for (list<ShardInfo>::iterator itList = itMap->second.begin();
        itList != itMap->second.end(); ++itList) {
            if (itList->shard.getHost().find(host) != std::string::npos) {
                itList->heartbeatTime = time;
                return Status::OK();
            }
        }
    }

    return Status(ErrorCodes::ShardServerNotFound, "no matching shard server");
}


StatusWith<Date_t> ShardServerManager::getLastHeartbeatTime(
    OperationContext* txn, const HostAndPort& hostAndPort) {
    {
        stdx::lock_guard<stdx::mutex> initLock(_initMutex);
        if (!_hasInited) {
            return Status(ErrorCodes::NotYetInitialized, "not yet initialized to primary");
        }
    }

    if (hostAndPort.empty() || (hostAndPort.port() == -1) || hostAndPort.host().empty()) {
        return Status(ErrorCodes::BadValue, "empty host or port");
    }

    string host = hostAndPort.toString();
    string shardServerNodeIP = hostAndPort.host();

    {
        stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        unordered_map<string, list<ShardInfo> >::iterator itMap =
            _shardServerNodeMap.find(shardServerNodeIP);
    
        if (itMap == _shardServerNodeMap.end()) {
            return Status(ErrorCodes::ShardServerNotFound, "no matching shard server");
        }

        for (list<ShardInfo>::iterator itList = itMap->second.begin();
        itList != itMap->second.end(); ++itList) {
            if (itList->shard.getHost().find(host) != std::string::npos) {
                return itList->heartbeatTime;
            }
        }
    }

    return Status(ErrorCodes::ShardServerNotFound, "no matching shard server");
}

StatusWith<ShardType> ShardServerManager::getPrimaryShard(OperationContext* txn){
    stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
    
    if (_shardServerNodeMap.size() == 0 ) {
       return Status(ErrorCodes::ShardServerNotFound, "no shard available in cluster");
    } 
    
    Date_t expireTime = repl::getGlobalReplicationCoordinator()->getExecutor()->now()
                        - kDefaultShardHeartbeatTimeoutPeriod; 
        
    unordered_map<string, list<ShardInfo> >::iterator itMap;
    list<ShardInfo>::iterator itList;
    ShardType shard;
    unsigned int minChunkNum = INT_MAX;
    bool isShardFound = false;
    for (itMap = _shardServerNodeMap.begin(); itMap != _shardServerNodeMap.end(); ++ itMap){
        for (itList = itMap->second.begin(); itList != itMap->second.end(); ++itList)
        {
            if ((int)itList->shard.getState() == (int)ShardType::ShardState::kShardActive
               && itList->heartbeatTime > expireTime)
            {
    
               //if the shard is already added , pick the shard which has least chunks
                  
                 auto findChunksStatus = 
                         Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                         txn,
                         kConfigReadSelector,
                         repl::ReadConcernLevel::kLocalReadConcern,
                         NamespaceString(ChunkType::ConfigNS),
                         BSON(ChunkType::shard(itList->shard.getName())), 
                         BSONObj(),  // no sort
                         boost::none);
                 if (findChunksStatus.isOK()){
                     auto chunkDocs = findChunksStatus.getValue().docs;
                     if (chunkDocs.size() < minChunkNum){
                        minChunkNum = chunkDocs.size();
                        shard = itList->shard;
                        isShardFound = true;
                     }
                 }
            }
    
        }
    }
    if (isShardFound){
       return shard;
    }
    return Status(ErrorCodes::ShardServerNotFound, "no shard available in cluster");
        
}

Status ShardServerManager::setDatabasePrimary(OperationContext* txn,std::string& dbname,ShardId& primaryShard){
    auto status = grid.catalogClient(txn)->getDatabase(txn, dbname);
    if (!status.isOK()) {
       log() << "fail to set primary for sharded-only database cause by " << status.getStatus(); 
       return Status(ErrorCodes::NotMaster, "get database failed");
    }

    const auto dbOpTimePair = status.getValue();
    shared_ptr<DBConfig> config =
    std::make_shared<DBConfig>(dbname, dbOpTimePair.value, dbOpTimePair.opTime);
    try {
        config->load(txn);
    } catch (const DBException& excep) {
        LOG(0) << "fail to load txn cause by " << excep.toStatus();
        if (excep.toStatus() != ErrorCodes::ShardNotFound){
           return Status(ErrorCodes::NotMaster, "get database failed");
        }       
    }
                      
    config->setPrimary(txn, primaryShard);
    try {
        config->reload(txn);
    } catch (const DBException& excep) {
        LOG(0) << "fail to reload txn cause by " << excep.toStatus();
        if (excep.toStatus() != ErrorCodes::ShardNotFound){
           return Status(ErrorCodes::NotMaster, "get database failed");
        }       
    }
    grid.catalogCache()->invalidate(dbname);
    return Status::OK(); 
}


StatusWith<std::vector<std::string>> ShardServerManager::getDbsOnFailedShard(OperationContext* txn,const std::string& failShardName){
    ShardId shardId(failShardName);
    const auto findDBStatus =
                Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                txn,
                kConfigPrimarySelector,
                repl::ReadConcernLevel::kLocalReadConcern,
                NamespaceString(DatabaseType::ConfigNS),
                BSON(DatabaseType::primary() <<  shardId),
                BSONObj(),
                boost::none);
    if (!findDBStatus.isOK()){
        return findDBStatus.getStatus();
    }
    std::vector<std::string> databases;
    const auto dbDocs = findDBStatus.getValue().docs;
    for (BSONObj dbDoc : dbDocs){
        auto dbParseStatus = DatabaseType::fromBSON(dbDoc);
        if (!dbParseStatus.isOK()){
            return dbParseStatus.getStatus();
        }
        DatabaseType dbType = dbParseStatus.getValue();
        databases.push_back(dbType.getName());
    }
    return databases;
}



void ShardServerManager::takeOverNonShardedChunk() {
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }
    std::string dbname;
    std::list<ChunkType> chunkList;
    std::list<ChunkType> chunksSuccess;
    std::list<ChunkType> chunksFailed;
    
    ChunkType chunk;
    LOG(0) << "begin to get one failed non sharded chunk";
    Status getFailedNonShardedChunksStatus =
        grid.catalogManager()->getShardServerManager()->getFailedNonShardedChunks(dbname,chunkList);
    if (!getFailedNonShardedChunksStatus.isOK()) {
        log() << "Failed to get failed non sharded chunk due to "
              << getFailedNonShardedChunksStatus.reason();
        return;
    }
    LOG(2) << "begin to take over non-shard chunks for database : " << dbname;
    ShardId targetShardId;
    std::list<ChunkType>::iterator it = chunkList.begin();
    while (it != chunkList.end()) {
        if (it->getStatus() == ChunkType::ChunkStatus::kAssigned) {
           targetShardId = it->getShard(); 
           chunksSuccess.push_back(*it);
        } else{
            chunksFailed.push_back(*it);       
        }
        it ++;     
    }

    while (true && chunksSuccess.empty()){        
        auto getTakeOverShardStatus =
        grid.catalogManager()->getShardServerManager()->getPrimaryShard(txn);
        if (getTakeOverShardStatus.isOK()) {
            ShardType takeOverShard = getTakeOverShardStatus.getValue();
            targetShardId = ShardId(takeOverShard.getName());
            LOG(2) << "get take over shard " << targetShardId.toString();
            break;
         }
        std::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSystemDuration());
    }

    LOG(0) << "finish get primary shard : " << targetShardId.toString() << " for db " << dbname;
    for (std::list<ChunkType>::iterator itList = chunksFailed.begin(); itList != chunksFailed.end(); ++itList) {
        Status validateStatus = itList->validate();
        if (!validateStatus.isOK()) {
            // TODO: mark unsolved chunk, need to be handled
            log() << "Failed to validate chunk document " << chunk
                << " due to " << validateStatus.reason();
            return;
        }
        // re-assign chunks 
        auto assignStatus = Balancer::get(txn)->assignChunk(txn,
                                                            *itList,
                                                            false,
                                                            true,
                                                            targetShardId);
        //if the chunk doesn't assign success,put db and chunk list into _nonShardedMap        
        if (!assignStatus.isOK()) {
           // break, re-try re-assign chunk
            log() << "Fail to re-assign chunk: " << itList->getName();
            auto updateChunkStatus = grid.catalogClient(txn)->updateConfigDocument(txn,
                                            ChunkType::ConfigNS,
                                            BSON(ChunkType::name(itList->getID())),
                                            itList->toBSON(),
                                            true,
                                            ShardingCatalogClient::kMajorityWriteConcern);
            if (updateChunkStatus.isOK()){
                LOG(1) << "finish to update chunk status in take over non shard";
            }  

            Grid::get(txn)->catalogManager()->getShardServerManager()->rollBackAssign(txn,chunksSuccess,targetShardId);
            Grid::get(txn)->catalogManager()->getShardServerManager()->processFailedDatabase(dbname,chunkList);
            return;
        } 

        LOG(1) << "finish assign non shard chunk : " << itList->toString();
        chunksSuccess.push_back(*itList);                
    }
    
    auto status = grid.catalogManager()->getShardServerManager()->setDatabasePrimary(txn,dbname,targetShardId);
    if (!status.isOK()) {
       log() << "fail to set primary cause by " << status;
       Grid::get(txn)->catalogManager()->getShardServerManager()->rollBackAssign(txn,chunksSuccess,targetShardId);
       Grid::get(txn)->catalogManager()->getShardServerManager()->processFailedDatabase(dbname,chunkList);
       return;
    }
    LOG(0) << "finish set primary to " << targetShardId.toString() << " for " << dbname; 
    Grid::get(txn)->catalogManager()->getShardServerManager()->removeFromIsProcessMap(dbname);
    return;
}

Status ShardServerManager::getFailedNonShardedChunks(std::string& dbname,std::list<ChunkType>& chunkList) {
    g_non_shard_lock.lock();
    if (_nonShardedChunksMap.empty()) {
       LOG(0) << "_nonShardedChunksMap.empty()";
       g_non_shard_lock.unlock();
       return Status(ErrorCodes::ChunkNotFound, "No failed non sharded chunks needs to handle");
    }
    unordered_map<string, std::list<ChunkType>>::iterator itMap = _nonShardedChunksMap.begin();
    dbname = itMap->first;
    chunkList = itMap->second;
    _nonShardedChunksMap.erase(itMap);
    g_non_shard_lock.unlock();
    LOG(1) << "finish find database " << dbname << " with " << chunkList.size() << " chunks !";
    return Status::OK();
}

Status ShardServerManager::_loadFailedDataBases(OperationContext* txn){
    stdx::lock_guard<stdx::mutex> mapLock(_nonShardedChunksMutex);
    _nonShardedChunksMap.clear();  
    //check shards.databases primary
    LOG(1) << "begin to load failed databases !";
    const auto findDBStatus =
                Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                txn,
                kConfigPrimarySelector,
                repl::ReadConcernLevel::kLocalReadConcern,
                NamespaceString(DatabaseType::ConfigNS),
                BSONObj(),
                BSONObj(),
                boost::none);
    if (!findDBStatus.isOK()) {
       return findDBStatus.getStatus(); 
    }
    const auto dbDocs = findDBStatus.getValue().docs;
    for (BSONObj dbDoc : dbDocs){
        auto dbParseStatus = DatabaseType::fromBSON(dbDoc);
        if (!dbParseStatus.isOK()){
            return dbParseStatus.getStatus();
        }
        DatabaseType dbType = dbParseStatus.getValue();
        std::string dbname = dbType.getName();
        //check if the database has been processed,maybe it has been processed in handleFailedShard
        bool isProcess = false;
        getIsProcessMap(dbname,isProcess);
        if (isProcess == true){
            continue;
        }
        ShardIdent shardIdent;
        ShardId shardId = dbType.getPrimary();
        LOG(2) << "shardId : " << shardId.toString();
        shardIdent.setShardId(shardId);
        std::list<ChunkType> chunkList;
        auto status = getShardTypeByShardId(shardId);
        //LOG(0) << "isShard " << shardId.toString() << "alive : " << isShardServerAlive(shardIdent);
        if (!status.isOK() && _nonShardedChunksMap.find(dbType.getName()) == _nonShardedChunksMap.end()){
           getAllNonShardChunks(txn,dbname,chunkList);
           LOG(2) << "put database " << dbname << "with " << chunkList.size() << " chunks into _nonShardedChunksMap";
           _nonShardedChunksMap[dbname] = chunkList;
        }
    }

    for (stdx::unordered_map<std::string, std::list<ChunkType>>::iterator it = _nonShardedChunksMap.begin();
        it != _nonShardedChunksMap.end(); ++ it){
        bool isProcess = false;
        std::string db = it->first;
        getIsProcessMap(db,isProcess);
        if (isProcess){
           continue;
        }
        setIsProcessMap(db,true);
        _takeOverNonShardThreadPool->schedule(ShardServerManager::takeOverNonShardedChunk);
    }     

    return Status::OK();
}

void ShardServerManager::getAllNonShardChunks(OperationContext* txn,std::string dbname,std::list<ChunkType>& chunkList) {
    BSONObjBuilder collQuery;
    std::string ns = "^";
    ns += dbname;
    ns += ".";
    collQuery.appendRegex(CollectionType::fullNs(), ns);
    collQuery.append(CollectionType::tabType(),static_cast<std::underlying_type<CollectionType::TableType>::type>(
                                CollectionType::TableType::kNonShard));
    auto findStatus = Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                txn,
                kConfigPrimarySelector,
                repl::ReadConcernLevel::kLocalReadConcern,
                NamespaceString(CollectionType::ConfigNS),
                collQuery.obj(),
                BSONObj(),
                boost::none);
    if (!findStatus.isOK()){
       log() << "fail to get nonShard collection";
       return;
    }
    const auto collDocs = findStatus.getValue().docs;
    for (BSONObj collDoc : collDocs) {        
        BSONObjBuilder chunkFilter;
        chunkFilter.append(ChunkType::rootFolder(), BSON("$ne" << "stalerootfolder"));
        chunkFilter.append(ChunkType::ns(),collDoc["_id"].String());

        auto findChunkStatus = Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                               txn,
                               kConfigPrimarySelector,
                               repl::ReadConcernLevel::kLocalReadConcern,
                               NamespaceString(ChunkType::ConfigNS),
                               chunkFilter.obj(),
                               BSONObj(),
                               boost::none);           
        const auto chunkDocs = findChunkStatus.getValue().docs;
        auto chunkParseStatus = ChunkType::fromBSON(chunkDocs.front());
        ChunkType chunk = chunkParseStatus.getValue();
        chunkList.push_back(chunk);
    }    
}


void ShardServerManager::setIsProcessMap(std::string dbname,bool isProcess){
    stdx::lock_guard<stdx::mutex> processLock(isProcessMapMutex);
    isProcessMap[dbname] = isProcess;
}

void ShardServerManager::getIsProcessMap(std::string dbname,bool& isProcess){
    stdx::lock_guard<stdx::mutex> processLock(isProcessMapMutex);
    if (isProcessMap.find(dbname) != isProcessMap.end()){
       isProcess = isProcessMap[dbname];
    }else{
        isProcess = false;
        isProcessMap[dbname] = isProcess; 
    }
}

void ShardServerManager::removeFromIsProcessMap(std::string& dbname){
   stdx::lock_guard<stdx::mutex> processLock(isProcessMapMutex);
   unordered_map<string, bool>::iterator it = isProcessMap.find(dbname);
   if (it != isProcessMap.end()){
        isProcessMap.erase(it);
   }
}


void ShardServerManager::addNonShardMap(std::string& dbname,std::list<ChunkType>& chunkList){
    stdx::lock_guard<stdx::mutex> mapLock(_nonShardedChunksMutex);
    _nonShardedChunksMap[dbname] = chunkList;
}

void ShardServerManager::rollBackAssign(OperationContext* txn,std::list<ChunkType>& chunkList,ShardId& targetShard){
    
    Status getShardStatus = Status(ErrorCodes::InternalError, "Internal error running command");
    HostAndPort host;
    bool isShardAlive = true;    
    LOG(0) << "begin to roll back assign ";
    if (chunkList.size() == 0){
        LOG(2) << "no chunks to roll back.";
        return;
    }
    while (!getShardStatus.isOK()){
        auto ShardStatus = grid.shardRegistry()->getShard(txn, targetShard.toString());
        if (ShardStatus.getStatus().isOK()){
            const auto shard = ShardStatus.getValue();
            const auto hostCS = shard->getConnString();
            invariant(hostCS.getServers().size() == 1);
            host = hostCS.getServers().front();     
            break;
        }
        if (ShardStatus.getStatus() == ErrorCodes::ShardNotFound){
            LOG(2) << "[rollBackAssign] shard " << targetShard.toString() << " not found";
            isShardAlive = false;
            break;
        }
        log()<<"[offloadChunk] fail to get shard cause by " << ShardStatus.getStatus();;
    }

    for (ChunkType chunkType : chunkList){
        Status commandStatus = Status(ErrorCodes::InternalError, "Internal error running command");
        while (!commandStatus.isOK() && isShardAlive){ 
            BSONObjBuilder builder;
            builder.append("offloadChunk",chunkType.getNS());
            builder.append("chunk", chunkType.toBSON());
            executor::TaskExecutor* const executor = Grid::get(txn)->getExecutorPool()->getFixedExecutor();
            const RemoteCommandRequest request(host,
                                       "admin",
                                       builder.obj(),
                                       txn);
            RemoteCommandResponse offResponse =
                Status(ErrorCodes::InternalError, "Internal error running command");

            auto callStatus = executor->scheduleRemoteCommand(
                request,
                [&offResponse](const RemoteCommandCallbackArgs& args) { offResponse = args.response; });
  
            auto findStatus = grid.catalogManager()->findShardByHost(txn,ShardType::ConfigNS,host.toString());
            if (findStatus.isOK()){
                if(findStatus.getValue().getState() == ShardType::ShardState::kShardFault){
                    isShardAlive = false;
                    break;
                }
            }
            if (!callStatus.isOK()) {
                //if retry times > maxRetryTime,break
                continue;
            }

            executor->wait(callStatus.getValue());
            if (!offResponse.status.isOK()) {
                if (offResponse.status.code() == ErrorCodes::ExceededTimeLimit) {
                    LOG(1) << "Operation timed out with status " << redact(offResponse.status);
                }
                continue;
            }
            commandStatus = getStatusFromCommandResult(offResponse.data);     
        }
        
        auto updateChunkStatus = grid.catalogClient(txn)->updateConfigDocument(txn,
                                            ChunkType::ConfigNS,
                                            BSON(ChunkType::name(chunkType.getID())),
                                            chunkType.toBSON(),
                                            true,
                                            ShardingCatalogClient::kMajorityWriteConcern);
        if (updateChunkStatus.isOK()){
            LOG(2) << "finish to update chunk status to " << chunkType.toString();
        }  

    }
    LOG(0) << "finish to roll back";
     
}


void ShardServerManager::processFailedDatabase(std::string& dbname,std::list<ChunkType>& chunkList){
    addNonShardMap(dbname,chunkList);
    _takeOverNonShardThreadPool->schedule(ShardServerManager::takeOverNonShardedChunk);
}


} // namesapce mongo

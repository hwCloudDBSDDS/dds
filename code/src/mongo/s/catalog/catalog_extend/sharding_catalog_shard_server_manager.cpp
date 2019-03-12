#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/s/catalog/catalog_extend/sharding_catalog_shard_server_manager.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/connection_string.h"
#include "mongo/client/replica_set_monitor.h"
#include "mongo/db/audit.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/replica_set_config.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/repl/replication_executor.h"
#include "mongo/db/s/balancer/balancer.h"
#include "mongo/db/server_options.h"
#include "mongo/executor/task_executor.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/executor/thread_pool_task_executor.h"
#include "mongo/platform/basic.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_locks.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/client/shard_connection.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"
#include "mongo/util/util_extend/GlobalConfig.h"
#include "mongo/util/util_extend/default_parameters.h"

#include <climits>
#include <iomanip>
#include <limits.h>
#include <time.h>
// add
#include "mongo/db/namespace_string.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/s/config.h"
#include "mongo/s/offload_chunk_request.h"
#include "mongo/util/scopeguard.h"


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


typedef std::map<string, int> FailedChunksMap;
std::mutex g_chun_alarm_mutex;


namespace {

const ReadPreferenceSetting kConfigReadSelector(ReadPreference::PrimaryOnly, TagSet{});
const ReadPreferenceSetting kConfigPrimarySelector(ReadPreference::PrimaryOnly);


const Milliseconds kDefaultTakeOverShardRetryInterval(500);
const Milliseconds kDefaultGetSourceShardRetryInterval(300);
const Milliseconds kDefaultCPUSleepTime(10);

const uint64_t kShardNameDigitLimt(UINT64_MAX);
const int kShardNameDigitWidth(4);

const long long kMaxSizeMBDefault = 0;

}  // namespace

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
    delete (_failureHandlingThreadPool);
    _failureHandlingThreadPool = nullptr;

    _failedChunksThreadPool->shutdown();
    _failedChunksThreadPool->join();
    delete (_failedChunksThreadPool);
    _failedChunksThreadPool = nullptr;

    _takeOverNonShardThreadPool->shutdown();
    _takeOverNonShardThreadPool->join();
    delete (_takeOverNonShardThreadPool);
    _takeOverNonShardThreadPool = nullptr;

    delete (_pSecureRandom);
    _pSecureRandom = nullptr;
}

void ShardServerManager::onTransitionToPrimary(OperationContext* txn) {
    stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
    if (_failureDetectionThreadState == kShutting) {
        return;
    }
    invariant(_failureDetectionThreadState == kStopped);
    _failureDetectionThreadState = kRunning;

    invariant(!_failureDetectionThread.joinable());
    invariant(!_threadOperationContext);
    _failureDetectionThread = stdx::thread([this] { _failureDetectionMainThread(); });
    index_log() << "[ShardServerManager] onTransitionToPrimary complete";
}

void ShardServerManager::onStepDownFromPrimary() {
    stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
    if (_failureDetectionThreadState != kRunning)
        return;

    _failureDetectionThreadState = kStopping;

    // Interrupt the failureDetectionThread if it has been started. We are guaranteed that the
    // operation
    // context of that thread is still alive, because we hold the failureDetectionThread mutex.
    if (_threadOperationContext) {
        stdx::lock_guard<Client> scopedClientLock(*_threadOperationContext->getClient());
        _threadOperationContext->markKilled(ErrorCodes::InterruptedDueToReplStateChange);
    }

    _failureDetectionThreadCondVar.notify_all();
    index_log() << "[ShardServerManager] onStepDownFromPrimary complete";
}

void ShardServerManager::onDrainComplete(OperationContext* txn) {
    invariant(!txn->lockState()->isLocked());

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
        if (_failureDetectionThreadState == kStopped || _failureDetectionThreadState == kShutting)
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

    {
        stdx::lock_guard<stdx::mutex> dataLock(_failedChunksMutex);
        _failedChunks.clear();
    }

    {
        stdx::lock_guard<stdx::mutex> mapLock(_nonShardedChunksMutex);
        _nonShardedChunksMap.clear();
    }

    index_log() << "[ShardServerManager] onDrainComplete complete";
}

bool ShardServerManager::hasInitialized() {
    stdx::lock_guard<stdx::mutex> initLock(_initMutex);
    if (!_hasInited) {
        return false;
    }
    return true;
}

bool ShardServerManager::hasShardLoaded() {
    stdx::lock_guard<stdx::mutex> shardLoadLock(_shardLoadMutex);
    return _hasShardLoaded;
}

Status ShardServerManager::_init(OperationContext* txn) {
    vector<string> shardNames;
    vector<string> shardProcessIds;

    auto status = _loadEnableFlag(txn);
    if (!status.isOK()) {
        index_err() << "[ShardServerManager] Failed to load enable flag " << causedBy(status);
        return Status(status.code(), stream() << "Failed to load enable flag" << causedBy(status));
    }

    status = _loadShardServerView(txn, shardNames, shardProcessIds);
    if (!status.isOK()) {
        index_err() << "[ShardServerManager] Failed to load shard server view " << causedBy(status);
        return Status(status.code(),
                      stream() << "Failed to load shard server view" << causedBy(status));
    }

    status = _loadFailedDataBases(txn);
    if (!status.isOK()) {
        index_err() << "[ShardServerManager] Failed to load databases which primary is failed "
                    << causedBy(status);
        return Status(status.code(),
                      stream() << "Failed to load databases which primary is failed"
                               << causedBy(status));
    }

    // wait statemachine to finish recovering rebalance events
    Grid::get(txn)->catalogManager()->getBalanceEventEngine()->waitForRecovery();

    status = _loadFailedChunks(txn, shardNames, shardProcessIds);
    if (!status.isOK()) {
        index_err() << "[ShardServerManager] Failed to load failed chunks " << causedBy(status);
        return Status(status.code(),
                      stream() << "Failed to load failed chunks" << causedBy(status));
    }

    {
        stdx::lock_guard<stdx::mutex> initLock(_initMutex);
        _hasInited = true;
    }

    return Status::OK();
}


Status ShardServerManager::_loadShardServerView(OperationContext* txn,
                                                vector<string>& shardNames,
                                                vector<string>& shardProcessIds) {
    index_log() << "[ShardServerManager] load shardservers begin";

    auto findStatus = Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
        txn,
        kConfigReadSelector,
        repl::ReadConcernLevel::kMajorityReadConcern,
        NamespaceString(ShardType::ConfigNS),
        BSONObj(),  // no query filter
        BSONObj(),  // no sort
        boost::none);

    if (!findStatus.isOK()) {
        index_err() << "[ShardServerManager] Failed to read " << ShardType::ConfigNS
                    << " collection documents" << causedBy(findStatus.getStatus());
        return Status(findStatus.getStatus().code(),
                      stream() << "Failed to read " << ShardType::ConfigNS
                               << " collection documents"
                               << causedBy(findStatus.getStatus()));
    }

    auto shards = std::move(findStatus.getValue().docs);
    index_log() << "[ShardServerManager] found " << shards.size() << " shardservers";

    // _corrShardDocHasSend = false;
    //_corrChunkDocHasSend = false;

    //_shardClusterDegradeCount = 0;
    //_shardClusterDegradeHasSend = true;
    //_clusterName = repl::getGlobalReplicationCoordinator()->getConfig().getClusterName();

    auto repl_coordinator = repl::getGlobalReplicationCoordinator();
    if (repl_coordinator == nullptr) {
        index_err() << "[ShardServerManager] getGlobalReplicationCoordinator failed.";
        return Status(ErrorCodes::InternalError, "getGlobalReplicationCoordinator failed.");
    }

    auto executor = repl_coordinator->getExecutor();
    if (executor == nullptr) {
        index_err() << "[ShardServerManager] getExecutor failed.";
        return Status(ErrorCodes::InternalError, "fail to get executor");
    }

    {
        stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        stdx::lock_guard<stdx::mutex> failedShardServerLock(_failedShardServerListMutex);
        stdx::lock_guard<stdx::mutex> shardNameLock(_shardNameMutex);
        stdx::lock_guard<stdx::mutex> shardLoadLock(_shardLoadMutex);

        _shardServerNodeMap.clear();
        _failedShardServerList.clear();
        _maxShardNameDigit = 0;
        _maxShardServerNum = 0;
        _hasShardLoaded = false;

        for (const BSONObj& doc : shards) {
            auto shardRes = ShardType::fromBSON(doc);
            if (!shardRes.isOK()) {
                index_err() << "[ShardServerManager] from bson " << doc.toString()
                            << " failed due to " << shardRes.getStatus();
                // sendCorrClstMgmtIndexAlarm(ShardType::ConfigNS, _corrShardDocHasSend);
                continue;
            }

            ShardType shardType = shardRes.getValue();

            auto conn_status = _validateSingleConnectionString(
                ConnectionString(shardType.getHost(), ConnectionString::ConnectionType::MASTER));
            if (!conn_status.isOK()) {
                index_err() << "[ShardServerManager] fail to validate connString for shard "
                            << shardType << " due to " << conn_status.getStatus();
                return Status(ErrorCodes::BadValue, "invalid shard host");
            }

            // add to failed list and start fault handling immediately
            if (shardType.getState() == ShardType::ShardState::kShardFault) {
                BSONObjBuilder filterBuilder;
                filterBuilder.append(ShardType::name(), shardType.getName());
                filterBuilder.append(ShardType::processIdentity(), shardType.getProcessIdentity());

                Status removeShardStatus =
                    Grid::get(txn)->catalogClient(txn)->removeConfigDocuments(
                        txn,
                        ShardType::ConfigNS,
                        filterBuilder.obj(),
                        ShardingCatalogClient::kMajorityWriteConcern);
                if (removeShardStatus.isOK()) {
                    index_log() << "[ShardServerManager] Finish remove shard : "
                                << shardType.getHost();
                    continue;
                }
                index_warning() << "[ShardServerManager] fail to remove shard : "
                                << shardType.getHost();
                return removeShardStatus;
                /*FailedShardType failedShardType;
                failedShardType.shard = shardType;
                failedShardType.isProcessing = false;

                _failedShardServerList.push_front(failedShardType);
                _failureHandlingThreadPool->schedule(ShardServerManager::handleFailedShardServer);

                continue;*/
            }
            // save shardNames and shardProcessIds of shards alive
            shardNames.push_back(shardType.getName());
            shardProcessIds.push_back(shardType.getProcessIdentity());
            string shardServerNodeIP = conn_status.getValue().host();

            ShardInfo shardInfo;
            shardInfo.shard = shardType;
            shardInfo.heartbeatTime = executor->now();

            _shardServerNodeMap[shardServerNodeIP].push_front(std::move(shardInfo));
        }
	_hasShardLoaded = true;
    }

    // send recover alarm
    // recoverCorrClstMgmtIndexAlarm(ShardType::ConfigNS, _corrShardDocHasSend);
    index_log() << "[ShardServerManager] load shardservers complete";

    return Status::OK();
}

Status ShardServerManager::_loadFailedChunks(OperationContext* txn,
                                             const vector<string>& shardNames,
                                             const vector<string>& shardProcessIds) {
    index_log() << "[ShardServerManager] load failed chunks begin";
    // BalanceEventEngine* balanceEventEngine =
    // Grid::get(txn)->catalogManager()->getBalanceEventEngine();

    vector<int> failedStates;
    failedStates.push_back(static_cast<int>(ChunkType::ChunkStatus::kOffloaded));
    failedStates.push_back(static_cast<int>(ChunkType::ChunkStatus::kAssigned));

    BSONObjBuilder builder;
    // builder.append(ChunkType::shard(), BSON("$nin" << shardNames));
    builder.append(ChunkType::processIdentity(), BSON("$nin" << shardProcessIds));
    builder.append(ChunkType::status(), BSON("$in" << failedStates));
    builder.append(ChunkType::rootFolder(),
                   BSON("$ne"
                        << "stalerootfolder"));

    auto findStatus = Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
        txn,
        kConfigReadSelector,
        repl::ReadConcernLevel::kMajorityReadConcern,
        NamespaceString(ChunkType::ConfigNS),
        builder.obj(),
        BSONObj(),
        boost::none);

    if (!findStatus.isOK()) {
        index_warning() << "[ShardServerManager] fail to find chunks not in current shards due to "
                        << findStatus.getStatus();
        return Status(findStatus.getStatus().code(),
                      stream() << "Failed to read " << ChunkType::ConfigNS
                               << " collection documents"
                               << causedBy(findStatus.getStatus()));
    }
    // for chunk belong shard has changed but chunk state is offloaded
    std::vector<BSONObj> failedChunks = std::move(findStatus.getValue().docs);
    BSONObjBuilder builderOffload;
    builderOffload.append(ChunkType::status(),
                          static_cast<int>(ChunkType::ChunkStatus::kOffloaded));
    builderOffload.append(ChunkType::shard(), BSON("$in" << shardNames));
    builderOffload.append(ChunkType::processIdentity(), BSON("$in" << shardProcessIds));
    builderOffload.append(ChunkType::rootFolder(),
                          BSON("$ne"
                               << "stalerootfolder"));
    auto findOffloadChunkStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            kConfigReadSelector,
            repl::ReadConcernLevel::kMajorityReadConcern,
            NamespaceString(ChunkType::ConfigNS),
            builderOffload.obj(),
            BSONObj(),
            boost::none);
    if (!findOffloadChunkStatus.isOK()) {
        index_warning() << "[ShardServerManager] fail to find offload chunks in current shards "
                           "when load failed chunks due to "
                        << findOffloadChunkStatus.getStatus();
        return Status(findOffloadChunkStatus.getStatus().code(),
                      stream() << "Failed to read " << ChunkType::ConfigNS
                               << " collection documents"
                               << causedBy(findOffloadChunkStatus.getStatus()));
    }
    std::vector<BSONObj> failedChunksInShards = std::move(findOffloadChunkStatus.getValue().docs);
    failedChunks.insert(
        failedChunks.end(), failedChunksInShards.begin(), failedChunksInShards.end());
    index_log() << "[ShardServerManager] found " << failedChunks.size() << " failed chunks";

    for (const BSONObj& doc : failedChunks) {
        auto chunkRes = ChunkType::fromBSON(doc);
        if (!chunkRes.isOK()) {
            index_err() << "[ShardServerManager] from bson " << doc.toString() << " failed due to "
                        << chunkRes.getStatus();
            // sendCorrClstMgmtIndexAlarm(ChunkType::ConfigNS, _corrChunkDocHasSend);
            continue;
        }

        // add chunk into alarm map
        // balanceEventEngine->addChunkAbnormalElementAfterElection(chunk);
        if (isBelongToSharded(txn, chunkRes.getValue())) {
            processFailedChunk(chunkRes.getValue());
        }
    }

    // recoverCorrClstMgmtIndexAlarm(ChunkType::ConfigNS, _corrChunkDocHasSend);
    index_log() << "[ShardServerManager] load failed chunks complete";

    //_addIsolatedChunksToAlarmMaps(txn);
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
    if (!hasInitialized()) {
        index_warning() << "[ShardServerManager] primary config server has not transition to "
                           "primary completely.";
        return Status{ErrorCodes::NotYetInitialized, "not yet initialized to primary"};
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
        index_err() << "[ShardServerManager] fail to get max shard name in config.shards due to "
                    << findStatus.getStatus();
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


StatusWith<uint64_t> ShardServerManager::_checkHeartbeatTimeOut(OperationContext* txn) {
    if (!hasInitialized()) {
        index_warning() << "[ShardServerManager] primary config server has not transition to "
                           "primary completely.";
        return Status{ErrorCodes::NotYetInitialized, "not yet initialized to primary"};
    }

    uint64_t newDetectedFailedShardServerNum = 0;
    uint64_t currentShardServerNum = 0;
    uint64_t runningShardServerNum = 0;
    uint64_t totalShardServerNum = 0;
    {
        stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);


        auto repl_coordinator = repl::getGlobalReplicationCoordinator();
        if (repl_coordinator == nullptr) {
            index_err() << "[ShardServerManager] getGlobalReplicationCoordinator failed.";
            return Status(ErrorCodes::InternalError, "getGlobalReplicationCoordinator failed.");
        }

        auto executor = repl_coordinator->getExecutor();
        if (executor == nullptr) {
            index_err() << "[ShardServerManager] getExecutor failed.";
            return Status(ErrorCodes::InternalError, "getExecutor failed.");
        }

        Date_t expireTime = executor->now() - kDefaultShardFailureDetectionTimeout;
        unordered_map<string, list<ShardInfo>>::iterator itMap = _shardServerNodeMap.begin();
        while (itMap != _shardServerNodeMap.end()) {
            list<ShardInfo>::iterator itList = itMap->second.begin();
            while (itList != itMap->second.end()) {
                currentShardServerNum++;
                if (itList->heartbeatTime <= expireTime) {
                    getGlobalServiceContext()->registerProcessStageTime("takeoverShard:" +
                                                                        itList->shard.getName());
                    getGlobalServiceContext()
                        ->getProcessStageTime("takeoverShard:" + itList->shard.getName())
                        ->noteStageStart("failedShardDetected");
                    FailedShardType failedShardType;
                    failedShardType.shard = itList->shard;
                    failedShardType.isProcessing = false;

                    stdx::lock_guard<stdx::mutex> failedShardServerLock(
                        _failedShardServerListMutex);
                    // Move the shard server with heartbeat timeout to the failed shard server set
                    _failedShardServerList.push_front(failedShardType);
                    itList = itMap->second.erase(itList);
                    index_LOG(0) << "[ShardServerManager] ShardNodeMap after shard "
                                 << failedShardType.shard.getName() << " time out : ";
                    _printShardServerNodeView();
                    newDetectedFailedShardServerNum++;
                } else {
                    runningShardServerNum++;
                    itList++;
                }
            }

            if (itMap->second.size() == 0) {
                itMap = _shardServerNodeMap.erase(itMap);
            } else {
                itMap++;
            }
        }

        if (currentShardServerNum > _maxShardServerNum) {
            _maxShardServerNum = currentShardServerNum;
        }
        totalShardServerNum = _maxShardServerNum;
    }

    index_LOG(2) << "[ShardNodeCount] " << totalShardServerNum;
    // check whether the ratio of fault nodes exceeds threshold in one round
    /*if (100 * runningShardServerNum <= totalShardServerNum * (100 -
    ALARM_THR_SHARD_DEGRADE_RATIO_PER_ROUND)) {
        _sendShardClusterDegradeAlarm();
    }
    else {
        _recoverShardClusterDegradeAlarm();
    }*/

    return newDetectedFailedShardServerNum;
}

void ShardServerManager::_failureDetectionMainThread() {
    Client::initThread(kShardFailureDetectionJobName.c_str());
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    index_log() << "[ShardServerManager] failure detector is starting";

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
                invariant(_failureDetectionThreadState == kStopping ||
                          _failureDetectionThreadState == kShutting);
                _threadOperationContext = nullptr;
            }

            index_log() << "[ShardServerManager] failure detector is now stopped, with failed "
                        << "data initialization before running failure detection main loop"
                        << causedBy(status);

            return;
        }
        index_warning() << "Failed to initialize data before running failure detection main loop"
                        << causedBy(status);
        fassertFailedWithStatus(
            40300,
            Status(
                status.code(),
                stream() << "Failed to initialize data before running failure detection main loop"
                         << causedBy(status)));
    }

    // Main loop
    while (!_stopRequested()) {
        _beginRound(txn);
        // bool enableFailoverFlag = getEnableFlag();
        if (GLOBAL_CONFIG_GET(SSHeartBeat)) {
            // Find out failed shard servers
            auto statusWithCheckFailedShardServers = _checkHeartbeatTimeOut(txn);
            if (statusWithCheckFailedShardServers.getStatus().isOK()) {
                uint64_t newDetectedFailedShardServerNum =
                    statusWithCheckFailedShardServers.getValue();
                for (uint64_t i = 0; i < newDetectedFailedShardServerNum; i++) {
                    // Schedule a task for each new failed shard server we found
                    _failureHandlingThreadPool->schedule(
                        ShardServerManager::handleFailedShardServer);
                }
            }
        }

        _endRound(txn, kDefaultShardFailureDetectionInterval);
    }

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
        invariant(_failureDetectionThreadState == kStopping ||
                  _failureDetectionThreadState == kShutting);
        _threadOperationContext = nullptr;
    }

    index_log() << "[ShardServerManager] failure detector is now stopped";

    return;
}

Status ShardServerManager::getOneFailedShardServer(ShardType& failedShard) {
    stdx::lock_guard<stdx::mutex> scopedLock(_failedShardServerListMutex);
    if (_failedShardServerList.empty()) {
        return Status(ErrorCodes::BadValue, stream() << "Empty list of failed shard servers");
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
        index_LOG(2) << "[ShardServerManager] " << std::to_string(getOneFailedShardRound)
                     << "-th getOneFailedShardServer round starts...";

        if (Grid::get(txn)->catalogManager()->getShardServerManager()->_stopRequested()) {
            index_log() << "[ShardServerManager] exits due to stopRequested";
            return;
        }

        Status getOneFailedShardServerStatus =
            Grid::get(txn)->catalogManager()->getShardServerManager()->getOneFailedShardServer(
                failedShard);
        if (getOneFailedShardServerStatus.isOK()) {
            index_LOG(2) << "[ShardServerManager] Get a failed shard: " << failedShard.getHost()
                         << ", processIdentity: " << failedShard.getProcessIdentity();
            break;
        }

        index_log() << "[ShardServerManager] Fail to get a failed shard "
                    << causedBy(getOneFailedShardServerStatus);

        stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSteadyDuration());
        getOneFailedShardRound++;
    }

    // Take over the shard if this shard sever is kShardActive
    if ((failedShard.getState() != ShardType::ShardState::kShardRegistering)) {
        getGlobalServiceContext()
            ->getProcessStageTime("takeoverShard:" + failedShard.getName())
            ->noteStageStart("takeOverShard");
        auto takeOverShardStatus =
            Grid::get(txn)->catalogManager()->getShardServerManager()->takeOverShard(txn,
                                                                                     failedShard);
        if (!takeOverShardStatus.isOK()) {
            index_err() << "[ShardServerManager] failed takeOverShard for shard: "
                        << failedShard.getName() << " " << causedBy(takeOverShardStatus);
            return;
        }
    }

    // remove shard
    getGlobalServiceContext()
        ->getProcessStageTime("takeoverShard:" + failedShard.getName())
        ->noteStageStart("removeShardFromConfigDB");
    int removeShardRound = 0;
    while (true) {
        index_LOG(2) << "[ShardServerManager] " << std::to_string(removeShardRound)
                     << "-th removeShard round starts...";

        if (Grid::get(txn)->catalogManager()->getShardServerManager()->_stopRequested()) {
            index_log() << "[ShardServerManager] exits due to stopRequested";
            return;
        }

        BSONObjBuilder filterBuilder;
        filterBuilder.append(ShardType::name(), failedShard.getName());
        filterBuilder.append(ShardType::processIdentity(), failedShard.getProcessIdentity());

        Status removeShardStatus = Grid::get(txn)->catalogClient(txn)->removeConfigDocuments(
            txn,
            ShardType::ConfigNS,
            filterBuilder.obj(),
            ShardingCatalogClient::kMajorityWriteConcern);
        if (removeShardStatus.isOK()) {
            index_log() << "[ShardServerManager] Finish remove shard: " << failedShard.getHost();
            break;
        }

        index_err() << "[ShardServerManager] Fail to remove shard: " << failedShard.getName() << " "
                    << causedBy(removeShardStatus);

        stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSteadyDuration());
        removeShardRound++;
    }

    Grid::get(txn)->catalogClient(txn)->logChange(txn,
                                                  "removeShard",
                                                  "",
                                                  BSON(ShardType::name() << failedShard.getName()),
                                                  ShardingCatalogClient::kMajorityWriteConcern);

    // delete shard in failedList
    getGlobalServiceContext()
        ->getProcessStageTime("takeoverShard:" + failedShard.getName())
        ->noteStageStart("removeShardFromFailedList");
    Grid::get(txn)->catalogManager()->getShardServerManager()->removeShardFromFailedList(
        failedShard.getName());

    getGlobalServiceContext()
        ->getProcessStageTime("takeoverShard:" + failedShard.getName())
        ->noteProcessEnd();
    index_log() << "[ShardServerManager] Time of "
                << "takeoverShard:" + failedShard.getName() << ": "
                << getGlobalServiceContext()
                       ->getProcessStageTime("takeoverShard:" + failedShard.getName())
                       ->toString();
    getGlobalServiceContext()->cancelProcessStageTime("takeoverShard:" + failedShard.getName());

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
    index_LOG(2) << "[ShardServerManager] process chunk: " << chunk.getID();
    _addFailedChunk(chunk);
    getGlobalServiceContext()->registerProcessStageTime("assignChunk:" + chunk.getName());
    getGlobalServiceContext()
        ->getProcessStageTime("assignChunk:" + chunk.getName())
        ->noteStageStart("scheduleTakeOverChunk:oldShard:" + chunk.getShard().toString());
    _failedChunksThreadPool->schedule(ShardServerManager::takeOverFailedChunk);
}

Status ShardServerManager::markFailedShardDocument(OperationContext* txn,
                                                   const ShardType& failedShard) {
    // update shard state to be fault
    Status status = Status::OK();
    if (failedShard.getState() == ShardType::ShardState::kShardRestarting) {
        return status;
    }

    if ((int)failedShard.getState() != (int)ShardType::ShardState::kShardFault) {
        int markFailedShardDocumentRound = 0;
        while (true) {
            index_LOG(2) << "[ShardServerManager] " << std::to_string(markFailedShardDocumentRound)
                         << "-th markFailedShardDocument round starts...";

            if (_stopRequested()) {
                index_err() << "[ShardServerManager] exits due to stopRequested.";
                status = Status(ErrorCodes::NotMaster,
                                "markFailedShardDocument exits due to stopRequested");
                break;
            }

            auto updateShardStateFaultStatus =
                Grid::get(txn)->catalogManager()->updateShardStateDuringFailover(
                    txn, failedShard, ShardType::ShardState::kShardFault);

            if (updateShardStateFaultStatus.isOK()) {
                index_LOG(2) << "[ShardServerManager] Finish update shard state be fault: "
                             << failedShard.getName();
                break;
            }

            if (ErrorCodes::ShardServerNotFound == updateShardStateFaultStatus.code()) {
                status = updateShardStateFaultStatus;
                break;
            }

            index_err() << "[ShardServerManager] Fail to markFailedShardDocument: "
                        << failedShard.getName() << " " << causedBy(updateShardStateFaultStatus);

            stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSteadyDuration());
            markFailedShardDocumentRound++;
        }
    }

    return status;
}

Status ShardServerManager::preProcessFailedChunks(OperationContext* txn,
                                                  const ShardType& failedShard) {
    // mark all chunk state to be fault
    getGlobalServiceContext()
        ->getProcessStageTime("takeoverShard:" + failedShard.getName())
        ->noteStageStart("updateMultiChunkStatePendingOpen");
    int updateChunkStatePendingOpenRound = 0;
    while (true) {
        bool enableFlag = getEnableFlag();
        if (enableFlag) {
            break;
        }
        stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSteadyDuration());
    }

    while (true) {
        index_LOG(2) << "[ShardServerManager] " << std::to_string(updateChunkStatePendingOpenRound)
                     << "-th updateChunkStatePendingOpen round starts...";

        if (_stopRequested()) {
            index_err() << "[ShardServerManager] exits due to stopRequested.";
            return Status(ErrorCodes::NotMaster,
                          "preProcessFailedChunks exits due to stopRequested");
        }

        auto updateChunkStatePendingOpenStatus =
            Grid::get(txn)->catalogManager()->updateMultiChunkStatePendingOpen(
                txn, failedShard.getName(), failedShard.getProcessIdentity());
        if (updateChunkStatePendingOpenStatus.isOK()) {
            index_LOG(2) << "[ShardServerManager] Finish update chunk state be fault: "
                         << failedShard.getName();
            break;
        }

        index_err() << "[ShardServerManager] Fail to update chunk state be fault: "
                    << failedShard.getName() << " " << causedBy(updateChunkStatePendingOpenStatus);

        stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSteadyDuration());
        updateChunkStatePendingOpenRound++;
    }

    // get all chunks document
    // stdx::lock_guard<stdx::mutex> mapLock(_nonShardedChunksMutex);
    getGlobalServiceContext()
        ->getProcessStageTime("takeoverShard:" + failedShard.getName())
        ->noteStageStart("processFailedChunks");
    int findFailedChunksRound = 0;
    /*while (failedShard.getState() == ShardType::ShardState::kShardRestarting) {
        bool enableFlag = getEnableFlag();
        if (enableFlag) {
            break;
        }
        stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSteadyDuration());
    }*/

    while (true) {
        index_LOG(2) << "[ShardServerManager] " << std::to_string(findFailedChunksRound)
                     << "-th findFailedChunks round starts...";

        if (_stopRequested()) {
            index_err() << "[ShardServerManager] exits due to stopRequested.";
            return Status(ErrorCodes::NotMaster,
                          "preProcessFailedChunks exits due to stopRequested");
        }

        BSONObjBuilder failShardChunkFilter;
        failShardChunkFilter.append(ChunkType::shard(), failedShard.getName());
        failShardChunkFilter.append(ChunkType::processIdentity(), failedShard.getProcessIdentity());
        failShardChunkFilter.append(ChunkType::rootFolder(),
                                    BSON("$ne"
                                         << "stalerootfolder"));
        failShardChunkFilter.append(
            ChunkType::status(),
            BSON("$ne" << static_cast<int>(ChunkType::ChunkStatus::kDisabled)));

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
            // add

            // std::list<std::string> dbs;
            auto getDbsStatus = getDbsOnFailedShard(txn, failedShard.getName());
            std::vector<DatabaseType>& dbsOnFailedShard = getDbsStatus.getValue();
            // end

            for (const BSONObj& failedChunk : findFailedChunksStatus.getValue().docs) {
                auto chunkRes = ChunkType::fromBSON(failedChunk);
                if (!chunkRes.isOK()) {
                    index_err() << "[ShardServerManager] from bson " << failedChunk.toString()
                                << " due to " << chunkRes.getStatus().reason();
                    // sendCorrClstMgmtIndexAlarm(ChunkType::ConfigNS, _corrChunkDocHasSend);
                    continue;
                }
                index_LOG(2) << "[ShardServerManager] chunk:" << chunkRes.getValue().toString()
                             << " eventid: " << GET_EVENTID(chunkRes.getValue());
                // process sharded chunk
                if (isBelongToSharded(txn, chunkRes.getValue())) {
                    processFailedChunk(chunkRes.getValue());
                }
            }

            for (DatabaseType database : dbsOnFailedShard) {
                std::list<ChunkType> chunkList;
                // get all non shard chunks belong to database
                getAllNonShardChunks(txn, database, chunkList);
                std::string dbName = database.getName();
                processFailedDatabase(dbName, chunkList);
            }

            index_LOG(2) << "[ShardServerManager] Finish find "
                         << findFailedChunksStatus.getValue().docs.size()
                         << " failed chunks for shard:" << failedShard.getName();
            break;
        }

        index_err() << "[ShardServerManager] Fail to find failed chunks for shard: "
                    << failedShard.getName() << causedBy(findFailedChunksStatus.getStatus());

        stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSteadyDuration());
        findFailedChunksRound++;
    }

    return Status::OK();
}

Status ShardServerManager::takeOverShard(OperationContext* txn, const ShardType& failedShard) {
    index_log() << "[ShardServerManager] Begin to take over shard: " << failedShard.getName();
    getGlobalServiceContext()
        ->getProcessStageTime("takeoverShard:" + failedShard.getName())
        ->noteStageStart("markFailedShardDocument");
    auto markFailedShardDocumentStatus = markFailedShardDocument(txn, failedShard);
    if (!markFailedShardDocumentStatus.isOK()) {
        return markFailedShardDocumentStatus;
    }

    getGlobalServiceContext()
        ->getProcessStageTime("takeoverShard:" + failedShard.getName())
        ->noteStageStart("preProcessFailedChunks");
    auto preProcessFailedChunksStatus = preProcessFailedChunks(txn, failedShard);
    if (!preProcessFailedChunksStatus.isOK()) {
        return preProcessFailedChunksStatus;
    }

    return Status::OK();
}

void ShardServerManager::takeOverFailedChunk() {
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
        index_err() << "[ShardServerManager] Failed to get failed chunk due to "
                    << getOneFailedChunkStatus.reason();
        return;
    }

    Status validateStatus = chunk.validate();
    if (!validateStatus.isOK()) {
        // TODO: mark unsolved chunk, need to be handled
        index_err() << "[ShardServerManager] Failed to validate chunk doc " << chunk.getFullNs()
                    << " due to " << validateStatus.reason();
        return;
    }

    index_LOG(2) << "[ShardServerManager] start to reassign chunk(" << chunk.getFullNs() << ")";
    // re-assign chunks
    getGlobalServiceContext()
        ->getProcessStageTime("assignChunk:" + chunk.getName())
        ->noteStageStart("assignChunk");

    ShardId sourceShard = chunk.getShard();
    Status status = grid.catalogManager()->getShardServerManager()->waitForSourceShard(txn,sourceShard);

    Status assignStatus = Status::OK();
    if (status.isOK()) {
        assignStatus = Balancer::get(txn)->assignChunk(txn, chunk, false, false, sourceShard);
    } else {
        assignStatus = Balancer::get(txn)->assignChunk(txn, chunk, false, false);
    }

    if (!assignStatus.isOK()) {
        // break, re-try re-assign chunk
        index_err() << "[ShardServerManager] Fail to re-assign chunk: " << chunk.getName();
    } else {
        index_LOG(2) << "[ShardServerManager] Finish to execute re-assign chunk event: "
                     << chunk.getName();
    }
    return;
}

StatusWith<ShardType> ShardServerManager::getShardTypeByShardId(const ShardId& shardId) {
    while (!hasShardLoaded()) {
        index_warning() << "[ShardServerManager] wait to load shard servers";        
        if (_stopRequested()) {
            return Status(ErrorCodes::NotMaster,
                      str::stream() << "Not primary while wait to load " << shardId.toString());                        
        }
        sleepsecs(1);
    }

    // traverse _shardServerNodeMap to get the shardType
    {
        stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        unordered_map<string, list<ShardInfo>>::iterator itMap =
            _shardServerNodeMap.begin();  // lint !e60
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

    std::string errmsg = str::stream() << "shardId: " << shardId << ", shard server not found.";
    return Status(ErrorCodes::ShardServerNotFound, errmsg);
}

StatusWith<ShardType> ShardServerManager::getShardTypeByHostInfo(const std::string& host) {
    stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
    auto getShardTypeStatus = getShardTypeByHostInfo_inlock(host);
    if (!getShardTypeStatus.isOK()) {
        index_warning() << "[ShardServerManager] cannot get shard with host " << host << " due to "
                        << getShardTypeStatus.getStatus().reason();
    }
    return getShardTypeStatus;
}


StatusWith<ShardType> ShardServerManager::getShardTypeByHostInfo_inlock(const std::string& host) {
    HostAndPort hostAndPort(host);
    string shardServerNodeIP = hostAndPort.host();

    unordered_map<string, list<ShardInfo>>::iterator itMap =
        _shardServerNodeMap.find(shardServerNodeIP);
    if (itMap != _shardServerNodeMap.end()) {
        list<ShardInfo>::iterator itList = itMap->second.begin();
        while (itList != itMap->second.end()) {
            if (itList->shard.getHost().find(host) != std::string::npos) {
                return itList->shard;
            }
            itList++;
        }
    }

    std::string errmsg = str::stream() << "host: " << host << ", shard server not found.";
    return Status(ErrorCodes::ShardServerNotFound, errmsg);
}

StatusWith<std::string> ShardServerManager::updateShardInMemory(const HostAndPort& hostandport,
                                                                const ShardType& shard) {
    std::string shardServerNodeIP = hostandport.host();
    stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);

    unordered_map<string, list<ShardInfo>>::iterator itMap =
        _shardServerNodeMap.find(shardServerNodeIP);
    if (itMap == _shardServerNodeMap.end()) {
        index_err() << "[ShardServerManager] shard " << shard.getHost()
                    << " IP cannot be found in shardServerNodeMap";
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
        index_err() << "[ShardServerManager] shard " << shard.getHost()
                    << " port cannot be found in shardServerNodeMap";
        return Status(ErrorCodes::BadValue,
                      stream() << "shard (" << shard.getHost()
                               << ")' port cannot be found in shardServerNodeMap");
    }
    itList->shard.setName(shard.getName());
    itList->shard.setState(shard.getState());
    itList->shard.setProcessIdentity(shard.getProcessIdentity());
    // itList->shard.setHost(shard.getHost());
    index_LOG(0) << "[ShardServerManager] update shard in shardNodeMap to : ";
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
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
    list<ShardType> shardList;
    unordered_map<string, list<ShardInfo>>::iterator itMap;
    list<ShardInfo>::iterator itList;
    for (itMap = _shardServerNodeMap.begin(); itMap != _shardServerNodeMap.end(); ++itMap) {
        Date_t expireTime = repl::getGlobalReplicationCoordinator()->getExecutor()->now() -
            kDefaultShardFailureDetectionTimeout;
        for (itList = itMap->second.begin(); itList != itMap->second.end(); ++itList) {
            if ((int)itList->shard.getState() == (int)ShardType::ShardState::kShardActive &&
                (itList->heartbeatTime > expireTime || !GLOBAL_CONFIG_GET(SSHeartBeat))) {
                ShardId shardId(itList->shard.getName());
                auto checkStatus = grid.catalogManager()->getShardServerManager()->checkTakeoverShard(txn,shardId);
                if (checkStatus.isOK()) {
                    shardList.push_back(itList->shard);
                }
                index_warning() << "[ShardServerManager] check " << shardId << " failed due to " << checkStatus.getStatus();
            }
        }
    }

    if (shardList.size() == 0) {
        return Status(ErrorCodes::ShardServerNotFound, "no shard available in cluster");
    }
    int pick_offset = std::abs(_pSecureRandom->nextInt64()) % (shardList.size());
    auto targetIt = shardList.begin();
    std::advance(targetIt, pick_offset);
    return *targetIt;
    /*stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
    for (unsigned int i = 0; i < _shardServerNodeMap.size() * 2; i++) {
        int pick_offset = std::abs(_pSecureRandom->nextInt64()) % (_shardServerNodeMap.size());
        auto targetIt = _shardServerNodeMap.begin();
        std::advance(targetIt, pick_offset);

        Date_t expireTime = repl::getGlobalReplicationCoordinator()->getExecutor()->now() -
            kDefaultShardHeartbeatTimeoutPeriod;

        if (targetIt->second.size() > 0) {
            list<ShardInfo>::iterator itList;
            int shd_offset = std::abs(_pSecureRandom->nextInt64()) % (targetIt->second.size());

            int count = 0;
            for (itList = targetIt->second.begin(); itList != targetIt->second.end(); ++itList) {
                if (count < shd_offset) {
                    count++;
                    continue;
                }
                if ((int)itList->shard.getState() == (int)ShardType::ShardState::kShardActive &&
                    (itList->heartbeatTime > expireTime || !GLOBAL_CONFIG_GET(SSHeartBeat))) {
                    return itList->shard;
                }
            }
        }
    }

    return Status(ErrorCodes::ShardServerNotFound, "no shard available in cluster");*/
}

StatusWith<ShardType> ShardServerManager::getTakeOverShard(OperationContext* txn,
                                                           bool userRequest) {
    int getTakeOverShardRound = 0;

    // pick shard for each chunk
    while (true) {
        index_LOG(2) << "[ShardServerManager] " << std::to_string(getTakeOverShardRound)
                     << "-th getTakeOverShard round starts...";

        if (_stopRequested()) {
            index_err() << "[ShardServerManager] exits due to stopRequested.";
            return Status(ErrorCodes::NotMaster, "Failed to get shard due to stopRequested");
        }

        auto activeShard = getAnActiveShard();
        if (activeShard.isOK()) {
            /*ShardId shardId(activeShard.getValue().getName());
            auto checkStatus = grid.catalogManager()->getShardServerManager()->checkTakeoverShard(txn,shardId);
            if (checkStatus.isOK()) {
                return activeShard.getValue();
            }
            index_err() << "[ShardServerManager] check " << shardId << " failed due to " << checkStatus.getStatus();
            if (userRequest) {
                return checkStatus.getStatus();
            }*/
            return activeShard.getValue();
        }

        if (userRequest) {
            return activeShard.getStatus();
        }

        stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSteadyDuration());
        getTakeOverShardRound++;
    }
}

bool ShardServerManager::_stopRequested() {
    stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
    return (_failureDetectionThreadState != kRunning || _failureDetectionThreadState == kShutting);
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
    _failureDetectionThreadCondVar.wait_for(lock, waitTimeout.toSteadyDuration(), [&] {
        return _failureDetectionThreadState != kRunning;
    });
}

void ShardServerManager::_printShardServerNodeView() {
    unordered_map<string, list<ShardInfo>>::iterator itMap;
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

Status ShardServerManager::determineShardState(
    OperationContext* txn,
    const ConnectionString& shardServerConn,  // lint !e839
    const std::string& processIdentity,
    bool& retryRequest,
    ShardType::ShardState& currentState,
    ShardType& existingShard) {
    HostAndPort hostAndPort = shardServerConn.getServers()[0];
    index_LOG(2) << "[ShardServerManager] determineShardState shard state for "
                 << hostAndPort.toString();

    // Get shard doc from configDB
    auto findShardStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            kConfigReadSelector,
            repl::ReadConcernLevel::kLocalReadConcern,
            NamespaceString(ShardType::ConfigNS),
            BSON(ShardType::host() << BSON("$regex" << hostAndPort.toString())),
            BSONObj(),
            boost::none);  // no limit

    if (!findShardStatus.isOK()) {
        index_err() << "[ShardServerManager] fail to find shard with host "
                    << hostAndPort.toString() << " due to " << findShardStatus.getStatus();
        return findShardStatus.getStatus();
    }

    const auto shardDocs = findShardStatus.getValue().docs;

    index_LOG(2) << "[ShardServerManager] find " << shardDocs.size() << " documents with host "
                 << hostAndPort.toString();

    if (shardDocs.size() == 0) {
        retryRequest = false;
        currentState = ShardType::ShardState::kShardRegistering;

        return Status::OK();
    }

    if (shardDocs.size() > 1) {
        return Status(ErrorCodes::TooManyMatchingDocuments,
                      str::stream() << "more than one shard document found for host "
                                    << hostAndPort.toString()
                                    << " in config databases");
    }

    auto shardDocStatus = ShardType::fromBSON(shardDocs.front());
    if (!shardDocStatus.isOK()) {
        index_err() << "[ShardServerManager] from bson " << shardDocs.front() << " failed due to "
                    << shardDocStatus.getStatus();
        return shardDocStatus.getStatus();
    }

    existingShard = shardDocStatus.getValue();

    if (processIdentity.compare(existingShard.getProcessIdentity()) == 0) {
        if ((existingShard.getState() == ShardType::ShardState::kShardRegistering) ||
            (existingShard.getState() == ShardType::ShardState::kShardRestarting)) {
            // retry register due to last response missed
            index_log() << "[ShardServerManager] retry to register shard "
                        << existingShard.toString() << " due to last response missed";
            retryRequest = true;
            currentState = existingShard.getState();
        } else {
            index_err()
                << "[ShardServerManager] not allowed to re-register a shard with process identity: "
                << existingShard.getProcessIdentity()
                << ", and state: " << (int)existingShard.getState();
            return Status(ErrorCodes::IllegalOperation,
                          stream() << "not allowed to re-register a shard with process identity: "
                                   << existingShard.getProcessIdentity()
                                   << ", and state: "
                                   << (int)existingShard.getState());
        }
    } else {
        // re-launch a shard due to the old one failed
        retryRequest = false;
        currentState = ShardType::ShardState::kShardRestarting;
    }

    return Status::OK();
}

Status ShardServerManager::processFailedPreviousShardServer(const HostAndPort& hostAndPort,
                                                            const std::string& processIdentity) {
    bool foundFlag = false;
    std::string hostAndPortStr = hostAndPort.toString();
    ShardType failShard;
    {
        stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        unordered_map<string, list<ShardInfo>>::iterator itMap =
            _shardServerNodeMap.find(hostAndPort.host());

        if (itMap == _shardServerNodeMap.end()) {
            std::string errmsg = str::stream() << "hostAndPort: " << hostAndPort
                                               << ", shard server not found.";
            return Status(ErrorCodes::ShardServerNotFound, errmsg);
        }
        for (list<ShardInfo>::iterator itList = itMap->second.begin();
             itList != itMap->second.end();
             ++itList) {
            if (itList->shard.getHost().find(hostAndPortStr) != std::string::npos) {  // lint !e1015
                foundFlag = true;

                getGlobalServiceContext()->registerProcessStageTime("takeoverShard:" +
                                                                    itList->shard.getName());
                getGlobalServiceContext()
                    ->getProcessStageTime("takeoverShard:" + itList->shard.getName())
                    ->noteStageStart("shardServerRestart");
                FailedShardType failedShardType;

                failedShardType.isProcessing = false;
                failShard = itList->shard;
                failShard.setState(ShardType::ShardState::kShardRestarting);  // for do not mark ss
                                                                              // fault when take
                                                                              // over
                failedShardType.shard = failShard;

                stdx::lock_guard<stdx::mutex> failedShardServerLock(_failedShardServerListMutex);
                // Move the shard server to the failed shard server set
                _failedShardServerList.push_front(failedShardType);
                index_LOG(0) << "[ShardServerManager] after remove failShard "
                             << failShard.getName() << ", ShardNodeMap : ";
                _printShardServerNodeView();
                // make passive take over do not happen
                itList->heartbeatTime =
                    repl::getGlobalReplicationCoordinator()->getExecutor()->now();
                // if assign chunk failed cause  by chunkbusy , chunk belong shard is not alive,set
                // errorcode and retry
                itList->shard.setProcessIdentity(processIdentity);
                // avoid choose shard not added
                itList->shard.setState(ShardType::ShardState::kShardRestarting);
                break;
            }
        }

        /*if (foundFlag && itMap->second.size() == 0) {
            _shardServerNodeMap.erase(itMap);
        }*/
    }

    if (!foundFlag) {
        std::string errmsg = str::stream() << "hostAndPort: " << hostAndPort
                                           << ", shard server not found.";
        return Status(ErrorCodes::ShardServerNotFound, errmsg);
    }

    _failureHandlingThreadPool->schedule(ShardServerManager::handleFailedShardServer);

    return Status::OK();
}
StatusWith<BSONObj> ShardServerManager::addShardDocument(
    OperationContext* txn,
    const ConnectionString& conn,  // lint !e839
    const std::string& extendIPs,
    const std::string& processIdentity) {
    if (!hasInitialized()) {
        index_warning() << "[ShardServerManager] primary config server has not transition to "
                           "primary completely.";
        return Status{ErrorCodes::NotYetInitialized, "not yet initialized to primary"};
    }

    auto conn_status = _validateSingleConnectionString(conn);
    if (!conn_status.isOK()) {
        index_warning() << "[ShardServerManager] fail to validate connect string "
                        << conn.toString() << " due to " << conn_status.getStatus();
        return conn_status.getStatus();
    }
    HostAndPort hostAndPort = conn_status.getValue();
    string shardServerNodeIP = hostAndPort.host();

    bool retryRequest = false;
    ShardType::ShardState state = ShardType::ShardState::kShardRegistering;
    ShardType shard;
    ShardInfo shardInfo;

    auto determineShardStateStatus =
        determineShardState(txn, conn, processIdentity, retryRequest, state, shard);
    if (!determineShardStateStatus.isOK()) {
        index_err() << "[ShardServerManager] fail to determine shard state due to "
                    << determineShardStateStatus;
        return determineShardStateStatus;
    }

    if (retryRequest) {
        auto repl_coordinator = repl::getGlobalReplicationCoordinator();
        if (repl_coordinator == nullptr) {
            index_err() << "[ShardServerManager] getGlobalReplicationCoordinator failed.";
            return Status(ErrorCodes::InternalError, "getGlobalReplicationCoordinator failed.");
        }

        auto executor = repl_coordinator->getExecutor();
        if (executor == nullptr) {
            index_err() << "[ShardServerManager] getExecutor failed.";
            return Status(ErrorCodes::InternalError, "getExecutor failed.");
        }
        shardInfo.shard = shard;
        shardInfo.heartbeatTime = executor->now() + kDefaultRegisterReservedTime;
        // Insert the shard into in-memory shard server map
        {
            stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
            auto getShardTypeStatus = getShardTypeByHostInfo_inlock(hostAndPort.toString());
            if (!getShardTypeStatus.isOK()) {
                index_warning() << "[ShardServerManager] cannot get shard with host "
                                << hostAndPort.toString() << " due to last register failed.";
                _shardServerNodeMap[shardServerNodeIP].push_front(std::move(shardInfo));
                index_log() << "[ShardServerManager] after retry register shard : "
                            << shard.toString();
                _printShardServerNodeView();
            }
        }
        BSONObjBuilder builder;
        builder.append("shardName", shard.getName());
        return builder.obj();
    }

    // take over former failed shard in this host:port
    if (state == ShardType::ShardState::kShardRestarting) {
        Status processStatus = processFailedPreviousShardServer(hostAndPort, processIdentity);
        if (processStatus.isOK()) {
            index_LOG(2) << "[ShardServerManager] The failed previous shard server (" << shard
                         << ") is processed in advance during restarting";
        } else {
            index_LOG(2) << "[ShardServerManager] The failed previous shard server (" << shard
                         << ") has been processed by failure detector";
        }
    }

    shard.setState(state);
    shard.setProcessIdentity(processIdentity);
    shard.setExtendIPs(extendIPs);

    // Insert the shard into configDB
    auto insOrUpdDocumentResult =
        Grid::get(txn)->catalogManager()->insertOrUpdateShardDocument(txn, shard, conn);

    if (!insOrUpdDocumentResult.isOK()) {
        index_err() << "[ShardServerManager] insOrUpdDocument for shard " << shard.getHost()
                    << " failed due to " << insOrUpdDocumentResult.getStatus();
        return insOrUpdDocumentResult.getStatus();
    }

    // MONGO_FAIL_POINT_PAUSE_WHILE_SET(hangRegisterAfterInsertShardDoc);
    auto inOrUpShardMapRes =
        grid.catalogManager()->getShardServerManager()->insertOrUpdateShardMap(shard, conn);
    if (!inOrUpShardMapRes.isOK()) {
        index_err() << "[ShardServerManager] insOrUpdShardMap for shard " << shard.getHost()
                    << " failed due to " << inOrUpShardMapRes;
    }
    index_LOG(2) << "[ShardServerManager] Finish addShardDocument for shard server " << conn;

    BSONObjBuilder builder;
    builder.append("shardName", shard.getName());
    return builder.obj();
}

Status ShardServerManager::insertOrUpdateShardMap(ShardType& shard, const ConnectionString& conn) {
    HostAndPort hostAndPort = conn.getServers()[0];
    if (shard.getState() == ShardType::ShardState::kShardRestarting) {
        auto updateRes =
            grid.catalogManager()->getShardServerManager()->updateShardInMemory(hostAndPort, shard);
        // maybe passive take over has begun
        if (updateRes.getStatus().isOK()) {
            return Status::OK();
        }
    }

    {
        ShardInfo shardInfo;
        shardInfo.shard = shard;
        shardInfo.heartbeatTime = repl::getGlobalReplicationCoordinator()->getExecutor()->now() +
            kDefaultRegisterReservedTime;
        string shardServerNodeIP = hostAndPort.host();
        // Insert the shard into in-memory shard server map
        {
            stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
            _shardServerNodeMap[shardServerNodeIP].push_front(std::move(shardInfo));
            index_LOG(0) << "[ShardServerManager] _shardServerNodeMap after register shard "
                         << shard << " : ";
            _printShardServerNodeView();
        }
    }
    return Status::OK();
}

Status ShardServerManager::setLastHeartbeatTime(OperationContext* txn,
                                                const HostAndPort& hostAndPort,
                                                const Date_t& time) {

    if (hostAndPort.empty() || (hostAndPort.port() == -1) || hostAndPort.host().empty()) {
        return Status(ErrorCodes::BadValue, "empty host or port");
    }

    if (!hasInitialized()) {
        index_warning() << "[ShardServerManager] primary config server has not transition to "
                           "primary completely.";
        return Status{ErrorCodes::NotYetInitialized, "not yet initialized to primary"};
    }

    string host = hostAndPort.toString();
    string shardServerNodeIP = hostAndPort.host();

    {
        stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        unordered_map<string, list<ShardInfo>>::iterator itMap =
            _shardServerNodeMap.find(shardServerNodeIP);

        if (itMap == _shardServerNodeMap.end()) {
            index_err() << "[ShardServerManager] host: " << hostAndPort.host()
                        << " not found in ShardNodeMap.";
            std::string errmsg = str::stream() << "hostAndPort: " << hostAndPort
                                               << ", shard server not found.";
            return Status(ErrorCodes::ShardServerNotFound, errmsg);
        }

        for (list<ShardInfo>::iterator itList = itMap->second.begin();
             itList != itMap->second.end();
             ++itList) {
            if (itList->shard.getHost().find(host) != std::string::npos) {
                itList->heartbeatTime = time;
                return Status::OK();
            }
        }
    }
    index_err() << "[ShardServerManager] port : " << hostAndPort.port()
                << " not found in ShardNodeMap.";
    std::string errmsg = str::stream() << "hostAndPort: " << hostAndPort
                                       << ", shard server not found.";
    return Status(ErrorCodes::ShardServerNotFound, errmsg);
}


StatusWith<Date_t> ShardServerManager::getLastHeartbeatTime(OperationContext* txn,
                                                            const HostAndPort& hostAndPort) {
    if (!hasInitialized()) {
        index_warning() << "[ShardServerManager] primary config server has not transition to "
                           "primary completely.";
        return Status{ErrorCodes::NotYetInitialized, "not yet initialized to primary"};
    }

    if (hostAndPort.empty() || (hostAndPort.port() == -1) || hostAndPort.host().empty()) {
        index_err() << "[ShardServerManager] empty host or port";
        return Status(ErrorCodes::BadValue, "empty host or port");
    }

    string host = hostAndPort.toString();
    string shardServerNodeIP = hostAndPort.host();

    {
        stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        unordered_map<string, list<ShardInfo>>::iterator itMap =
            _shardServerNodeMap.find(shardServerNodeIP);

        if (itMap == _shardServerNodeMap.end()) {
            index_err() << "[ShardServerManager] host: " << hostAndPort.host()
                        << " not found in ShardNodeMap.";
            std::string errmsg = str::stream() << "hostAndPort: " << hostAndPort
                                               << ", shard server not found.";
            return Status(ErrorCodes::ShardServerNotFound, errmsg);
        }

        for (list<ShardInfo>::iterator itList = itMap->second.begin();
             itList != itMap->second.end();
             ++itList) {
            if (itList->shard.getHost().find(host) != std::string::npos) {
                return itList->heartbeatTime;
            }
        }
    }

    index_err() << "[ShardServerManager] port : " << hostAndPort.port()
                << " not found in ShardNodeMap.";

    std::string errmsg = str::stream() << "hostAndPort: " << hostAndPort
                                       << ", shard server not found.";
    return Status(ErrorCodes::ShardServerNotFound, errmsg);
}

Status ShardServerManager::waitForSourceShard(OperationContext* txn,const ShardId& sourceShard) {
    int retryTimes = 10;
    ShardType shard;
    for (int i = 0; i < retryTimes; i++) {
        auto status =
            grid.catalogManager()->getShardServerManager()->getShardTypeByShardId(sourceShard);
        if (!status.isOK()) {
            return status.getStatus();
        }
        shard = status.getValue();
        if (shard.getState() == ShardType::ShardState::kShardActive) {
            ShardId shardId(sourceShard.toString()); 
            auto checkStatus = grid.catalogManager()->getShardServerManager()->checkTakeoverShard(txn,shardId);
            return checkStatus.getStatus();
        }
        stdx::this_thread::sleep_for(kDefaultGetSourceShardRetryInterval.toSteadyDuration());
    }

    std::string errmsg = str::stream() << "shardId: " << sourceShard << ", shard server not found.";
    return Status(ErrorCodes::ShardServerNotFound, errmsg);
}

StatusWith<ShardType> ShardServerManager::getPrimaryShard(OperationContext* txn) {
    stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);

    if (_shardServerNodeMap.size() == 0) {
        index_err() << "[ShardServerManager] no shard available in cluster";
        return Status(ErrorCodes::ShardServerNotFound, "no shard available in cluster");
    }

    auto repl_coordinator = repl::getGlobalReplicationCoordinator();
    if (!repl_coordinator) {
        index_err() << "[ShardServerManager] getGlobalReplicationCoordinator failed.";
        return Status(ErrorCodes::ShardServerNotFound, "getGlobalReplicationCoordinator failed.");
    }

    auto exec = repl_coordinator->getExecutor();
    if (!exec) {
        index_err() << "[ShardServerManager] repl_coordinator->getExecutor failed.";
        return Status(ErrorCodes::ShardServerNotFound, "repl_coordinator->getExecutor failed.");
    }

    Date_t expireTime = exec->now() - kDefaultShardFailureDetectionTimeout;

    unordered_map<string, list<ShardInfo>>::iterator itMap;
    list<ShardInfo>::iterator itList;
    ShardType shard;
    unsigned int minChunkNum = INT_MAX;
    bool isShardFound = false;
    for (itMap = _shardServerNodeMap.begin(); itMap != _shardServerNodeMap.end(); ++itMap) {
        for (itList = itMap->second.begin(); itList != itMap->second.end(); ++itList) {
            if ((int)itList->shard.getState() == (int)ShardType::ShardState::kShardActive &&
                itList->heartbeatTime > expireTime) {
                // if the shard is already added , pick the shard which has least chunks

                auto findChunksStatus =
                    Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                        txn,
                        kConfigReadSelector,
                        repl::ReadConcernLevel::kLocalReadConcern,
                        NamespaceString(ChunkType::ConfigNS),
                        BSON(ChunkType::shard(itList->shard.getName())),
                        BSONObj(),  // no sort
                        boost::none);
                if (findChunksStatus.isOK()) {
                    auto chunkDocs = findChunksStatus.getValue().docs;
                    ShardId shardId(itList->shard.getName());
                    auto checkStatus = grid.catalogManager()->getShardServerManager()->checkTakeoverShard(txn,shardId);                   
                    if (chunkDocs.size() < minChunkNum && checkStatus.isOK()) {
                        minChunkNum = chunkDocs.size();
                        shard = itList->shard;
                        isShardFound = true;
                    }
                    continue;
                }
                index_err() << "[ShardServerManager] fail to find chunk with shard "
                            << itList->shard.getName() << " due to "
                            << findChunksStatus.getStatus();
            }
        }
    }
    if (isShardFound) {
        return shard;
    }
    return Status(ErrorCodes::ShardServerNotFound, "no shard available in cluster");
}

Status ShardServerManager::setDatabasePrimary(OperationContext* txn,
                                              std::string& dbname,
                                              ShardId& primaryShard) {
    auto status = grid.catalogCache()->getDatabase(txn, dbname);
    if (!status.isOK()) {
        index_log()
            << "[ShardServerManager] fail to set primary for sharded-only database cause by "
            << status.getStatus();
        return Status(ErrorCodes::NotMaster, "get database failed");
    }
    shared_ptr<DBConfig> config = status.getValue();
    try {
        config->setPrimary(txn, primaryShard);
        config->reload(txn);
    } catch (const DBException& excep) {
        index_LOG(0) << "[ShardServerManager] fail to load txn cause by " << excep.toStatus();
        return excep.toStatus();
    }

    grid.catalogCache()->invalidate(dbname);
    return Status::OK();
}


StatusWith<std::vector<DatabaseType>> ShardServerManager::getDbsOnFailedShard(
    OperationContext* txn, const std::string& failShardName) {
    ShardId shardId(failShardName);
    const auto findDBStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            kConfigPrimarySelector,
            repl::ReadConcernLevel::kLocalReadConcern,
            NamespaceString(DatabaseType::ConfigNS),
            BSON(DatabaseType::primary() << shardId),
            BSONObj(),
            boost::none);
    if (!findDBStatus.isOK()) {
        index_err() << "[ShardServerManager] fail to get databases in shard " << failShardName
                    << " due to " << findDBStatus.getStatus();
        return findDBStatus.getStatus();
    }
    std::vector<DatabaseType> databases;
    const auto dbDocs = findDBStatus.getValue().docs;
    for (BSONObj dbDoc : dbDocs) {
        auto dbParseStatus = DatabaseType::fromBSON(dbDoc);
        if (!dbParseStatus.isOK()) {
            index_err() << "[ShardServerManager] from bson " << dbDoc << " failed due to "
                        << dbParseStatus.getStatus();
            return dbParseStatus.getStatus();
        }
        DatabaseType dbType = dbParseStatus.getValue();
        databases.push_back(dbType);
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
    index_LOG(0) << "[ShardServerManager] begin to get one failed non sharded map in nonShardMap.";
    Status getFailedNonShardedChunksStatus =
        grid.catalogManager()->getShardServerManager()->getFailedNonShardedChunks(dbname,
                                                                                  chunkList);
    if (!getFailedNonShardedChunksStatus.isOK()) {
        index_err() << "[ShardServerManager] Failed to get failed non sharded map due to "
                    << getFailedNonShardedChunksStatus.reason();
        return;
    }
    index_LOG(0) << "[ShardServerManager] begin to take over non-shard chunks for database : "
                 << dbname;

    ShardId targetShardId;
    // find DatabaseType by dbname
    const auto findDBStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            kConfigPrimarySelector,
            repl::ReadConcernLevel::kLocalReadConcern,
            NamespaceString(DatabaseType::ConfigNS),
            BSON(DatabaseType::name() << dbname),
            BSONObj(),
            boost::none);
    if (!findDBStatus.isOK()) {
        index_err() << "[ShardServerManager] fail to get databaseType due to "
                    << findDBStatus.getStatus();
        return;
    }
    const auto dbDocs = findDBStatus.getValue().docs;
    if (dbDocs.size() == 0) {
        index_warning() << "[ShardServerManager] database " << dbname
                        << " can not be found in config.databases";
        return;
    }
    auto dbParseStatus = DatabaseType::fromBSON(dbDocs.front());
    DatabaseType dbType = dbParseStatus.getValue();
    ShardId sourceShard = dbType.getPrimary();
    // get success assigned chunk and not success chunk and take over shard
    std::list<ChunkType>::iterator it = chunkList.begin();
    while (it != chunkList.end()) {
        ShardId shardId = it->getShard();
        std::string processIdent = it->getProcessIdentity();
        ShardIdent shard = ShardIdent(shardId, processIdent);
        if (grid.catalogManager()->getShardServerManager()->isShardServerAlive(shard)) {
            if (it->getStatus() == ChunkType::ChunkStatus::kAssigned) {
                chunksSuccess.push_back(*it); 
            } else {
		chunksFailed.push_back(*it);
            }
            targetShardId = it->getShard();
        } else {
            chunksFailed.push_back(*it);
        }
        it++;
    }

    while (chunksSuccess.empty()) {
        if (grid.catalogManager()->getShardServerManager()->_stopRequested()) {
            return;
        }

        if (targetShardId.isValid()) {
            break;
        }

        auto waitStatus =
            grid.catalogManager()->getShardServerManager()->waitForSourceShard(txn,sourceShard);
        if (waitStatus.isOK()) {
            targetShardId = sourceShard;
            break;
        }

        auto getTakeOverShardStatus =
            grid.catalogManager()->getShardServerManager()->getPrimaryShard(txn);
        if (getTakeOverShardStatus.isOK()) {
            ShardType takeOverShard = getTakeOverShardStatus.getValue();
            targetShardId = ShardId(takeOverShard.getName());
            index_LOG(2) << "[ShardServerManager] get take over shard " << targetShardId.toString();
            break;
        }
        stdx::this_thread::sleep_for(kDefaultTakeOverShardRetryInterval.toSteadyDuration());
    }

    index_LOG(0) << "[ShardServerManager] finish get primary shard : " << targetShardId.toString()
                 << " for db " << dbname;
    bool sucTakeover = false;
    auto guard = MakeGuard([&] {
        if (!sucTakeover) {
            try {
                Grid::get(txn)->catalogManager()->getShardServerManager()->rollBackAssign(
                    txn, chunksSuccess, chunkList);
                Grid::get(txn)->catalogManager()->getShardServerManager()->processFailedDatabase(
                    dbname, chunkList);
            } catch (const DBException& excep) {
                index_log() << "[ShardServerManager] fail to roll back for db " << dbname
                            << "cause by " << excep.toStatus();
            }
        }
    });
    for (std::list<ChunkType>::iterator itList = chunksFailed.begin(); itList != chunksFailed.end();
         ++itList) {
        Status validateStatus = itList->validate();
        if (!validateStatus.isOK()) {
            // TODO: mark unsolved chunk, need to be handled
            index_err() << "[ShardServerManager] Failed to validate chunk document " << chunk
                        << " due to " << validateStatus.reason();
            return;
        }

        // re-assign chunks
        auto assignStatus =
            Balancer::get(txn)->assignChunk(txn, *itList, false, true, targetShardId, false, true);
        if (!assignStatus.isOK()) {
            (void)Balancer::get(txn)->getResultsForTakeOver(txn, dbname, chunksSuccess);
            index_err() << "[ShardServerManager] assign chunk failed due to " << assignStatus;
            return;
        }
    }

    if (!chunksFailed.empty()) {
        Status assign_status =
            Balancer::get(txn)->getResultsForTakeOver(txn, dbname, chunksSuccess);
        if (!assign_status.isOK()) {
            index_err() << "[ShardServerManager] assign chunk failed due to " << assign_status;
            return;
        }
    }

    try {
        uassertStatusOK(grid.catalogManager()->getShardServerManager()->setDatabasePrimary(
                txn, dbname, targetShardId));
    } catch (const DBException& excep) {
        index_err() << "[ShardServerManager] fail to set primary cause by " << excep.toStatus();
        return;
    }

    sucTakeover = true;
    index_LOG(0) << "[ShardServerManager] finish set primary to " << targetShardId.toString()
                 << " for " << dbname;
    return;
}

Status ShardServerManager::getFailedNonShardedChunks(std::string& dbname,
                                                     std::list<ChunkType>& chunkList) {
    stdx::lock_guard<stdx::mutex> mapLock(_nonShardedChunksMutex);
    if (_nonShardedChunksMap.empty()) {
        index_LOG(0) << "[ShardServerManager] _nonShardedChunksMap is empty.";
        return Status(ErrorCodes::ChunkNotFound, "No failed non sharded chunks needs to handle");
    }
    unordered_map<string, std::list<ChunkType>>::iterator itMap = _nonShardedChunksMap.begin();
    dbname = itMap->first;
    chunkList = itMap->second;
    _nonShardedChunksMap.erase(itMap);
    index_LOG(1) << "[ShardServerManager] finish find database " << dbname << " with "
                 << chunkList.size() << " chunks !";
    return Status::OK();
}

Status ShardServerManager::_loadFailedDataBases(OperationContext* txn) {

    // check shards.databases primary
    index_LOG(0) << "[ShardServerManager] begin to load failed databases !";
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
        index_err()
            << "[ShardServerManager] fail to find failed databases in config.databases due to "
            << findDBStatus.getStatus();
        return findDBStatus.getStatus();
    }


    const auto dbDocs = findDBStatus.getValue().docs;
    for (BSONObj dbDoc : dbDocs) {
        auto dbParseStatus = DatabaseType::fromBSON(dbDoc);
        if (!dbParseStatus.isOK()) {
            index_err() << "[ShardServerManager] from bson " << dbDoc << " failed due to "
                        << dbParseStatus.getStatus();
            return dbParseStatus.getStatus();
        }
        DatabaseType dbType = dbParseStatus.getValue();
        std::string dbname = dbType.getName();

        // for db does not have chunks
        ShardId shardId = dbType.getPrimary();
        auto getShardStatus = getShardTypeByShardId(shardId);

        std::list<ChunkType> chunkList;
        getAllNonShardChunks(txn, dbType, chunkList);

        if (!chunkList.empty() || !getShardStatus.isOK()) {
            index_log() << "[ShardServerManager] put db " << dbname << " into _nonShardedChunksMap";
            processFailedDatabase(dbname, chunkList);
        }
    }

    return Status::OK();
}

void ShardServerManager::getAllNonShardChunks(OperationContext* txn,
                                              DatabaseType& dbType,
                                              std::list<ChunkType>& chunkList) {
    std::string dbname = dbType.getName();
    BSONObjBuilder collQuery;
    std::string ns = "^";
    ns += dbname;
    ns += "\\.";
    collQuery.appendRegex(CollectionType::fullNs(), ns);
    collQuery.append(CollectionType::tabType(),
                     static_cast<std::underlying_type<CollectionType::TableType>::type>(
                         CollectionType::TableType::kNonShard));

    auto findStatus = Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
        txn,
        kConfigPrimarySelector,
        repl::ReadConcernLevel::kLocalReadConcern,
        NamespaceString(CollectionType::ConfigNS),
        collQuery.obj(),
        BSONObj(),
        boost::none);
    if (!findStatus.isOK()) {
        index_err() << "[ShardServerManager] fail to get nonShard collection due to "
                    << findStatus.getStatus();
        return;
    }
    bool shouldTakeover = false;
    const auto collDocs = findStatus.getValue().docs;
    for (BSONObj collDoc : collDocs) {

        if (collDoc.getBoolField("dropped") || !collDoc.getBoolField("created")) {
            continue;
        }

        BSONObjBuilder chunkFilter;
        chunkFilter.append(ChunkType::rootFolder(),
                           BSON("$ne"
                                << "stalerootfolder"));
        chunkFilter.append(ChunkType::status(),
                           BSON("$ne" << static_cast<int>(ChunkType::ChunkStatus::kDisabled)));
        chunkFilter.append(ChunkType::ns(), collDoc["_id"].String());


        auto findChunkStatus =
            Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                txn,
                kConfigPrimarySelector,
                repl::ReadConcernLevel::kLocalReadConcern,
                NamespaceString(ChunkType::ConfigNS),
                chunkFilter.obj(),
                BSONObj(),
                boost::none);
        if (!findChunkStatus.isOK()) {
            index_log() << "[ShardServerManager] fail to get chunks for collection ";
            continue;
        }
        const auto chunkDocs = findChunkStatus.getValue().docs;
        if (chunkDocs.size() == 0) {
            index_log() << "[ShardServerManager] collection has no chunks . ";
            continue;
        }
        auto chunkParseStatus = ChunkType::fromBSON(chunkDocs.front());
        ChunkType chunk = chunkParseStatus.getValue();

        if (!shouldTakeover) {
            ShardId shardId = chunk.getShard();
            std::string processIdent = chunk.getProcessIdentity();
            ShardIdent shard = ShardIdent(shardId, processIdent);

            if (!isShardServerAlive(shard) ||
                chunk.getStatus() == ChunkType::ChunkStatus::kOffloaded ||
                dbType.getPrimary() != shardId) {
                shouldTakeover = true;
            }
        }

        chunkList.push_back(chunk);
    }

    if (!shouldTakeover) {
        chunkList.clear();
    }
}

void ShardServerManager::addNonShardMap(std::string& dbname, std::list<ChunkType>& chunkList) {
    stdx::lock_guard<stdx::mutex> mapLock(_nonShardedChunksMutex);
    _nonShardedChunksMap[dbname] = chunkList;
}

void ShardServerManager::rollBackAssign(OperationContext* txn,
                                        std::list<ChunkType>& chunkSuccess,
                                        std::list<ChunkType>& chunkList) {

    Status getShardStatus = Status(ErrorCodes::InternalError, "Internal error running command");
    HostAndPort host;
    bool isShardAlive = true;
    index_LOG(0) << "[ShardServerManager] begin to roll back assign ";
    auto guard = MakeGuard([&] {
        for (ChunkType chunk : chunkList) {
            auto updateChunkStatus = grid.catalogClient(txn)->updateConfigDocument(
                txn,
                ChunkType::ConfigNS,
                BSON(ChunkType::name(chunk.getID())),
                chunk.toBSON(),
                false,
                ShardingCatalogClient::kMajorityWriteConcern);
            if (updateChunkStatus.isOK()) {
                index_LOG(0) << "[ShardServerManager] finish to update chunk to " << chunk.getID();
            }
        }
    });
    if (chunkSuccess.size() == 0) {
        index_LOG(2) << "[ShardServerManager] no chunks to roll back.";
        return;
    }
    ShardId targetShard;
    for (ChunkType chunk : chunkSuccess) {
        targetShard = chunk.getShard();
        break;
    }

    while (!getShardStatus.isOK()) {

        if (grid.catalogManager()->getShardServerManager()->_stopRequested()) {
            return;
        }

        auto ShardStatus = grid.shardRegistry()->getShard(txn, targetShard.toString());
        if (ShardStatus.getStatus().isOK()) {
            const auto shard = ShardStatus.getValue();
            const auto hostCS = shard->getConnString();
            invariant(hostCS.getServers().size() == 1);
            host = hostCS.getServers().front();
            break;
        }
        if (ShardStatus.getStatus() == ErrorCodes::ShardNotFound) {
            index_LOG(2) << "[ShardServerManager] shard " << targetShard.toString() << " not found";
            isShardAlive = false;
            break;
        }
        index_log() << "[ShardServerManager] fail to get shard cause by "
                    << ShardStatus.getStatus();
    }

    for (ChunkType& chunkType : chunkSuccess) {
        Status offloadStatus = Status(ErrorCodes::InternalError, "Internal error running command");
        while (!offloadStatus.isOK() && isShardAlive) {
            offloadStatus = Balancer::get(txn)->offloadChunk(txn, chunkType, true);
            if (offloadStatus == ErrorCodes::ShardNotFound) {
                isShardAlive = false;
                break;
            }
            auto findStatus =
                grid.catalogManager()->findShardByHost(txn, ShardType::ConfigNS, host.toString());
            if (findStatus.isOK()) {
                if (findStatus.getValue().getState() == ShardType::ShardState::kShardFault) {
                    isShardAlive = false;
                    break;
                }
            }
        }
        if (!isShardAlive) {
            break;
        }
    }


    index_LOG(0) << "[ShardServerManager] finish to roll back assign";
}


void ShardServerManager::processFailedDatabase(std::string& dbname,
                                               std::list<ChunkType>& chunkList) {
    addNonShardMap(dbname, chunkList);
    _takeOverNonShardThreadPool->schedule(ShardServerManager::takeOverNonShardedChunk);
}

StatusWith<bool> ShardServerManager::hasTakeOverTask(OperationContext* txn) {
    BSONObjBuilder failShardChunkFilter{};
    failShardChunkFilter.append(ChunkType::status(),
                                static_cast<std::underlying_type<ChunkType::ChunkStatus>::type>(
                                    ChunkType::ChunkStatus::kOffloaded));
    auto findChunksStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            kConfigReadSelector,
            repl::ReadConcernLevel::kMajorityReadConcern,
            NamespaceString(ChunkType::ConfigNS),
            failShardChunkFilter.obj(),
            BSONObj(),  // no sort
            boost::none);
    if (!findChunksStatus.isOK()) {
        index_err() << "[ShardServerManager] Failed to read " << ChunkType::ConfigNS
                    << " collection documents due to " << findChunksStatus.getStatus();
        if (findChunksStatus.getStatus().code() == 40136) {
            return true;
        }
        return Status(findChunksStatus.getStatus().code(),
                      stream() << "Failed to read " << ChunkType::ConfigNS
                               << " collection documents"
                               << causedBy(findChunksStatus.getStatus()));
    }
    auto chunks = std::move(findChunksStatus.getValue().docs);
    if (chunks.size() != 0) {
        return true;
    }

    return false;
}

Status ShardServerManager::enableFailOver(OperationContext* txn, bool enabled) {
    if (!hasInitialized()) {
        index_warning() << "[ShardServerManager] primary config server has not transition to "
                           "primary completely.";
        return Status{ErrorCodes::NotYetInitialized, "not yet initialized to primary"};
    }

    stdx::lock_guard<stdx::mutex> flagLock(enableMutex);
    BSONObjBuilder updateBuilder{};
    updateBuilder.append("enabled", enabled);
    auto updStatus = Grid::get(txn)->catalogClient(txn)->updateConfigDocument(
        txn,
        "config.settings",
        BSON("_id"
             << "failover"),
        BSON("$set" << updateBuilder.obj()),
        true,
        ShardingCatalogClient::kMajorityWriteConcern);
    if (!updStatus.isOK()) {
        index_log() << "[ShardServerManager] fail to update config.settings due to "
                    << updStatus.getStatus();
        return updStatus.getStatus();
    }
    if (enabled) {
        failoverEnabled = true;
        return Status::OK();
    }

    failoverEnabled = false;
    bool hasTask = true;

    while (hasTask) {
        auto status = hasTakeOverTask(txn);
        if (!status.isOK()) {
            index_LOG(0) << "[ShardServerManager] can't get take over task cause by "
                         << status.getStatus();
            return status.getStatus();
        }
        hasTask = status.getValue();
    }

    return Status::OK();
}

bool ShardServerManager::getEnableFlag() {
    stdx::lock_guard<stdx::mutex> flagLock(enableMutex);
    return failoverEnabled;
}

Status ShardServerManager::_loadEnableFlag(OperationContext* txn) {
    index_log() << "[ShardServerManager] begin to load enable flag.";
    stdx::lock_guard<stdx::mutex> flagLock(enableMutex);
    auto findStatus = Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
        txn,
        kConfigReadSelector,
        repl::ReadConcernLevel::kMajorityReadConcern,
        NamespaceString("config.settings"),
        BSON("_id"
             << "failover"),  // no query filter
        BSONObj(),            // no sort
        boost::none);
    if (!findStatus.isOK()) {
        index_err() << "[ShardServerManager] Failed to read "
                    << "config.settings"
                    << " collection documents" << causedBy(findStatus.getStatus());
        return Status(findStatus.getStatus().code(),
                      stream() << "Failed to read "
                               << "config.settings"
                               << " collection documents"
                               << causedBy(findStatus.getStatus()));
    }

    auto settings = std::move(findStatus.getValue().docs);
    string failover = "failover";
    for (const BSONObj& doc : settings) {
        if (doc.getField("_id").toString(false).find(failover) != std::string::npos) {
            failoverEnabled = doc.getBoolField("enabled");
            index_log() << "set failover enable flag to " << failoverEnabled;
        }
    }
    return Status::OK();
}

void ShardServerManager::shutDown() {
    stdx::lock_guard<stdx::mutex> scopedLock(_failureDetectionThreadStateMutex);
    _failureDetectionThreadState = kShutting;
}

bool ShardServerManager::isBelongToSharded(OperationContext* txn, const ChunkType& chunk) {
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
    if (!findCollectionStatus.isOK()) {
        index_err() << "[ShardServerManager] fail to find " << coll
                    << " in config.collections due to " << findCollectionStatus.getStatus();
        return false;
    }
    const auto& docs = findCollectionStatus.getValue().docs;
    if (docs.empty()) {
        index_warning() << "[ShardServerManager] chunk " << chunk.getID() << " without collection.";
        return false;
    }
    auto parseCollStatus = CollectionType::fromBSON(docs.front());
    if (!parseCollStatus.isOK()) {
        index_err() << "[ShardServerManager] fail to parse cause by "
                    << parseCollStatus.getStatus();
        return false;
    }
    CollectionType collType = parseCollStatus.getValue();
    if (collType.getDropped() || !collType.getCreated()) {
        return false;
    }
    if (collType.getTabType() == CollectionType::TableType::kSharded) {
        return true;
    }
    return false;
}


Status ShardServerManager::removeShardTypeByShardName(const string& shardName) {
    // traverse _shardServerNodeMap to get the shardType
    {
        stdx::lock_guard<stdx::mutex> shardServerNodeMapLock(_shardServerNodeMapMutex);
        unordered_map<string, list<ShardInfo>>::iterator itMap =
            _shardServerNodeMap.begin();  // lint !e60
        while (itMap != _shardServerNodeMap.end()) {
            list<ShardInfo>::iterator itList = itMap->second.begin();
            for (itList = itMap->second.begin(); itList != itMap->second.end(); ++itList) {
                if (itList->shard.getName().compare(shardName) == 0) {
                    itList = itMap->second.erase(itList);
                    break;
                }
            }
            itMap++;
        }
    }

    return Status::OK();
}


StatusWith<bool> ShardServerManager::checkTakeoverShard(OperationContext* txn,ShardId& shardId) {
    const auto findShardStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            kConfigPrimarySelector,
            repl::ReadConcernLevel::kLocalReadConcern,
            NamespaceString(ShardType::ConfigNS),
            BSON(ShardType::name() << shardId.toString()),
            BSONObj(),
            boost::none);  // no limit 
    if (!findShardStatus.isOK()) {
        return Status(findShardStatus.getStatus().code(),
                      str::stream() << "Failed to find existing shards during addShard"
                                    << causedBy(findShardStatus.getStatus()));
    }

    const auto shardDocs = findShardStatus.getValue().docs;

    if (shardDocs.size() > 1) {
        return Status(ErrorCodes::TooManyMatchingDocuments,
                      str::stream() << "More than one document for shard " << shardId.toString()
                                    << " in config databases");
    }

    if (shardDocs.size() == 0) {
        //grid.catalogManager()->getShardServerManager()->removeShardTypeByShardId(shardId);
        return Status(ErrorCodes::ShardNotFound,
                      str::stream() << "Shard " << shardId.toString() << " does not exist");
    }

    auto existingShardParseStatus = ShardType::fromBSON(shardDocs.front());
    if (!existingShardParseStatus.isOK()) {
        return existingShardParseStatus.getStatus();
    }

    ShardType existingShard = existingShardParseStatus.getValue();
    if (existingShard.getDraining()) {
        std::string errmsg = str::stream() << "shard: " << existingShard.toString() << " is on draining mode.";
        return Status(ErrorCodes::ShardServerNotFound, errmsg);        
    }
    return true;   
    
}


}  // namesapce mongo

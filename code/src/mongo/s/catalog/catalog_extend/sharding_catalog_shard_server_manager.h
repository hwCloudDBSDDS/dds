#pragma once

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/client/read_preference.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/platform/random.h"
#include "mongo/s/catalog/sharding_catalog_manager.h"
#include "mongo/s/catalog/type_shard_server.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/time_support.h"
#include <cstdint>
#include <list>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "mongo/s/catalog/type_chunk.h"
// add
#include "mongo/s/catalog/type_database.h"


#define GET_EVENTID(chunk) ((chunk).getName()) + (std::to_string((chunk).getVersion().toLong()))

namespace mongo {

class ShardServerType;
class ConnectionString;
class Status;
template <typename T>  // used for StatusWith
class StatusWith;
class HostAndPort;
class ThreadPool;
class RootPlogList;
using std::shared_ptr;
using std::string;
/**
 * Implements the shard server manager for config servers.
 */
class ShardServerManager {
    MONGO_DISALLOW_COPYING(ShardServerManager);

public:
    struct ShardServerInfo {
        ShardServerType shardServer;
        Date_t heartbeatTime;
    };

    struct ShardInfo {
        ShardType shard;
        Date_t heartbeatTime;
    };

    ShardServerManager();

    virtual ~ShardServerManager();

    /**
     * Invoked when the config server primary enters the 'PRIMARY' state and is invoked while the
     * caller is holding the global X lock. Kicks off the main failure detection thread and returns
     * immediately.
     *
     * Must only be called if the failure detection is in the stopped state (i.e., just constructed
     * or
     * onDrainComplete has been called before). Any code in this call must not try to acquire any
     * locks or to wait on operations, which acquire locks.
     */
    void onTransitionToPrimary(OperationContext* txn);

    /**
     * Invoked when this node which is currently serving as a 'PRIMARY' steps down and is invoked
     * while the global X lock is held. Requests the main failure detection thread to stop and
     * returns
     * immediately without waiting for it to terminate.
     *
     * This method might be called multiple times in succession, which is what happens as a result
     * of incomplete transition to primary so it is resilient to that.
     *
     * The onDrainComplete method must be called afterwards in order to wait for the main failure
     * detection
     * thread to terminate and to allow onTransitionToPrimary to be called again.
     */
    void onStepDownFromPrimary();

    /**
     * Invoked when a node on its way to becoming a primary finishes draining and is about to
     * acquire the global X lock in order to allow writes. Waits for the failure detection thread to
     * terminate and primes the failure detection so that onTransitionToPrimary can be called.
     *
     * This method is called without any locks held.
     */
    void onDrainComplete(OperationContext* txn);

    StatusWith<stdx::unordered_map<std::string, std::list<ShardInfo>>> getShardServerNodeMap(
        OperationContext* txn);


    /**
     * Generates a unique name to be given to a newly added shard.
     */
    StatusWith<std::string> generateNewShardName(OperationContext* txn);

    // Determine shard state in register flow
    // if shard already finishes register, parameter shard will be set as
    Status determineShardState(OperationContext* txn,
                               const ConnectionString& shardServerConn,
                               const std::string& processIdentity,
                               bool& retryRequest,
                               ShardType::ShardState& currentState,
                               ShardType& existingShard);

    void insOrUpdShardInfoInMemory(const std::string& shardServerNodeIP,
                                   const ShardInfo& shardInfo);

    Status processFailedPreviousShardServer(const HostAndPort& hostAndPort,
                                            const std::string& processIdentity);
    /*
     * Insert shard server information to shard server view.
     */
    StatusWith<BSONObj> addShardDocument(OperationContext* txn,
                                         const ConnectionString& conn,
                                         const std::string& extendIPs,
                                         const std::string& processIdentity);

    /*
     * Set last heartbeat time for target shard server.
     */
    Status setLastHeartbeatTime(OperationContext* txn,
                                const HostAndPort& hostAndPort,
                                const Date_t& time);

    StatusWith<Date_t> getLastHeartbeatTime(OperationContext* txn, const HostAndPort& hostAndPort);

    /**
     * Get a failed shard server for handleFailedShardServer().
     */
    Status getOneFailedShardServer(ShardType& failedShardServer);

    void removeShardFromFailedList(std::string shardName);

    Status markFailedShardDocument(OperationContext* txn, const ShardType& failedShard);
    /**
     * Take over a shard for a failed non-standby shard server.
     */
    Status takeOverShard(OperationContext* txn, const ShardType& failedShard);

    StatusWith<ShardType> getShardTypeByShardId(const ShardId& shardId);
    StatusWith<ShardType> getShardTypeByHostInfo(const std::string& host);
    StatusWith<ShardType> getShardTypeByHostInfo_inlock(const std::string& host);
    StatusWith<ShardType> getTakeOverShard(OperationContext* txn, bool userRequest = false);
    Status waitForSourceShard(OperationContext* txn,const ShardId& sourceShard);
    StatusWith<ShardType> getPrimaryShard(OperationContext* txn);
    StatusWith<ShardType> getAnActiveShard();

    // determine whether the shard server is alive
    bool isShardServerAlive(const ShardIdent& shardIdent);
    StatusWith<std::string> updShardInMemory(const std::string& shardServerNodeIP,
                                             const ShardType& shard);
    /**
     * Handle a failed shard server.
     */
    static void handleFailedShardServer();

    // Handle a failed chunk by thread pool
    static void takeOverFailedChunk();
    void processFailedChunk(const ChunkType& chunk);
    Status getOneFailedChunk(ChunkType& chunk);
    Status preProcessFailedChunks(OperationContext* txn, const ShardType& failedShard);

    StatusWith<std::string> updateShardInMemory(const HostAndPort& hostandport,
                                                const ShardType& shard);
    Status setDatabasePrimary(OperationContext* txn, std::string& dbname, ShardId& primaryShard);
    StatusWith<std::vector<DatabaseType>> getDbsOnFailedShard(OperationContext* txn,
                                                              const std::string& failShardName);
    static void takeOverNonShardedChunk();
    Status getFailedNonShardedChunks(std::string& dbname, std::list<ChunkType>& chunkList);
    void addNonShardMap(std::string& dbname, std::list<ChunkType>& chunkList);
    void rollBackAssign(OperationContext* txn,
                        std::list<ChunkType>& chunkSuccess,
                        std::list<ChunkType>& chunkList);
    void processFailedDatabase(std::string& dbname, std::list<ChunkType>& chunkList);
    Status enableFailOver(OperationContext* txn, bool enabled);
    void shutDown();
    Status insertOrUpdateShardMap(ShardType& shard, const ConnectionString& conn);
    bool isBelongToSharded(OperationContext* txn, const ChunkType& chunk);
    StatusWith<bool> checkTakeoverShard(OperationContext* txn,ShardId& shardId);
    Status removeShardTypeByShardName(const string& shardName);

private:
    /**
     * Possible runtime states of the failureDetectionThread.
     * The comments indicate the allowed next state.
     */
    enum FailureDetectionThreadState {
        kStopped,   // kRunning
        kRunning,   // kStopping
        kStopping,  // kStopped
        kShutting,
    };

    /*
     * Initialize shardServer view in memory from db.
     */
    Status _init(OperationContext* txn);

    bool hasInitialized();
    bool hasShardLoaded();
    Status _loadShardServerView(OperationContext* txn,
                                std::vector<std::string>& shardNames,
                                std::vector<std::string>& shardProcessIds);

    Status _loadFailedDataBases(OperationContext* txn);
    void getAllNonShardChunks(OperationContext* txn,
                              DatabaseType& dbType,
                              std::list<ChunkType>& chunkList);
    Status _loadFailedChunks(OperationContext* txn,
                             const std::vector<std::string>& shardNames,
                             const std::vector<std::string>& shardProcessIds);

    /*
     * Check last heartbeat time for all shard servers, record failed shard servers
     * in a list, and return the number of failed shard servers.
     */
    StatusWith<uint64_t> _checkHeartbeatTimeOut(OperationContext* txn);

    /**
     * The main failure detection loop, which runs in a separate thread.
     */
    void _failureDetectionMainThread();

    /**
     * Checks whether the failure detection main thread has been requested to stop.
     */
    bool _stopRequested();

    /**
     * Signals the beginning and end of a failure detection round.
     */
    void _beginRound(OperationContext* txn);
    void _endRound(OperationContext* txn, Milliseconds waitTimeout);

    /**
     * Blocks the caller for the specified timeout or until the failure detection
     * condition variable is signaled, whichever comes first.
     */
    void _sleepFor(OperationContext* txn, Milliseconds waitTimeout);

    void _printShardServerNodeView();

    StatusWith<HostAndPort> _validateSingleConnectionString(const ConnectionString& conn);
    void _sendShardClusterDegradeAlarm();

    void _recoverShardClusterDegradeAlarm();
    void _removeShardDocInConfig(OperationContext* txn, ShardType shard, const std::string& ns);

    StatusWith<bool> hasTakeOverTask(OperationContext* txn);
    bool getEnableFlag();
    Status _loadEnableFlag(OperationContext* txn);

    // Map of shard server nodes
    typedef stdx::unordered_map<std::string, std::list<ShardInfo>> ShardServerMap;
    ShardServerMap _shardServerNodeMap;
    // Number of shard server in cluster, used for shard cluster degrade
    // At now, it is maintained by primary config server. Maybe an item in configuration table
    // later.
    uint64_t _maxShardServerNum = 0;
    stdx::mutex _shardServerNodeMapMutex;

    // Number for generating shard name
    uint64_t _maxShardNameDigit = 0;
    stdx::mutex _shardNameMutex;

    // Flag indicating if we made an initialization
    bool _hasInited = false;
    stdx::mutex _initMutex;
    bool _hasShardLoaded = false;
    stdx::mutex _shardLoadMutex;
    // The main thread for failure detection
    stdx::thread _failureDetectionThread;

    // Indicates the current state of the failureDetectionThread
    FailureDetectionThreadState _failureDetectionThreadState = kStopped;
    // Protects the FailureDetectionThreadState above
    stdx::mutex _failureDetectionThreadStateMutex;
    // Condition variable, which is signalled every time the above runtime state of
    // the failureDetectionThread changes.
    stdx::condition_variable _failureDetectionThreadCondVar;

    // The operation context of the failureDetectionThread. This value may only be available in the
    // kRunning state and is used to force interrupt of any blocking calls made by the
    // thread.
    OperationContext* _threadOperationContext = nullptr;

    // Indicates whether the failureDetectionThread is currently executing a round
    bool _inFailureDetectionRound = false;

    // Thread pool for failure hanlding
    ThreadPool* _failureHandlingThreadPool;

    // Thread pool for handling failed chunks
    ThreadPool* _failedChunksThreadPool;
    // Thread pool for set primary for databases
    ThreadPool* _takeOverNonShardThreadPool;

    // List of failed shard servers detected by failureDetectionThread
    // std::list<ShardServerType> _failedShardServerList;
    struct FailedShardType {
        ShardType shard;
        bool isProcessing = false;
    };
    std::list<FailedShardType> _failedShardServerList;

    // Protects the FailureDetectionThreadState above
    stdx::mutex _failedShardServerListMutex;

    // List of failed chunks
    stdx::mutex _failedChunksMutex;
    std::list<ChunkType> _failedChunks;
    // map of non-shard chunks
    stdx::mutex _nonShardedChunksMutex;
    stdx::unordered_map<std::string, std::list<ChunkType>> _nonShardedChunksMap;
    stdx::mutex isProcessMapMutex;
    stdx::unordered_map<std::string, bool> isProcessMap;

    // flag of failover
    stdx::mutex enableMutex;
    bool failoverEnabled = true;

    // Add a failed chunk into _failedChunks
    void _addFailedChunk(const ChunkType& chunk);
    // Uses for get random number during failover
    SecureRandom* _pSecureRandom;

    // remained for future, default name is "IndexCluster"
    std::string _clusterName = "IndexCluster";
    // config server extendip consists ip+port, so extract ip as _extendIp
    std::string _extendIp = "";
    // flag for corrupted document in collections(config db)
    // No accumulative time. Once there is corrupted document, report the alarm immediately
    // If there is no alarm sended during loading data, it will recover the alarm
    bool _corrShardDocHasSend = false;
    bool _corrChunkDocHasSend = false;

    uint32_t _shardClusterDegradeCount = 0;
    bool _shardClusterDegradeHasSend = true;
};

}  // namepsace mongo

#pragma once
#include <mongo/stdx/memory.h>
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "Chunk/PartitionedRocksEngine.h"
#include "mongo/s/write_ops/batched_command_request.h"
#include "Chunk/i_shared_resource_manager.h"

namespace mongo {

class ChunkType;
class OperationContext;
class Status;
class PartitionedRocksEngine;


/**
 * The gc manager is a background task that tries to track the shared files between chunks on a signle shard server.
 *
 * The  gc manager does act continuously but in "rounds". At a given round, it would decide if
 * there is shared file by talking with the shared resouce manager on each chunk. It would issue a request for add/dec 
 * reference for the file per round, if it found so.
 */
class GCManager {
    
    const std::string GcNS;

public:
    //GCManager(PartitionedRocksEngine* engine);

    ~GCManager();

    static GCManager& Get(){
       static GCManager instance;
       return instance;

    }

    void Start() ;
    
    void Stop() ;

    mongo::PartitionedRocksEngine* getEngine(){return _engine;};
    void setEngine (PartitionedRocksEngine* engine);

    /**
     * Appends the runtime state of the gc manager instance to the specified builder.
     */
    void report(OperationContext* txn, BSONObjBuilder* builder);

private:
    /**
     * Possible runtime states of the gc manager. The comments indicate the allowed next state.
     */
    enum State {
        kStopped,   // kRunning
        kRunning,   // kStopping
        kStopping,  // kStopped
    };

    GCManager(){};
    GCManager(GCManager const&)      = delete;
    void operator=(GCManager const&)  = delete;

    /**
     * The main gc manager loop, which runs in a separate thread.
     */
    void _mainThread();

    /**
     * Checks whether the gc manager main thread has been requested to stop.
     */
    bool _stopRequested();

    /**
     * Signals the beginning and end of a GC round.
     */
    void _beginRound(OperationContext* txn);
    void _endRound(OperationContext* txn, Seconds waitTimeout);

    /**
     * Blocks the caller for the specified timeout or until the gc manager condition variable is
     * signaled, whichever comes first.
     */
    void _sleepFor(OperationContext* txn, Seconds waitTimeout);


    // collect shared resource (plog or sstfile) operations from each chunk
    //bool GetSharedResourcesOperations(std::shared_ptr<TransactionLog::SharedResourceOperationLogRecordProvider> provider, 
    //                                        std::vector<TransactionLog::SharedResourceOperation>& shareResources, 
    //                                        int maxBatchMaxSize);


    //send shared reource add/remove ref request and add/remove records in GC collection
    Status sendSharedResourceRefUpdate (OperationContext* txn,  const uint64_t chunkID,
                                                            std::vector<mongo::TransactionLog::SharedResourceReferenceOperation> unprocessedRecords,
                                                            uint64_t lastPrcossedLSN);

    // For recording the reference count of GC object (plog or sstfile).
    // It is implemented by cluster writing, because the GC reference is recorded into a special sharded collection 
    // who has chunks across many shard servers.
    // The batchWriteObj could be a batch write with many documents request, like insert/update/delete,
    // so in the result, it will include result for every write document
   // Status writeReferenceLog(OperationContext* txn,
   //                             const BSONObj& batchWriteObj,
   //                             BSONObjBuilder& result);
    Status writeReferenceLog(
        OperationContext* txn,
        const BatchedCommandRequest& request) ;

    // For reading the reference count of GC object (plog or sstfile)
    // We have a GC peridoc job on each shard server to scan the reference of GC object, and delete it if there is no reference on.
    // TODO: since each SS has a GC manager, it could just read  GC reference log on itself, no need to do cluster reading
    Status readReferenceLog(OperationContext* txn,
                                const BSONObj& query,
                                std::vector<BSONObj>& refdocs);

    // Protects the state below
    stdx::mutex _mutex;

    // Indicates the current state of the gc manager
    State _state{kStopped};

    // The main gc manager thread
    stdx::thread _thread;

    // The operation context of the main gc manager thread. This value may only be available in the
    // kRunning state and is used to force interrupt of any blocking calls made by the gc manager
    // thread.
    OperationContext* _threadOperationContext{nullptr};

    // Indicates whether the gc manager is currently executing a gc manager round
    bool _inGCRound{false};

    // Counts the number of balancing rounds performed since the gc manager thread was first activated
    int64_t _numGCRounds{0};

    // Condition variable, which is signalled every time the above runtime state of the balancer
    // changes (in particular, state/balancer round and number of balancer rounds).
    stdx::condition_variable _condVar;

    PartitionedRocksEngine* _engine;

};

}  

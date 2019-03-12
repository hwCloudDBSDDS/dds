#pragma once
#include <mongo/stdx/memory.h>
#include <atomic>
#include "Chunk/PartitionedRocksEngine.h"
#include "Chunk/i_shared_resource_manager.h"
#include "gc_common.h"
#include "mongo/s/write_ops/batched_command_request.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"

namespace mongo {

    class ChunkType;
    class OperationContext;
    class Status;
    class PartitionedRocksEngine;
    class GCSettings;

    //
    // The gc client is a background task that tries to track the shared files between chunks on a
    // single shard server.
    //
    // The gc client does act continuously but in "rounds". At a given round, it would decide if
    // there is shared plog by talking with the shared resouce manager on each chunk. It would issue
    // a request for add/dec
    // reference for the plog per round, if it found so.
    //
    // Each shard server has an independent gc client thread
    //
    class GCClient {
    public:
        GCClient(PartitionedRocksEngine* engine);

        GCClient(GCClient const&) = delete;
        void operator=(GCClient const&) = delete;

        ~GCClient();

        void start();

        void stop();

        mongo::PartitionedRocksEngine* getEngine() { return _engine; }

        bool pause();
        bool continues();

        bool isRuning();

    private:
        //
        // Possible runtime states of the gc client. The comments indicate the allowed next state.
        //
        enum State {
            kStopped,  // kStopping
            kRunning,  // kRunning
            kPause,
        };

        //
        // The main gc client loop, which runs in a separate thread.
        //
        void _mainThread();

        //
        // Checks whether the gc client main thread has been requested to stop.
        //
        bool _stopRequested();

        //
        // Signals the beginning and end of a GC round.
        //
        void _beginRound(OperationContext* txn);
        void _endRound(OperationContext* txn, Seconds waitTimeout);

        //
        // Blocks the caller for the specified timeout or until the gc client condition variable is
        // signaled, whichever comes first.
        //
        void _sleepFor(OperationContext* txn, Seconds waitTimeout);

        //
        // This func is just for test/debug
        //
        Status _sendSharedResourceRefUpdateForTest(OperationContext* txn);

        //
        // Send shared reource add/remove ref request and add/remove records in GC collection
        //
        Status _sendSharedResourceRefUpdate(
            OperationContext* txn, const uint64_t chunkId,
            const std::vector<mongo::TransactionLog::SharedResourceReferenceOperation>&
                unprocessedRecords,
            const std::string& ident, uint64_t& lastPrcossedLSN);

        //
        // Indicates the current state of the gc client
        //
        std::atomic<State> _state{kStopped};

        //
        // Protects the state below
        //
        stdx::mutex _mutex;

        //
        // The main gc client thread
        //
        stdx::thread _thread;

        //
        // The operation context of the main gc client thread. This value may only be available in
        // the
        // kRunning state and is used to force interrupt of any blocking calls made by the gc client
        // thread.
        //
        OperationContext* _threadOperationContext{nullptr};

        //
        // Indicates whether the gc client is currently executing a gc client round
        //
        bool _inGCRound{false};

        //
        // Condition variable, which is signalled every time the above runtime state of the balancer
        // changes (in particular, state/balancer round and number of balancer rounds).
        //
        stdx::condition_variable _condVar;

        mongo::GCSettings _gcSettings;

        PartitionedRocksEngine* _engine;

        std::string _lastProcessedChunkNs = "";
    };
}

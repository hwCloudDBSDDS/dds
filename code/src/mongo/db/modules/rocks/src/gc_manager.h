#pragma once
#include <mongo/stdx/memory.h>
#include <atomic>
#include "Chunk/PartitionedRocksEngine.h"
#include "Chunk/i_shared_resource_manager.h"
#include "gc_common.h"
#include "mongo/client/dbclientcursor.h"
#include "mongo/db/dbdirectclient.h"
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
    // The gc manager is a background task that tries find and delete obsolete shared plogs.
    //
    // The gc manager does act continuously but in "rounds". At a given round, it would decide if
    // a shared plog is obsoleted by scanning the gc.references collection. If it found, then delete
    // the shared plog.
    //
    // Each gc.references chunk instance has an independent gc manager thread
    //
    class GCManager {
    public:
        GCManager(GCManager const&) = delete;

        void operator=(GCManager const&) = delete;

        GCManager();

        ~GCManager();

        void start();

        void stop();

        bool pause();
        bool continues();

        bool isRuning();

        void setGCInfo(const std::string& ns, const std::string& dataPath);

    private:
        //
        // Possible runtime states of the gc manager. The comments indicate the allowed next state.
        //
        enum State {
            kStopped,  // kStopped
            kRunning,  // kRunning
            kPause,
        };

        //
        // The main gc manager loop, which runs in a separate thread.
        //
        void _mainThread();

        //
        // Checks whether the gc manager main thread has been requested to stop.
        //
        bool _stopRequested();

        //
        // Signals the beginning and end of a GC round.
        //
        void _beginRound(OperationContext* txn);
        void _endRound(OperationContext* txn, Seconds waitTimeout);

        //
        // Blocks the caller for the specified timeout or until the gc manager condition variable is
        // signaled, whichever comes first.
        //
        void _sleepFor(OperationContext* txn, Seconds waitTimeout);

        //
        // Check if the plog is stayed enough time since it has been mark deleted
        //
        bool _checkIfStayEnoughTimeBeforeRemove(const std::string& oldDateString);

        //
        // Scan gc.reference collection to find which plog is needed to be deleted, if so, delete it
        //
        StatusWith<bool> _findAndRemoveGarbageSharedResources(OperationContext* txn);

        //
        // Build delete doc request to gc.references collection
        //
        BatchedDeleteDocument* _buildDeleteDoc(const std::string& plogIdStr);

        //
        // Build add deleted info bson-type doc to gc.removeInfo collection
        //
        BSONObj _buildAddRemoveInfoBSONDoc(const std::string& plogIdStr,
                                           const std::string& chunkView, const std::string& ident);

        void _resetRefScanningStates();

        Status _checkIfSharedResourceShouldBeRemoved(
            BSONObj docObj, std::vector<std::string>& shouldBeRemovedPlogId,
            std::map<std::string, std::string>& plogIDToChunkidView,
            std::map<std::string, std::string>& sharedIdToIdent);

        //
        // For reading the reference count of GC object (plog or sstfile)
        // We have a GC peridoc job on each shard server to scan the reference of GC object, and
        // delete it if there is no reference on.
        // TODO: since each SS has a GC manager, it could just read  GC reference log on itself, no
        // need to do cluster reading
        //
        Status _readReferenceLog(OperationContext* txn, const BSONObj& query,
                                 std::vector<std::string>& shouldBeRemovedPlogId,
                                 std::map<std::string, std::string>& plogIDToChunkidView,
                                 std::map<std::string, std::string>& sharedIdToIdent);

        rocksdb::Status delSharedResource(const std::string& sharedResource,
                                          const std::string& ident);

        std::string getSharedResourcePath(const std::string& sharedResource,
                                          const std::string& ident);

        //
        // Indicates the current state of the gc manager
        //
        std::atomic<State> _state{kStopped};

        //
        // Protects the state below
        //
        stdx::mutex _mutex;

        //
        // The main gc manager thread
        //
        stdx::thread _thread;

        //
        // The operation context of the main gc manager thread. This value may only be available in
        // the
        // kRunning state and is used to force interrupt of any blocking calls made by the gc
        // manager
        // thread.
        //
        OperationContext* _threadOperationContext{nullptr};

        //
        // Indicates whether the gc manager is currently executing a gc manager round
        //
        bool _inGCRound{false};

        //
        // Condition variable, which is signalled every time the above runtime state of the balancer
        // changes (in particular, state/balancer round and number of balancer rounds).
        //
        stdx::condition_variable _condVar;

        mongo::GCSettings _gcSettings;

        std::string _chunkNs;

        //
        // These 5 variable indicate the last scanning state of gc.refereces collection
        //
        std::unique_ptr<DBDirectClient> _client;

        std::unique_ptr<DBClientCursor> _clientCursor;

        std::string _lastPlogID = "";

        std::string _chunkIdView = "";

        bool _shouldBeRemoved = true;

        std::string _dataPath = "";

        std::string _ident = "";
    };
}

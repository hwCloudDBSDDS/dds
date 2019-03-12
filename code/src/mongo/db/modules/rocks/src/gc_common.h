#pragma once
#include <mongo/stdx/memory.h>
#include "Chunk/PartitionedRocksEngine.h"
#include "Chunk/i_shared_resource_manager.h"
#include "mongo/s/write_ops/batched_command_request.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"

namespace mongo {

    class ChunkType;
    class OperationContext;
    class Status;

    extern const int kGCMinIntervalSec;
    extern const int kGCMinShortIntervalSec;
    extern const int kGCClientDefaultRoundIntervalSec;
    extern const int kGCClientMaxBatchSize;
    extern const int kGCManagerDefaultRoundIntervalSec;
    extern const int kGCManagerMaxBatchSize;
    extern const int kGCManagerDeleteCountPerRound;
    extern const int kGCManagerSleepTimePerDeleteRound;
    extern const int kGCManagerDefaultDelayedDelSecs;
    extern const unsigned int kGCManagerMaxChunkViewLen;

    extern const Seconds kGCClientRoundDefaultInterval;
    extern const Seconds kGCManagerRoundDefaultInterval;
    extern const Seconds kGCPauseTimeout;

    // Field names and types in doc of the gc collections.
    extern const std::string kResourceIdStr;
    extern const std::string kIdentStr;
    extern const std::string kChunkIdStr;
    extern const std::string kDeletedStr;
    extern const std::string kChunkViewStr;
    extern const std::string kSharedResourcePath;
    extern const std::string kUpdateTimeStr;
    extern const std::string kDeleteTimeStr;
    extern const std::string kIdStr;

    extern const std::string GcRefCollName;
    extern const std::string GcRemoveInfoCollName;
    extern const std::string GcRefTestNs;

    extern const uint32_t MAX_TRY_TIMES;
    extern const uint32_t TRY_WAIT_TIME;

    extern const std::string GCClientThreadName;
    extern const std::string GCManagerThreadName;

    const std::string GcDbName = "Internal%Garbage%Collection";
    const std::string GcRemoveInfoNs = "Internal%Garbage%Collection.removeInfo";
    const std::string GcRefNs = "Internal%Garbage%Collection.references";

    class GCSettings {
    public:
        GCSettings() {
            _testFlag = false;
            _clearRemoveInfoFlag = false;
            _openGCClientFlag = true;
            _openGCManagerFlag = true;
            _gcClientInterval = kGCClientDefaultRoundIntervalSec;
            _gcManagerInterval = kGCManagerDefaultRoundIntervalSec;
            _gcDelayedDelSecs = kGCManagerDefaultDelayedDelSecs;
            _gcScanMaxBatchSize = kGCManagerMaxBatchSize;
        }

        void reset() {
            _testFlag = false;
            _clearRemoveInfoFlag = false;
            _openGCClientFlag = true;
            _openGCManagerFlag = true;
            _gcClientInterval = kGCClientDefaultRoundIntervalSec;
            _gcManagerInterval = kGCManagerDefaultRoundIntervalSec;
            _gcDelayedDelSecs = kGCManagerDefaultDelayedDelSecs;
            _gcScanMaxBatchSize = kGCManagerMaxBatchSize;
        }

        ~GCSettings() {}

        void setGCTestFlag(bool flag) { _testFlag = flag; }

        void setGCClearRemoveInfoFlag(bool flag) { _clearRemoveInfoFlag = flag; }

        void setGCClientOpenFlag(bool flag) { _openGCClientFlag = flag; }

        void setGCManagerOpenFlag(bool flag) { _openGCManagerFlag = flag; }

        void setGCClientInterval(int64_t gcInterval) { _gcClientInterval = gcInterval; }

        void setGCManagerInterval(int64_t gcInterval) { _gcManagerInterval = gcInterval; }

        void setGCDelayedDelSecs(int64_t gcDelayedDelSecs) { _gcDelayedDelSecs = gcDelayedDelSecs; }

        void setGCScanMaxBatchSize(int64_t gcScanMaxBatchSize) {
            _gcScanMaxBatchSize = gcScanMaxBatchSize;
        }

        bool shouldGCTest() { return _testFlag; }

        bool shouldGCClearRemoveInfoColl() { return _clearRemoveInfoFlag; }

        bool shouldGCClient() { return _openGCClientFlag; }

        bool shouldGCManager() { return _openGCManagerFlag; }

        int64_t getGCClientInterval() { return _gcClientInterval; }

        int64_t getGCManagerInterval() { return _gcManagerInterval; }

        int64_t getGCDelayedDelSecs() { return _gcDelayedDelSecs; }

        int64_t getGCScanMaxBatchSize() { return _gcScanMaxBatchSize; }

    private:
        bool _testFlag;              // decide if gc test is opened
        bool _clearRemoveInfoFlag;   // decide if should clear gc.removeInfo collection periodically
        bool _openGCClientFlag;      // decide if should open gc client func
        bool _openGCManagerFlag;     // decide if should open gc manager func
        int64_t _gcClientInterval;   // if opened, decide the gc round interval for gc client
        int64_t _gcManagerInterval;  // if opened, decide the gc round interval for gc manager
        int64_t _gcDelayedDelSecs;   // decide the delayed seconds before a plog can be deleted
        int64_t _gcScanMaxBatchSize;  // decide he max scanning number per round for gc manager
    };

    //
    // GCCommon class includes some common functions that are used by
    // GCManager and GCClient
    //
    class GCCommon {
    public:
        static Status runBatchedCommandRequestWithFixedRetry(OperationContext* txn,
                                                             const BatchedCommandRequest& request,
                                                             bool isInsert);

        static void waitForStorageEngineToBeCreated();

        //
        // reset the old operation context and create a new one
        //
        static void resetThreadOperationContext(ServiceContext::UniqueOperationContext& txn,
                                                stdx::mutex& gcMutex,
                                                OperationContext** threadOperationContext);

        //
        // Update the newest gc settings
        //
        static void updateGCSettings(OperationContext* txn, GCSettings& gcSettings);

        //
        // Build add reference bson-type doc to gc.references collection
        //
        static BSONObj buildAddRefBSONDoc(const std::string& ident, uint64_t chunkId,
                                          const SharedResourceId& plogId);

        //
        // Build remove reference update doc to gc.references collection
        //
        static BatchedUpdateDocument* buildRemoveRefUpdateDoc(const std::string& ident,
                                                              uint64_t chunkId,
                                                              const SharedResourceId& plogId);

        //
        // Check if we set gc test option
        //
        static bool isGcTestOpened();

        //
        // Used for gc func validity test, insert original references info into gc.referencesTest
        // collection
        //
        static Status insertRefUpdateToReferencesTestCollection(
            const std::vector<mongo::TransactionLog::SharedResourceReferenceOperation>&
                unprocessedRecords);

    private:
        static std::string iToStr(uint64_t id);

        static Status _getBatchedCommandRequestResult(
            BatchedCommandResponse* response, ErrorCodes::Error bypassedCode = ErrorCodes::OK);
    };
}

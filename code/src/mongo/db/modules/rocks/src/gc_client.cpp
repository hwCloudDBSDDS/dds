#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include <algorithm>
#include <string>
#include "mongo/platform/basic.h"

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/client/read_preference.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"

#include "gc_client.h"
#include "gc_common.h"

#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_catalog_entry.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/s/balancer/balancer.h"
#include "mongo/s/commands/cluster_write.h"
#include "mongo/s/grid.h"
#include "mongo/s/write_ops/batched_command_request.h"
#include "mongo/s/write_ops/batched_command_response.h"
#include "mongo/util/log.h"

namespace mongo {

    GCClient::GCClient(PartitionedRocksEngine* engine) { _engine = engine; }

    GCClient::~GCClient() {
        // The GCClient thread must have been stopped
        invariant(_state == kStopped);
    }

    bool GCClient::_stopRequested() { return (_state == kStopped); }

    void GCClient::start() {
        index_log() << "[GC Client] GC client is starting";
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        invariant(_state == kStopped);
        _state = kRunning;

        invariant(!_thread.joinable());
        invariant(!_threadOperationContext);
        _thread = stdx::thread([this] { _mainThread(); });
    }

    void GCClient::stop() {
        index_log() << "[GC Client] GC client is stopping";

        if (_state != kRunning) {
            return;
        }
        _state = kStopped;

        // Wake up it if the main thread is sleeping now
        {
            stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
            _condVar.notify_all();

            // Interrupt the GCClient thread if it has been started. We are guaranteed that the
            // operation
            // context of that thread is still alive, because we hold the GC mutex.
            if (_threadOperationContext) {
                stdx::lock_guard<Client> scopedClientLock(*_threadOperationContext->getClient());
                _threadOperationContext->markKilled(ErrorCodes::Interrupted);
            }
        }

        invariant(_thread.joinable());

        _thread.join();
        _thread = {};
    }

    bool GCClient::pause() {
        index_log() << "[GC Client] start GCClient::pause()";

        if (kRunning != _state) {
            index_err() << "[GC Client] GCClient::pause() faild current status: " << int(_state);
            return false;
        }
        _state = kPause;

        bool suc = true;
        {
            stdx::unique_lock<stdx::mutex> lock(_mutex);
            if (_inGCRound) {
                suc = _condVar.wait_for(lock, kGCPauseTimeout.toSteadyDuration(),
                                        [&] { return (_state != kPause) || !_inGCRound; });
            }
        }

        index_log() << "[GC Client] end GCClient::pause() suc: " << suc;
        return suc;
    }

    bool GCClient::continues() {
        index_log() << "[GC Client] GCClient::continues()";
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        if (kRunning == _state) {
            index_err() << "[GC Client] GCClient::continues() faild current status: kRunning";
            return true;
        } else if (kStopped == _state) {
            index_err() << "[GC Client] GCClient::continues() faild current status: kStopped";
            return false;
        } else {
            invariant(kPause == _state);
            _state = kRunning;
            return true;
        }
    }

    void GCClient::_beginRound(OperationContext* txn) {
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        _inGCRound = true;
        _condVar.notify_all();
    }

    void GCClient::_endRound(OperationContext* txn, Seconds waitTimeout) {
        {
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            _inGCRound = false;
            _condVar.notify_all();
        }

        _sleepFor(txn, waitTimeout);
    }

    bool GCClient::isRuning() {
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        return _state == kRunning;
    }

    void GCClient::_sleepFor(OperationContext* txn, Seconds waitTimeout) {
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        _condVar.wait_for(lock, waitTimeout.toSteadyDuration(), [&] { return _state != kRunning; });
    }

    Status GCClient::_sendSharedResourceRefUpdateForTest(OperationContext* txn) {
        index_log() << "[GC Client] _sendSharedResourceRefUpdateForTest end OK";
        return Status::OK();
    }

    Status GCClient::_sendSharedResourceRefUpdate(
        OperationContext* txn, const uint64_t chunkId,
        const std::vector<mongo::TransactionLog::SharedResourceReferenceOperation>&
            unprocessedRecords,
        const std::string& ident, uint64_t& lastPrcossedLSN) {
        int addCount = 0;
        int removeCount = 0;

        const WriteConcernOptions writeConcern(2, WriteConcernOptions::SyncMode::JOURNAL, Milliseconds(20000));

        std::unique_ptr<BatchedInsertRequest> addRefInsertRequest(new BatchedInsertRequest());
        addRefInsertRequest->setNS(NamespaceString(StringData(GcRefNs)));
        addRefInsertRequest->setOrdered(false);
        addRefInsertRequest->setWriteConcern(writeConcern.toBSON());

        std::unique_ptr<BatchedUpdateRequest> removeRefUpdateRequest(new BatchedUpdateRequest());
        removeRefUpdateRequest->setNS(NamespaceString(GcRefNs));
        removeRefUpdateRequest->setOrdered(false);
        removeRefUpdateRequest->setWriteConcern(writeConcern.toBSON());

        index_LOG(3) << "[GC Client] _sendSharedResourceRefUpdate: chunkID:" << chunkId
                     << " records :" << unprocessedRecords.size();
        lastPrcossedLSN = 0;
        for (const auto& operation : unprocessedRecords) {
            if (lastPrcossedLSN < operation.GetLsn()) {
                lastPrcossedLSN = operation.GetLsn();
            }

            if (operation.GetType() ==
                mongo::TransactionLog::SharedResourceOperationType::AddReference) {
                addRefInsertRequest->addToDocuments(GCCommon::buildAddRefBSONDoc(
                    ident, operation.GetParentChunkId(), operation.GetResource()));
                addCount++;

                addRefInsertRequest->addToDocuments(GCCommon::buildAddRefBSONDoc(
                    ident, operation.GetChildChunkId(), operation.GetResource()));
                addCount++;

                index_LOG(3) << "[GC Client] AddRef: lsn(" << operation.GetLsn() << ") resourceid("
                             << operation.GetResource().toString() << ") parentId("
                             << operation.GetParentChunkId() << ") childId("
                             << operation.GetChildChunkId() << ") chunkId(" << chunkId << ")";
            } else if (operation.GetType() ==
                       mongo::TransactionLog::SharedResourceOperationType::RemoveReference) {
                removeRefUpdateRequest->addToUpdates(
                    GCCommon::buildRemoveRefUpdateDoc(ident, chunkId, operation.GetResource()));
                removeCount++;

                index_LOG(3) << "[GC Client] RmvRef: lsn(" << operation.GetLsn() << ") resourceid("
                             << operation.GetResource().toString() << ") chunkId(" << chunkId
                             << ")";
            }
        }

        // try to insert the AddRef doc into gc.reference
        {
            if (addCount) {
                BatchedCommandRequest addRefRequest(addRefInsertRequest.release());
                auto status =
                    GCCommon::runBatchedCommandRequestWithFixedRetry(txn, addRefRequest, true);
                if (status != Status::OK()) {
                    index_err() << "[GC Client] failed to update the gc.references collection for "
                                   "adding reference, status("
                                << status << ")";
                    return status;
                }
            }
        }

        // try to update the corresponding RemoveRef doc in gc.reference
        {
            if (removeCount) {
                BatchedCommandRequest removeRefRequest(removeRefUpdateRequest.release());
                auto status =
                    GCCommon::runBatchedCommandRequestWithFixedRetry(txn, removeRefRequest, false);
                if (status != Status::OK()) {
                    index_err() << "[GC Client] failed to update the gc.references collection for "
                                   "removing reference, status("
                                << status << ")";
                    return status;
                }
            }
        }

        return Status::OK();
    }

    void GCClient::_mainThread() {
        Client::initThread(GCClientThreadName.c_str());
        ServiceContext::UniqueOperationContext txn;

        //
        // Make sure operation context is created after storageEngine is created
        //
        GCCommon::waitForStorageEngineToBeCreated();

        // sleep(30);
        // GCCommon::resetThreadOperationContext(txn, _mutex, &_threadOperationContext);
        //_sendSharedResourceRefUpdateForTest(_threadOperationContext); // just for debug test

        while (!_stopRequested()) {
            _beginRound(_threadOperationContext);

            if (kPause == _state) {
                _endRound(_threadOperationContext, kGCClientRoundDefaultInterval);
                continue;
            }

            //
            // Each round, we reset the operation context because it includes snapshot info, this
            // help us get the latest data in gc.removeInfo and gc.references collections.
            //
            GCCommon::resetThreadOperationContext(txn, _mutex, &_threadOperationContext);

            // GCCommon::updateGCSettings(_threadOperationContext, _gcSettings);
            if (!_gcSettings.shouldGCClient()) {
                _endRound(_threadOperationContext, kGCClientRoundDefaultInterval);
                continue;
            }

            int64_t gcRoundInterval = _gcSettings.getGCClientInterval() > kGCMinIntervalSec
                                          ? _gcSettings.getGCClientInterval()
                                          : kGCMinIntervalSec;
            int64_t gcShortRoundInterval = (gcRoundInterval / 10) > kGCMinShortIntervalSec
                                               ? (gcRoundInterval / 10)
                                               : kGCMinShortIntervalSec;
            const Seconds kGCRoundInterval(gcRoundInterval);
            //
            // Sleep between GC rounds in the case where the last round found some shared resourced
            // which needed to
            // garbage collected
            //
            const Seconds kShortGCRoundInterval(gcShortRoundInterval);

            index_LOG(1) << "[GC Client] gcRoundInterval(" << gcRoundInterval << ")";
            try {
                std::vector<mongo::TransactionLog::SharedResourceReferenceOperation>
                    unprocessedRecords;
                uint64_t chunkId = 0;
                std::string ident;
                bool toGC = getEngine()->GetSharedResourcesOperations(chunkId, unprocessedRecords,
                                                                      kGCClientMaxBatchSize,
                                                                      _lastProcessedChunkNs, ident);

                index_LOG(1) << "[GC Client] GC" << toGC << " chunkID:" << chunkId
                             << "; ident: " << ident << " records :" << unprocessedRecords.size();
                if (toGC) {
                    uint64_t lastProcessLSN = 0;
                    // auto testStatus =
                    // GCCommon::insertRefUpdateToReferencesTestCollection(chunkID,
                    // unprocessedRecords);

                    if (_sendSharedResourceRefUpdate(_threadOperationContext, chunkId,
                                                     unprocessedRecords, ident, lastProcessLSN)
                            .isOK()) {
                        index_LOG(1) << "[GC Client] before SetLastProcessedOperationLSN chunkNs("
                                     << _lastProcessedChunkNs << ") lastProcessLSN("
                                     << lastProcessLSN << ")";

                        getEngine()->SetLastProcessedOperationLSN(_lastProcessedChunkNs,
                                                                  lastProcessLSN);
                    }
                }

                _endRound(_threadOperationContext, toGC ? kShortGCRoundInterval : kGCRoundInterval);
            } catch (const std::exception& e) {
                index_err() << "[GC Client] caught exception while doing GC : " << e.what();

                // Just to match the opening statement if in log level 1
                index_LOG(1) << "*** End of GC client round";

                // Sleep a fair amount before retrying because of the error
                _endRound(_threadOperationContext, kGCRoundInterval);
            }
        }

        {
            stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
            _threadOperationContext = nullptr;
        }
    }
}  // namespace mongo

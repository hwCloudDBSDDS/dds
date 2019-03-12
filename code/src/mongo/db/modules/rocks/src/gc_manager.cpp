#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include <time.h>
#include <algorithm>
#include <string>
#include "mongo/platform/basic.h"

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/client/read_preference.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"

#include "gc_common.h"
#include "gc_manager.h"

#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_catalog_entry.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/s/balancer/balancer.h"
#include "mongo/s/commands/cluster_write.h"
#include "mongo/s/grid.h"
#include "mongo/s/write_ops/batched_command_request.h"
#include "mongo/s/write_ops/batched_command_response.h"
#include "mongo/util/log.h"

namespace mongo {

    GCManager::GCManager() {
        _client.reset(new DBDirectClient(nullptr));
    }

    void GCManager::setGCInfo(const std::string& ns, const std::string& dataPath) {
        index_log() << "[GC Manager] ns: " << ns << "; dataPath: " << dataPath;
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        if (_chunkNs.empty()) {
            _chunkNs = ns;
            _dataPath = dataPath;
        } else {
            index_warning() << "gc referencer has splited!";
        }
    }

    GCManager::~GCManager() {
        // The GCManager thread must have been stopped
        invariant(_state == kStopped);
    }

    bool GCManager::_stopRequested() { return (_state == kStopped); }

    void GCManager::start() {
        index_log() << "[GC Manager] GC manager is starting, chunkNS(" << _chunkNs << ")";
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        invariant(_state == kStopped);
        _state = kRunning;

        invariant(!_thread.joinable());
        invariant(!_threadOperationContext);
        _thread = stdx::thread([this] { _mainThread(); });
    }

    void GCManager::stop() {
        index_log() << "[GC Manager] GC manager is stopping, chunkNS(" << _chunkNs << ")";

        if (_state != kRunning) {
            return;
        }
        _state = kStopped;
        // Wake up it if the main thread is sleeping now
        {
            stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
            _condVar.notify_all();

            // Interrupt the GCManager thread if it has been started. We are guaranteed that the
            // operation
            // context of that thread is still alive, because we hold the GC mutex.
            if (_threadOperationContext) {
                stdx::lock_guard<Client> scopedClientLock(*_threadOperationContext->getClient());
                _threadOperationContext->markKilled(ErrorCodes::Interrupted);
            }
        }

        invariant(_thread.joinable());

        // Thread join progress should not hold the mutex to avoid deadlock
        _thread.join();
        _thread = {};
    }

    bool GCManager::isRuning() {
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        return _state == kRunning;
    }

    bool GCManager::pause() {
        index_log() << "[GC Manager] shart GCManager pause! chunkNS: " << _chunkNs;

        if (kRunning != _state) {
            index_err() << "[GC Manager] GCManager::pause() faild current status: " << int(_state);
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

        index_log() << "[GC Manager] end GCManager pause! chunkNS: " << _chunkNs
                    << "; suc: " << suc;
        return suc;
    }

    bool GCManager::continues() {
        index_log() << "[GC Manager] GCManager continues! chunkNS: " << _chunkNs;
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        if (kRunning == _state) {
            index_err() << "[GC Manager] GCManager::continues() faild current status: kRunning";
            return true;
        } else if (kStopped == _state) {
            index_err() << "[GC Manager] GCManager::continues() faild current status: kStopped";
            return false;
        } else {
            invariant(kPause == _state);
            _state = kRunning;
            return true;
        }
    }

    void GCManager::_beginRound(OperationContext* txn) {
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        _inGCRound = true;
        _condVar.notify_all();
    }

    void GCManager::_endRound(OperationContext* txn, Seconds waitTimeout) {
        {
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            _inGCRound = false;
            _condVar.notify_all();
        }

        _sleepFor(txn, waitTimeout);
    }

    void GCManager::_sleepFor(OperationContext* txn, Seconds waitTimeout) {
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        _condVar.wait_for(lock, waitTimeout.toSteadyDuration(), [&] { return _state != kRunning; });
    }

    BatchedDeleteDocument* GCManager::_buildDeleteDoc(const std::string& plogIdStr) {
        std::unique_ptr<BatchedDeleteDocument> deleteDoc(new BatchedDeleteDocument());
        BSONObjBuilder query;

        query.append(kResourceIdStr, plogIdStr);
        deleteDoc->setQuery(query.obj());
        deleteDoc->setLimit(0);  // delete all

        return deleteDoc.release();
    }

    BSONObj GCManager::_buildAddRemoveInfoBSONDoc(const std::string& plogIdStr,
                                                  const std::string& chunkView,
                                                  const std::string& ident) {
        Date_t curDate(Date_t::now(Date_t::ClockType::kSystemClock));
        BSONObjBuilder indexDoc;

        indexDoc.append(kResourceIdStr, plogIdStr);
        indexDoc.append(kChunkViewStr, chunkView);
        indexDoc.append(kDeleteTimeStr, curDate.toString());
        indexDoc.append(kSharedResourcePath, getSharedResourcePath(plogIdStr, ident));

        return indexDoc.obj();
    }

    rocksdb::Status GCManager::delSharedResource(const std::string& sharedResource,
                                                 const std::string& ident) {
        std::string filename = getSharedResourcePath(sharedResource, ident);
        auto status = deleteFile(filename);
        if (!status.ok()) {
            index_err() << "[GC Manager] delSharedResource file: " << filename
                        << "; fail: " << status.ToString();
        } else {
            index_log() << "[GC Manager] delSharedResource suc: " << sharedResource
                        << "; filename: " << filename;
        }

        return status;
    }

    std::string GCManager::getSharedResourcePath(const std::string& sharedResource,
                                                 const std::string& ident) {
        return _dataPath + "/" + ident + "/" + sharedResource;
    }

    StatusWith<bool> GCManager::_findAndRemoveGarbageSharedResources(OperationContext* txn) {
        std::vector<std::string> shouldBeRemovedPlogId;
        std::map<std::string, std::string> plogIDToChunkidView;
        int shouldBeRemovedCount = 0;
        std::map<std::string, std::string> sharedIdToIdent;

        auto readRefLogStatus = _readReferenceLog(txn, BSONObj(), shouldBeRemovedPlogId,
                                                  plogIDToChunkidView, sharedIdToIdent);

        if (!readRefLogStatus.isOK()) {
            index_err() << "[GC Manager] failed to readReferenceLog due to "
                        << readRefLogStatus.reason();
            return readRefLogStatus;
        }

        const WriteConcernOptions writeConcern(2, WriteConcernOptions::SyncMode::JOURNAL, Milliseconds(20000));
        int addCount = 0;
        std::unique_ptr<BatchedInsertRequest> addRmvInfoInsertRequest(new BatchedInsertRequest());
        addRmvInfoInsertRequest->setNS(NamespaceString(StringData(GcRemoveInfoNs)));
        addRmvInfoInsertRequest->setOrdered(false);
        addRmvInfoInsertRequest->setWriteConcern(writeConcern.toBSON());

        int delCount = 0;
        std::unique_ptr<BatchedDeleteRequest> deleteRequest(new BatchedDeleteRequest());
        deleteRequest->setNS(NamespaceString(StringData(GcRefNs)));
        deleteRequest->setOrdered(false);
        deleteRequest->setWriteConcern(writeConcern.toBSON());

        for (const auto& plogIdStr : shouldBeRemovedPlogId) {
            shouldBeRemovedCount++;
            index_LOG(1) << "[GC Manager] find: cnt(" << shouldBeRemovedCount << ") plogid("
                         << plogIdStr << ") chunkidView(" << plogIDToChunkidView[plogIdStr] << ")"
                         << "; ident(" << sharedIdToIdent[plogIdStr] << ")";

            // Construct the request which add deleted info to gc.removeInfo collection
            addRmvInfoInsertRequest->addToDocuments(_buildAddRemoveInfoBSONDoc(
                plogIdStr, plogIDToChunkidView[plogIdStr], sharedIdToIdent[plogIdStr]));
            addCount++;
        }

        // Step1: Launch the request which add deleted info to gc.removeInfo collection
        if (addCount && GLOBAL_CONFIG_GET(IsShardingTest)) {
            BatchedCommandRequest addRmvInfoRequest(addRmvInfoInsertRequest.release());
            auto status =
                GCCommon::runBatchedCommandRequestWithFixedRetry(txn, addRmvInfoRequest, true);
            if (status != Status::OK()) {
                index_err() << "[GC Manager] failed to update the gc.removeInfo collection for "
                               "adding deleted info, status("
                            << status << ")";
                return status;
            }
        }

        // Step2: Remove the plogs
        int roundDelCount = 0;
        for (const auto& plogIdStr : shouldBeRemovedPlogId) {
            // To control the plog deletion speed
            if (++roundDelCount > kGCManagerDeleteCountPerRound) {
                sleep(kGCManagerSleepTimePerDeleteRound);
                roundDelCount = 0;
            }

            // Delete the plog
            index_LOG(1) << "[GC Manager] Ready to Delete plog with id(" << plogIdStr << ")";
            // int ret = delete_plog_x(NULL, &plogId, 0, NULL);
            auto ret = delSharedResource(plogIdStr, sharedIdToIdent[plogIdStr]);

            if (!ret.ok()) {
                index_err() << "[GC Manager] Failed to delete shared plog with id("
                            << ") ret(" << ret.ToString() << ")";
                continue;
            }
            index_LOG(1) << "[GC Manager] Delete plog success with id(" << plogIdStr << ")";

            // Construct the request which delete related docs in gc.references collection
            deleteRequest->addToDeletes(_buildDeleteDoc(plogIdStr));
            delCount++;
        }

        // Step3: Launch the request which delete the related docs in gc.references
        if (delCount) {
            BatchedCommandRequest deleteDocRequest(deleteRequest.release());
            auto status =
                GCCommon::runBatchedCommandRequestWithFixedRetry(txn, deleteDocRequest, false);
            if (status != Status::OK()) {
                index_err() << "[GC Manager] failed to update the gc.references collection for "
                               "deleting the removed plogid info, status("
                            << status << ")";
                return status;
            }
        }

        return shouldBeRemovedCount > 0 ? true : false;
    }

    void GCManager::_resetRefScanningStates() {
        _shouldBeRemoved = true;
        _chunkIdView = "";
        _lastPlogID = "";
    }

    bool GCManager::_checkIfStayEnoughTimeBeforeRemove(const std::string& oldDateString) {
        Date_t curDate(Date_t::now(Date_t::ClockType::kSystemClock));
        if (_gcSettings.getGCDelayedDelSecs() <= 0) {
            return true;
        }

        auto getOldDateStatus = dateFromISOString(StringData(oldDateString));
        if (getOldDateStatus.getStatus() != Status::OK()) {
            index_err() << "[GC Manager] dateFromISOString failed oldDateString(" << oldDateString
                        << ") due to " << getOldDateStatus.getStatus().reason();
            return true;
        }

        Date_t oldDate = getOldDateStatus.getValue();
        time_t oldTime = oldDate.toTimeT();
        time_t curTime = curDate.toTimeT();
        double diffSecs = difftime(curTime, oldTime);

        index_LOG(2) << "[GC Manager] oldDate(" << oldDate.toString() << ") curDate("
                     << curDate.toString() << ") diffSecs(" << diffSecs << ")";
        index_LOG(2) << "[GC Manager] gcDelayedDelSecs(" << _gcSettings.getGCDelayedDelSecs()
                     << ")";

        return diffSecs > _gcSettings.getGCDelayedDelSecs();
    }

    Status GCManager::_checkIfSharedResourceShouldBeRemoved(
        BSONObj docObj, std::vector<std::string>& shouldBeRemovedPlogId,
        std::map<std::string, std::string>& plogIDToChunkidView,
        std::map<std::string, std::string>& sharedIdToIdent) {
        index_LOG(3) << "_checkIfSharedResourceShouldBeRemoved: docObj: " << docObj;
        std::string plogID = "";
        std::string updateTime = "";
        std::string chunkId = "";
        bool deletedFlag = false;

        Status plogIDStatus = bsonExtractStringField(docObj, kResourceIdStr, &plogID);
        if (!plogIDStatus.isOK()) {
            index_err() << "Extract pid faild doc: " << docObj;
            return Status(plogIDStatus.code(), str::stream() << "Missing expected field \""
                                                             << kResourceIdStr << "\"");
        }

        Status chunkIDStatus = bsonExtractStringField(docObj, kChunkIdStr, &chunkId);
        if (!chunkIDStatus.isOK()) {
            index_err() << "Extract cid faild doc: " << docObj;
            return Status(chunkIDStatus.code(), str::stream() << "Missing expected field \""
                                                              << kChunkIdStr << "\"");
        }

        Status updateTimeStatus = bsonExtractStringField(docObj, kUpdateTimeStr, &updateTime);
        if (!updateTimeStatus.isOK()) {
            index_err() << "Extract ut faild doc: " << docObj;
            return Status(updateTimeStatus.code(), str::stream() << "Missing expected field \""
                                                                 << kUpdateTimeStr << "\"");
        }

        std::string ident = "";
        Status identStatus = bsonExtractStringField(docObj, kIdentStr, &ident);
        if (!identStatus.isOK()) {
            index_err() << "Extract ident faild doc: " << docObj;
            return Status(identStatus.code(), str::stream() << "Missing expected field \""
                                                            << kIdentStr << "\"");
        }

        Status deletedFlagStatus = bsonExtractBooleanField(docObj, kDeletedStr, &deletedFlag);

        index_LOG(1) << "[GC Manager] docObj: " << docObj;
        index_LOG(1) << "[GC Manager] Begin: plogID: " << plogID << ", chunkId: " << chunkId
                     << ", deletedFlag: " << deletedFlag << ", lastPlogID: " << _lastPlogID
                     << ", shouldBeRemoved: " << _shouldBeRemoved
                     << ", chunkIdView: " << _chunkIdView
                     << ", chunkIdViewLength: " << _chunkIdView.length() << ", ident: " << ident;

        if (plogID != _lastPlogID) {
            //
            // A new plogid appear, start a new round to decide if this plogid should be remove
            //
            if (_lastPlogID != "" && _shouldBeRemoved) {
                index_LOG(3) << "[GC Manager] This plog should be deleted! "
                             << "plogID(" << _lastPlogID << ") chunkIdView(" << _chunkIdView << ")"
                             << "; ident: " << _ident << "; plogId: " << _lastPlogID;
                shouldBeRemovedPlogId.push_back(_lastPlogID);
                plogIDToChunkidView[_lastPlogID] = _chunkIdView;
                sharedIdToIdent[_lastPlogID] = _ident;
                if (_chunkIdView.length() >= kGCManagerMaxChunkViewLen) {
                    plogIDToChunkidView[_lastPlogID] += "...";
                }
            }
            _resetRefScanningStates();
        }

        if (false == deletedFlag || false == _checkIfStayEnoughTimeBeforeRemove(updateTime)) {
            _shouldBeRemoved = false;
        }

        _lastPlogID = plogID;
        _ident = ident;

        if (_chunkIdView.length() < kGCManagerMaxChunkViewLen) {
            _chunkIdView += "_";
            _chunkIdView += chunkId;
        }

        index_LOG(1) << "[GC Manager] End: plogID: " << plogID << ", chunkId: " << chunkId
                     << ", deletedFlag: " << deletedFlag << ", lastPlogID: " << _lastPlogID
                     << ", shouldBeRemoved: " << _shouldBeRemoved
                     << ", chunkIdView: " << _chunkIdView
                     << ", chunkIdViewLength: " << _chunkIdView.length();

        return Status::OK();
    }

    Status GCManager::_readReferenceLog(OperationContext* txn, const BSONObj& query,
                                        std::vector<std::string>& shouldBeRemovedPlogId,
                                        std::map<std::string, std::string>& plogIDToChunkidView,
                                        std::map<std::string, std::string>& sharedIdToIdent) {
        NamespaceString nss(GcRefNs);
        int readCount = 0;
        const int64_t maxScanCountPerRound = _gcSettings.getGCScanMaxBatchSize();
        const uint64_t maxDelCountPerRound = (BatchedCommandRequest::kMaxWriteBatchSize > 100)
                                                 ? BatchedCommandRequest::kMaxWriteBatchSize - 100
                                                 : BatchedCommandRequest::kMaxWriteBatchSize;

        // Lock::DBLock dbLock(txn->lockState(), nss.db(), MODE_IS);
        AutoGetCollectionForRead ctx(txn, _chunkNs);
        Database* db = dbHolder().get(txn, nss.db());
        if (!db) {
            return Status(ErrorCodes::NamespaceNotFound, str::stream() << "db is not on this shard "
                                                                       << nss.db());
        }

        Collection* collection = db->getCollection(_chunkNs);
        if (!collection) {
            return Status(ErrorCodes::NamespaceNotFound, str::stream() << "collection is not found "
                                                                       << _chunkNs);
        }

        _client->setOpCtx(txn);

        index_LOG(2) << "[GC Manager] _readReferenceLog ns(" << collection->ns().ns() << " query("
                     << query << ")";

        if (nullptr == _clientCursor) {
            index_LOG(1) << "[GC Manager] New scanning round";
            _clientCursor = _client->query(collection->ns().ns(), Query(query), 0, 0, 0,
                                           QueryOption_NoCursorTimeout,  // set cursor no timeout
                                           0);
            _clientCursor->setBatchSize(maxScanCountPerRound);
        }

        //
        // Scanning the reference documents to check if a shared resource should be removed or not
        //
        while (_clientCursor->more() && readCount < maxScanCountPerRound) {
            Status checkStatus = _checkIfSharedResourceShouldBeRemoved(
                _clientCursor->nextSafe().getOwned(), shouldBeRemovedPlogId, plogIDToChunkidView,
                sharedIdToIdent);

            if (!checkStatus.isOK()) {
                return checkStatus;
            }

            // Limit the max del count of plog for each scanning round
            if (shouldBeRemovedPlogId.size() >= maxDelCountPerRound - 1) {
                break;
            }

            readCount++;
        }

        //
        // Check if the last scanned shared plog shoule be removed or not
        //
        if (!_clientCursor->more()) {
            index_LOG(1) << "[GC Manager] Scanning at the end";
            if (_lastPlogID != "" && _shouldBeRemoved) {
                index_LOG(1) << "[GC Manager] This plog should be deleted!"
                             << "plogID(" << _lastPlogID << ") chunkIdView(" << _chunkIdView
                             << ") chunkIdViewLength( " << _chunkIdView.length() << ")";
                shouldBeRemovedPlogId.push_back(_lastPlogID);
                plogIDToChunkidView[_lastPlogID] = _chunkIdView;
                sharedIdToIdent[_lastPlogID] = _ident;
            }
            _resetRefScanningStates();
            _clientCursor.reset();
        }

        txn->recoveryUnit()->abandonSnapshot();
        return Status::OK();
    }

    void GCManager::_mainThread() {
        Client::initThread(GCManagerThreadName.c_str());
        ServiceContext::UniqueOperationContext txn;

        //
        // Make sure operation context is created after storageEngine is created
        //
        GCCommon::waitForStorageEngineToBeCreated();

        while (!_stopRequested()) {
            _beginRound(_threadOperationContext);

            if (kPause == _state) {
                _endRound(_threadOperationContext, kGCManagerRoundDefaultInterval);
                continue;
            }

            //
            // Each round, we reset the operation context because it includes snapshot info, this
            // help us get the latest data in gc.removeInfo and gc.references collections.
            //
            GCCommon::resetThreadOperationContext(txn, _mutex, &_threadOperationContext);

            // GCCommon::updateGCSettings(_threadOperationContext, _gcSettings);
            if (!_gcSettings.shouldGCManager()) {
                _endRound(_threadOperationContext, kGCManagerRoundDefaultInterval);
                continue;
            }

            int64_t gcRoundInterval = _gcSettings.getGCManagerInterval() > kGCMinIntervalSec
                                          ? _gcSettings.getGCManagerInterval()
                                          : kGCMinIntervalSec;
            int64_t gcShortRoundInterval = (gcRoundInterval / 10) > kGCMinShortIntervalSec
                                               ? (gcRoundInterval / 10)
                                               : kGCMinShortIntervalSec;
            const Seconds kGCRoundInterval(gcRoundInterval);
            //
            // Sleep between GC rounds in the case where the last round found some shared resourced
            // which needed to
            // be removed
            //
            const Seconds kShortGCRoundInterval(gcShortRoundInterval);

            if (_chunkNs.empty()) {
                _endRound(_threadOperationContext, kGCManagerRoundDefaultInterval);
                continue;
            }

            index_LOG(1) << "[GC Manager] gcRoundInterval(" << gcRoundInterval << ") chunkNs("
                         << _chunkNs << ")";
            try {
                auto status = _findAndRemoveGarbageSharedResources(_threadOperationContext);


                // when scanning failed, reset the cursor because it may failed to
                // update the scanning states
                if (!status.isOK()) {
                    // TODO: Add alarm when gc failed in a certain successive round
                    _resetRefScanningStates();
                    _clientCursor.reset();
                }

                if (status.isOK() && true == status.getValue()) {
                    _endRound(_threadOperationContext, kShortGCRoundInterval);
                } else {
                    // TODO: Add alarm when gc failed in a certain successive round
                    _endRound(_threadOperationContext, kGCRoundInterval);
                }
            } catch (std::exception& e) {
                index_err() << "[GC Manager] caught DBException while doing GC : " << e.what();
                index_LOG(1) << "*** End of GC manager round";

                _resetRefScanningStates();
                _clientCursor.reset();


                // Sleep a fair amount before retrying because of the error
                _endRound(_threadOperationContext, kGCRoundInterval);
            }
        }

        _clientCursor.reset();

        {
            stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
            _threadOperationContext = nullptr;
        }
    }
}  // namespace mongo

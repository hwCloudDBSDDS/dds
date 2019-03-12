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
    // These params are used by both gc client and gc manager
    const int kGCMinIntervalSec = 1;
    const int kGCMinShortIntervalSec = 1;

    // These params are used by gc client only
    const int kGCClientDefaultRoundIntervalSec =
        30;  // the default round interval seconds between each round for gc client
    const int kGCClientMaxBatchSize =
        450;  // the max number of getting unprocessed shared resource operations per round

    // These params are used by gc manager only
    const int kGCManagerDefaultRoundIntervalSec =
        10;  // the default round interval seconds between each round for gc manager
    const int kGCManagerMaxBatchSize = 10000;  // the max scanning number per round
    const int kGCManagerDeleteCountPerRound =
        20;  // Each round we delete 20 plog, and then sleep 1 second
    const int kGCManagerSleepTimePerDeleteRound = 1;
    const int kGCManagerDefaultDelayedDelSecs = 0;
    const unsigned int kGCManagerMaxChunkViewLen = 4096;

    const Seconds kGCClientRoundDefaultInterval(kGCClientDefaultRoundIntervalSec);
    const Seconds kGCManagerRoundDefaultInterval(kGCManagerDefaultRoundIntervalSec);
    const Seconds kGCPauseTimeout(2 * 60);

    const std::string kGCKey = "globalGC";
    const std::string kGCClientOpenValue = "gcClientOpened";
    const std::string kGCManagerOpenValue = "gcManagerOpened";
    const std::string kGCTestValue = "tested";
    const std::string kGCClearRemoveInfoValue = "shouldClearRemoveInfo";
    const std::string kGCClientIntervalValue = "gcClientInterval";
    const std::string kGCManagerIntervalValue = "gcManagerInterval";
    const std::string kGCScanMaxBatchSizeValue = "gcScanMaxBatchSize";
    const std::string kGCDelayedSecsValue = "gcDelayedDelSecs";

    // Field names and types in doc of the gc collections.
    const std::string kResourceIdStr = "pid";
    const std::string kChunkIdStr = "cid";
    const std::string kIdentStr = "ident";
    const std::string kDeletedStr = "del";
    const std::string kChunkViewStr = "cv";
    const std::string kSharedResourcePath = "path";
    const std::string kUpdateTimeStr = "ut";
    const std::string kDeleteTimeStr = "dt";
    const std::string kIdStr = "_id";

    const std::string GcRefCollName = "references";
    const std::string GcRemoveInfoCollName = "removeInfo";
    const std::string GcRefTestNs = "gctest.referencesTest";

    const uint32_t TRY_WAIT_TIME = 3;

    const std::string GCClientThreadName = "GC Client";
    const std::string GCManagerThreadName = "GC Manager";

    Status GCCommon::_getBatchedCommandRequestResult(BatchedCommandResponse* response,
                                                     ErrorCodes::Error bypassedCode) {
        invariant(response != nullptr);

        if (!response->getOk()) {
            index_LOG(2) << "[GC Common] error_code1 is " << response->getErrCode();
            if (ErrorCodes::OK != ErrorCodes::fromInt(response->getErrCode()) &&
                bypassedCode != ErrorCodes::fromInt(response->getErrCode()))
                return Status(ErrorCodes::fromInt(response->getErrCode()),
                              response->getErrMessage());
            return Status::OK();
        }

        if (response->isErrDetailsSet()) {
            size_t errSize = response->sizeErrDetails();

            for (size_t pos = 0; pos < errSize; pos++) {
                const WriteErrorDetail* errDetail = response->getErrDetailsAt(pos);
                int err_code = errDetail->getErrCode();
                index_LOG(2) << "[GC Common] error_code2 is " << err_code << " , pos is " << pos;
                if (ErrorCodes::OK != ErrorCodes::fromInt(errDetail->getErrCode()) &&
                    bypassedCode != ErrorCodes::fromInt(errDetail->getErrCode())) {
                    return Status(ErrorCodes::fromInt(errDetail->getErrCode()),
                                  errDetail->getErrMessage());
                }
            }

            return Status::OK();
        }

        if (response->isWriteConcernErrorSet()) {
            const WriteConcernErrorDetail* errDetail = response->getWriteConcernError();
            int err_code = errDetail->getErrCode();
            index_LOG(2) << "[GC Common] error_code3 is " << err_code;
            if (ErrorCodes::OK != ErrorCodes::fromInt(errDetail->getErrCode()) &&
                bypassedCode != ErrorCodes::fromInt(errDetail->getErrCode())) {
                return Status(ErrorCodes::fromInt(errDetail->getErrCode()),
                              errDetail->getErrMessage());
            }

            return Status::OK();
        }

        return Status::OK();
    }

    //
    // For example,
    // gc.references should be like:
    // { "_id" : ObjectId("59eafaca21cf85f8d1965820"), "cid" : NumberLong(3), "pid" :
    // "00000010000000006DB82F86FFFFFFFF0000000000000000", "ut" : "2017-10-21 15:44:10.134+0800" }
    // { "_id" : ObjectId("59eafaca21cf85f8d1965821"), "cid" : NumberLong(4), "pid" :
    // "00000010000000006DB82F86FFFFFFFF0000000000000000", "ut" : "2017-10-21 15:44:10.134+0800" }
    // { "_id" : ObjectId("59eafaca21cf85f8d1965821"), "cid" : NumberLong(4), "pid" :
    // "00000010000000006DB82F86FFFFFFFF0000000000000000", "ut" : "2017-10-21 15:45:10.134+0800",
    // "del" : true }
    //
    // gc.removeInfo should be like:
    // { "_id" : ObjectId("59eafacb21cf85f8d1965b63"), "pid" :
    // "000000100000000063AD1325033DDE160000000000000000", "cv" : "_3_4", "dt" : "2017-10-21
    // 15:44:11.423+0800" }
    //

    Status GCCommon::runBatchedCommandRequestWithFixedRetry(OperationContext* txn,
                                                            const BatchedCommandRequest& request,
                                                            bool isInsertRequest) {
        BatchedCommandResponse response;
        ClusterWriter writer(true, 30000);  // set request timeout to 30 seconds
        writer.write(txn, request, &response);

        Status status(Status::OK());
        if (isInsertRequest) {
            status = _getBatchedCommandRequestResult(&response, ErrorCodes::DuplicateKey);
        } else {
            status = _getBatchedCommandRequestResult(&response);
        }

        if (!status.isOK()) {
            index_err() << "[GC Common] failed to _runBatchedCommandRequest, status(" << status;
        }
        return status;
    }

    void GCCommon::waitForStorageEngineToBeCreated() {
        StorageEngine* storageEngine = nullptr;

        do {
            storageEngine = getGlobalServiceContext()->getGlobalStorageEngine();

            if (storageEngine) {
                break;
            }
            sleep(2);
        } while (1);
    }

    void GCCommon::resetThreadOperationContext(ServiceContext::UniqueOperationContext& txn,
                                               stdx::mutex& gcMutex,
                                               OperationContext** threadOperationContext) {
        txn.reset();
        txn = cc().makeOperationContext();

        {
            stdx::lock_guard<stdx::mutex> scopedLock(gcMutex);
            *threadOperationContext = txn.get();
        }
    }

    void GCCommon::updateGCSettings(OperationContext* txn, GCSettings& gcSettings) {
        gcSettings.reset();

        auto shardingCataClient = grid.catalogClient(txn);
        if (nullptr == shardingCataClient) {
            index_err() << "[GC Common] updateGCSettings failed due to not find primary config";
            return;
        }

        auto gcSettingsObjStatus = shardingCataClient->getGlobalSettings(txn, kGCKey);

        if (gcSettingsObjStatus.isOK()) {
            bool gcTestFlag = false;
            bool gcClearRemoveInfoFlag = false;
            bool gcClientOpenFlag = true;
            bool gcManagerOpenFlag = true;
            long long gcClientInterval = kGCClientDefaultRoundIntervalSec;
            long long gcManagerInterval = kGCManagerDefaultRoundIntervalSec;
            long long gcDelayedDelSecs = kGCManagerDefaultDelayedDelSecs;
            long long gcScanMaxBatchSize = kGCManagerMaxBatchSize;
            Status status(Status::OK());

            status =
                bsonExtractBooleanField(gcSettingsObjStatus.getValue(), kGCTestValue, &gcTestFlag);
            if (status.isOK()) {
                gcSettings.setGCTestFlag(gcTestFlag);
            }

            status = bsonExtractBooleanField(gcSettingsObjStatus.getValue(),
                                             kGCClearRemoveInfoValue, &gcClearRemoveInfoFlag);
            if (status.isOK()) {
                gcSettings.setGCClearRemoveInfoFlag(gcClearRemoveInfoFlag);
            }

            status = bsonExtractBooleanField(gcSettingsObjStatus.getValue(), kGCClientOpenValue,
                                             &gcClientOpenFlag);
            if (status.isOK()) {
                gcSettings.setGCClientOpenFlag(gcClientOpenFlag);
            }

            status = bsonExtractBooleanField(gcSettingsObjStatus.getValue(), kGCManagerOpenValue,
                                             &gcManagerOpenFlag);
            if (status.isOK()) {
                gcSettings.setGCManagerOpenFlag(gcManagerOpenFlag);
            }

            status = bsonExtractIntegerField(gcSettingsObjStatus.getValue(), kGCClientIntervalValue,
                                             &gcClientInterval);
            if (status.isOK() && gcClientInterval >= kGCMinIntervalSec) {
                gcSettings.setGCClientInterval(gcClientInterval);
            }

            status = bsonExtractIntegerField(gcSettingsObjStatus.getValue(),
                                             kGCManagerIntervalValue, &gcManagerInterval);
            if (status.isOK() && gcManagerInterval >= kGCMinIntervalSec) {
                gcSettings.setGCManagerInterval(gcManagerInterval);
            }

            status = bsonExtractIntegerField(gcSettingsObjStatus.getValue(), kGCDelayedSecsValue,
                                             &gcDelayedDelSecs);
            if (status.isOK() && gcDelayedDelSecs >= 0) {
                gcSettings.setGCDelayedDelSecs(gcDelayedDelSecs);
            }

            status = bsonExtractIntegerField(gcSettingsObjStatus.getValue(),
                                             kGCScanMaxBatchSizeValue, &gcScanMaxBatchSize);
            if (status.isOK() && gcScanMaxBatchSize > 0) {
                gcSettings.setGCScanMaxBatchSize(gcScanMaxBatchSize);
            }
        }

        index_LOG(2) << "[GC Common] gcSettings: gcClientOpenFlag(" << gcSettings.shouldGCClient()
                     << ") gcManagerOpenFlag(" << gcSettings.shouldGCManager() << ") gcTestFlag("
                     << gcSettings.shouldGCTest() << ") gcClearRemoveInfoFlag("
                     << gcSettings.shouldGCClearRemoveInfoColl() << ") gcClientInterval("
                     << gcSettings.getGCClientInterval() << ") gcManagerInterval("
                     << gcSettings.getGCManagerInterval() << ") gcDelayedDelSecs("
                     << gcSettings.getGCDelayedDelSecs() << ") gcScanMaxBatchSize("
                     << gcSettings.getGCScanMaxBatchSize() << ")";
    }

    BSONObj GCCommon::buildAddRefBSONDoc(const std::string& ident, uint64_t chunkId,
                                         const SharedResourceId& srID) {
        Date_t curDate(Date_t::now(Date_t::ClockType::kSystemClock));
        BSONObjBuilder indexDoc;
        invariant(!ident.empty());
        index_LOG(2) << "[GC Client] curDate(" << curDate.toString() << ") isFormattable("
                     << curDate.isFormattable() << ")";

        std::string _id = iToStr(chunkId) + "/" + srID.toString();
        indexDoc.append(kIdStr, _id);
        indexDoc.append(kIdentStr, ident);
        indexDoc.append(kChunkIdStr, iToStr(chunkId));
        indexDoc.append(kResourceIdStr, srID.toString());
        indexDoc.append(kUpdateTimeStr, curDate.toString());
        indexDoc.append(kDeletedStr, false);

        return indexDoc.obj();
    }

    BatchedUpdateDocument* GCCommon::buildRemoveRefUpdateDoc(const std::string& ident,
                                                             uint64_t chunkId,
                                                             const SharedResourceId& srID) {
        Date_t curDate(Date_t::now(Date_t::ClockType::kSystemClock));
        std::unique_ptr<BatchedUpdateDocument> updateDoc(new BatchedUpdateDocument());
        BSONObjBuilder query;

        invariant(!ident.empty());
        std::string _id = iToStr(chunkId) + "/" + srID.toString();
        query.append(kIdStr, _id);
        query.append(kResourceIdStr, srID.toString());

        updateDoc->setQuery(query.obj());
        updateDoc->setUpdateExpr(BSON(
            "$set" << BSON(kIdStr << _id << kIdentStr << ident << kChunkIdStr << iToStr(chunkId)
                                  << kResourceIdStr << srID.toString() << kUpdateTimeStr
                                  << curDate.toString() << kDeletedStr << true)));
        updateDoc->setUpsert(true);

        return updateDoc.release();
    }

    bool GCCommon::isGcTestOpened() {
        Client::initThreadIfNotAlready();
        auto txn = cc().getOperationContext();
        ServiceContext::UniqueOperationContext txnOrigin;
        if (!txn) {
            txnOrigin = cc().makeOperationContext();
            invariant(txnOrigin);
            txn = txnOrigin.get();
        }

        invariant(txn);

        mongo::GCSettings gcSettings;
        updateGCSettings(txn, gcSettings);

        return gcSettings.shouldGCTest();
    }

    Status GCCommon::insertRefUpdateToReferencesTestCollection(
        const std::vector<mongo::TransactionLog::SharedResourceReferenceOperation>&
            unprocessedRecords) {
        //    Client::initThreadIfNotAlready();
        //    int addCount    = 0;
        //    int removeCount = 0;
        //    auto txn = cc().getOperationContext();
        //    ServiceContext::UniqueOperationContext txnOrigin;
        //    if (!txn) {
        //        txnOrigin = cc().makeOperationContext();
        //        invariant(txnOrigin);
        //        txn = txnOrigin.get();
        //    }
        //
        //    invariant(txn);
        //
        //    std::unique_ptr<BatchedInsertRequest> addRefInsertRequest(new BatchedInsertRequest());
        //    addRefInsertRequest->setNS(NamespaceString(StringData(GcRefTestNs)));
        //    addRefInsertRequest->setOrdered(false);
        //
        //    std::unique_ptr<BatchedUpdateRequest> removeRefUpdateRequest(new
        //    BatchedUpdateRequest());
        //    removeRefUpdateRequest->setNS(NamespaceString(GcRefTestNs));
        //    removeRefUpdateRequest->setOrdered(false);
        //
        //    for (const auto& operation : unprocessedRecords) {
        //        if (operation.GetType() ==
        //        mongo::TransactionLog::SharedResourceOperationType::AddReference){
        //            addRefInsertRequest->addToDocuments(GCCommon::buildAddRefBSONDoc(operation.GetParentChunkId(),
        //            operation.GetResource()));
        //            addCount ++;
        //
        //            addRefInsertRequest->addToDocuments(GCCommon::buildAddRefBSONDoc(operation.GetChildChunkId(),
        //            operation.GetResource()));
        //            addCount ++;
        //
        //            index_log() << "[GC Test] AddRef: lsn(" << operation.GetLsn()
        //                        << ") resourceid(" << operation.GetResource().toString()
        //                        << ") parentId(" << operation.GetParentChunkId()
        //                        << ") childId(" << operation.GetChildChunkId() << ")";
        //        }
        //        else if (operation.GetType() ==
        //        mongo::TransactionLog::SharedResourceOperationType::RemoveReference){
        //
        //            removeRefUpdateRequest->addToUpdates(GCCommon::buildRemoveRefUpdateDoc(operation.GetParentChunkId(),
        //            operation.GetResource()));
        //            removeCount ++;
        //
        //            index_log() << "[GC Test] RmvRef: lsn(" << operation.GetLsn()
        //                        << ") resourceid(" << operation.GetResource().toString()
        //                        << ") parentId(" << operation.GetParentChunkId() << ")";
        //        }
        //    }
        //
        //
        //    // try to insert the AddRef doc into gc.referencesTest
        //    {
        //        if (addCount) {
        //            BatchedCommandRequest addRefRequest(addRefInsertRequest.release());
        //            auto status = GCCommon::runBatchedCommandRequestWithFixedRetry(txn,
        //            addRefRequest, true);
        //            if (status != Status::OK()) {
        //                index_err() << "[GC Test] failed to update the gc.referencesTest
        //                collection for adding reference, status(" << status << ")";
        //                return status;
        //            }
        //        }
        //    }
        //
        //    // try to update the corresponding RemoveRef doc in gc.referencesTest
        //    {
        //        if (removeCount) {
        //            BatchedCommandRequest removeRefRequest(removeRefUpdateRequest.release());
        //            auto status = GCCommon::runBatchedCommandRequestWithFixedRetry(txn,
        //            removeRefRequest, false);
        //            if (status != Status::OK()) {
        //                index_err() << "[GC Test] failed to update the gc.referencesTest
        //                collection for removing reference, status(" << status << ")";
        //                return status;
        //            }
        //        }
        //    }
        //
        return Status::OK();
    }

    std::string GCCommon::iToStr(uint64_t id) {
        return ChunkType::widthChunkID(ChunkType::toSex(id));
    }

}  // namespace mongo


#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "../gc_common.h"
#include "TransLogRecordStore.h"
#include "i_log_record_store.h"
#include "i_shared_resource_manager.h"
#include "maas_shared_resource_manager.h"
#include "mongo/com/include/error_code.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"

namespace mongo {

    // Failpoint which causes assign chunk command timeout
    // but the chunk is already served by shardserver
    MONGO_FP_DECLARE(failToRegisterSharedResources);
    MONGO_FP_DECLARE(failToRewriteImportantFailedLog);

    namespace TransactionLog {

        //
        //  Load-balancing transaction log related functions
        //  Is used by GC manager: GC manager periodically asks for batch of unprocessed log records
        //  calling
        //  function GetSharedResourceLogRecords. If there are some records that GC manager can
        //  process,
        //  provider will return this messages at "unprocessedRecords" list. When GC manager
        //  finished processing
        //  some messages from the batch it can set the lowest LSN of processed messages using
        //  function
        //  SetLastProcessedSharedResourceLSN, so with the next call GetSharedResourceLogRecords
        //  only records
        //  having higher LSN will be returned.
        //

        //
        // Methods of class SharedResourceOperation
        //
        uint32_t SharedResourceOperation::EncodeTo(char*& buf) {
            buf = nullptr;
            uint32_t buf_size = GetEncodeLength();
            if (0 == buf_size) {
                index_err() << "[SRM]GetEncodeLength() failed.";
                return 0;
            }
            buf = new (std::nothrow) char[buf_size];
            if (nullptr == buf) {
                index_err() << "[SRM]New buffer failed. size(" << buf_size << ")";
                return 0;
            }
            uint32_t encode_size = EncodeToBuf(buf);
            if (encode_size != buf_size) {
                index_err() << "[SRM]Encode failed. encode(" << encode_size << "), buf(" << buf_size
                            << ")";
                delete[] buf;
                buf = nullptr;
                return 0;
            }
            return encode_size;
        }
        uint32_t SharedResourceOperation::GetEncodeLength() const {
            return GetSharedResourceOperationTypeEncodeLength(type_);
        }
        uint32_t SharedResourceOperation::EncodeToBuf(char* buf) {
            return EncodeSharedResourceOperationTypeToBuf(type_, buf);
        }
        uint32_t SharedResourceOperation::DecodeBaseFrom(const LogRecord& record) {
            lsn_ = record.GetLsn();
            return DecodeSharedResourceOperationTypeFromBuf(type_, record.GetData());
        }

        //
        // Methods of class SharedResourceFilterOperation
        //
        uint32_t SharedResourceFilterOperation::GetEncodeLength() const {
            return SharedResourceOperation::GetEncodeLength() +
                   GetSharedResourceIdEncodeLength(resource_);
        }
        uint32_t SharedResourceFilterOperation::EncodeToBuf(char* buf) {
            uint32_t offset = 0;
            offset += SharedResourceOperation::EncodeToBuf(buf + offset);
            offset += EncodeSharedResourceIdToBuf(resource_, buf + offset);
            return offset;
        }
        SharedResourceFilterOperation* SharedResourceFilterOperation::DecodeFrom(
            const LogRecord& record) {
            // Allocate operation.
            SharedResourceFilterOperation* oper = new (std::nothrow) SharedResourceFilterOperation;
            if (nullptr == oper) {
                return nullptr;
            }
            // Decode fields.
            const char* buf = record.GetData();
            uint32_t offset = 0;
            offset += oper->DecodeBaseFrom(record);
            offset += DecodeSharedResourceIdFromBuf(oper->resource_, buf + offset);
            // Output operation.
            return oper;
        }

        //
        // Methods of class SharedResourceReferenceOperation
        //
        uint32_t SharedResourceReferenceOperation::GetEncodeLength() const {
            return SharedResourceOperation::GetEncodeLength() +
                   GetSharedResourceIdEncodeLength(resource_) +
                   GetChunkIdEncodeLength(parent_chunk_id_) +
                   GetChunkIdEncodeLength(child_chunk_id_);
        }
        uint32_t SharedResourceReferenceOperation::EncodeToBuf(char* buf) {
            uint32_t offset = 0;
            offset += SharedResourceOperation::EncodeToBuf(buf + offset);
            offset += EncodeSharedResourceIdToBuf(resource_, buf + offset);
            offset += EncodeChunkIdToBuf(parent_chunk_id_, buf + offset);
            offset += EncodeChunkIdToBuf(child_chunk_id_, buf + offset);
            return offset;
        }
        SharedResourceReferenceOperation* SharedResourceReferenceOperation::DecodeFrom(
            const LogRecord& record) {
            // Allocate operation.
            SharedResourceReferenceOperation* oper =
                new (std::nothrow) SharedResourceReferenceOperation;
            if (nullptr == oper) {
                return nullptr;
            }
            // Decode fields.
            const char* buf = record.GetData();
            uint32_t offset = 0;
            offset += oper->DecodeBaseFrom(record);
            offset += DecodeSharedResourceIdFromBuf(oper->resource_, buf + offset);
            offset += DecodeChunkIdFromBuf(oper->parent_chunk_id_, buf + offset);
            offset += DecodeChunkIdFromBuf(oper->child_chunk_id_, buf + offset);
            // Output operation.
            return oper;
        }

        //
        // Methods of class SplitDescription
        //
        uint32_t SplitDescription::GetEncodeLength() const {
            uint32_t len = 0;
            len += GetChunkIdEncodeLength(parentChunkId);
            len += GetChunkIdEncodeLength(childChunkId);
            len += GetBsonEncodeLength(originalKeyLow);
            len += GetBsonEncodeLength(originalKeyHigh);
            len += GetBsonEncodeLength(splitPointKey);
            len += GetStringEncodeLength(rightDbPathWithRootPlog);
            return len;
        }
        uint32_t SplitDescription::EncodeToBuf(char* buf) {
            uint32_t offset = 0;
            offset += EncodeChunkIdToBuf(parentChunkId, (buf + offset));
            offset += EncodeChunkIdToBuf(childChunkId, (buf + offset));
            offset += EncodeBsonToBuf(originalKeyLow, (buf + offset));
            offset += EncodeBsonToBuf(originalKeyHigh, (buf + offset));
            offset += EncodeBsonToBuf(splitPointKey, (buf + offset));
            offset += EncodeStringToBuf(rightDbPathWithRootPlog, (buf + offset));
            return offset;
        }
        uint32_t SplitDescription::DecodeFromBuf(const char* buf) {
            uint32_t offset = 0;
            std::string str;
            offset += DecodeChunkIdFromBuf(parentChunkId, (buf + offset));
            offset += DecodeChunkIdFromBuf(childChunkId, (buf + offset));
            offset += DecodeBsonFromBuf(originalKeyLow, (buf + offset));
            offset += DecodeBsonFromBuf(originalKeyHigh, (buf + offset));
            offset += DecodeBsonFromBuf(splitPointKey, (buf + offset));
            offset += DecodeStringFromBuf(rightDbPathWithRootPlog, (buf + offset));
            return offset;
        }

        uint32_t SplitOperation::GetEncodeLength() const {
            return SharedResourceOperation::GetEncodeLength() + GetSplitIdEncodeLength(id_) +
                   description_->GetEncodeLength();
        }
        uint32_t SplitOperation::EncodeToBuf(char* buf) {
            uint32_t offset = 0;
            offset += SharedResourceOperation::EncodeToBuf(buf + offset);
            offset += EncodeSplitIdToBuf(id_, buf + offset);
            offset += description_->EncodeToBuf(buf + offset);
            return offset;
        }
        SplitOperation* SplitOperation::DecodeFrom(const LogRecord& record) {
            // Allocate operation.
            SplitOperation* oper = new (std::nothrow) SplitOperation;
            if (nullptr == oper) {
                return nullptr;
            }
            // Decode fields.
            const char* buf = record.GetData();
            uint32_t offset = 0;
            offset += oper->DecodeBaseFrom(record);
            offset += DecodeSplitIdFromBuf(oper->id_, buf + offset);
            auto description = std::make_shared<SplitDescription>();
            offset += description->DecodeFromBuf(buf + offset);
            oper->SetDescription(description);
            // Output operation.
            return oper;
        }
        SharedResourceOperationType SplitOperation::GetOperTypeOfLogRecord(
            const LogRecord& record) {
            SharedResourceOperation oper;
            oper.DecodeBaseFrom(record);
            return oper.GetType();
        }

        //
        // Methods of class ProcessedLSNOperation
        //
        uint32_t ProcessedLSNOperation::GetEncodeLength() const {
            return SharedResourceOperation::GetEncodeLength() + GetLsnEncodeLength(processed_lsn_);
        }
        uint32_t ProcessedLSNOperation::EncodeToBuf(char* buf) {
            uint32_t offset = 0;
            offset += SharedResourceOperation::EncodeToBuf(buf + offset);
            offset += EncodeLsnToBuf(processed_lsn_, buf + offset);
            return offset;
        }
        ProcessedLSNOperation* ProcessedLSNOperation::DecodeFrom(const LogRecord& record) {
            // Allocate operation.
            ProcessedLSNOperation* oper = new (std::nothrow) ProcessedLSNOperation;
            if (nullptr == oper) {
                return nullptr;
            }
            // Decode fields.
            const char* buf = record.GetData();
            uint32_t offset = 0;
            offset += oper->DecodeBaseFrom(record);
            offset += DecodeLsnFromBuf(oper->processed_lsn_, buf + offset);
            // Output operation.
            return oper;
        }

        // Check if a resource list is unique and sequential.
        bool ResourceListIsUniqueAndSequential(const SharedResourceIds& resources,
                                               uint64_t chunk_id) {
            for (uint32_t i = 1; i < resources.size(); i++) {
                if (!CompareResourceId(resources[i - 1], resources[i])) {
                    index_err() << "[SRM][" << chunk_id
                                << "]Resources is not unique or sequential : "
                                << " prev:" << resources[i - 1].toString()
                                << ", curr:" << resources[i].toString();
                    return false;
                }
            }
            return true;
        }

        // Sort a resource list and check it's uniqueness.
        bool ResourceListSortAndCheckUniqueness(SharedResourceIds& resources, uint64_t chunk_id) {
            std::sort(resources.begin(), resources.end(), CompareResourceId);
            return ResourceListIsUniqueAndSequential(resources, chunk_id);
        }

        //
        // Methods of class SharedResourceManager
        //

        // Get chunk id.
        uint64_t SharedResourceManager::GetChunkId() { return srm_module_->GetChunkId(); }

        // It's very inefficient to sort the vecotr each time we insert a resource. So we use
        // the following method to improve it.
        // During the log replay, AddFilter will be inserted disorderly into the active vector
        // and RemoveFilter will be inserted into the temporary vector. And at the end of the
        // log replay, we sort the active vector only once and remove all the shared resources
        // indicated by the temporary vector.
        void SharedResourceManager::ApplyTemporarySharedResources() {
            if (!shared_resources_.empty()) {
                // Sort the AddFilter list and check that it is unique.
                if (!ResourceListSortAndCheckUniqueness(shared_resources_, GetChunkId())) {
                    srm_err() << "The AddFilter list replayed is not unique or sequential.";
                    GetModule()->SetCheckFailed();
                }
                // Sort the RemoveFilter list and check that it is unique.
                if (!ResourceListSortAndCheckUniqueness(shared_resources_rmv_, GetChunkId())) {
                    srm_err() << "The AddFilter list replayed is not unique or sequential.";
                    GetModule()->SetCheckFailed();
                }
                // Remove the resources in temporary vector from the active vector.
                RemoveSharedResources(shared_resources_rmv_);
            } else {
                // Record error runlog for each resource in temporary vector.
                for (auto resource : shared_resources_rmv_) {
                    srm_err() << "Replay a RemoveFilter operation related to an unexisted shared "
                                 "resource: "
                              << resource;
                }
            }
            // Clear temporary vector.
            shared_resources_rmv_.clear();
        }

        rocksdb::Status SharedResourceManager::InitializationEnd(ILogRecordWriter& writer) {
            // At the end of ReplayLogRecord, apply the temporary resources into active vector.
            index_log() << "SharedResourceManager::InitializationEnd()";
            ApplyTemporarySharedResources();
            // Record the log wirter.
            SetLogWriter(&writer);
            return rocksdb::Status::OK();
        };

        // Add shared resources in batch.
        void SharedResourceManager::AddSharedResources(const SharedResourceIds& resources) {
            if (resources.size() > 0) {
                SharedResourceIds new_shared_resources;
                QueryNewSharedResources(resources, new_shared_resources);

                if (!new_shared_resources.empty()) {
                    // Append the shared resources in batch.
                    shared_resources_.insert(shared_resources_.end(), new_shared_resources.begin(),
                                             new_shared_resources.end());
                    // Keep the active vector sorted in order to support binary search.
                    std::sort(shared_resources_.begin(), shared_resources_.end(),
                              CompareResourceId);
                }
            }
        }

        // Remove shared resource in batch.
        void SharedResourceManager::RemoveSharedResources_Method1(
            SharedResourceIds& resources_rmv) {
            for (auto resource : resources_rmv) {
                auto it = std::lower_bound(shared_resources_.begin(), shared_resources_.end(),
                                           resource, CompareResourceId);
                if (it == shared_resources_.end() || (!EqualResourceId((*it), resource))) {
                    srm_err() << "Try to remove an unexisted shared resource: " << resource;
                    GetModule()->SetCheckFailed();
                    continue;
                }
                shared_resources_.erase(it);
            }
        }
        void SharedResourceManager::RemoveSharedResources_Method2(
            SharedResourceIds& resources_rmv) {
            std::vector<SharedResourceId>::iterator prev = shared_resources_.begin();
            std::vector<SharedResourceId>::iterator next = shared_resources_.end();
            SharedResourceIds resources_result;

            // The logic before calling this function make it sure that all the resources in
            // 'resources_rmv' must be in
            // 'shared_resources_'.
            if (shared_resources_.size() > resources_rmv.size()) {
                resources_result.reserve(shared_resources_.size() - resources_rmv.size());
            }

            // Skip the resources to be removed one by one, and copy all the remained resources to
            // the 'resources_result'.
            for (auto resource : resources_rmv) {
                if (prev == shared_resources_.end()) {
                    // Record the runlog for each unexisted shared resource to be deleted.
                    srm_err() << "Try to remove an unexisted shared resource: " << resource;
                    GetModule()->SetCheckFailed();
                    continue;
                }
                next = std::lower_bound(prev, shared_resources_.end(), resource, CompareResourceId);
                // Copy current segment to the tail of the temporary vector.
                if (next != prev) {
                    resources_result.insert(resources_result.end(), prev, next);
                }
                // Position the start of next segment.
                if (next == shared_resources_.end() || (!EqualResourceId((*next), resource))) {
                    srm_err() << "Try to remove an unexisted shared resource: " << resource;
                    GetModule()->SetCheckFailed();
                    prev = next;
                } else {
                    prev = (next + 1);
                }
            }

            // Copy the last segment to the tail of the temporary vector.
            if (prev != shared_resources_.end()) {
                resources_result.insert(resources_result.end(), prev, shared_resources_.end());
            }

            // Save the result after remove.
            shared_resources_.swap(resources_result);
        }
        void SharedResourceManager::RemoveSharedResources(SharedResourceIds& resources_rmv) {
            if (resources_rmv.size() <= 5) {
                RemoveSharedResources_Method1(resources_rmv);
            } else {
                RemoveSharedResources_Method2(resources_rmv);
            }
        }

        // Called by SharedResourceOperationLogRecordProvider::RegisterSharedResources to find new
        // shared resources.
        void SharedResourceManager::QueryNewSharedResources(const SharedResourceIds& list,
                                                            SharedResourceIds& shared_list) {
            if (0 == list.size()) {
                return;
            }
            shared_list.reserve(list.size());
            for (auto resource : list) {
                if (!IsResourceShared(resource)) {
                    shared_list.push_back(resource);
                }
            }
            return;
        }

        // Called by compaction job to query if the resources to be deleted are shared or not.
        /*rocksdb::Status
        SharedResourceManager::OnResourcesQuery(std::vector<SharedResourceDescription>& list) {
            // Check provider is ready or not.
            if (share_res_provider_ == nullptr) {
                srm_err() << "LogProvider is not ready.";
                return rocksdb::Status::Aborted();
            }

            // Querying share resource is not permitted during a split.
            if (share_res_provider_->GetSplitContext()->IsStateStart()) {
                srm_err() << "Querying shared resource is not permitted during a split.";
                return rocksdb::Status::Aborted();
            }

            // Query if resources are shared or not.
            for (auto it = list.begin(); it != list.end(); it++) {
                it->shared_flag = IsResourceShared(it->id);
                // Write runlog for shared resources query.
                srm_dbg() << "Query : resource:" << PLOG_ID_SVAL(it->id) \
                          << ", shared_flag:" << it->shared_flag \
                          << ", total:" << shared_resources_.size();
            }
            return rocksdb::Status::OK();
        }
        */

        // Set the remove_result of all the shared resources to fault if failed to write
        // RemoveFilter logs.
        void SharedResourceManager::SetSharedResourcesRemoveResultToFail(
            std::vector<SharedResourceRemoveDescription>& list) {
            for (auto it = list.begin(); it != list.end(); it++) {
                if (it->shared_flag) {
                    it->remove_result = RET_ERR;
                }
            }
        }

        // Called by compaction job to delete resources in batch.
        rocksdb::Status SharedResourceManager::OnResourcesDeletion(
            std::vector<SharedResourceRemoveDescription>& list) {
            LogRecordVector log_records;
            std::vector<SharedResourceId> resources;
            rocksdb::Status s = rocksdb::Status::OK();

            // Check provider is ready or not.
            if (share_res_provider_ == nullptr) {
                srm_err() << "LogProvider is not ready.";
                return rocksdb::Status::Aborted();
            }

            //// Deleting share resource is not permitted during a split even if the resource
            /// belongs to
            //// a previous split but current split. That is, all the operations between the
            /// StartSplit
            //// and the related CommitSplit/RollbackSplit belongs to the split identified and
            /// bounded by
            //// the StartSplit and CommitSplit/RollbackSplit.
            // if (share_res_provider_->GetSplitContext()->IsStateStart()) {
            //    srm_err() << "Deleting shared resource is not permitted during a split.";
            //    return rocksdb::Status::Aborted();
            //}

            if (list.empty()) {
                srm_log() << "No shared resource to be deleted.";
                return rocksdb::Status::OK();
            }

            // Init all the resources to be removed failed.
            for (auto it = list.begin(); it != list.end(); it++) {
                it->shared_flag = false;
                it->remove_result = RET_ERR;
            }

            // Copy the shared resource list to be deleted.
            SharedResourceIds shared_resources_rmv;
            for (auto it = list.begin(); it != list.end(); ++it) {
                shared_resources_rmv.push_back(it->id);
            }

            // Sort the shared resource list to be deleted and check that it is unique.
            if (!ResourceListSortAndCheckUniqueness(shared_resources_rmv, GetChunkId())) {
                srm_err() << "The shared resource list to be deleted is not unique or sequential.";
                return rocksdb::Status::Aborted();
            }

            // Construct RemoveFilter and RemoveReferebce log records for each resource.
            for (auto it = list.begin(); it != list.end(); it++) {
                s = OnResourceDeletion(log_records, resources, it);
                if (!s.ok()) {
                    srm_err() << "Write log records failed.";
                    // Set the remove_result of all the shared resources to fault because no
                    // RemoveFilter log is writed.
                    SetSharedResourcesRemoveResultToFail(list);
                    return rocksdb::Status::OK();
                }
            }

            // Write RemoveFilter and RemoveReferebce log records in batch.
            s = share_res_provider_->WriteRecords(log_records, true);
            if (!s.ok()) {
                srm_err() << "Write log records failed.";
                // Set the remove_result of all the shared resources to fault because RemoveFilter
                // logs is writed failed.
                SetSharedResourcesRemoveResultToFail(list);
                return rocksdb::Status::OK();
            }

            auto total = shared_resources_.size();

            // Update shared resources list in memory.
            RemoveSharedResources(resources);

            // Append reference operations in memory.
            share_res_provider_->AppendSharedResourceReferenceOperation(log_records);

            // Write runlog for shared resources query.
            auto size = list.size();
            srm_log()
                << "Remove : number:(" << size << "=" << total << "-" << shared_resources_.size()
                << "), "
                << "lsn:(" /*<< log_records.front().GetLsn() << ","  << log_records.back().GetLsn() << ")."*/;

            return rocksdb::Status::OK();
        }

        // Construct two log records for one resource to be deleted, one is RemoveFilter, the other
        // is RemoveReferebce.
        rocksdb::Status SharedResourceManager::OnResourceDeletion(
            LogRecordVector& log_records, std::vector<SharedResourceId>& resources,
            std::vector<SharedResourceRemoveDescription>::iterator& resource_it) {
            // Check current resource is shared or not.
            resource_it->shared_flag = IsResourceShared(resource_it->id);
            if (!resource_it->shared_flag) {
                // Non-shared resource.
                resource_it->remove_result = RET_OK;
                return rocksdb::Status::OK();
            }

            resource_it->remove_result = RET_ERR;

            // Construct a RemoveFilter log for current shared resource.
            rocksdb::Status s = PrepareFilterLog(
                log_records, SharedResourceOperationType::RemoveFilter, resource_it->id);
            if (!s.ok()) {
                srm_err() << "Construct RemoveFilter log failed. resource(" << resource_it->id;
                return s;
            }
            // Construct a RemoveReference log for current shared resource.
            s = share_res_provider_->PrepareReferenceLog(
                log_records, SharedResourceOperationType::RemoveReference, resource_it->id,
                GetChunkId(), INVALID_CHUNK_ID);
            if (!s.ok()) {
                srm_err() << "Construct RemoveReference log failed. resource(" << resource_it->id;
                return s;
            }
            // Construct the two logs successfully.
            resource_it->remove_result = RET_OK;
            resources.push_back(resource_it->id);
            return s;
        }

        // Construct one filter log.
        rocksdb::Status SharedResourceManager::PrepareFilterLog(
            LogRecordVector& log_records, const SharedResourceOperationType oper_type,
            const SharedResourceId& resource) {
            SharedResourceFilterOperation operation(oper_type, resource);
            char* dst_buf = nullptr;
            uint32_t buf_size = 0;

            // Allocate unique buffer and encode current operation into the buffer.
            buf_size = operation.EncodeTo(dst_buf);
            if (nullptr == dst_buf) {
                srm_err() << "Fetal Error when alloc dst_buf fail.";
                return rocksdb::Status::Aborted();
            }

            // Construct and write the log record of current operation.
            log_records.push_back(LogRecord(LogRecordType::SharedResourceFilterLog,
                                            rocksdb::Slice(dst_buf, buf_size)));
            return rocksdb::Status::OK();
        }
        // Construct filter logs in batch and output them.
        rocksdb::Status SharedResourceManager::PrepareFilterLog(
            LogRecordVector& log_records, const SharedResourceOperationType oper_type,
            const std::vector<SharedResourceId>& resources) {
            rocksdb::Status s;
            for (auto resource : resources) {
                s = PrepareFilterLog(log_records, oper_type, resource);
                if (!s.ok()) {
                    log_records.clear();
                    return s;
                }
            }
            return rocksdb::Status::OK();
        }

        // Get the approximate length of WriteCheckpoint.
        uint32_t SharedResourceManager::GetApproximateLengthOfWriteCheckpoint() {
            if (shared_resources_.size() <= 0) {
                return 0;
            }

            SharedResourceFilterOperation oper(SharedResourceOperationType::AddReference,
                                               (*shared_resources_.begin()));
            return (uint32_t)(oper.GetEncodeLength() * shared_resources_.size());
        }

        // Write Checkpoint
        rocksdb::Status SharedResourceManager::WriteCheckpoint(bool realloc_lsn) {
            srm_log() << "WriteCheckpoint filter " << shared_resources_.size();

            // Prepare filter logs.
            LogRecordVector log_records;
            RDB_RIF(PrepareFilterLog(log_records, SharedResourceOperationType::AddFilter,
                                     shared_resources_));

            // Write filter logs.
            if (!log_records.empty()) {
                RDB_RIF(share_res_provider_->WriteRecords(log_records, false));
            }

            // Run log for CheckPoint.
            for (auto resource : shared_resources_) {
                srm_dbg() << "Memory Filter when CheckPoint: resource:" << resource;
            }

            return rocksdb::Status::OK();
        }

        // Replay log records.
        rocksdb::Status SharedResourceManager::ReplayLogRecord(const LogRecord& record,
                                                               bool& needMoreRecords) {
            rocksdb::Status s = rocksdb::Status::OK();
            SharedResourceFilterOperation* oper = nullptr;
            do {
                // Check the type of the log record.
                if (record.GetType() != LogRecordType::SharedResourceFilterLog) {
                    srm_err() << "Invalid log record type (" << std::to_string(record.GetType());
                    s = rocksdb::Status::Aborted();
                    break;
                }

                // Decode the log record.
                oper = SharedResourceFilterOperation::DecodeFrom(record);
                if (oper == nullptr) {
                    srm_err() << "Fetal Err when decode filter.";
                    s = rocksdb::Status::Aborted();
                    break;
                }

                SharedResourceOperationType type = oper->GetType();

                // Replay an Addfilter.
                if (type == SharedResourceOperationType::AddFilter) {
                    // Save the AddFilter in the active vector which is not sorted for the time
                    // being.
                    // The active vector will be sorted at the end of RelayLog.
                    shared_resources_.push_back(oper->GetResource());
                    // Write runlog of filters
                    srm_dbg() << "Replay add filter: " << oper->GetResource().toString();
                    break;
                }

                if (type != SharedResourceOperationType::RemoveFilter) {
                    // Unknown operation.
                    srm_err() << "Invalid filter operation type (" << static_cast<int>(type);
                    s = rocksdb::Status::Aborted();
                    break;
                }

                // Replay an Rmvfilter.
                // 1) Check split state.
                // if (share_res_provider_->GetSplitContext()->IsStateStart()) {
                //    srm_err() << "Replay " << GetOperTypeName(type) << ", shouldn't be during a
                //    split : ";
                //    s = rocksdb::Status::Aborted();
                //    break;
                //}
                // 2) Save the RemoveFilter in the temporary vector for the time being.
                shared_resources_rmv_.push_back(oper->GetResource());
                // Write runlog of filters
                srm_dbg() << "Replay RemoveFilter" << oper->GetResource().toString();
                break;
            } while (0);

            if (oper != nullptr) {
                delete oper;
            }

            if (s.ok()) {
                // Replay successfully.
                needMoreRecords = true;
                return rocksdb::Status::OK();
            }

            // Replay failed.
            GetModule()->SetCheckFailed();
            // if (GetModule()->IsRescueMode()) {
            //    // 1) Under rescue mode, the failure will not stop the replay process of the
            //    subsequent log records.
            //    needMoreRecords = true;
            //    return rocksdb::Status::OK();
            //}
            // 2) Under normal mode, any failure will stop the replay process of the subsequent log
            // records.
            needMoreRecords = false;
            return s;
        }

        //
        // Implementation of the methods of SharedResourceOperationLogRecordProvider
        //

        void SharedResourceOperationLogRecordProvider::ReplayNewSharedResources() {
            // Sort the shared resources of the split context.
            split_ctx_.SortSharedResources();
            auto& shared_resources = split_ctx_.GetSharedResources();
            // Query new shared resources.
            SharedResourceIds new_shared_resources;
            share_res_manager_->QueryNewSharedResources(shared_resources, new_shared_resources);
            // Register new shared resources into the split context.
            split_ctx_.RegisterNewSharedResources(new_shared_resources);
        }

        rocksdb::Status SharedResourceOperationLogRecordProvider::InitializationEnd(
            ILogRecordWriter& logWriter) {
            index_log() << "SharedResourceOperationLogRecordProvider::InitializationEnd()";
            // Set log writer.
            SetLogWriter(&logWriter);

            // Replay new shared resources of the latest split.
            // Comment: Before call this method, share_res_manager_->InitializationEnd() must have
            // been
            //          called before.
            ReplayNewSharedResources();

            // Judge the latest split replayed is successful or not.
            if (!split_ctx_.IsStateStart()) {
                return rocksdb::Status::OK();
            }

            // Check split description pointer.
            auto& split_description = split_ctx_.GetDescription();
            if (split_description == nullptr) {
                srm_err() << "InitializationEnd: split description pointer is NULL. ";
                GetModule()->SetCheckFailed();
                return rocksdb::Status::Aborted();
            }

            mongo::ChunkMetadata::KeyRange left_key_range =
                split_ctx_.GetDescription()->GetLeftKeyRange();
            mongo::ChunkMetadata::KeyRange parent_key_range =
                split_ctx_.GetDescription()->GetKeyRange();

            if (key_range_ == left_key_range) {
                // Add shared resource into filters list.
                share_res_manager_->AddSharedResources(split_ctx_.GetNewSharedResources());
                // Update operation list.
                split_ctx_.SetStateToSuccess();  // Set the split state to success.
                RemoveStartSplitOperation();     // Split is over, then remove the StartSplit
                                                 // operation.
                srm_err() << "CommitSplit recover successfully. "
                          << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                          << "split_id:" << split_ctx_.GetId();
                // Split OK and CommitSplit is missing.
                failed_split_end_oper_ = SharedResourceOperationType::CommitSplit;
                return rocksdb::Status::OK();
            } else if (key_range_ == parent_key_range) {
                // Update operation list.
                split_ctx_.SetStateToFault();  // Set the split state to fault
                RollbackSplitOperations();     // Rollback all the operations related to the split.
                srm_err() << "RollbackSplit recover successfully. "
                          << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                          << "split_id:" << split_ctx_.GetId();
                // Split failed and RollbackSplit is missing.
                failed_split_end_oper_ = SharedResourceOperationType::RollbackSplit;
                return rocksdb::Status::OK();
            } else {
                // Unconsistent key ranges.
                srm_err() << "Unconsistent key range: ";
                GetModule()->SetCheckFailed();
                return rocksdb::Status::Aborted();
            }
        }

        // Write run log for an operation.
        void SharedResourceOperationLogRecordProvider::RunLogOperation(
            const SharedResourceOperation* oper) {
            uint64_t lsn = oper->GetLsn();
            SharedResourceOperationType oper_type = oper->GetType();
            const SharedResourceReferenceOperation* reference_oper = nullptr;
            const SplitOperation* split_oper = nullptr;
            const ProcessedLSNOperation* processed_oper = nullptr;
            SharedResourceId resource;

            switch (oper_type) {
                case SharedResourceOperationType::AddReference:
                    reference_oper =
                        reinterpret_cast<const SharedResourceReferenceOperation*>(oper);
                    resource = reference_oper->GetResource();
                    srm_dbg() << "CheckPoint " << GetOperTypeName(oper_type) << " : "
                              << "lsn:" << lsn << ", "
                              << "resource:" << resource << ", "
                              << "chunk:"
                              << "(" << reference_oper->GetParentChunkId() << ","
                              << reference_oper->GetChildChunkId() << ").";
                    break;
                case SharedResourceOperationType::RemoveReference:
                    reference_oper =
                        reinterpret_cast<const SharedResourceReferenceOperation*>(oper);
                    resource = reference_oper->GetResource();
                    srm_dbg() << "CheckPoint " << GetOperTypeName(oper_type) << " : "
                              << "lsn:" << lsn << ", "
                              << "resource:" << resource << ", "
                              << "chunk:" << reference_oper->GetParentChunkId();
                    break;
                case SharedResourceOperationType::StartSplit:
                    split_oper = reinterpret_cast<const SplitOperation*>(oper);
                    srm_log() << "CheckPoint " << GetOperTypeName(oper_type) << " : "
                              << "lsn:" << lsn << ", "
                              << "split_id:" << split_oper->GetId();
                    break;
                case SharedResourceOperationType::CommitSplit:
                case SharedResourceOperationType::RollbackSplit:
                    // Should be never here.
                    split_oper = reinterpret_cast<const SplitOperation*>(oper);
                    srm_err() << "CheckPoint " << GetOperTypeName(oper_type) << " : "
                              << "lsn:" << lsn << ", "
                              << "split_id:" << split_oper->GetId();
                    break;
                case SharedResourceOperationType::Processed:
                    // Should be never here.
                    processed_oper = reinterpret_cast<const ProcessedLSNOperation*>(oper);
                    srm_err() << "CheckPoint " << GetOperTypeName(oper_type) << " : "
                              << "lsn:" << lsn << ", "
                              << "processed_lsn:" << processed_oper->GetProcessedLSN();
                    break;
                default:
                    // Should be never here.
                    srm_err() << "CheckPoint " << GetOperTypeName(oper_type) << " : "
                              << "lsn:" << lsn;
                    break;
            }
        }

        // Get chunk id.
        uint64_t SharedResourceOperationLogRecordProvider::GetChunkId() {
            return srm_module_->GetChunkId();
        }

        // Get the approximate length of WriteCheckpoint.
        uint32_t SharedResourceOperationLogRecordProvider::GetApproximateLengthOfWriteCheckpoint() {
            if (operations_.size() <= 0) {
                return 0;
            }

            // Only reference operations reside in 'operations_' when this function is called in
            // StartSplit(), so
            // a multiplication operation is enough and it's no need to accumulate the length of
            // transaction log
            // by traversing all the operations.
            auto type = (*operations_.begin())->GetType();
            if (!IsReferenceOper(type)) {
                srm_err() << "WriteCheckpoint operation " << operations_.size();
            }
            return (uint32_t)((*operations_.begin())->GetEncodeLength() * operations_.size());
        }

        rocksdb::Status SharedResourceOperationLogRecordProvider::PrepareCheckpointRecords(
            LogRecordVector& log_records, bool realloc_lsn) {
            rocksdb::Slice slice;
            char* dst_buf = nullptr;
            uint32_t buf_size = 0;

            // Prepare log records in batch.
            for (SharedResourceOperation* oper : operations_) {
                // Allocate unique buffer and encode current operation into the buffer.
                buf_size = oper->EncodeTo(dst_buf);
                if (!dst_buf) {
                    srm_err() << "Fatal Error when alloc dst_buf.";
                    return rocksdb::Status::Aborted();
                }

                // Construct the log record of current operation.
                // Comment:
                // (1) If 'realloc_lsn' is TRUE, it means that the old lsn of each operation will be
                //     ignored and reallocated. But the LSN of StartSplit operation can't be changed
                //     because it is also used as the split id which is used to pair StartSplit with
                //     CommitSplit or RollbackSplit.
                // (2) Only the db rescue tool will set the 'realloc_lsn' to TRUE to call this
                // method,
                //     and the tool also makes it sure that no StartSplit operation is existed.
                log_records.push_back(LogRecord(LogRecordType::LoadBalancingTransactionLog,
                                                (realloc_lsn ? INVALID_LSN : oper->GetLsn()),
                                                rocksdb::Slice(dst_buf, buf_size)));
            }

            return rocksdb::Status::OK();
        }

        // Write Checkpoint, that is, rewrite all logs related to all the shared resource operations
        // in memory.
        rocksdb::Status SharedResourceOperationLogRecordProvider::WriteCheckpoint(
            bool realloc_lsn) {
            LogRecordVector log_records;

            srm_log() << "WriteCheckpoint operation " << operations_.size();
            RDB_RIF(PrepareCheckpointRecords(log_records, realloc_lsn));

            // Write log records in batch.
            RDB_RIF(WriteRecords(log_records, false));

            // Run log for CheckPoint.
            for (const SharedResourceOperation* oper : operations_) {
                RunLogOperation(oper);
            }

            // Write log records in batch.
            return rocksdb::Status::OK();
        }

        // Construct one reference log.
        rocksdb::Status SharedResourceOperationLogRecordProvider::PrepareReferenceLog(
            LogRecordVector& log_records, const SharedResourceOperationType oper_type,
            const SharedResourceId& resource, const uint64_t& parent_chunk_id,
            const uint64_t& child_chunk_id) {
            SharedResourceReferenceOperation operation(oper_type, resource, parent_chunk_id,
                                                       child_chunk_id);
            rocksdb::Status s;
            rocksdb::Slice slice;
            char* dst_buf = nullptr;
            uint32_t buf_size = 0;

            // Allocate unique buffer and encode current operation into the buffer.
            buf_size = operation.EncodeTo(dst_buf);
            if (nullptr == dst_buf) {
                srm_err() << "Fetal Error when alloc dst_buf fail.";
                return rocksdb::Status::Aborted();
            }

            // Construct and write the log record of current operation.
            log_records.push_back(LogRecord(LogRecordType::LoadBalancingTransactionLog,
                                            rocksdb::Slice(dst_buf, buf_size)));
            return rocksdb::Status::OK();
        }
        // Construct reference logs in batch.
        rocksdb::Status SharedResourceOperationLogRecordProvider::PrepareReferenceLog(
            LogRecordVector& log_records, const SharedResourceOperationType oper_type,
            const std::vector<SharedResourceId>& resources, const uint64_t& parent_chunk_id,
            const uint64_t& child_chunk_id) {
            rocksdb::Status s;
            for (auto resource : resources) {
                s = PrepareReferenceLog(log_records, oper_type, resource, parent_chunk_id,
                                        child_chunk_id);
                if (!s.ok()) {
                    log_records.clear();
                    return s;
                }
            }
            return rocksdb::Status::OK();
        }

        // SplitState [ SplitStart SplitCommit SplitRollback ]
        rocksdb::Status SharedResourceOperationLogRecordProvider::PrepareSplitLog(
            LogRecordVector& log_records, const SharedResourceOperationType oper_type,
            const uint64_t split_id, const std::shared_ptr<SplitDescription>& description) {
            SplitOperation operation(oper_type, split_id, description);
            char* dst_buf = nullptr;
            uint32_t buf_size = 0;

            // Allocate unique buffer and encode current operation into the buffer.
            buf_size = operation.EncodeTo(dst_buf);
            if (nullptr == dst_buf) {
                srm_err() << "Fetal Error when alloc dst_buf fail.";
                return rocksdb::Status::Aborted();
            }

            log_records.push_back(LogRecord(LogRecordType::LoadBalancingTransactionLog,
                                            rocksdb::Slice(dst_buf, buf_size)));
            return rocksdb::Status::OK();
        }

        // Write a Processed log.
        rocksdb::Status SharedResourceOperationLogRecordProvider::LogProcessedLSN(
            bool rewrite_flag) {
            invariant(processed_lsn_ != INVALID_LSN);

            ProcessedLSNOperation operation(processed_lsn_);
            rocksdb::Status s;
            char* dst_buf = nullptr;
            uint32_t buf_size = 0;

            // Alloc unique buffer for writting log
            buf_size = operation.EncodeTo(dst_buf);
            if (nullptr == dst_buf) {
                srm_err() << "Fetal Error when alloc dst_buf. "
                          << "processed_lsn:" << processed_lsn_;
                // Record the missing processed LSN state.
                processed_lsn_failed_flag_ = true;
                return rocksdb::Status::Aborted();
            }

            // Write the ProcessedLSN log
            LogRecord log_rec(LogRecordType::LoadBalancingTransactionLog,
                              rocksdb::Slice(dst_buf, buf_size));
            s = WriteRecord(log_rec, true, rewrite_flag);
            if (!s.ok()) {
                srm_err() << "Write ProcessedLSN log failed."
                          << "processed_lsn:" << processed_lsn_;
                // Record the missing processed LSN state.
                processed_lsn_failed_flag_ = true;
            } else {
                // Clear the missing processed LSN state.
                processed_lsn_failed_flag_ = false;
            }

            if (rewrite_flag) {
                srm_err() << "RewriteProcessed: lsn:" << log_rec.GetLsn();
            } else {
                srm_log() << "SetProcessed: lsn:" << log_rec.GetLsn();
            }

            // Alawys return OK even if write log failed.
            return rocksdb::Status::OK();
        }

        // Append shared resource operation(s) exclude split operation.
        void SharedResourceOperationLogRecordProvider::AppendSharedResourceReferenceOperation(
            LogRecordVector& log_records) {
            SharedResourceReferenceOperation* oper = nullptr;
            SharedResourceOperationType type;
            for (auto record : log_records) {
                type = SplitOperation::GetOperTypeOfLogRecord(record);
                if (!IsReferenceOper(type)) {
                    // Ignore other operations.
                    continue;
                }
                // Append reference operation.
                oper = SharedResourceReferenceOperation::DecodeFrom(record);
                operations_.push_back(oper);
            }
        }

        // Called by PlogEnv to register shared plogs.
        rocksdb::Status SharedResourceOperationLogRecordProvider::RegisterSharedResources(
            SharedResourceIds& resources) {
            // The split state must be 'SplitStart' when this operation occurs.
            if (!split_ctx_.IsStateStart()) {
                srm_err() << "Invalid split state(" << static_cast<int>(split_ctx_.GetState());
                return rocksdb::Status::Aborted();
            }

            auto size = resources.size();
            if (size <= 0) {
                srm_log() << "No resource.";
                return rocksdb::Status::OK();
            }

            // Copy the shared resource list to be registered.
            SharedResourceIds shared_resources;
            shared_resources.insert(shared_resources.end(), resources.begin(), resources.end());

            // Sort the shared resource list to be registered and check that it is unique.
            if (!ResourceListSortAndCheckUniqueness(shared_resources, GetChunkId())) {
                srm_err()
                    << "The shared resource list to be registered is not unique or sequential : ";
                return rocksdb::Status::Aborted();
            }

            // Query new shared resources.
            SharedResourceIds new_shared_resources;
            share_res_manager_->QueryNewSharedResources(shared_resources, new_shared_resources);
            if (!shared_resources.empty()) {
                // Write reference logs.
                LogRecordVector log_records;
                SharedResourceOperationType oper_type = SharedResourceOperationType::AddReference;
                // 1) Construct log records in batch.
                rocksdb::Status s = PrepareReferenceLog(log_records, oper_type, shared_resources,
                    split_ctx_.GetDescription()->GetParentChunkId(),
                    split_ctx_.GetDescription()->GetChildChunkId());
                if (!s.ok()) {
                    srm_err() << "Prepare reference log failed.";
                    return s;
                }
                // 2) Write log records in batch.
                s = WriteRecords(log_records, true);
                if (!s.ok()) {
                    srm_err() << "Write log records failed";
                    return s;
                }
                if (MONGO_FAIL_POINT(failToRegisterSharedResources)) {
                    srm_err() << "Fail point : failToRegisterSharedResources : "
                        << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                        << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                        << "split_id:" << split_ctx_.GetId();
                    return rocksdb::Status::Aborted();
                }

                // Append operations in batch.
                AppendSharedResourceReferenceOperation(log_records);

                // Write runlog.
                size = log_records.size();
                if (size > 0) {
                    srm_log() << "AddRef : number:" << size << ", lsn:(" << log_records.front().GetLsn()
                        << "," << log_records.back().GetLsn() << ").";
                }
                for (uint32_t i = 0; i < size; i++) {
                    srm_dbg() << "AddRef : "
                        << "lsn:" << log_records.get_at(i).GetLsn() << ", "
                        << "resource:" << resources[i].id << ", "
                        << "parent:" << split_ctx_.GetDescription()->GetParentChunkId() << ", "
                        << "child:" << split_ctx_.GetDescription()->GetChildChunkId();
                }
            }

            // Register shared resources into split context.
            split_ctx_.RegisterSharedResources(shared_resources, new_shared_resources);
            return rocksdb::Status::OK();
        }

        //
        // GC related functions
        //

        // Get some unprocessed operations of a number limited by maxBatchMaxSize.
        void SharedResourceOperationLogRecordProvider::GetUnprocessedOperations(
            uint32_t maxBatchMaxSize,
            std::vector<SharedResourceReferenceOperation>& unprocessedRecords) {
            int old_size = unprocessedRecords.size();
            SharedResourceReferenceOperation* reference_oper = nullptr;

            for (SharedResourceOperation* oper : operations_) {
                if (unprocessedRecords.size() >= maxBatchMaxSize) {
                    break;
                }

                switch (oper->GetType()) {
                    case SharedResourceOperationType::StartSplit:
                        // A SplitStart operation means a split is in progress. All the later shared
                        // resource operations
                        // must be related to the uncompleted split and have not yet become
                        // effective.
                        srm_log() << "End at a StartSplit operation."
                                  << "split_lsn:" << oper->GetLsn();
                        return;
                    case SharedResourceOperationType::AddReference:
                    case SharedResourceOperationType::RemoveReference:
                        // Output the operations that are outside of split or belongs to a committed
                        // split.
                        reference_oper = reinterpret_cast<SharedResourceReferenceOperation*>(oper);
                        unprocessedRecords.push_back(*reference_oper);
                        break;
                    default:
                        // Invalid operations.
                        srm_err() << "Invalid operation type ("
                                  << static_cast<int>(oper->GetType());
                        dassert(false);
                        return;
                }
            }

            int num = unprocessedRecords.size() - old_size;
            if (num <= 0) {
                srm_dbg() << "GetUnprocessed : number:" << num << ", max:" << maxBatchMaxSize
                          << ", old:" << old_size;
            } else {
                uint64_t lsn1 = unprocessedRecords[old_size].GetLsn();
                uint64_t lsn2 = unprocessedRecords.back().GetLsn();
                srm_log() << "GetUnprocessed : number:" << num << ", lsn:(" << lsn1 << "," << lsn2
                          << ")"
                          << ", max:" << maxBatchMaxSize << ", old:" << old_size;
            }

            // Write runlog of GetUnprocessedOperations.
            for (auto reference_oper = unprocessedRecords.begin() + old_size;
                 reference_oper != unprocessedRecords.end(); reference_oper++) {
                SharedResourceId resource = reference_oper->GetResource();
                srm_dbg() << "GetUnprocessed : "
                          << "lsn:" << reference_oper->GetLsn() << ", "
                          << "resource:" << resource << ", "
                          << "parent:" << reference_oper->GetParentChunkId() << ", "
                          << "child:" << reference_oper->GetChildChunkId();
            }
        }

        rocksdb::Status SharedResourceOperationLogRecordProvider::SetLastProcessedOperationLSN(
            uint64_t processed_lsn) {
            // Check the processed LSN to be set.
            if (processed_lsn == INVALID_LSN) {
                srm_err() << "SetProcessed: invalid processed lsn (" << processed_lsn << ").";
                return rocksdb::Status::Aborted();
            }
            if (split_ctx_.IsStateStart() && (processed_lsn >= split_ctx_.GetLsn())) {
                srm_err() << "SetProcessed: the processed lsn confilcts with the split lsn. "
                          << "processed_lsn(" << processed_lsn << ") >= split_lsn("
                          << split_ctx_.GetLsn() << ").";
                return rocksdb::Status::Aborted();
            }
            if ((processed_lsn_ != INVALID_LSN) && (processed_lsn_ >= processed_lsn)) {
                srm_err() << "SetProcessed: re-sent or disorder processed lsn "
                          << "(old:" << processed_lsn_ << ", new:" << processed_lsn << ").";
                return rocksdb::Status::OK();
            }

            srm_log() << "SetProcessed: (old:" << processed_lsn_ << ", new:" << processed_lsn
                      << ").";

            // Remove the processed operations from memory.
            operations_.RemoveProcessedOperations(processed_lsn);
            processed_lsn_ = processed_lsn;

            // Write the processed lsn into transaction log.
            // If WriteCheckPoint occurs with this log, all the processed operations have been
            // removed
            // from 'operations_' above and will not appear in the new CheckPoint.
            (void)LogProcessedLSN();
            return rocksdb::Status::OK();
        }

        //
        // Split related functions
        //

        // Write the log of a split operation.
        rocksdb::Status SharedResourceOperationLogRecordProvider::LogSplitState(
            SharedResourceOperationType oper_type, uint64_t split_id,
            const std::shared_ptr<SplitDescription>& description, bool rewrite_flag) {
            LogRecordVector log_records;
            rocksdb::Status s = rocksdb::Status::OK();
            std::string header = "LogSplitState. ";

            do {
                auto new_shared_resources = split_ctx_.GetNewSharedResources();
                if (oper_type == SharedResourceOperationType::CommitSplit) {
                    // Construct AddFilter logs in batch.
                    index_LOG(0) << "write Manager loadBalancer log shared file num: "
                                 << new_shared_resources.size();
                    for (auto resource : new_shared_resources) {
                        index_LOG(0) << "write Manager loadBalancer log shared file: "
                                     << resource.toString();
                    }

                    s = share_res_manager_->PrepareFilterLog(
                        log_records, SharedResourceOperationType::AddFilter, new_shared_resources);
                    if (!s.ok()) {
                        header = "PrepareFilterLog fail. ";
                        break;
                    }
                }

                // Construct split operation log.
                s = PrepareSplitLog(log_records, oper_type, split_id, description);
                if (!s.ok()) {
                    header = "Prepare split log failed. ";
                    break;
                }

                // Write the log record of split operation.
                //   'WriteRecord' will call RewriteImportantFailedLog() at first before writting
                //   current log.
                //   'split_oper_to_write_' is used to avoid writting CommitSplit or RollbackSplit
                //   repeatedly.
                //   After calling 'WriteRecord', 'split_oper_to_write_' must be reset.
                split_oper_to_write_ = oper_type;
                bool allow_check_point = (oper_type == SharedResourceOperationType::StartSplit);
                s = WriteRecords(log_records, allow_check_point, rewrite_flag);
                split_oper_to_write_ = SharedResourceOperationType::None;
                if (!s.ok()) {
                    header = "Write split log failed. ";
                    break;
                }

                // Clear the missing split end operation.
                failed_split_end_oper_ = SharedResourceOperationType::None;

                // Set the LSN of this new split.
                if (oper_type == SharedResourceOperationType::StartSplit) {
                    split_ctx_.SetLsn(log_records.front().GetLsn());
                    break;
                }

                if (oper_type != SharedResourceOperationType::CommitSplit) {
                    break;
                }

                // Write runlog for filters generated by CommitSplit.
                auto size = new_shared_resources.size();
                if (size >= 1) {
                    srm_log() << "AddFilter : number:" << size << ", lsn:("
                              << log_records.front().GetLsn() << ","
                              << log_records.get_at(size - 1).GetLsn() << ").";
                    for (uint32_t i = 0; i < size; i++) {
                        // Write runlog for each filter.
                        srm_dbg() << "AddFilter : "
                                  << "lsn:" << log_records.get_at(i).GetLsn() << ", "
                                  << "resource:" << new_shared_resources[i];
                    }
                }
                break;
            } while (0);

            if ((!s.ok()) || rewrite_flag) {
                srm_err() << "" << header << "rewrite:" << rewrite_flag << ", "
                          << "oper_type:" << static_cast<int>(oper_type) << ", "
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                          << "split_id:" << split_ctx_.GetId();
            }
            if (!s.ok()) {
                // Record the missing split end operation.
                failed_split_end_oper_ =
                    (IsSplitEnd(oper_type) ? oper_type : SharedResourceOperationType::None);
            }

            return s;
        }

        // Start split
        rocksdb::Status SharedResourceOperationLogRecordProvider::StartSplit(
            const std::shared_ptr<SplitDescription>& description) {
            rocksdb::Status s;
            SharedResourceOperationType oper_type = SharedResourceOperationType::StartSplit;

            if (description == nullptr) {
                srm_err() << "Input a null pointer to SplitDescription. ";
                return rocksdb::Status::Aborted();
            }

            if (description->parentChunkId != GetChunkId()) {
                srm_err() << "Unmatched parent chunk id. (" << GetChunkId() << ","
                          << description->parentChunkId << ").";
                return rocksdb::Status::Aborted();
            }

            // Check if there are too many logs that have not been consumed by GlobalGC.
            RDB_RIF(CheckCheckpointSize());

            // Prohibit concurrent split
            if (split_ctx_.IsStateStart()) {
                srm_err() << "Overlap split transactions. "
                          << "old("
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ","
                          << "split_id:" << split_ctx_.GetId();

                return rocksdb::Status::Aborted();
            }

            // Write StartSplit log.
            s = LogSplitState(oper_type, split_ctx_.GetId(), description);
            if (!s.ok()) {
                srm_err() << "Write split log failed. "
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                          << "split_id:" << split_ctx_.GetId();
                return s;
            }

            // Append StartSplit operation.
            SplitOperation* split_oper = new (std::nothrow)
                SplitOperation(split_ctx_.GetLsn(), SharedResourceOperationType::StartSplit,
                               split_ctx_.GetLsn(), description);
            if (nullptr == split_oper) {
                return rocksdb::Status::Aborted();
            }
            operations_.push_back(reinterpret_cast<SharedResourceOperation*>(split_oper));

            // Update the split context.
            split_ctx_.SetId(split_ctx_.GetLsn());
            split_ctx_.SetDescription(description);
            split_ctx_.SetStateToStart();
            srm_log() << "StartSplit success. "
                      << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                      << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                      << "split_id:" << split_ctx_.GetId();
            return rocksdb::Status::OK();
        }

        // Rollback split
        rocksdb::Status SharedResourceOperationLogRecordProvider::RollbackSplit() {
            if (split_ctx_.IsStateNone()) {
                srm_err() << "Fetal Error when RollbackSplit without related StartSplit. "
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                          << "split_id:" << split_ctx_.GetId();
                return rocksdb::Status::Aborted();
            } else if (split_ctx_.IsStateSuccess()) {
                srm_err() << "Fetal Error when RollbackSplit without related StartSplit. "
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                          << "split_id:" << split_ctx_.GetId();
                return rocksdb::Status::Aborted();
            } else if (split_ctx_.IsStateFault()) {
                srm_err() << "Maybe re-sent RollbackSplit, ignore it. "
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                          << "split_id:" << split_ctx_.GetId();
                return rocksdb::Status::OK();
            }

            // Rollback operation list.
            split_ctx_.SetStateToFault();  // Set the split state to fault
            RollbackSplitOperations();     // Rollback all the operations related to the split.
            srm_log() << "RollbackSplit success. "
                      << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                      << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                      << "split_id:" << split_ctx_.GetId();

            // Write StartSplit log.
            (void)LogSplitState(SharedResourceOperationType::RollbackSplit, split_ctx_.GetId(),
                                split_ctx_.GetDescription());
            return rocksdb::Status::OK();
        }

        // Commit split
        rocksdb::Status SharedResourceOperationLogRecordProvider::CommitSplit() {
            if (split_ctx_.IsStateNone()) {
                srm_err() << "Fetal Error when CommitSplit without related StartSplit. "
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                          << "split_id:" << split_ctx_.GetId();
                return rocksdb::Status::Aborted();
            } else if (split_ctx_.IsStateFault()) {
                srm_err() << "Fetal Error when CommitSplit without related StartSplit. "
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                          << "split_id:" << split_ctx_.GetId();
                return rocksdb::Status::Aborted();
            } else if (split_ctx_.IsStateSuccess()) {
                srm_err() << "Maybe re-sent CommitSplit, ignore it. "
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                          << "split_id:" << split_ctx_.GetId();
                return rocksdb::Status::OK();
            }

            // Add shared resource filters.
            auto total = share_res_manager_->GetSharedResources().size();
            share_res_manager_->AddSharedResources(split_ctx_.GetNewSharedResources());
            // Append AddReference operations.
            split_ctx_.SetStateToSuccess();  // Set the split state to success.
            RemoveStartSplitOperation();     // Split is over, then remove the StartSplit operation.
            srm_log() << "CommitSplit success. "
                      << "number:(" << split_ctx_.GetNewSharedResources().size() << "+" << total
                      << "=" << share_res_manager_->GetSharedResources().size() << "), "
                      << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                      << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                      << "split_id:" << split_ctx_.GetId();

            // Check if correctly generated the filters of all the shared resources of this split
            // procedure.
            if (true /*BASE_GetParaS64Val(E_SPA_CONFIG_LB_TLOG_CHECK_SWITCH)*/) {
                auto& resources = split_ctx_.GetSharedResources();
                for (auto& resource : resources) {
                    if (share_res_manager_->IsResourceShared(resource)) {
                        continue;
                    }
                    srm_dbg() << "Missing shared resource : "
                              << "resource:" << resource << "), "
                              << "number:(" << split_ctx_.GetNewSharedResources().size() << "+"
                              << total << "=" << share_res_manager_->GetSharedResources().size()
                              << "), "
                              << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                              << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState())
                              << ", "
                              << "split_id:" << split_ctx_.GetId();
                }
            }

            // Write StartSplit log.
            (void)LogSplitState(SharedResourceOperationType::CommitSplit, split_ctx_.GetId(),
                                split_ctx_.GetDescription());

            return rocksdb::Status::OK();
        }

        rocksdb::Status SharedResourceOperationLogRecordProvider::CheckCheckpointSize() {
            // Check if there are too many logs that have not been consumed by GlobalGC.
            // Too many unconsumed logs will cause that TransLogRecordStore write check point
            // frequently.
            uint64_t max_log_size_ = 16 * 1024 * 1024LL;
            uint32_t encode_len = share_res_manager_->GetApproximateLengthOfWriteCheckpoint() +
                                  GetApproximateLengthOfWriteCheckpoint();
            if (encode_len >= max_log_size_) {
                srm_err() << "[CheckCheckpointSize] Refuse operation because of too many "
                             "unconsumed logs.";
                // Write without any record to try RewriteImportantFailedLog() and
                // CheckAndWriteCheckPoint().
                LogRecordVector log_records;
                WriteRecords(log_records, true);
                return rocksdb::Status::Aborted();
            }

            return rocksdb::Status::OK();
        }

        // Write one log record.
        rocksdb::Status SharedResourceOperationLogRecordProvider::WriteRecord(
            LogRecord& log_record, bool allow_check_point, bool rewrite_flag) {
            // Rewrite the log(s) written failed last time.
            if (!rewrite_flag) {
                rocksdb::Status s = RewriteImportantFailedLog();
                if (!s.ok()) {
                    srm_err() << "Fail to rewrite the log(s) written failed last time.";
                    return s;
                }
            }

            // Write the log(s) this time.
            return GetLogWriter()->WriteRecord(log_record, allow_check_point);
        }

        // Write log records in batch.
        rocksdb::Status SharedResourceOperationLogRecordProvider::WriteRecords(
            LogRecordVector& log_records, bool allow_check_point, bool rewrite_flag) {
            // Rewrite the log(s) written failed last time.
            if (!rewrite_flag) {
                rocksdb::Status s = RewriteImportantFailedLog();
                if (!s.ok()) {
                    srm_err() << "Fail to rewrite the log(s) written failed last time.";
                    return s;
                }
            }

            // Write the log(s) this time.
            return GetLogWriter()->WriteRecord(log_records.GetLogRecords(), allow_check_point);
        }

        // Rewrite the split end log written failed last time, which is one of
        // CommitSplit,RollbackSplit.
        rocksdb::Status SharedResourceOperationLogRecordProvider::RewriteSplitEndLog() {
            if (split_oper_to_write_ == failed_split_end_oper_) {
                // If the split operation to be written is same as the one written failed last time,
                // only
                // one of them need to be written. So filter out the one written failed last time.
                return rocksdb::Status::OK();
            }
            switch (failed_split_end_oper_) {
                case SharedResourceOperationType::CommitSplit:
                case SharedResourceOperationType::RollbackSplit:
                    return LogSplitState(failed_split_end_oper_, split_ctx_.GetId(),
                                         split_ctx_.GetDescription(), true);
                case SharedResourceOperationType::None:
                    return rocksdb::Status::OK();
                default:
                    srm_err() << "Invalid failed operation type ("
                              << static_cast<int>(failed_split_end_oper_) << ").";
                    GetModule()->SetCheckFailed();
                    return rocksdb::Status::Aborted();
            }
        }

        // Rewrite the processed lsn log written failed last time.
        rocksdb::Status SharedResourceOperationLogRecordProvider::RewriteProcessedLsnLog() {
            if (!processed_lsn_failed_flag_) {
                return rocksdb::Status::OK();
            }
            return LogProcessedLSN(true);
        }

        // Rewrite the log(s) written failed last time.
        rocksdb::Status SharedResourceOperationLogRecordProvider::RewriteImportantFailedLog() {
            if (MONGO_FAIL_POINT(failToRewriteImportantFailedLog)) {
                error() << "Fail point : RewriteImportantFailedLog";
                return rocksdb::Status::Aborted();
            }

            // Rewrite split end log.
            rocksdb::Status s = RewriteSplitEndLog();
            if (!s.ok()) {
                srm_err() << "Fail to rewrite " << GetOperTypeName(failed_split_end_oper_);
                return s;
            }

            // Rewrite Processed log.
            s = RewriteProcessedLsnLog();
            if (!s.ok()) {
                srm_err() << "Fail to rewrite processed lsn (" << processed_lsn_ << ").";
                return s;
            }

            return rocksdb::Status::OK();
        }

        // Remove all the operations between RollbackSplit and the latest StartSplit from memory.
        rocksdb::Status SharedResourceOperationLogRecordProvider::RemoveStartSplitOperation() {
            // Get the split LSN.
            const uint64_t lsn = split_ctx_.GetId();
            if (lsn == INVALID_LSN) {
                return rocksdb::Status::OK();
            }

            // Find the StartSplit operation by LSN.
            auto oper_it = operations_.find(lsn);
            if ((oper_it == operations_.end()) ||
                ((*oper_it)->GetType() != SharedResourceOperationType::StartSplit)) {
                // Abnormal: no StartSplit was found.
                srm_err() << "Missing StartSplit related to CommitSplit. IsEnd("
                          << (oper_it == operations_.end()) << ")";
                GetModule()->SetCheckFailed();
                return rocksdb::Status::Aborted();
            }

            // Remove the StartSplit operation.
            delete (*oper_it);
            *oper_it = nullptr;
            operations_.erase(oper_it);
            return rocksdb::Status::OK();
        }

        // Remove all the operations between RollbackSplit and the latest StartSplit from memory.
        rocksdb::Status SharedResourceOperationLogRecordProvider::RollbackSplitOperations() {
            // Find the StartSplit operation.
            const uint64_t lsn = split_ctx_.GetId();
            auto oper_it = operations_.find(lsn);
            if ((oper_it == operations_.end()) ||
                ((*oper_it)->GetType() != SharedResourceOperationType::StartSplit)) {
                // Abnormal: no StartSplit was found.
                srm_err() << "Missing StartSplit related to RollbackSplit. IsEnd("
                          << (oper_it == operations_.end()) << ")";
                GetModule()->SetCheckFailed();
                return rocksdb::Status::Aborted();
            }
            // Release all the operations of the split.
            for (auto it = oper_it; it != operations_.end(); it++) {
                delete (*it);
                *it = nullptr;
            }
            // Remove all the operations of the split.
            operations_.erase(oper_it, operations_.end());
            split_ctx_.SetStateToFault();
            return rocksdb::Status::OK();
        }

        // Replay StartSplit log
        rocksdb::Status SharedResourceOperationLogRecordProvider::ReplayStartSplitLog(
            const LogRecord& record) {
            // Decode operation.
            auto oper = SplitOperation::DecodeFrom(record);
            if (oper == nullptr) {
                srm_err() << "Fetal Err when decode StartSplit operation.";
                return rocksdb::Status::Aborted();
            }
            // The id of split just is the LSN of the related SplitStart operation.
            oper->SetId(oper->GetLsn());

            // Check StartSplit overlap.
            if (split_ctx_.IsStateStart()) {
                srm_err() << "Overlap split transactions. "
                          << "old("
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_id:" << split_ctx_.GetId() << "new("
                          << "lsn:" << oper->GetLsn() << ","
                          << "split_id:" << oper->GetId();
                delete oper;
                return rocksdb::Status::Aborted();
            }

            // Append the operation.
            operations_.push_back(reinterpret_cast<SharedResourceOperation*>(oper));

            // Reconstruct SplitContext
            split_ctx_.ClearSharedResources();
            split_ctx_.SetId(oper->GetId());
            split_ctx_.SetDescription(oper->GetDescription());
            split_ctx_.SetStateToStart();
            srm_log() << "Replay StartSplit successfully. "
                      << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                      << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                      << "split_id:" << split_ctx_.GetId();
            return rocksdb::Status::OK();
        }

        // Replay CommitSplit log
        rocksdb::Status SharedResourceOperationLogRecordProvider::ReplayCommitSplitLog(
            const LogRecord& record) {
            // Decode operation
            auto oper = SplitOperation::DecodeFrom(record);
            if (oper == nullptr) {
                srm_err() << "Fetal Err when decode CommitSplit operation.";
                return rocksdb::Status::Aborted();
            }

            // Check CommitSplit.
            if (split_ctx_.IsStateSuccess() && (oper->GetId() == split_ctx_.GetId())) {
                // Ignore the CommitSplit written repeatedly.
                srm_err() << "Ignore the CommitSplit written repeatedly. "
                          << "lsn:" << oper->GetLsn() << ", "
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_id:" << oper->GetId();
                delete oper;
                return rocksdb::Status::OK();
            } else if (split_ctx_.IsStateNone()) {
                srm_err() << "CommitSplit without related StartSplit. "
                          << "lsn:" << oper->GetLsn() << ", "
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_id:" << oper->GetId();
                delete oper;
                return rocksdb::Status::Aborted();
            } else if ((!split_ctx_.IsStateStart()) || (oper->GetId() != split_ctx_.GetId())) {
                srm_err() << "CommitSplit without related StartSplit. "
                          << "old("
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                          << "split_id:" << split_ctx_.GetId() << "), "
                          << "new("
                          << "lsn:" << oper->GetLsn() << ", "
                          << "split_id:" << oper->GetId();
                delete oper;
                return rocksdb::Status::Aborted();
            }

            // Matched split id means the split is over, then remove the StartSplit operation.
            RemoveStartSplitOperation();
            // Set the split state to success.
            split_ctx_.SetStateToSuccess();
            srm_log() << "Replay CommitSplit successfully. "
                      << "split_ctx("
                      << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                      << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                      << "split_id:" << split_ctx_.GetId() << "oper("
                      << "lsn:" << oper->GetLsn() << ", "
                      << "split_id:" << oper->GetId() << ").";
            delete oper;
            return rocksdb::Status::OK();
        }

        // Replay RollbackSplit log
        rocksdb::Status SharedResourceOperationLogRecordProvider::ReplayRollbackSplitLog(
            const LogRecord& record) {
            // Decode operation
            auto oper = SplitOperation::DecodeFrom(record);
            if (oper == nullptr) {
                srm_err() << "Fetal Err when decode StartSplit operation.";
                return rocksdb::Status::Aborted();
            }

            if (split_ctx_.IsStateFault() && (oper->GetId() == split_ctx_.GetId())) {
                // Ignore the RollbackSplit written repeatedly.
                srm_err() << "Ignore the RollbackSplit written repeatedly. "
                          << "lsn:" << oper->GetLsn() << ", "
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_id:" << oper->GetId();
                delete oper;
                return rocksdb::Status::OK();
            } else if (split_ctx_.IsStateNone()) {
                srm_err() << "RollbackSplit without related StartSplit. "
                          << "lsn:" << oper->GetLsn() << ", "
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_id:" << oper->GetId();
                delete oper;
                return rocksdb::Status::Aborted();
            } else if ((!split_ctx_.IsStateStart()) || (oper->GetId() != split_ctx_.GetId())) {
                srm_err() << "RollbackSplit without related StartSplit. "
                          << "split_ctx("
                          << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                          << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                          << "split_id:" << split_ctx_.GetId() << "oper("
                          << "lsn:" << oper->GetLsn() << ", "
                          << "split_id:" << oper->GetId();
                delete oper;
                return rocksdb::Status::Aborted();
            }

            srm_log() << "Replay RollbackSplit successfully. "
                      << "old("
                      << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", "
                      << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                      << "split_id:" << split_ctx_.GetId() << "), "
                      << "new("
                      << "lsn:" << oper->GetLsn() << ", "
                      << "split_id:" << oper->GetId() << ").";
            delete oper;
            return RollbackSplitOperations();
        }

        // Replay reference log
        rocksdb::Status SharedResourceOperationLogRecordProvider::ReplayReferenceLog(
            const LogRecord& record) {
            // Decode operation
            auto oper = SharedResourceReferenceOperation::DecodeFrom(record);
            if (oper == nullptr) {
                srm_err() << "Fetal Err when decode Reference operation.";
                rocksdb::Status::Aborted();
            }

            // Append the operation into the operations vector in memory.
            operations_.push_back(reinterpret_cast<SharedResourceOperation*>(oper));
            // Append shared resource
            auto type = oper->GetType();
            if ((split_ctx_.IsStateStart()) &&
                (type == SharedResourceOperationType::AddReference)) {
                SharedResourceId resource = oper->GetResource();
                index_log() << "[SRM]Replay AddReferenceLog. "
                            << "[" << split_ctx_.GetSharedResources().size() << "] "
                            << "resource:" << resource << ", "
                            << "id_:" << split_ctx_.GetId() << ", "
                            << "LSN:" << oper->GetLsn();
                split_ctx_.ReplaySharedResource(oper->GetResource());
            }
            // Write runlog of replaying filters
            srm_dbg() << "Replay " << GetOperTypeName(type) << " : "
                      << "lsn:" << oper->GetLsn() << ", "
                      << "resource:" << oper->GetResource() << ", "
                      << "parent:" << oper->GetParentChunkId() << ", "
                      << "child:" << oper->GetChildChunkId() << ", "
                      << "split_state:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", "
                      << "split_id:" << split_ctx_.GetId();
            return rocksdb::Status::OK();
        }

        // Replay processed log
        rocksdb::Status SharedResourceOperationLogRecordProvider::ReplayProcessedLog(
            const LogRecord& record) {
            // Parse the record to an operation.
            auto oper = ProcessedLSNOperation::DecodeFrom(record);
            if (oper == nullptr) {
                srm_err() << "Fetal Err when decode Processed operation.";
                return rocksdb::Status::Aborted();
            }

            // Check the processed LSN to be replay.
            uint64_t processed_lsn = oper->GetProcessedLSN();
            if (processed_lsn == INVALID_LSN) {
                srm_err() << "Replay ProcessedLog: invalid processed lsn (" << processed_lsn;
                delete oper;
                return rocksdb::Status::Aborted();
            }
            if (split_ctx_.IsStateStart() && (processed_lsn >= split_ctx_.GetLsn())) {
                srm_err() << "Replay ProcessedLog: the processed lsn conflicts with the split lsn. "
                          << "lsn:" << oper->GetLsn() << ", "
                          << "processed_lsn(" << processed_lsn << ") >= split_lsn("
                          << split_ctx_.GetLsn() << ").";
                delete oper;
                return rocksdb::Status::Aborted();
            }
            if ((processed_lsn_ != INVALID_LSN) && (processed_lsn_ >= processed_lsn)) {
                srm_log() << "Replay ProcessedLog: disorder or repetitive processed lsn "
                          << "(old:" << processed_lsn_ << ", new:" << processed_lsn << ").";
                delete oper;
                return rocksdb::Status::OK();
            }
            srm_log() << "Replay ProcessedLog. "
                      << "processed_lsn:" << processed_lsn << ", "
                      << "record_lsn:" << record.GetLsn();
            delete oper;

            // Remove the processed operations with a LSN less than the processed LSN from memory.
            operations_.RemoveProcessedOperations(processed_lsn);
            processed_lsn_ = processed_lsn;
            return rocksdb::Status::OK();
        }

        // Replay log records.
        rocksdb::Status SharedResourceOperationLogRecordProvider::ReplayLogRecord(
            const LogRecord& record, bool& needMoreRecords) {
            rocksdb::Status s;
            SharedResourceOperationType type = SplitOperation::GetOperTypeOfLogRecord(record);
            index_LOG(1) << "SharedResourceOperationLogRecordProvider::ReplayLogRecord() type: "
                         << (int)type;
            switch (type) {
                case SharedResourceOperationType::StartSplit:
                    s = ReplayStartSplitLog(record);
                    break;
                case SharedResourceOperationType::CommitSplit:
                    s = ReplayCommitSplitLog(record);
                    break;
                case SharedResourceOperationType::RollbackSplit:
                    s = ReplayRollbackSplitLog(record);
                    break;
                case SharedResourceOperationType::AddReference:
                case SharedResourceOperationType::RemoveReference:
                    s = ReplayReferenceLog(record);
                    break;
                case SharedResourceOperationType::Processed:
                    s = ReplayProcessedLog(record);
                    break;
                default:
                    srm_err() << "Invalid operation type (" << static_cast<int>(type) << ").";
                    needMoreRecords = false;
                    return rocksdb::Status::Aborted();
            }

            needMoreRecords = s.ok() ? true : false;
            return s;
        }

        // Write AddFilter and AddReference logs of all shared resources related to the split to
        // right chunk.
        rocksdb::Status SharedResourceOperationLogRecordProvider::WriteSharedResourceLogToRight(
            SharedResourceOperationLogRecordProvider* right_provider) {
            LogRecordVector log_records;
            rocksdb::Status s;
            SharedResourceOperationType oper_type;

            // Copy split context to right provider.
            right_provider->split_ctx_.Copy(&split_ctx_);
            right_provider->split_ctx_.SetStateToSuccess();

            // Prepare shared resource reference logs for right chunk.
            oper_type = SharedResourceOperationType::AddReference;
            s = PrepareReferenceLog(log_records, oper_type, split_ctx_.GetSharedResources(),
                                    split_ctx_.GetDescription()->GetParentChunkId(),
                                    split_ctx_.GetDescription()->GetChildChunkId());
            if (!s.ok()) {
                srm_err() << "Prepare reference log of right chunk failed.";
                return s;
            }

            // Prepare shared resource filter logs for right chunk.
            oper_type = SharedResourceOperationType::AddFilter;
            s = share_res_manager_->PrepareFilterLog(log_records, oper_type,
                                                     split_ctx_.GetSharedResources());
            if (!s.ok()) {
                srm_err() << "Prepare reference log of right chunk failed.";
                return s;
            }

            // Write log records in batch.
            s = right_provider->WriteRecords(log_records, false);
            if (!s.ok()) {
                srm_err() << "Write log records of right chunk failed";
                return s;
            }

            return rocksdb::Status::OK();
            // !!! Comment: WriteSharedResourceLogToRight() don't ensure that the related memory
            // information is
            //              constructed. So 'right_provider' mustn't be used after calling this
            //              method.
        }

        //
        // Methods of SharedResourceMemLog
        //
        void SharedResourceMemLog::DumpSharedResourceRecord(std::ofstream& dump_stream) {
            SharedResourceOperationType type = GetType();
            switch (type) {
                case SharedResourceOperationType::AddFilter:
                case SharedResourceOperationType::AddReference:
                case SharedResourceOperationType::RemoveReference:
                case SharedResourceOperationType::GetUnprocessed:
                    DumpResource(dump_stream);
                    break;
                case SharedResourceOperationType::QueryFilter:
                    DumpQueryFilter(dump_stream);
                    break;
                case SharedResourceOperationType::RemoveFilter:
                    DumpRmvFilter(dump_stream);
                    break;
                case SharedResourceOperationType::StartSplit:
                case SharedResourceOperationType::CommitSplit:
                case SharedResourceOperationType::RollbackSplit:
                    DumpSplit(dump_stream);
                    break;
                case SharedResourceOperationType::Processed:
                    DumpProcessed(dump_stream);
                    break;
                default:
                    DumpOthers(dump_stream);
                    break;
            }
        }
        void SharedResourceMemLog::DumpResource(std::ofstream& dump_stream) {
            dump_stream << "[" << GetTimeStr() << "] "
                        << "lsn:" << GetLsn() << ", "
                        << "type:" << GetOperTypeName(GetType()) << ", "
                        << "resource:" << resource_ << std::endl;
        }
        void SharedResourceMemLog::DumpQueryFilter(std::ofstream& dump_stream) {
            dump_stream << "[" << GetTimeStr() << "] "
                        << "lsn:" << GetLsn() << ", "
                        << "type:"
                        << "RmvFilter&Ref"
                        << ", "
                        << "resource:" << resource_ << ", "
                        << "shared:" << shared_flag_ << std::endl;
        }
        void SharedResourceMemLog::DumpRmvFilter(std::ofstream& dump_stream) {
            dump_stream << "[" << GetTimeStr() << "] "
                        << "lsn:" << GetLsn() << ", "
                        << "type:"
                        << "RmvFilter&Ref"
                        << ", "
                        << "resource:" << resource_ << "shared:" << shared_flag_ << std::endl;
        }
        void SharedResourceMemLog::DumpSplit(std::ofstream& dump_stream) {
            dump_stream << "[" << GetTimeStr() << "] "
                        << "lsn:" << GetLsn() << ", "
                        << "type:" << GetOperTypeName(GetType()) << ", "
                        << "split_id:" << split_id_ << std::endl;
        }
        void SharedResourceMemLog::DumpProcessed(std::ofstream& dump_stream) {
            dump_stream << "[" << GetTimeStr() << "] "
                        << "lsn:" << GetLsn() << ", "
                        << "type:" << GetOperTypeName(GetType()) << ", "
                        << "processed_lsn:" << processed_lsn_ << std::endl;
        }
        void SharedResourceMemLog::DumpOthers(std::ofstream& dump_stream) {
            dump_stream << "[" << GetTimeStr() << "] "
                        << "lsn:" << GetLsn() << ", "
                        << "type:" << GetOperTypeName(GetType()) << std::endl;
        }

        //
        // Methods of SharedResourceModule
        //
        inline std::string ptr_to_string(const void* ptr) {
            char buf[24] = {0};
            sprintf(buf, "%p", ptr);
            return std::string(buf);
        }

        SharedResourceModule::~SharedResourceModule() {
            srm_log() << " Destory share resource module. (module=" << ptr_to_string(this) << ").";
        }

        int SharedResourceModule::RegisterSharedResource(std::vector<SharedResourceId>& resources) {
            rocksdb::Status s;
            stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
            if (!init_flag_) {
                srm_err() << "Uninitialized share resource module. (module=" << ptr_to_string(this);
                return RET_ERR;
            }
            s = log_provider_->RegisterSharedResources(resources);
            if (s.ok()) {
                MemLogRegisterSharedResources(resources);
            }
            return (s.ok() ? RET_OK : RET_ERR);
        }

        // if return fail, it means logRecordprovider is splitting, space gc of rocks db must retry
        // to remove
        // the plogs in resource_id_list
        // int SharedResourceModule::QuerySharedResource(std::vector<SharedResourceDescription>&
        // list) {
        //    rocksdb::Status s;
        //    stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
        //    if (!init_flag_) {
        //        srm_err() << "Uninitialized share resource module. (module=" <<
        //        ptr_to_string(this) << ").";
        //        return RET_ERR;
        //    }
        //    s = filter_manager_->OnResourcesQuery(list);
        //    if (s.ok()) {
        //        MemLogQuerySharedResource(list);
        //    }
        //    return (s.ok() ? RET_OK : RET_ERR);
        //}

        // if return fail, it means logRecordprovider is splitting, space gc of rocks db must retry
        // to remove
        // the plogs in resource_id_list
        int SharedResourceModule::RemoveSharedResource(
            std::vector<SharedResourceRemoveDescription>& list) {
            rocksdb::Status s;
            stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
            if (!init_flag_) {
                index_err() << "[SRM]Uninitialized share resource module. (module="
                            << ptr_to_string(this) << ").";
                return RET_ERR;
            }
            s = filter_manager_->OnResourcesDeletion(list);
            if (s.ok()) {
                MemLogRemoveSharedResource(list);
            }
            return (s.ok() ? RET_OK : RET_ERR);
        }

        rocksdb::Status SharedResourceModule::Init(rocksdb::Env* env, const std::string& db_path,
                                                   ChunkMetadata* chunk_meta_data) {
            rocksdb::Status s = rocksdb::Status::OK();

            stdx::unique_lock<stdx::mutex> lock(rw_mutex_);

            if (init_flag_) {
                srm_err() << "It's not allowed to re-initialize a shared resource module. (module="
                          << ptr_to_string(this) << ").";
                return rocksdb::Status::Aborted();
            }

            do {
                // Save the chunk id.
                chunk_id_ = strtoull(chunk_meta_data->GetChunk().getName().c_str(), nullptr, 16);
                index_log() << "chunkid: " << chunk_id_ << "; db_path: " << db_path;

                // Create store, provider, manager
                log_store_.reset(new (std::nothrow) TransLogRecordStore(env, db_path));
                log_provider_.reset(new (std::nothrow) SharedResourceOperationLogRecordProvider(
                    this, chunk_meta_data));
                // filter_manager_.reset(new (std::nothrow) SharedResourceManager(this));
                filter_manager_.reset(new (std::nothrow) MaaSSharedResourceManager(this, db_path));
                if (!log_store_ || !log_provider_ || !filter_manager_) {
                    srm_err() << "Fail to new members. (module=" << ptr_to_string(this) << ").";
                    s = rocksdb::Status::Aborted();
                    break;
                }

                // Register providers
                s = log_store_->RegisterProvider(*filter_manager_);
                if (!s.ok()) {
                    srm_err() << "Fail to register the filter mananger. (module="
                              << ptr_to_string(this) << ").";
                    break;
                }
                s = log_store_->RegisterProvider(*log_provider_);
                if (!s.ok()) {
                    srm_err() << "Fail to register the log provider mananger. (module="
                              << ptr_to_string(this) << ").";
                    break;
                }

                // Associate provider and manager with each other.
                log_provider_->SetSharedResourceManager(filter_manager_.get());
                filter_manager_->SetLogRecordProvider(log_provider_.get());

                // Init log storage. (Include of the process of replaying log)
                s = log_store_->Init();
                if (!s.ok()) {
                    srm_err() << "Fail to initiate the log storage. (module="
                              << ptr_to_string(this);
                    break;
                }

                // Init memory log vector.
                if (MemLogIsEnable()) {
                    mem_logs_.init(8 * 1024);
                    MemLogReplayLog();
                }

            } while (0);

            if (s.ok()) {
                init_flag_ = true;
                mongo::ChunkMetadata::KeyRange key_range = chunk_meta_data->GetKeyRange();
                srm_log() << "Succeed to initiate the log storage. (module=" << ptr_to_string(this)
                          << "), "
                          << "env(" << static_cast<void*>(env) << "), "
                          << "db_apth(" << db_path << "), ";
            }
            return s;
        }

        // Set check failed flag during the init procedure.
        void SharedResourceModule::SetCheckFailed() {
            if (!check_failed_) {
                srm_err() << "Set check failed flag. Critical, spaceGC stop working!";
                check_failed_ = true;
            }
        }

        rocksdb::Status SharedResourceModule::StartSplit(
            const std::shared_ptr<SplitDescription>& description) {
            rocksdb::Status s;
            stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
            if (!init_flag_) {
                srm_err() << "Uninitialized share resource module. (module=" << ptr_to_string(this);
                return rocksdb::Status::Aborted();
            }

            if (check_failed_) {
                return rocksdb::Status::Aborted();
            }

            s = log_provider_->StartSplit(description);
            if (s.ok()) {
                MemLogStartSplit();
            }
            return s;
        }

        rocksdb::Status SharedResourceModule::RollbackSplit() {
            rocksdb::Status s;
            stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
            if (!init_flag_) {
                srm_err() << "Uninitialized share resource module. (module=" << ptr_to_string(this);
                return rocksdb::Status::Aborted();
            }
            s = log_provider_->RollbackSplit();
            if (s.ok()) {
                MemLogRollbackSplit();
            }
            return s;
        }

        rocksdb::Status SharedResourceModule::CommitSplit() {
            rocksdb::Status s;
            stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
            if (!init_flag_) {
                srm_err() << "Uninitialized share resource module. (module=" << ptr_to_string(this);
                return rocksdb::Status::Aborted();
            }
            s = log_provider_->CommitSplit();
            if (s.ok()) {
                MemLogCommitSplit();
            }
            return s;
        }

        rocksdb::Status SharedResourceModule::WriteSharedResourceLogToRight(
            SharedResourceModule& right_module) {
            rocksdb::Status s;
            stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
            if (!init_flag_) {
                srm_err() << "Uninitialized share resource module. (module=" << ptr_to_string(this);
                return rocksdb::Status::Aborted();
            }
            s = log_provider_->WriteSharedResourceLogToRight(right_module.GetLogProvider());
            return s;
        }

        void SharedResourceModule::GetUnprocessedOperations(
            uint32_t maxBatchMaxSize,
            std::vector<SharedResourceReferenceOperation>& unprocessedRecords) {
            int old_size = unprocessedRecords.size();
            stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
            if (!init_flag_) {
                srm_err() << "Uninitialized share resource module. (module=" << ptr_to_string(this);
                return;
            }
            log_provider_->GetUnprocessedOperations(maxBatchMaxSize, unprocessedRecords);
            MemLogGetUnprocessed(unprocessedRecords, old_size);
            return;
        }

        rocksdb::Status SharedResourceModule::SetLastProcessedOperationLSN(uint64_t processed_lsn) {
            rocksdb::Status s;
            stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
            if (!init_flag_) {
                srm_err() << "Uninitialized share resource module. (module=" << ptr_to_string(this);
                return rocksdb::Status::Aborted();
            }
            s = log_provider_->SetLastProcessedOperationLSN(processed_lsn);
            if (s.ok()) {
                MemLogSetProcessed(processed_lsn);
            }
            return s;
        }

        // Record the history of RegisterSharedResources.
        void SharedResourceModule::MemLogRegisterSharedResources(
            std::vector<SharedResourceId>& resources) {
            if (MemLogIsEnable()) {
                SharedResourceMemLog mem_log(SharedResourceOperationType::AddReference);
                for (auto resource : resources) {
                    mem_log.SetResource(resource);
                    mem_logs_.append(mem_log);
                }
            }
        }
        // Record the history of QuerySharedResource.
        /*void
        SharedResourceModule::MemLogQuerySharedResource(std::vector<SharedResourceDescription>&
        list) {
            if (MemLogIsEnable()) {
                SharedResourceMemLog mem_log(SharedResourceOperationType::QueryFilter);
                for (auto desc : list) {
                    mem_log.SetResource(desc.id);
                    mem_log.SetSharedFlag(desc.shared_flag);
                    mem_logs_.append(mem_log);
                }
            }
        }*/

        // Record the history of RemoveSharedResource.
        void SharedResourceModule::MemLogRemoveSharedResource(
            std::vector<SharedResourceRemoveDescription>& list) {
            if (MemLogIsEnable()) {
                SharedResourceMemLog mem_log(SharedResourceOperationType::RemoveFilter);
                for (auto resource : list) {
                    mem_log.SetResource(resource.id);
                    mem_log.SetSharedFlag(true);
                    mem_logs_.append(mem_log);
                }
            }
        }
        // Record the history of StartSplit.
        void SharedResourceModule::MemLogStartSplit() {
            if (MemLogIsEnable()) {
                SharedResourceMemLog mem_log(SharedResourceOperationType::StartSplit);
                mem_log.SetSplitId(log_provider_->GetSplitContext()->GetId());
                mem_logs_.append(mem_log);
            }
        }
        // Record the history of AddFilter during the process of CommitSplit.
        void SharedResourceModule::MemLogAddFilters(
            const std::vector<SharedResourceId>& resources) {
            if (MemLogIsEnable()) {
                SharedResourceMemLog mem_log(SharedResourceOperationType::AddFilter);
                for (auto resource : resources) {
                    mem_log.SetResource(resource);
                    mem_logs_.append(mem_log);
                }
            }
        }
        // Record the history of CommitSplit.
        void SharedResourceModule::MemLogCommitSplit() {
            const std::vector<SharedResourceId>& resources =
                log_provider_->GetSplitContext()->GetSharedResources();
            MemLogAddFilters(resources);
            if (MemLogIsEnable()) {
                SharedResourceMemLog mem_log(SharedResourceOperationType::CommitSplit);
                mem_log.SetSplitId(log_provider_->GetSplitContext()->GetId());
                mem_logs_.append(mem_log);
            }
        }
        // Record the history of RollbackSplit.
        void SharedResourceModule::MemLogRollbackSplit() {
            if (MemLogIsEnable()) {
                SharedResourceMemLog mem_log(SharedResourceOperationType::RollbackSplit);
                mem_log.SetSplitId(log_provider_->GetSplitContext()->GetId());
                mem_logs_.append(mem_log);
            }
        }
        // Record the history of GetUnprocessedOperations.
        void SharedResourceModule::MemLogGetUnprocessed(
            std::vector<SharedResourceReferenceOperation>& unprocessedRecords, uint32_t old_size) {
            if (MemLogIsEnable()) {
                SharedResourceMemLog mem_log(SharedResourceOperationType::GetUnprocessed);
                auto reference_oper = unprocessedRecords.begin() + old_size;
                for (; reference_oper != unprocessedRecords.end(); reference_oper++) {
                    mem_log.SetLsn(reference_oper->GetLsn());
                    mem_log.SetResource(reference_oper->GetResource());
                    mem_logs_.append(mem_log);
                }
            }
        }
        // Record the history of GetUnprocessedOperations.
        void SharedResourceModule::MemLogSetProcessed(uint64_t processed_lsn) {
            if (MemLogIsEnable()) {
                SharedResourceMemLog mem_log(SharedResourceOperationType::Processed);
                mem_log.SetProcessedLSN(processed_lsn);
                mem_logs_.append(mem_log);
            }
        }
        // Record shared resource operation managed in memory.
        void SharedResourceModule::MemLogOperation(const SharedResourceOperation* oper) {
            SharedResourceOperationType oper_type = oper->GetType();
            const SharedResourceReferenceOperation* reference_oper = nullptr;
            const SplitOperation* split_oper = nullptr;
            const ProcessedLSNOperation* processed_oper = nullptr;
            SharedResourceMemLog mem_log(oper_type);
            mem_log.SetLsn(oper->GetLsn());
            switch (oper_type) {
                case SharedResourceOperationType::AddReference:
                case SharedResourceOperationType::RemoveReference:
                    reference_oper =
                        reinterpret_cast<const SharedResourceReferenceOperation*>(oper);
                    mem_log.SetResource(reference_oper->GetResource());
                    mem_logs_.append(mem_log);
                    break;
                case SharedResourceOperationType::StartSplit:
                case SharedResourceOperationType::CommitSplit:
                case SharedResourceOperationType::RollbackSplit:
                    split_oper = reinterpret_cast<const SplitOperation*>(oper);
                    mem_log.SetSplitId(split_oper->GetId());
                    mem_logs_.append(mem_log);
                    break;
                case SharedResourceOperationType::Processed:
                    processed_oper = reinterpret_cast<const ProcessedLSNOperation*>(oper);
                    mem_log.SetProcessedLSN(processed_oper->GetProcessedLSN());
                    mem_logs_.append(mem_log);
                    break;
                default:
                    mem_logs_.append(mem_log);
                    break;
            }
        }
        // Record shared resource operations managed in memory.
        void SharedResourceModule::MemLogOperations(SharedResourceOperationVector& operations) {
            if (MemLogIsEnable()) {
                for (auto oper = operations.begin(); oper != operations.end(); oper++) {
                    MemLogOperation(*oper);
                }
            }
        }
        // Record shared resources filters managed in memory.
        void SharedResourceModule::MemLogFilters(const std::vector<SharedResourceId>& resources) {
            if (MemLogIsEnable()) {
                SharedResourceMemLog mem_log(SharedResourceOperationType::AddFilter);
                for (auto resource : resources) {
                    mem_log.SetResource(resource);
                    mem_logs_.append(mem_log);
                }
            }
        }
        // Record shared resources after replay log.
        void SharedResourceModule::MemLogReplayLog() {
            MemLogOperations(GetLogProvider()->GetSharedResourceOperationVector());
            MemLogFilters(GetFilterManager()->GetSharedResources());
            if (MemLogIsEnable()) {
                SharedResourceMemLog mem_log(SharedResourceOperationType::ReplayLog);
                mem_logs_.append(mem_log);
            }
            return;
        }

        // Dump an operation.
        void SharedResourceModule::DumpOperation(std::ofstream& out_stream,
                                                 const SharedResourceOperation* oper) {
            uint64_t lsn = oper->GetLsn();
            SharedResourceOperationType oper_type = oper->GetType();
            const SharedResourceReferenceOperation* reference_oper = nullptr;
            const SplitOperation* split_oper = nullptr;
            SharedResourceId resource;

            switch (oper_type) {
                case SharedResourceOperationType::AddReference:
                    reference_oper =
                        reinterpret_cast<const SharedResourceReferenceOperation*>(oper);
                    resource = reference_oper->GetResource();
                    out_stream << GetOperTypeName(oper_type) << " : "
                               << "lsn:" << lsn << ", resource:" << resource << ", chunk:("
                               << reference_oper->GetParentChunkId() << ","
                               << reference_oper->GetChildChunkId() << ").";
                    break;
                case SharedResourceOperationType::RemoveReference:
                    reference_oper =
                        reinterpret_cast<const SharedResourceReferenceOperation*>(oper);
                    resource = reference_oper->GetResource();
                    out_stream << GetOperTypeName(oper_type) << " : "
                               << "lsn:" << lsn << ", resource:" << resource
                               << ", chunk:" << reference_oper->GetParentChunkId();
                    break;
                case SharedResourceOperationType::StartSplit:
                case SharedResourceOperationType::CommitSplit:
                case SharedResourceOperationType::RollbackSplit:
                    split_oper = reinterpret_cast<const SplitOperation*>(oper);
                    out_stream << GetOperTypeName(oper_type) << " : "
                               << "lsn:" << lsn << ", split_id:" << split_oper->GetId()
                               << ", split_point:"
                               << tojson(split_oper->GetDescription()->splitPointKey)
                               << ", key_range:("
                               << tojson(split_oper->GetDescription()->originalKeyLow) << ","
                               << tojson(split_oper->GetDescription()->originalKeyHigh) << ")";

                    break;
                default:
                    out_stream << GetOperTypeName(oper_type) << " : "
                               << "lsn:" << lsn;
                    break;
            }
            out_stream << std::endl;
        }

        // Dump all history records into appointed file.
        void SharedResourceModule::DumpSharedResourceList(const char* history_path) {
            // Open the output stream.
            std::ofstream history_out;
            stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
            history_out.open(history_path, std::ios::app);
            if (!history_out) {
                return;
            }

            std::string chunk("[chunk:");
            chunk.append(std::to_string(GetChunkId()));
            chunk.append("]");

            // Dump filters from filter manager to the output stream.
            history_out << chunk << "[DumpFilters] Begin at " << GetCurrentTimeStr() << std::endl;
            auto& resources = filter_manager_->GetSharedResources();
            for (auto resource : resources) {
                history_out << "Filter : resource:" << resource << std::endl;
            }
            history_out << chunk << "[DumpFilters] End at   " << GetCurrentTimeStr() << std::endl;

            // Dump operations from log provider to the output stream.
            history_out << chunk << "[DumpOperations] Begin at " << GetCurrentTimeStr()
                        << std::endl;
            auto& operations = log_provider_->GetSharedResourceOperationVector();
            for (auto iter = operations.begin(); iter != operations.end(); iter++) {
                DumpOperation(history_out, *iter);
            }
            history_out << chunk << "[DumpOperations] End at   " << GetCurrentTimeStr()
                        << std::endl;

            // Dump memlogs from shared resource module to the output stream.
            history_out << chunk << "[DumpMemLog] Begin at " << GetCurrentTimeStr() << std::endl;
            if (MemLogIsEnable()) {
                for (auto mem_log = mem_logs_.begin(); mem_log != mem_logs_.end();
                     mem_logs_.inc(mem_log)) {
                    mem_log->DumpSharedResourceRecord(history_out);
                }
            } else {
                history_out << "The history log function is disable." << std::endl;
            }
            history_out << chunk << "[DumpMemLog] End at   " << GetCurrentTimeStr() << std::endl;

            // Close the output stream.
            history_out.close();
        }

    }  //  namespace TransactionLog

}  //  namespace mongo

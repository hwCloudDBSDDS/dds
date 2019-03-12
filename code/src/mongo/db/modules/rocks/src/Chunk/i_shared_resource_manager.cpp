
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdarg.h>
#include <memory>
#include <fcntl.h>
#include <stdint.h>
#include <semaphore.h>
#include <string.h>
#include <string>
#include <algorithm>
#include <vector>
#include <list>
#include <fstream>
#include <iostream>
#include <mutex>
#include <time.h>


#include "i_log_record_store.h"
#include "TransLogRecordStore.h"
#include "i_shared_resource_manager.h"
#include "mongo/util/log.h"
#include "maas_shared_resource_manager.h"

#define RET_OK   (0)
#define RET_ERR  (-1)

namespace mongo
{

namespace TransactionLog
{

    //
    //  Load-balancing transaction log related functions
    //  Is used by GC manager: GC manager periodically asks for batch of unprocessed log records calling
    //  function GetSharedResourceLogRecords. If there are some records that GC manager can process,
    //  provider will return this messages at "unprocessedRecords" list. When GC manager finished processing
    //  some messages from the batch it can set the lowest LSN of processed messages using function
    //  SetLastProcessedSharedResourceLSN, so with the next call GetSharedResourceLogRecords only records
    //  having higher LSN will be returned.
    //

//
// Methods of class SharedResourceOperation
//
uint32_t SharedResourceOperation::EncodeTo(char*& buf) {
    buf = nullptr;
    uint32_t buf_size = GetEncodeLength();
    if (0 == buf_size) {
        return 0;
    }
    buf = new char[buf_size];
    if (buf != nullptr) {
        return EncodeToBuf(buf);
    }
    return 0;
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
    return SharedResourceOperation::GetEncodeLength()
           + GetSharedResourceIdEncodeLength(resource_);
}
uint32_t SharedResourceFilterOperation::EncodeToBuf(char* buf) {
    uint32_t offset = 0;
    offset += SharedResourceOperation::EncodeToBuf(buf+offset);
    offset += EncodeSharedResourceIdToBuf(resource_, buf+offset);
    return offset;
}
SharedResourceFilterOperation* SharedResourceFilterOperation::DecodeFrom(const LogRecord& record) {
    // Allocate operation.
    SharedResourceFilterOperation* oper = new SharedResourceFilterOperation;
    // Decode fields.
    const char* buf = record.GetData();
    uint32_t offset = 0;
    offset += oper->DecodeBaseFrom(record);
    offset += DecodeSharedResourceIdFromBuf(oper->resource_, buf+offset);
    // Output operation.
    return oper;
}

//
// Methods of class SharedResourceReferenceOperation
//
uint32_t SharedResourceReferenceOperation::GetEncodeLength() const {
    return SharedResourceOperation::GetEncodeLength()
           + GetSharedResourceIdEncodeLength(resource_)
           + GetChunkIdEncodeLength(parent_chunk_id_)
           + GetChunkIdEncodeLength(child_chunk_id_);
}
uint32_t SharedResourceReferenceOperation::EncodeToBuf(char* buf) {
    uint32_t offset = 0;
    offset += SharedResourceOperation::EncodeToBuf(buf+offset);
    offset += EncodeSharedResourceIdToBuf(resource_, buf+offset);
    offset += EncodeChunkIdToBuf(parent_chunk_id_, buf+offset);
    offset += EncodeChunkIdToBuf(child_chunk_id_, buf+offset);
    return offset;
}
SharedResourceReferenceOperation* SharedResourceReferenceOperation::DecodeFrom(const LogRecord& record) {
    // Allocate operation.
    SharedResourceReferenceOperation* oper = new SharedResourceReferenceOperation;
    // Decode fields.
    const char* buf = record.GetData();
    uint32_t offset = 0;
    offset += oper->DecodeBaseFrom(record);
    offset += DecodeSharedResourceIdFromBuf(oper->resource_, buf+offset);
    offset += DecodeChunkIdFromBuf(oper->parent_chunk_id_, buf+offset);
    offset += DecodeChunkIdFromBuf(oper->child_chunk_id_, buf+offset);
    // Output operation.
    return oper;
}

//
// Methods of class SplitOperation
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
    offset += DecodeBsonFromBuf(originalKeyLow, (buf+offset));
    offset += DecodeBsonFromBuf(originalKeyHigh, (buf+offset));
    offset += DecodeBsonFromBuf(splitPointKey, (buf+offset));
    offset += DecodeStringFromBuf(rightDbPathWithRootPlog, (buf+offset));
    return offset;
}

uint32_t SplitOperation::GetEncodeLength() const {
    return SharedResourceOperation::GetEncodeLength()
           + GetSplitIdEncodeLength(id_)
           + description_->GetEncodeLength();
}
uint32_t SplitOperation::EncodeToBuf(char* buf) {
    uint32_t offset = 0;
    offset += SharedResourceOperation::EncodeToBuf(buf+offset);
    offset += EncodeSplitIdToBuf(id_, buf+offset);
    offset += description_->EncodeToBuf(buf+offset);
    return offset;
}
SplitOperation* SplitOperation::DecodeFrom(const LogRecord& record) {
    // Allocate operation.
    SplitOperation* oper = new SplitOperation;
    // Decode fields.
    const char* buf = record.GetData();
    uint32_t offset = 0;
    offset += oper->DecodeBaseFrom(record);
    offset += DecodeSplitIdFromBuf(oper->id_, buf+offset);
    std::shared_ptr<SplitDescription> description = std::make_shared<SplitDescription>();
    offset += description->DecodeFromBuf(buf+offset);
    oper->SetDescription(description);
    // Output operation.
    return oper;
}
const SharedResourceOperationType SplitOperation::GetOperTypeOfLogRecord(const LogRecord& record) {
    SharedResourceOperation oper;
    oper.DecodeBaseFrom(record);
    return oper.GetType();
}

void SplitContext::SetDescription(const std::shared_ptr<SplitDescription> description) {
    if (description == nullptr) {
        description_ = nullptr;
        return;
    }
    // Encode Split Description
    int buf_size = description->GetEncodeLength();
    char* buf = new char[buf_size];
    description->EncodeToBuf(buf);
    // Decode Split Description
    if (description_ == nullptr) {
        description_ = std::make_shared<SplitDescription>();
    }
    description_->DecodeFromBuf(buf);
}

//
// Methods of class ProcessedLSNOperation
//
uint32_t ProcessedLSNOperation::GetEncodeLength() const {
    return SharedResourceOperation::GetEncodeLength()
           + GetLsnEncodeLength(processed_lsn_);
}
uint32_t ProcessedLSNOperation::EncodeToBuf(char* buf) {
    uint32_t offset = 0;
    offset += SharedResourceOperation::EncodeToBuf(buf+offset);
    offset += EncodeLsnToBuf(processed_lsn_, buf+offset);
    return offset;
}
ProcessedLSNOperation* ProcessedLSNOperation::DecodeFrom(const LogRecord& record) {
    // Allocate operation.
    ProcessedLSNOperation* oper = new ProcessedLSNOperation;
    // Decode fields.
    const char* buf = record.GetData();
    uint32_t offset = 0;
    offset += oper->DecodeBaseFrom(record);
    offset += DecodeLsnFromBuf(oper->processed_lsn_, buf+offset);
    // Output operation.
    return oper;
}


//
// Methods of class SharedResourceManager
//

// It's very inefficient to sort the vecotr each time we insert a resource. So we use
// the following method to improve it.
// During the log replay, AddFilter will be inserted disorderly into the active vector
// and RemoveFilter will be inserted into the temporary vector. And at the end of the
// log replay, we sort the active vector only once and remove all the shared resources
// indicated by the temporary vector.
void SharedResourceManager::ApplyTemporarySharedResources() {
    if (!shared_resources_.empty()) {
        // Sort the active vector.
        std::sort(shared_resources_.begin(), shared_resources_.end(), CompareResourceId);
        // Remove the resources in temporary vector from the active vector.
        RemoveSharedResources(shared_resources_rmv_);
    }  else {
        // Record error runlog for each resource in temporary vector.
        for (auto resource : shared_resources_rmv_) {
            index_err() << "[SRM]Replay a RemoveFilter operation related to an unexisted shared resource: " << resource.toString();
        }
    }
    // Clear temporary vector.
    shared_resources_rmv_.clear();
}

rocksdb::Status SharedResourceManager::InitializationEnd(ILogRecordWriter& writer) {
    // At the end of ReplayLogRecord, apply the temporary resources into active vector.
    log() << "SharedResourceManager::InitializationEnd()";
    ApplyTemporarySharedResources();
    // Record the log wirter.
    SetLogWriter(&writer);
    return rocksdb::Status::OK();
};

// Add shared resources in batch.
void SharedResourceManager::AddSharedResources(const SharedResourceIds& resources)
{
    log() << "SharedResourceManager::AddSharedResources()";
    for(auto resource : resources) {
        log() << "SharedResourceManager::AddSharedResources() : " << resource.toString();
    }

    if (resources.size() > 0) {
        // Append the shared resources in batch.
        shared_resources_.insert(shared_resources_.end(), resources.begin(), resources.end());
        // Keep the active vector sorted in order to support binary search.
        std::sort(shared_resources_.begin(), shared_resources_.end(), CompareResourceId);
    }
}

// Remove shared resource in batch.
void SharedResourceManager::RemoveSharedResources_Method1(SharedResourceIds& resources_rmv) {
    std::vector<SharedResourceId>::iterator it;
    for (auto resource : resources_rmv) {
        it = std::lower_bound(shared_resources_.begin(), shared_resources_.end(), resource, CompareResourceId);
        if (it == shared_resources_.end() || (!EqualResourceId((*it), resource))) {
            index_err() << "[SRM]Try to remove an unexisted shared resource: " << resource.toString();
            continue;
        }
        shared_resources_.erase(it);
    }
}
void SharedResourceManager::RemoveSharedResources_Method2(SharedResourceIds& resources_rmv) {
    std::vector<SharedResourceId>::iterator prev = shared_resources_.begin();
    std::vector<SharedResourceId>::iterator next = shared_resources_.end();
    SharedResourceIds resources_result;

    // Sort the shared resources to be deleted in order that the following algorithm can work well.
    std::sort(resources_rmv.begin(), resources_rmv.end(), CompareResourceId);

    // The logic before calling this function make it sure that all the resources in 'resources_rmv' must be in
    // 'shared_resources_'.
    resources_result.reserve(shared_resources_.size() - resources_rmv.size());

    // Skip the resources to be removed one by one, and copy all the remained resources to the 'resources_result'.
    for (auto resource : resources_rmv) {
        if (prev == shared_resources_.end()) {
            // Record the runlog for each unexisted shared resource to be deleted.
            index_err() << "[SRM]Try to remove an unexisted shared resource: " << resource.toString();
            continue;
        }
        next = std::lower_bound(prev, shared_resources_.end(), resource, CompareResourceId);
        if (next == shared_resources_.end() || (!EqualResourceId((*next), resource))) {
            index_err() << "[SRM]Try to remove an unexisted shared resource: " << resource.toString();
            prev = next;
            continue;
        }
        // Copy the segment between two adjacent resources to be removed to the tail of the temporary vector.
        if (next != prev) {
            resources_result.insert(resources_result.end(), prev, next);
        }
        prev = (next + 1);
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

// Set the remove_result of all the shared resources to fault if failed to write RemoveFilter logs.
void SharedResourceManager::SetSharedResourcesRemoveResultToFail(std::vector<SharedResourceRemoveDescription>& list) {
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
        index_err() << "[SRM]LogProvider is not ready.";
        return rocksdb::Status::Aborted();
    }

    // Deleting share resource is not permitted during a split even if the resource belongs to
    // a previous split but current split. That is, all the operations between the StartSplit
    // and the related CommitSplit/RollbackSplit belongs to the split identified and bounded by
    // the StartSplit and CommitSplit/RollbackSplit.
    if (share_res_provider_->GetSplitContext()->IsStateStart()) {
        index_log() << "[SRM]Deleting share resource is not permitted during a split.";
        return rocksdb::Status::Aborted();
    }

    // Init all the resources to be removed failed.
    for (auto it = list.begin(); it != list.end(); it++) {
        it->shared_flag = false;
        it->remove_result = RET_ERR;
    }

    // Construct RemoveFilter and RemoveReferebce log records for each resource.
    for (auto it = list.begin(); it != list.end(); it++) {
        s = OnResourceDeletion(log_records, resources, it);
        if (!s.ok()) {
            index_err() << "[SRM]Construct log records failed.";
            // Set the remove_result of all the shared resources to fault because no RemoveFilter log is writed.
            SetSharedResourcesRemoveResultToFail(list);
            return rocksdb::Status::OK();
        }
    }

    // Write RemoveFilter and RemoveReferebce log records in batch.
    if (!log_records.empty()) {
        s = share_res_provider_->WriteRecords(log_records);
        if (!s.ok()) {
            index_err() << "[SRM]Write log records failed.";
            // Set the remove_result of all the shared resources to fault because RemoveFilter logs is writed failed.
            SetSharedResourcesRemoveResultToFail(list);
            return rocksdb::Status::OK();
        }
    }

    // Update shared resources list in memory.
    RemoveSharedResources(resources);

    // Append reference operations in memory.
    share_res_provider_->AppendSharedResourceReferenceOperation(log_records);
    return rocksdb::Status::OK();
}
// Construct two log records for one resource to be deleted, one is RemoveFilter, the other is RemoveReferebce.
rocksdb::Status SharedResourceManager::OnResourceDeletion(
        LogRecordVector& log_records,
        std::vector<SharedResourceId>& resources,
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
    rocksdb::Status s = PrepareFilterLog(log_records,
                                SharedResourceOperationType::RemoveFilter, resource_it->id);
    if (!s.ok()) {
        index_err() << "[SRM]Construct RemoveFilter log failed.";
        return s;
    }
    // Construct a RemoveReference log for current shared resource.
    s = share_res_provider_->PrepareReferenceLog(log_records,
                                                 SharedResourceOperationType::RemoveReference,
                                                 resource_it->id,
                                                 INVALID_CHUNK_ID,
                                                 INVALID_CHUNK_ID);
    if (!s.ok()) {
        index_err() << "[SRM]Construct RemoveReference log failed.";
        return s;
    }
    // Construct the two logs successfully.
    resource_it->remove_result = RET_OK;
    resources.push_back(resource_it->id);
    return s;
}

// Construct one filter log.
rocksdb::Status SharedResourceManager::PrepareFilterLog(
        LogRecordVector& log_records,
        const SharedResourceOperationType oper_type,
        const SharedResourceId& resource)
{
    SharedResourceFilterOperation operation(oper_type, resource);
    char* dst_buf = nullptr;
    uint32_t buf_size = 0;

    // Allocate unique buffer and encode current operation into the buffer.
    buf_size = operation.EncodeTo(dst_buf);
    if (nullptr == dst_buf)
    {
        index_err() << "[SRM]Fetal Error when alloc dst_buf fail.";
        return rocksdb::Status::Aborted();
    }

    // Construct and write the log record of current operation.
    log_records.push_back(LogRecord(LogRecordType::SharedResourceFilterLog,
                          rocksdb::Slice(dst_buf, buf_size)));
    return rocksdb::Status::OK();
}
// Construct filter logs in batch and output them.
rocksdb::Status SharedResourceManager::PrepareFilterLog(
        LogRecordVector& log_records,
        const SharedResourceOperationType oper_type,
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

// Write Checkpoint
rocksdb::Status SharedResourceManager::WriteCheckpoint() {
    index_log() << "[SRM]WriteCheckpoint filter " << shared_resources_.size();

    // Prepare filter logs.
    LogRecordVector log_records;
    rocksdb::Status s = PrepareFilterLog(log_records,
                                         SharedResourceOperationType::AddFilter,
                                         shared_resources_);
    if (!s.ok()) {
        return s;
    }

    // Write filter logs.
    if (!log_records.empty()) {
        s = share_res_provider_->WriteRecords(log_records);
        if (!s.ok())
        {
            index_err() << "[SRM]Write log records failed.";
            return s;
        }
    }

    // Run log for CheckPoint.
    if (SharedResourceModule::RunLogIsEnable()) {
        for (auto resource : shared_resources_) {
            index_log() << "[SRM]Memory Filter when CheckPoint: resource:" << resource.toString();
        }
    }

    return rocksdb::Status::OK();
}

// Replay log records.
rocksdb::Status SharedResourceManager::ReplayLogRecord(const LogRecord& record, bool& needMoreRecords) {
    needMoreRecords = false;
    log() << "SharedResourceManager::ReplayLogRecord() type: " << (int)(record.GetType());
    if (record.GetType() != LogRecordType::SharedResourceFilterLog) {
        index_err() << "[SRM]Invalid log record type (" << std::to_string(record.GetType()) << ").";
        return rocksdb::Status::Aborted();
    }

    SharedResourceFilterOperation* oper = SharedResourceFilterOperation::DecodeFrom(record);
    const SharedResourceOperationType type = oper->GetType();
    if (type == SharedResourceOperationType::AddFilter) {
        // Save the AddFilter in the active vector which is not sorted for the time being.
        // The active vector will be sorted at the end of RelayLog.
        shared_resources_.push_back(oper->GetResource());
    }else if (type == SharedResourceOperationType::RemoveFilter) {
        // Save the RemoveFilter in the temporary vector for the time being.
        shared_resources_rmv_.push_back(oper->GetResource());
    }else {
        delete oper;
        // Unknown operation.
        index_err() << "[SRM]Invalid filter operation type (" << static_cast<int>(type) << ").";
        return rocksdb::Status::Aborted();
    }

    delete oper;
    needMoreRecords = true;
    return rocksdb::Status::OK();
}


//
// Implementation of the methods of SharedResourceOperationLogRecordProvider
//

rocksdb::Status SharedResourceOperationLogRecordProvider::InitializationEnd(ILogRecordWriter& logWriter) {
    log() << "SharedResourceOperationLogRecordProvider::InitializationEnd()";
    // Set log writer.
    SetLogWriter(&logWriter);
    // Sort the shared resources.
    split_ctx_.SortSharedResources();

    // Judge the latest split replayed is successful or not.
    if (!split_ctx_.IsStateStart()) {
        if (nullptr != split_ctx_.GetDescription())
        {
            mongo::ChunkMetadata::KeyRange left_key_range = split_ctx_.GetDescription()->GetLeftKeyRange();
            mongo::ChunkMetadata::KeyRange parent_key_range = split_ctx_.GetDescription()->GetKeyRange();
            log() << "SharedResourceOperationLogRecordProvider::InitializationEnd status: " << (int)(split_ctx_.GetState());
            log() << "SharedResourceOperationLogRecordProvider::InitializationEnd leftrange: " << left_key_range.GetKeyLow()
                << "  --  " << left_key_range.GetKeyHigh() << "; parent_key_range: " << parent_key_range.GetKeyLow() <<
                "  --  " << parent_key_range.GetKeyHigh();
            for (auto id : split_ctx_.GetSharedResources()) {
                log() << "SharedResourceOperationLogRecordProvider::InitializationEnd id: " << id.toString();
            }
        }

        return rocksdb::Status::OK();
    }

    log() << "SharedResourceOperationLogRecordProvider::InitializationEnd()-> ";
    mongo::ChunkMetadata::KeyRange left_key_range = split_ctx_.GetDescription()->GetLeftKeyRange();
    mongo::ChunkMetadata::KeyRange parent_key_range = split_ctx_.GetDescription()->GetKeyRange();
    index_log() << "[SRM]InitializationEnd: " \
                << "chunk(" << tojson(key_range_.GetKeyLow()) << "," << tojson(key_range_.GetKeyHigh()) << "), " \
                << "left(" << tojson(left_key_range.GetKeyLow()) << "," << tojson(left_key_range.GetKeyHigh()) << "), " \
                << "parent(" << tojson(parent_key_range.GetKeyLow()) << "," << tojson(parent_key_range.GetKeyHigh()) << ").";
    if (key_range_ == left_key_range) {
        // Add shared resource into filters list.
        share_res_manager_->AddSharedResources(split_ctx_.GetSharedResources());
        // Update operation list.
        split_ctx_.SetStateToSuccess(); // Set the split state to success.
        RemoveStartSplitOperation();    // Split is over, then remove the StartSplit operation.
        index_err() << "[SRM]CommitSplit recover successfully. " \
                    << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                    << "id_:" << split_ctx_.GetId();
        // Split OK and CommitSplit is missing.
        failed_split_end_oper_ = SharedResourceOperationType::CommitSplit;
        return rocksdb::Status::OK();
    }else if (key_range_ == parent_key_range) {
        // Update operation list.
        split_ctx_.SetStateToFault(); // Set the split state to fault
        RollbackSplitOperations();    // Rollback all the operations related to the split.
        index_err() << "[SRM]RollbackSplit recover successfully. " \
                    << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                    << "id_:" << split_ctx_.GetId();
        // Split failed and RollbackSplit is missing.
        failed_split_end_oper_ = SharedResourceOperationType::RollbackSplit;
        return rocksdb::Status::OK();
    }else {
        // Unconsistent key ranges.
        index_err() << "[SRM]Unconsistent key range: " \
                    << "chunk(" \
                      << tojson(key_range_.GetKeyLow()) << "," \
                      << tojson(key_range_.GetKeyHigh()) \
                    << "), " \
                    << "split(" \
                      << tojson(split_ctx_.GetDescription()->splitPointKey) <<"," \
                      << tojson(split_ctx_.GetDescription()->originalKeyLow) <<"," \
                      << tojson(split_ctx_.GetDescription()->originalKeyHigh)
                    << ").";
    }
    return rocksdb::Status::Aborted();
}

// Write run log for an operation.
void RunLogOperation(const SharedResourceOperation* oper, const char* proc) {
    uint64_t lsn = oper->GetLsn();
    SharedResourceOperationType oper_type = oper->GetType();
    const SharedResourceReferenceOperation* reference_oper = nullptr;
    const SplitOperation* split_oper = nullptr;
    SharedResourceId resource;

    switch (oper_type) {
    case SharedResourceOperationType::AddReference:
    case SharedResourceOperationType::RemoveReference:
        reference_oper = reinterpret_cast<const SharedResourceReferenceOperation*>(oper);
        resource = reference_oper->GetResource();
        index_log() << "[SRM]Memory Operation when " << proc << ": " \
                    << "lsn:" << lsn << ", " \
                    << "type:" << GetOperTypeName(oper_type) << ", " \
                    << "resource:" << resource << ", " \
                    << "chunk:" << "(" << reference_oper->GetParentChunkId() << "," \
                                       << reference_oper->GetChildChunkId() << ").";
        break;
    case SharedResourceOperationType::StartSplit:
    case SharedResourceOperationType::CommitSplit:
    case SharedResourceOperationType::RollbackSplit:
        split_oper = reinterpret_cast<const SplitOperation*>(oper);
        index_log() << "[SRM]Memory Operation when " << proc << ": " \
                    << "lsn:" << lsn << ", " \
                    << "type:" << GetOperTypeName(oper_type) << ", " \
                    << "split_id:" << split_oper->GetId() << ", " \
                    << "split_point:" << tojson(split_oper->GetDescription()->splitPointKey) << "," \
                    << "key_range:(" << tojson(split_oper->GetDescription()->originalKeyLow) << "," \
                                     << tojson(split_oper->GetDescription()->originalKeyHigh) << ")";

        break;
    default:
        index_log() << "[SRM]Memory Operation when " << proc << ": " \
                    << "lsn:" << lsn << ", " \
                    << "type:" << GetOperTypeName(oper_type);
        break;
    }
}

// Write Checkpoint, that is, rewrite all logs related to all the shared resource operations in memory.
rocksdb::Status SharedResourceOperationLogRecordProvider::WriteCheckpoint()
{
    LogRecordVector log_records;
    rocksdb::Status s;
    rocksdb::Slice slice;
    char* dst_buf = nullptr;
    uint32_t buf_size = 0;

    index_log() << "[SRM]WriteCheckpoint operation " << operations_.size();

    // Prepare log records in batch.
    for (auto iter = operations_.begin(); iter != operations_.end(); iter++)
    {
        // Allocate unique buffer and encode current operation into the buffer.
        buf_size = (*iter)->EncodeTo(dst_buf);
        if (nullptr == dst_buf)
        {
            index_err() << "[SRM]Fetal Error when alloc dst_buf.";
            return rocksdb::Status::Aborted();
        }

        // Construct the log record of current operation.
        log_records.push_back(LogRecord(LogRecordType::LoadBalancingTransactionLog,
                                        rocksdb::Slice(dst_buf, buf_size)));
    }

    // Write log records in batch.
    s = WriteRecords(log_records);
    if (!s.ok())
    {
        index_err() << "[SRM]Write log records failed";
        return s;
    }

    // Run log for CheckPoint.
    if (SharedResourceModule::RunLogIsEnable()) {
        for (auto oper = operations_.begin(); oper != operations_.end(); oper++) {
            RunLogOperation(*oper, "CheckPoint");
        }
    }

    // Write log records in batch.
    return rocksdb::Status::OK();
}

// Construct one reference log.
rocksdb::Status SharedResourceOperationLogRecordProvider::PrepareReferenceLog(
        LogRecordVector& log_records,
        const SharedResourceOperationType oper_type,
        const SharedResourceId& resource,
        const uint64_t& parent_chunk_id,
        const uint64_t& child_chunk_id)
{
    SharedResourceReferenceOperation operation(oper_type, resource, parent_chunk_id, child_chunk_id);
    rocksdb::Status s;
    rocksdb::Slice slice;
    char* dst_buf = nullptr;
    uint32_t buf_size = 0;

    // Allocate unique buffer and encode current operation into the buffer.
    buf_size = operation.EncodeTo(dst_buf);
    if (nullptr == dst_buf)
    {
        index_err() << "[SRM]Fetal Error when alloc dst_buf fail.";
        return rocksdb::Status::Aborted();
    }

    // Construct and write the log record of current operation.
    log_records.push_back(LogRecord(LogRecordType::LoadBalancingTransactionLog,
                          rocksdb::Slice(dst_buf, buf_size)));
    return rocksdb::Status::OK();
}
// Construct reference logs in batch.
rocksdb::Status SharedResourceOperationLogRecordProvider::PrepareReferenceLog(
        LogRecordVector& log_records,
        const SharedResourceOperationType oper_type,
        const std::vector<SharedResourceId>& resources,
        const uint64_t& parent_chunk_id,
        const uint64_t& child_chunk_id) {
    rocksdb::Status s;
    for (auto resource : resources) {
        s = PrepareReferenceLog(log_records, oper_type, resource, parent_chunk_id, child_chunk_id);
        if (!s.ok()) {
            log_records.clear();
            return s;
        }
    }
    return rocksdb::Status::OK();
}

// SplitState [ SplitStart SplitCommit SplitRollback ]
rocksdb::Status SharedResourceOperationLogRecordProvider::PrepareSplitLog(
        LogRecordVector& log_records,
        const SharedResourceOperationType oper_type,
        const uint64_t split_id,
        std::shared_ptr<SplitDescription> description)
{
    SplitOperation operation(oper_type, split_id, description);
    char* dst_buf = nullptr;
    uint32_t buf_size = 0;

    // Allocate unique buffer and encode current operation into the buffer.
    buf_size = operation.EncodeTo(dst_buf);
    if (nullptr == dst_buf)
    {
        index_err() << "[SRM]Fetal Error when alloc dst_buf fail.";
        return rocksdb::Status::Aborted();
    }

    log_records.push_back(LogRecord(LogRecordType::LoadBalancingTransactionLog, rocksdb::Slice(dst_buf, buf_size)));
    return rocksdb::Status::OK();
}

// Write a Processed log.
rocksdb::Status SharedResourceOperationLogRecordProvider::LogProcessedLSN()
{
    invariant(processed_lsn_ != INVALID_LSN);

    ProcessedLSNOperation operation(processed_lsn_);
    rocksdb::Status s;
    char* dst_buf = nullptr;
    uint32_t buf_size = 0;

    // Alloc unique buffer for writting log
    buf_size = operation.EncodeTo(dst_buf);
    if (nullptr == dst_buf) {
        index_err() << "[SRM]Fetal Error when alloc dst_buf. " << "processed_lsn:" << processed_lsn_;
        // Record the missing processed LSN state.
        processed_lsn_failed_flag_ = true;
        return rocksdb::Status::Aborted();
    }

    // Write the ProcessedLSN log
    LogRecord log_rec(LogRecordType::LoadBalancingTransactionLog, rocksdb::Slice(dst_buf, buf_size));
    s = WriteRecord(log_rec);
    if (!s.ok()) {
        index_err() << "[SRM]Write ProcessedLSN log failed." << "processed_lsn:" << processed_lsn_;
        // Record the missing processed LSN state.
        processed_lsn_failed_flag_ = true;
    } else {
        // Clear the missing processed LSN state.
        processed_lsn_failed_flag_ = false;
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
        const SharedResourceIds& resources) {
    // The split state must be 'SplitStart' when this operation occurs.
    if (!split_ctx_.IsStateStart())
    {
        index_err() << "[SRM]Invalid split state(" << static_cast<int>(split_ctx_.GetState()) << ").";
        return rocksdb::Status::Aborted();
    }

    // Write reference logs.
    LogRecordVector log_records;
    SharedResourceOperationType oper_type = SharedResourceOperationType::AddReference;
    // 1) Construct log records in batch.
    rocksdb::Status s = PrepareReferenceLog(log_records,
                                            oper_type,
                                            resources,
                                            split_ctx_.GetDescription()->GetParentChunkId(),
                                            split_ctx_.GetDescription()->GetChildChunkId());
    if (!s.ok())
    {
        index_err() << "[SRM]Prepare reference log failed.";
        return s;
    }
    // 2) Write log records in batch.
    s = WriteRecords(log_records);
    if (!s.ok())
    {
        index_err() << "[SRM]Write log records failed";
        return s;
    }

    // Register shared resources into split context.
    split_ctx_.RegisterSharedResources(resources);

    // Append operations in batch.
    AppendSharedResourceReferenceOperation(log_records);
    return rocksdb::Status::OK();
}

//
// GC related functions
//

// Output the operations in an appointed range.
void SharedResourceOperationLogRecordProvider::GetOperationsByRange(
        uint32_t max_size,
        std::vector<SharedResourceReferenceOperation>& records,
        const SharedResourceOperationIterator& start_it,
        const SharedResourceOperationIterator& end_it) {
    for (auto it = start_it; it != end_it; it++) {
        if (records.size() >= max_size) {
            break;
        }
        SharedResourceOperation* oper = *it;
        if (IsReferenceOper(oper->GetType())) {
            const SharedResourceReferenceOperation *reference_oper
                = reinterpret_cast<const SharedResourceReferenceOperation*>(oper);
            records.push_back(*reference_oper);
        }
    }
}

// Get some unprocessed operations of a number limited by maxBatchMaxSize.
void SharedResourceOperationLogRecordProvider::GetUnprocessedOperations(
        uint32_t maxBatchMaxSize, std::vector<SharedResourceReferenceOperation>& unprocessedRecords) {
    SharedResourceOperationIterator oper_it;
    SharedResourceReferenceOperation* reference_oper;
    int old_size = unprocessedRecords.size();

    for (auto oper_it = operations_.begin(); oper_it != operations_.end(); oper_it++) {
        if (unprocessedRecords.size() >= maxBatchMaxSize) {
            break;
        }
        SharedResourceOperation* oper = *oper_it;
        switch (oper->GetType()) {
        case SharedResourceOperationType::StartSplit:
            // A SplitStart operation means a split is in progress. All the later shared resource operations
            // must be related to the uncompleted split and have not yet become effective.
            index_log() << "[SRM]End at a StartSplit operation." << "split_lsn:" << oper->GetLsn();
            return;
        case SharedResourceOperationType::AddReference:
        case SharedResourceOperationType::RemoveReference:
            // Output the operations that are outside of split or belongs to a committed split.
            reference_oper = reinterpret_cast<SharedResourceReferenceOperation*>(oper);
            unprocessedRecords.push_back(*reference_oper);
            break;
        default:
            // Invalid operations.
            index_err() << "[SRM]Invalid operation type (" << static_cast<int>(oper->GetType()) << ").";
            dassert(false);
            return;
        }
    }

    int num = unprocessedRecords.size() - old_size;
    if (num <= 0) {
        index_log() << "[SRM]GetUnprocessed : number:" << num;
    }else {
        uint64_t lsn1 = unprocessedRecords[old_size].GetLsn();
        uint64_t lsn2 = unprocessedRecords.back().GetLsn();
        index_log() << "[SRM]GetUnprocessed : number:" << num << "lsn:(" << lsn1 << "," << lsn2 << ")";
    }
}

rocksdb::Status SharedResourceOperationLogRecordProvider::SetLastProcessedOperationLSN(uint64_t processed_lsn) {
    // Check the processed LSN to be set.
    if (processed_lsn == INVALID_LSN) {
        index_err() << "[SRM]SetProcessed: invalid processed lsn (" << processed_lsn << ").";
        return rocksdb::Status::Aborted();
    }
    if (split_ctx_.IsStateStart() && (processed_lsn >= split_ctx_.GetLsn())) {
        index_err() << "[SRM]SetProcessed: the processed lsn confilcts with the split lsn. " \
                    << "processed_lsn(" << processed_lsn << ") >= split_lsn(" << split_ctx_.GetLsn() << ").";
        return rocksdb::Status::Aborted();
    }
    if ((processed_lsn_ != INVALID_LSN) && (processed_lsn_ >= processed_lsn)) {
        index_err() << "[SRM]SetProcessed: re-sent or disorder processed lsn " \
                    << "(old:" << processed_lsn_ << ", new:" << processed_lsn << ").";
        return rocksdb::Status::OK();
    }

    index_log() << "[SRM]SetProcessed: (old:" << processed_lsn_ << ", new:" << processed_lsn << ").";

    // Remove the processed operations from memory.
    operations_.RemoveProcessedOperations(processed_lsn);
    processed_lsn_ = processed_lsn;

    // Write the processed lsn into transaction log.
    (void)LogProcessedLSN();
    return rocksdb::Status::OK();
}

//
// Split related functions
//

// Write the log of a split operation.
rocksdb::Status SharedResourceOperationLogRecordProvider::LogSplitState(
        SharedResourceOperationType oper_type,
        uint64_t split_id,
        std::shared_ptr<SplitDescription> description) {
    LogRecordVector log_records;
    rocksdb::Status s = rocksdb::Status::OK();
    std::string header;

    do {
        if (oper_type == SharedResourceOperationType::CommitSplit) {
            // Construct AddFilter logs in batch.
            s = share_res_manager_->PrepareFilterLog(log_records,
                                                     SharedResourceOperationType::AddFilter,
                                                     split_ctx_.GetSharedResources());
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
        //   'WriteRecord' will call RewriteImportantFailedLog() at first before writting current log.
        //   'split_oper_to_write_' is used to avoid writting CommitSplit or RollbackSplit repeatedly.
        //   After calling 'WriteRecord', 'split_oper_to_write_' must be reset.
        split_oper_to_write_ = oper_type;
        s = WriteRecords(log_records);
        split_oper_to_write_ = SharedResourceOperationType::None;
        if (!s.ok()) {
            header = "Write split log failed. ";
            break;
        }

        // Set the LSN of this new split.
        if (oper_type == SharedResourceOperationType::StartSplit) {
            split_ctx_.SetLsn(log_records.front().GetLsn());
        }

        // Clear the missing split end operation.
        failed_split_end_oper_ = SharedResourceOperationType::None;
        break;
    }while(0);

    if (!s.ok()) {
        index_err() << "[SRM]" << header \
                    << "oper_type:" << static_cast<int>(oper_type) << ", " \
                    << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                    << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                    << "id_:" << split_ctx_.GetId() << ", " \
                    << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << ", " \
                    << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                     << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")";
        // Record the missing split end operation.
        failed_split_end_oper_ = (IsSplitEnd(oper_type) ? oper_type : SharedResourceOperationType::None);
    }

    return s;
}

// Start split
rocksdb::Status SharedResourceOperationLogRecordProvider::StartSplit(std::shared_ptr<SplitDescription> description) {
    rocksdb::Status s;
    SharedResourceOperationType oper_type = SharedResourceOperationType::StartSplit;

    if (split_ctx_.IsStateStart()) {
        index_err() << "[SRM]Overlap split transactions. " \
                    << "old(" \
                      << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                      << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << "," \
                      << "id_:" << split_ctx_.GetId() << "," \
                      << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << "," \
                      << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                       << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")" \
                    << "), " \
                    << "new(" \
                      << "split_point:" << tojson(description->splitPointKey) << "," \
                      << "key_range:(" << tojson(description->originalKeyLow) << "," \
                                       << tojson(description->originalKeyHigh) << ")" \
                    << ").";

        return rocksdb::Status::Aborted();
    }

    // Write StartSplit log.
    s = LogSplitState(oper_type, split_ctx_.GetId(), description);
    if (!s.ok()) {
        index_err() << "[SRM]Write split log failed. " \
                    << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                    << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                    << "id_:" << split_ctx_.GetId() << ", " \
                    << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << "," \
                    << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                     << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")";
        return s;
    }

    // Append StartSplit operation.
    SplitOperation* split_oper = new SplitOperation(split_ctx_.GetLsn(),
                                                    SharedResourceOperationType::StartSplit,
                                                    split_ctx_.GetLsn(),
                                                    description);
    operations_.push_back(reinterpret_cast<SharedResourceOperation*>(split_oper));

    // Update the split context.
    split_ctx_.SetId(split_ctx_.GetLsn());
    split_ctx_.SetDescription(description);
    split_ctx_.SetStateToStart();
    index_log() << "[SRM]StartSplit success. " \
                << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                << "id_:" << split_ctx_.GetId() << ", " \
                << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << ", " \
                << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                 << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")";
    return rocksdb::Status::OK();
}

// Rollback split
rocksdb::Status SharedResourceOperationLogRecordProvider::RollbackSplit() {
    if (split_ctx_.IsStateNone()) {
        index_err() << "[SRM]Fetal Error when RollbackSplit without related StartSplit. " \
                    << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                    << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                    << "id_:" << split_ctx_.GetId();
        return rocksdb::Status::Aborted();
    }
    else if (split_ctx_.IsStateSuccess()) {
        index_err() << "[SRM]Fetal Error when RollbackSplit without related StartSplit. " \
                    << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                    << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                    << "id_:" << split_ctx_.GetId() << ", " \
                    << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << ", " \
                    << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                     << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")";
        return rocksdb::Status::Aborted();
    }
    else if (split_ctx_.IsStateFault()) {
        index_err() << "[SRM]Maybe re-sent RollbackSplit, ignore it. " \
                    << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                    << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                    << "id_:" << split_ctx_.GetId() << ", " \
                    << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << ", " \
                    << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                     << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")";
        return rocksdb::Status::OK();
    }

    // Rollback operation list.
    split_ctx_.SetStateToFault(); // Set the split state to fault
    RollbackSplitOperations();    // Rollback all the operations related to the split.
    index_log() << "[SRM]RollbackSplit success. " \
                << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                << "id_:" << split_ctx_.GetId() << ", " \
                << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << ", " \
                << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                 << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")";

    // Write StartSplit log.
    (void)LogSplitState(SharedResourceOperationType::RollbackSplit,
                        split_ctx_.GetId(),
                        split_ctx_.GetDescription());
    return rocksdb::Status::OK();
}

// Commit split
rocksdb::Status SharedResourceOperationLogRecordProvider::CommitSplit() {
    if (split_ctx_.IsStateNone()) {
        index_err() << "[SRM]Fetal Error when CommitSplit without related StartSplit. " \
                    << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                    << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                    << "id_:" << split_ctx_.GetId();
        return rocksdb::Status::Aborted();
    }
    else if (split_ctx_.IsStateFault()) {
        index_err() << "[SRM]Fetal Error when CommitSplit without related StartSplit. " \
                    << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                    << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                    << "id_:" << split_ctx_.GetId() << ", " \
                    << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << ", " \
                    << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                     << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")";
        return rocksdb::Status::Aborted();
    }
    else if (split_ctx_.IsStateSuccess()) {
        index_err() << "[SRM]Maybe re-sent CommitSplit, ignore it. " \
                    << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                    << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                    << "id_:" << split_ctx_.GetId() << ", " \
                    << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << ", " \
                    << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                     << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")";
        return rocksdb::Status::OK();
    }

    // Add shared resource filters.
    share_res_manager_->AddSharedResources(split_ctx_.GetSharedResources());
    // Append AddReference operations.
    split_ctx_.SetStateToSuccess(); // Set the split state to success.
    RemoveStartSplitOperation();    // Split is over, then remove the StartSplit operation.
    index_log() << "[SRM]CommitSplit success. " \
                << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                << "id_:" << split_ctx_.GetId() << ", " \
                << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << ", " \
                << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                 << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")";

    // Write StartSplit log.
    (void)LogSplitState(SharedResourceOperationType::CommitSplit,
                        split_ctx_.GetId(),
                        split_ctx_.GetDescription());
    return rocksdb::Status::OK();
}

// Rewrite the split end log written failed last time, which is one of CommitSplit,RollbackSplit.
rocksdb::Status SharedResourceOperationLogRecordProvider::RewriteSplitEndLog() {
    if (split_oper_to_write_ == failed_split_end_oper_) {
        // If the split operation to be written is same as the one written failed last time, only
        // one of them need to be written. So filter out the one written failed last time.
        return rocksdb::Status::OK();
    }
    switch (failed_split_end_oper_) {
    case SharedResourceOperationType::CommitSplit:
    case SharedResourceOperationType::RollbackSplit:
        return LogSplitState(failed_split_end_oper_, split_ctx_.GetId(), split_ctx_.GetDescription());
    case SharedResourceOperationType::None:
        return rocksdb::Status::OK();
    default:
        index_err() << "[SRM]Invalid failed operation type (" << static_cast<int>(failed_split_end_oper_) << ").";
        return rocksdb::Status::Aborted();
    }
}
// Rewrite the log(s) written failed last time.
rocksdb::Status SharedResourceOperationLogRecordProvider::RewriteImportantFailedLog() {
    // Rewrite split end log.
    rocksdb::Status s = RewriteSplitEndLog();
    if (!s.ok()) {
        index_err() << "[SRM]Fail to rewrite split end log (" << static_cast<int>(failed_split_end_oper_) << ").";
        return s;
    }

    // Rewrite Processed log.
    if (processed_lsn_failed_flag_) {
        return LogProcessedLSN();
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
    if (oper_it == operations_.end()) {
        // Abnormal: no StartSplit was found.
        index_err() << "[SRM]Missing StartSplit related to CommitSplit.";
        dassert(false);
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
    if (oper_it == operations_.end()) {
        // Abnormal: no StartSplit was found.
        index_err() << "[SRM]Missing StartSplit related to RollbackSplit.";
        dassert(false);
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
rocksdb::Status SharedResourceOperationLogRecordProvider::ReplayStartSplitLog(const LogRecord& record) {
    // Decode operation.
    auto oper = SplitOperation::DecodeFrom(record);
    if (oper == nullptr) {
        index_err() << "Fetal Err when decode StartSplit operation.";
        return rocksdb::Status::Aborted();
    }
    // The id of split just is the LSN of the related SplitStart operation.
    oper->SetId(oper->GetLsn());

    // Check StartSplit overlap.
    if (split_ctx_.IsStateStart()) {
        index_err() << "[SRM]Overlap split transactions. " \
                    << "old(" \
                      << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                      << "id_:" << split_ctx_.GetId() << "," \
                      << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << "," \
                      << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                       << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")" \
                    << "), " \
                    << "new(" \
                      << "id_:" << oper->GetId() << "," \
                      << "split_point:" << tojson(oper->GetDescription()->splitPointKey) << "," \
                      << "key_range:(" << tojson(oper->GetDescription()->originalKeyLow) << "," \
                                       << tojson(oper->GetDescription()->originalKeyHigh) << ")" \
                    << ").";
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
    index_log() << "[SRM]Replay StartSplit successfully. " \
                << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                << "id_:" << split_ctx_.GetId() << ", " \
                << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << ", " \
                << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                 << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")";
    return rocksdb::Status::OK();
}

// Replay CommitSplit log
rocksdb::Status SharedResourceOperationLogRecordProvider::ReplayCommitSplitLog(const LogRecord& record) {
    // Decode operation
    auto oper = SplitOperation::DecodeFrom(record);
    if (oper == nullptr) {
        index_err() << "[SRM]Fetal Err when decode CommitSplit operation.";
        rocksdb::Status::Aborted();
    }

    // Check CommitSplit.
    if (split_ctx_.IsStateSuccess() && (oper->GetId() == split_ctx_.GetId())) {
        // Ignore the CommitSplit written repeatedly.
        index_err() << "[SRM]Ignore the CommitSplit written repeatedly. " \
                    << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                    << "id_:" << oper->GetId() << "," \
                    << "split_point:" << tojson(oper->GetDescription()->splitPointKey) << ", " \
                    << "key_range:(" << tojson(oper->GetDescription()->originalKeyLow) << "," \
                                     << tojson(oper->GetDescription()->originalKeyHigh) << ")";
        delete oper;
        return rocksdb::Status::OK();
    }
    else if (split_ctx_.IsStateNone()) {
        index_err() << "[SRM]CommitSplit without related StartSplit. " \
                    << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                    << "id_:" << oper->GetId();
        delete oper;
        return rocksdb::Status::Aborted();
    }
    else if ((!split_ctx_.IsStateStart()) || (oper->GetId() != split_ctx_.GetId())) {
        index_err() << "[SRM]CommitSplit without related StartSplit. " \
                    << "old(" \
                      << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                      << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                      << "id_:" << split_ctx_.GetId() << "," \
                      << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << "," \
                      << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                       << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")" \
                    << "), " \
                    << "new(" \
                      << "id_:" << oper->GetId() << "," \
                      << "split_point:" << tojson(oper->GetDescription()->splitPointKey) << "," \
                      << "key_range:(" << tojson(oper->GetDescription()->originalKeyLow) << "," \
                                       << tojson(oper->GetDescription()->originalKeyHigh) << ")" \
                    << ").";
        delete oper;
        return rocksdb::Status::Aborted();
    }

    // Matched split id means the split is over, then remove the StartSplit operation.
    RemoveStartSplitOperation();
    // Set the split state to success.
    split_ctx_.SetStateToSuccess();
    index_log() << "[SRM]Replay CommitSplit successfully. " \
                << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                << "id_:" << split_ctx_.GetId() << ", " \
                << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << ", " \
                << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                 << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")";
    delete oper;
    return rocksdb::Status::OK();
}

// Replay RollbackSplit log
rocksdb::Status SharedResourceOperationLogRecordProvider::ReplayRollbackSplitLog(const LogRecord& record) {
    // Decode operation
    auto oper = SplitOperation::DecodeFrom(record);
    if (oper == nullptr) {
        index_err() << "[SRM]Fetal Err when decode StartSplit operation.";
        rocksdb::Status::Aborted();
    }

    if (split_ctx_.IsStateFault() && (oper->GetId() == split_ctx_.GetId())) {
        // Ignore the RollbackSplit written repeatedly.
        index_err() << "[SRM]Ignore the RollbackSplit written repeatedly. " \
                    << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                    << "id_:" << oper->GetId() << "," \
                    << "split_point:" << tojson(oper->GetDescription()->splitPointKey) << ", " \
                    << "key_range:(" << tojson(oper->GetDescription()->originalKeyLow) << "," \
                                     << tojson(oper->GetDescription()->originalKeyHigh) << ")";
        delete oper;
        return rocksdb::Status::OK();
    }
    else if (split_ctx_.IsStateNone()) {
        index_err() << "[SRM]RollbackSplit without related StartSplit. " \
                    << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                    << "id_:" << oper->GetId();
        delete oper;
        return rocksdb::Status::Aborted();
    }
    else if ((!split_ctx_.IsStateStart()) || (oper->GetId() != split_ctx_.GetId())) {
        index_err() << "[SRM]RollbackSplit without related StartSplit. " \
                    << "old(" \
                      << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                      << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                      << "id_:" << split_ctx_.GetId() << "," \
                      << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << "," \
                      << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                       << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")" \
                    << "), " \
                    << "new(" \
                      << "id_:" << oper->GetId() << "," \
                      << "split_point:" << tojson(oper->GetDescription()->splitPointKey) << "," \
                      << "key_range:(" << tojson(oper->GetDescription()->originalKeyLow) << "," \
                                       << tojson(oper->GetDescription()->originalKeyHigh) << ")" \
                    << ").";
        delete oper;
        return rocksdb::Status::Aborted();
    }

    index_log() << "[SRM]Replay RollbackSplit successfully. " \
                << "failed_oper:" << static_cast<int>(failed_split_end_oper_) << ", " \
                << "state_:" << static_cast<uint32_t>(split_ctx_.GetState()) << ", " \
                << "id_:" << split_ctx_.GetId() << ", " \
                << "split_point:" << tojson(split_ctx_.GetDescription()->splitPointKey) << ", " \
                << "key_range:(" << tojson(split_ctx_.GetDescription()->originalKeyLow) << "," \
                                 << tojson(split_ctx_.GetDescription()->originalKeyHigh) << ")";
    delete oper;
    return RollbackSplitOperations();
}

// Replay reference log
rocksdb::Status SharedResourceOperationLogRecordProvider::ReplayReferenceLog(const LogRecord& record) {
    // Decode operation
    log() << "SharedResourceOperationLogRecordProvider::ReplayReferenceLog";
    auto oper = SharedResourceReferenceOperation::DecodeFrom(record);
    if (oper == nullptr) {
        index_err() << "[SRM]Fetal Err when decode Reference operation.";
        rocksdb::Status::Aborted();
    }
    // Save LSN in operation.
    oper->SetLsn(record.GetLsn());
    // Append the operation into the operations vector in memory.
    operations_.push_back(reinterpret_cast<SharedResourceOperation*>(oper));
    log() << "SharedResourceOperationLogRecordProvider::ReplayReferenceLog(): status: " << (int)(split_ctx_.GetState());
    // Append shared resource
    if ((split_ctx_.IsStateStart()) && (oper->GetType() == SharedResourceOperationType::AddReference)) {
        SharedResourceId resource = oper->GetResource();
        index_log() << "[SRM]Replay AddReferenceLog. " \
                    << "[" << split_ctx_.GetSharedResources().size() << "] " \
                    << "resource:" << resource << ", " \
                    << "id_:" << split_ctx_.GetId() << ", " \
                    << "LSN:" << oper->GetLsn();
        split_ctx_.ReplaySharedResource(oper->GetResource());
        log() << "SharedResourceOperationLogRecordProvider::ReplayReferenceLog(): 2";
    }
    return rocksdb::Status::OK();
}

// Replay processed log
rocksdb::Status SharedResourceOperationLogRecordProvider::ReplayProcessedLog(const LogRecord& record) {
    // Parse the record to an operation.
    auto oper = ProcessedLSNOperation::DecodeFrom(record);
    if (oper == nullptr) {
        index_err() << "[SRM]Fetal Err when decode Processed operation.";
        rocksdb::Status::Aborted();
    }

    // Check the processed LSN to be replay.
    uint64_t processed_lsn = oper->GetProcessedLSN();
    if (processed_lsn == INVALID_LSN) {
        index_err() << "[SRM]Replay ProcessedLog: invalid processed lsn (" << processed_lsn << ").";
        delete oper;
        return rocksdb::Status::Aborted();
    }
    if (split_ctx_.IsStateStart() && (processed_lsn >= split_ctx_.GetLsn())) {
        index_err() << "[SRM]Replay ProcessedLog: the processed lsn confilcts with the split lsn. " \
                    << "processed_lsn(" << processed_lsn << ") >= split_lsn(" << split_ctx_.GetLsn() << ").";
        delete oper;
        return rocksdb::Status::Aborted();
    }
    if ((processed_lsn_ != INVALID_LSN) && (processed_lsn_ >= processed_lsn)) {
        index_warning() << "[SRM]Replay ProcessedLog: disorder or repetitive processed lsn " \
                    << "(old:" << processed_lsn_ << ", new:" << processed_lsn << ").";
        delete oper;
        return rocksdb::Status::OK();
    }
    index_log() << "[SRM]Replay ProcessedLog. " \
                << "processed_lsn:" << processed_lsn << ", " \
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
    const SharedResourceOperationType type = SplitOperation::GetOperTypeOfLogRecord(record);
    log() << "SharedResourceOperationLogRecordProvider::ReplayLogRecord() type: " << (int)type;
    switch (type)
    {
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
        index_err() << "[SRM]Invalid operation type (" << static_cast<int>(type) << ").";
        needMoreRecords = false;
        return rocksdb::Status::Aborted();
    }

    needMoreRecords = s.ok() ? true : false;
    return s;
}

// Write AddFilter and AddReference logs of all shared resources related to the split to right chunk.
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
    s = PrepareReferenceLog(log_records,
                            oper_type,
                            split_ctx_.GetSharedResources(),
                            split_ctx_.GetDescription()->GetParentChunkId(),
                            split_ctx_.GetDescription()->GetChildChunkId());
    if (!s.ok())
    {
        index_err() << "[SRM]Prepare reference log of right chunk failed.";
        return s;
    }

    // Prepare shared resource filter logs for right chunk.
    oper_type = SharedResourceOperationType::AddFilter;
    s = share_res_manager_->PrepareFilterLog(log_records, oper_type, split_ctx_.GetSharedResources());
    if (!s.ok())
    {
        index_err() << "[SRM]Prepare reference log of right chunk failed.";
        return s;
    }

    // Write log records in batch.
    s = right_provider->WriteRecords(log_records);
    if (!s.ok())
    {
        index_err() << "[SRM]Write log records of right chunk failed";
        return s;
    }

    return rocksdb::Status::OK();
    // !!! Comment: WriteSharedResourceLogToRight() don't ensure that the related memory information is
    //              constructed. So 'right_provider' mustn't be used after calling this method.
}


//
// Methods of SharedResourceMemLog
//
void SharedResourceMemLog::DumpSharedResourceRecord(std::ofstream& dump_stream) {
    SharedResourceOperationType type = GetType();
    switch(type) {
    case SharedResourceOperationType::AddFilter:
    case SharedResourceOperationType::AddReference:
    case SharedResourceOperationType::RemoveReference:
    case SharedResourceOperationType::GetUnprocessed:
        DumpResource(dump_stream);
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
    dump_stream << "[" << GetTimeStr() << "] " \
                << "lsn:" << GetLsn() << ", " \
                << "type:" << GetOperTypeName(GetType()) << ", " \
                << "resource:" << resource_ \
                << std::endl;
}
void SharedResourceMemLog::DumpRmvFilter(std::ofstream& dump_stream) {
    dump_stream << "[" << GetTimeStr() << "] " \
                << "lsn:" << GetLsn() << ", " \
                << "type:" << "RmvFilter&Ref" << ", " \
                << "resource:" << resource_ \
                << "shared:" << shared_flag_ << std::endl;
}
void SharedResourceMemLog::DumpSplit(std::ofstream& dump_stream) {
    dump_stream << "[" << GetTimeStr() << "] " \
                << "lsn:" << GetLsn() << ", " \
                << "type:" << GetOperTypeName(GetType()) << ", " \
                << "split_id:" << split_id_ << std::endl;
}
void SharedResourceMemLog::DumpProcessed(std::ofstream& dump_stream) {
    dump_stream << "[" << GetTimeStr() << "] " \
                << "lsn:" << GetLsn() << ", " \
                << "type:" << GetOperTypeName(GetType()) << ", " \
                << "processed_lsn:" << processed_lsn_ << std::endl;
}
void SharedResourceMemLog::DumpOthers(std::ofstream& dump_stream) {
    dump_stream << "[" << GetTimeStr() << "] " \
                << "lsn:" << GetLsn() << ", " \
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
    index_err() << "[SRM] Destory share resource module. (module=" << ptr_to_string(this) << ").";
}

int SharedResourceModule::RegisterSharedResource(std::vector<SharedResourceId>& resources) {
    rocksdb::Status s;
    stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
    if (!init_flag_) {
        index_err() << "[SRM]Uninitialized share resource module. (module=" << ptr_to_string(this) << ").";
        return RET_ERR;
    }
    s = log_provider_->RegisterSharedResources(resources);
    if (s.ok()) {
        MemLogRegisterSharedResources(resources);
    }
    return (s.ok() ? RET_OK : RET_ERR);
}

// if return fail, it means logRecordprovider is splitting, space gc of rocks db must retry to remove
// the plogs in resource_id_list
int SharedResourceModule::RemoveSharedResource(std::vector<SharedResourceRemoveDescription>& list) {
    rocksdb::Status s;
    stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
    if (!init_flag_) {
        index_err() << "[SRM]Uninitialized share resource module. (module=" << ptr_to_string(this) << ").";
        return RET_ERR;
    }
    s = filter_manager_->OnResourcesDeletion(list);
    if (s.ok()) {
        MemLogRemoveSharedResource(list);
    }
    return (s.ok() ? RET_OK : RET_ERR);
}

rocksdb::Status SharedResourceModule::Init(rocksdb::Env* env,
                              const std::string& db_path,
                              ChunkMetadata* chunk_meta_data)
{
    rocksdb::Status s = rocksdb::Status::OK();

    stdx::unique_lock<stdx::mutex> lock(rw_mutex_);

    if (init_flag_) {
        index_err() << "[SRM]Re-initialize shared resource module. (module=" << ptr_to_string(this) << ").";
        init_flag_ = false;
    }

    do {
        // Create store, provider, manager
        log_store_.reset(new TransLogRecordStore(env, db_path));
        log_provider_.reset(new SharedResourceOperationLogRecordProvider(chunk_meta_data));
        //filter_manager_.reset(new SharedResourceManager());
        filter_manager_.reset(new MaaSSharedResourceManager(db_path));

        // Reigist providers
        s = log_store_->RegisterProvider(*filter_manager_);
        if (!s.ok()) {
            index_err() << "[SRM]Fail to register the filter mananger. (module=" << ptr_to_string(this) << ").";
            break;
        }
        s = log_store_->RegisterProvider(*log_provider_);
        if (!s.ok()) {
            index_err() << "[SRM]Fail to register the log provider mananger. (module=" << ptr_to_string(this) << ").";
            break;
        }

        // Associate provider and manager with each other.
        log_provider_->SetSharedResourceManager(filter_manager_.get());
        filter_manager_->SetLogRecordProvider(log_provider_.get());

        // Init log storage. (Include of the process of replaying log)
        s = log_store_->Init();
        if (!s.ok()) {
            index_err() << "[SRM]Fail to initiate the log storage. (module=" << ptr_to_string(this) << ").";
            break;
        }

        // Init memory log vector.
        if (MemLogIsEnable()) {
            mem_logs_.init(8*1024);
            MemLogReplayLog();
        }

        init_flag_ = true;
    }while(0);

    if (s.ok()) {
        mongo::ChunkMetadata::KeyRange key_range = chunk_meta_data->GetKeyRange();
        index_log() << "[SRM]Succeed to initiate the log storage. (module=" << ptr_to_string(this) << "), " \
                    << "db_apth(" << db_path << "), " \
                    << "chunk(" << tojson(key_range.GetKeyLow()) << "," << tojson(key_range.GetKeyHigh()) << ").";
    }
    return s;
}

rocksdb::Status SharedResourceModule::StartSplit(std::shared_ptr<SplitDescription> description) {
    rocksdb::Status s;
    stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
    if (!init_flag_) {
        index_err() << "[SRM]Uninitialized share resource module. (module=" << ptr_to_string(this) << ").";
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
        index_err() << "[SRM]Uninitialized share resource module. (module=" << ptr_to_string(this) << ").";
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
        index_err() << "[SRM]Uninitialized share resource module. (module=" << ptr_to_string(this) << ").";
        return rocksdb::Status::Aborted();
    }
    s = log_provider_->CommitSplit();
    if (s.ok()) {
        MemLogCommitSplit();
    }
    return s;
}

rocksdb::Status SharedResourceModule::WriteSharedResourceLogToRight(SharedResourceModule& right_module) {
    rocksdb::Status s;
    stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
    if (!init_flag_) {
        index_err() << "[SRM]Uninitialized share resource module. (module=" << ptr_to_string(this) << ").";
        return rocksdb::Status::Aborted();
    }
    s = log_provider_->WriteSharedResourceLogToRight(right_module.GetLogProvider());
    return s;
}

void SharedResourceModule::GetUnprocessedOperations(
            uint32_t maxBatchMaxSize, std::vector<SharedResourceReferenceOperation>& unprocessedRecords) {
    int old_size = unprocessedRecords.size();
    stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
    if (!init_flag_) {
        index_err() << "[SRM]Uninitialized share resource module. (module=" << ptr_to_string(this) << ").";
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
        index_err() << "[SRM]Uninitialized share resource module. (module=" << ptr_to_string(this) << ").";
        return rocksdb::Status::Aborted();
    }
    s = log_provider_->SetLastProcessedOperationLSN(processed_lsn);
    if (s.ok()) {
        MemLogSetProcessed(processed_lsn);
    }
    return s;
}

// Record the history of RegisterSharedResources.
void SharedResourceModule::MemLogRegisterSharedResources(std::vector<SharedResourceId>& resources) {
    if (RunLogIsEnable()) {
        for (auto resource : resources) {
            index_log() << "[SRM]Register : (module=" << ptr_to_string(this) << ") " \
                << "resource:" <<  resource.toString();
        }
        if (resources.size() <= 0) {
            index_log() << "[SRM]Register : (module=" << ptr_to_string(this) << ") " \
                        << "no resource.";
        }
    }
    if (MemLogIsEnable()) {
        SharedResourceMemLog mem_log(SharedResourceOperationType::AddReference);
        for (auto resource : resources) {
            mem_log.SetResource(resource);
            mem_logs_.append(mem_log);
        }
    }
}
// Record the history of RemoveSharedResource.
void SharedResourceModule::MemLogRemoveSharedResource(std::vector<SharedResourceRemoveDescription>& list) {
    if (RunLogIsEnable()) {
        for (auto desc : list) {
            index_log() << "[SRM]RmvFilter : (module=" << ptr_to_string(this) << ") " \
                        << "resource:" << desc.id.toString() \
                        << "shared:" << std::to_string(desc.shared_flag) << ", " \
                        << "result:" << std::to_string(desc.remove_result);
        }
        if (list.size() <= 0) {
            index_log() << "[SRM]RmvFilter : (module=" << ptr_to_string(this) << ") " \
                        << "no resource.";
        }
    }
    if (MemLogIsEnable()) {
        SharedResourceMemLog mem_log(SharedResourceOperationType::RemoveFilter);
        for (auto desc : list) {
            if (desc.remove_result == RET_OK) {
                mem_log.SetResource(desc.id);
                mem_log.SetSharedFlag(desc.shared_flag);
                mem_logs_.append(mem_log);
            }
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
void SharedResourceModule::MemLogAddFilters(const std::vector<SharedResourceId>& resources) {
    if (RunLogIsEnable()) {
        for (auto resource : resources) {
            index_log() << "[SRM]AddFilter : (module=" << ptr_to_string(this) << ") " \
                << "resource:" << resource.toString();
        }
    }
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
    const std::vector<SharedResourceId>& resources = log_provider_->GetSplitContext()->GetSharedResources();
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
    if (RunLogIsEnable()) {
        auto reference_oper = unprocessedRecords.begin() + old_size;
        for (; reference_oper != unprocessedRecords.end(); reference_oper++) {
            SharedResourceId resource = reference_oper->GetResource();
            index_log() << "[SRM]GetUnprocessed : (module=" << ptr_to_string(this) << ") " \
                << "lsn:" << reference_oper->GetLsn() << ", " \
                << "resource:" << resource.toString()<< ", " \
                << "parent:" << reference_oper->GetParentChunkId() << ", " \
                << "child:" << reference_oper->GetChildChunkId();
        }
    }
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
        reference_oper = reinterpret_cast<const SharedResourceReferenceOperation*>(oper);
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
    if (RunLogIsEnable()) {
        for (auto oper = operations.begin(); oper != operations.end(); oper++) {
            RunLogOperation(*oper, "ReplayLog");
        }
    }
    if (MemLogIsEnable()) {
        for (auto oper = operations.begin(); oper != operations.end(); oper++) {
            MemLogOperation(*oper);
        }
    }
}
// Record shared resources filters managed in memory.
void SharedResourceModule::MemLogFilters(const std::vector<SharedResourceId>& resources) {
    if (RunLogIsEnable()) {
        for (auto resource : resources) {
            index_log() << "[SRM]Memory Filter: (module=" << ptr_to_string(this) << ") " \
                << "resource:" << resource.toString();
        }
    }
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

// Dump all history records into appointed file.
void SharedResourceModule::DumpSharedResourceList(const char* history_path) {
    std::ofstream history_out;
    stdx::unique_lock<stdx::mutex> lock(rw_mutex_);
    history_out.open(history_path, std::ios::app);
    if (!history_out)
    {
        return;
    }
    std::string current_time = GetCurrentTimeStr();
    history_out << "[DumpSharedResourceList] Begin at " << current_time << std::endl;
    if (MemLogIsEnable()) {
        for (auto mem_log = mem_logs_.begin(); mem_log != mem_logs_.end(); mem_logs_.inc(mem_log)) {
            mem_log->DumpSharedResourceRecord(history_out);
        }
    }else {
        history_out << "The history log function is disable." << std::endl;
    }
    current_time = GetCurrentTimeStr();
    history_out << "[DumpSharedResourceList] End at   " << current_time << std::endl;
    history_out.close();
}


}   //  namespace TransactionLog

}   //  namespace mongo



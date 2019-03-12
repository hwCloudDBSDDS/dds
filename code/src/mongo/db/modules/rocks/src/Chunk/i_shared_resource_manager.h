
#pragma once

#include <mongo/platform/basic.h>
#include <mongo/bson/bsonobj.h>
#include <mongo/bson/json.h>
#include <rocksdb/shared_resource.h>

#include "i_log_record_store.h"
#include "ChunkMetadata.h"
#include "TransLogRecordStore.h"


namespace mongo
{

namespace TransactionLog
{

class SharedResourceFilterOperation;
class SharedResourceReferenceOperation;
class SplitOperation;
class ProcessedLSNOperation;
class SharedResourceOperation;
class SharedResourceManager;
class TransLogRecordStore;

#ifdef MAAS

#else
// Be used for binary search in a forward range.
static inline bool CompareResourceId(const SharedResourceId& res1, const SharedResourceId& res2) {
    const uint8_t* buf1 = reinterpret_cast<const uint8_t*>(&res1);
    const uint8_t* buf2 = reinterpret_cast<const uint8_t*>(&res2);
    for (uint32_t i = 0; i < sizeof(SharedResourceId); i++) {
        if (buf1[i] < buf2[i]) {
            return true;
        }else if (buf1[i] > buf2[i]) {
            return false;
        }
    }
    return false;
}
// Be used for checking if the two SharedResourceIds are equal.
static inline bool EqualResourceId(const SharedResourceId& res1, const SharedResourceId& res2) {
    const uint8_t* buf1 = reinterpret_cast<const uint8_t*>(&res1);
    const uint8_t* buf2 = reinterpret_cast<const uint8_t*>(&res2);
    for (uint32_t i = 0; i < sizeof(SharedResourceId); i++) {
        if (buf1[i] != buf2[i]) {
            return false;
        }
    }
    return true;
}
#endif

// Get the encode length of a string.
static inline uint32_t GetStringEncodeLength(const std::string& str) {
    return (str.length() + 1);
}
// Encode a string into the buffer
static inline uint32_t EncodeStringToBuf(const std::string& str, char* buf) {
    sprintf(buf, "%s", str.c_str());
    return GetStringEncodeLength(str);
}
// Decode a string from the buffer
static inline uint32_t DecodeStringFromBuf(std::string& str, const char* buf) {
    str.assign(buf);
    return GetStringEncodeLength(str);
}

// Get the encode length of a string.
static inline uint32_t GetUint64EncodeLength(const uint64_t& value) {
    return sizeof(value);
}
// Encode a uint64_t into the buffer
static inline uint32_t EncodeUint64ToBuf(const uint64_t& value, char* buf) {
    memcpy(buf, reinterpret_cast<const char*>(&value), sizeof(value));
    return GetUint64EncodeLength(value);
}
// Decode a uint64_t from the buffer
static inline uint32_t DecodeUint64FromBuf(uint64_t& value, const char* buf) {
    memcpy(reinterpret_cast<char*>(&value), buf, sizeof(value));
    return GetUint64EncodeLength(value);
}

#define INVALID_CHUNK_ID    (std::numeric_limits<uint64_t>::max())
// Get the encode length of a chunk id.
static inline uint32_t GetChunkIdEncodeLength(const uint64_t& chunk_id) {
    return GetUint64EncodeLength(chunk_id);
}
// Encode a chunk id into the buffer
static inline uint32_t EncodeChunkIdToBuf(const uint64_t& chunk_id, char* buf) {
    return EncodeUint64ToBuf(chunk_id, buf);
}
// Decode a chunk id from the buffer
static inline uint32_t DecodeChunkIdFromBuf(uint64_t& chunk_id, const char* buf) {
    return DecodeUint64FromBuf(chunk_id, buf);
}

// Get the encode length of a LSN.
static inline uint32_t GetLsnEncodeLength(const uint64_t& lsn) {
    return GetUint64EncodeLength(lsn);
}
// Encode a LSN into the buffer
static inline uint32_t EncodeLsnToBuf(const uint64_t& lsn, char* buf) {
    return EncodeUint64ToBuf(lsn, buf);
}
// Decode a LSN from the buffer
static inline uint32_t DecodeLsnFromBuf(uint64_t& lsn, const char* buf) {
    return DecodeUint64FromBuf(lsn, buf);
}

// Get the encode length of a shared resource id.
static inline uint32_t GetSharedResourceIdEncodeLength(const SharedResourceId& resource) {
    return sizeof(resource);
}
// Encode a shared resource id into the buffer
static inline uint32_t EncodeSharedResourceIdToBuf(const SharedResourceId& resource, char* buf) {
    memcpy(buf, reinterpret_cast<const char*>(&resource), sizeof(resource));
    return sizeof(resource);
}
// Decode a shared resource id from the buffer
static inline uint32_t DecodeSharedResourceIdFromBuf(SharedResourceId& resource, const char* buf) {
    memcpy(reinterpret_cast<char*>(&resource), buf, sizeof(resource));
    return sizeof(resource);
}

// Get the encode length of a split id.
static inline uint32_t GetSplitIdEncodeLength(const uint64_t& split_id) {
    return GetUint64EncodeLength(split_id);
}
// Encode a split id into the buffer
static inline uint32_t EncodeSplitIdToBuf(const uint64_t& split_id, char* buf) {
    return EncodeUint64ToBuf(split_id, buf);
}
// Decode a split id from the buffer
static inline uint32_t DecodeSplitIdFromBuf(uint64_t& split_id, const char* buf) {
    return DecodeUint64FromBuf(split_id, buf);
}

// Get the string of the current time.
static inline std::string GetCurrentTimeStr() {
    time_t tm_;
    time(&tm_);
    struct timeval tv;
    gettimeofday(&tv, NULL);
    char buffer[32];
    strftime(buffer, 20, "%Y-%m-%d %H:%M:%S", std::localtime(&tm_));
    sprintf(buffer+19, ".%06lu", tv.tv_usec);
    return buffer;
}


// TLV format for encoding and decoding the field of SharedResourceOperation.
struct SharedResourceOperationFieldTlv{
    uint32  type;       // field type
    uint32  length;     // the total length of TLV, include of <type>, <length> and <value>.
    char    value[0];
};


// Operation types for the log with the type LogRecordType::LoadBalancingTransactionLog
enum class SharedResourceOperationType
{
    None = 0,
    AddReference,
    RemoveReference,
    StartSplit,
    CommitSplit,
    RollbackSplit,
    Processed,
    AddFilter,
    RemoveFilter,
    // The below values are only for debug.
    GetUnprocessed,
    ReplayLog
};
static inline std::string GetOperTypeName(const SharedResourceOperationType type) {
    switch(type) {
    case SharedResourceOperationType::None:
        return "None";
    case SharedResourceOperationType::AddReference:
        return "AddRef";
    case SharedResourceOperationType::RemoveReference:
        return "RmvRef";
    case SharedResourceOperationType::StartSplit:
        return "StartSplit";
    case SharedResourceOperationType::CommitSplit:
        return "CommitSplit";
    case SharedResourceOperationType::RollbackSplit:
        return "RollbackSplit";
    case SharedResourceOperationType::Processed:
        return "Processed";
    case SharedResourceOperationType::AddFilter:
        return "AddFilter";
    case SharedResourceOperationType::RemoveFilter:
        return "RmvFilter";
    case SharedResourceOperationType::GetUnprocessed:
        return "GetUnprocessed";
    case SharedResourceOperationType::ReplayLog:
        return "ReplayLog";
    default:
        return "Unknown";
    }
}

static inline bool IsSplitOper(SharedResourceOperationType type) {
    return ((SharedResourceOperationType::StartSplit == type)
            || (SharedResourceOperationType::CommitSplit == type)
            || (SharedResourceOperationType::RollbackSplit == type));
}
static inline bool IsSplitEnd(SharedResourceOperationType type) {
    return ((SharedResourceOperationType::CommitSplit == type)
            || (SharedResourceOperationType::RollbackSplit == type));
}

static inline bool IsReferenceOper(SharedResourceOperationType type) {
    return ((SharedResourceOperationType::AddReference == type)
            || (SharedResourceOperationType::RemoveReference == type));
}

// Get the encode length of a shared resource operation type.
static inline uint32_t GetSharedResourceOperationTypeEncodeLength(const SharedResourceOperationType& type) {
    return sizeof(type);
}
// Encode a shared resource operation type into the buffer
static inline uint32_t EncodeSharedResourceOperationTypeToBuf(const SharedResourceOperationType& type, char* buf) {
    memcpy(buf, reinterpret_cast<const char*>(&type), sizeof(type));
    return GetSharedResourceOperationTypeEncodeLength(type);
}
// Decode a shared resource operation type from the buffer
static inline uint32_t DecodeSharedResourceOperationTypeFromBuf(SharedResourceOperationType& type, const char* buf) {
    memcpy(reinterpret_cast<char*>(&type), buf, sizeof(type));
    return GetSharedResourceOperationTypeEncodeLength(type);
}

class SharedResourceOperation
{
public:
    SharedResourceOperation() {}
    SharedResourceOperation(uint64_t lsn) : lsn_(lsn) {}
    virtual ~SharedResourceOperation() {}

    uint32_t EncodeTo(char*& buf);
    virtual uint32_t GetEncodeLength() const;
    virtual uint32_t EncodeToBuf(char* buf);
    uint32_t DecodeBaseFrom(const LogRecord& record);

    void SetLsn(const uint64_t lsn) { lsn_ = lsn; }
    const uint64_t GetLsn() const { return lsn_; }

    void SetType(const SharedResourceOperationType type) { type_ = type; }
    const SharedResourceOperationType GetType() const { return type_; }

private:
    uint64_t lsn_ = INVALID_LSN;
    SharedResourceOperationType type_ = SharedResourceOperationType::None;
};

// Comapre two SharedResourceOperation by LSN field.
static inline bool CompareLSN(const SharedResourceOperation* oper1, const SharedResourceOperation* oper2) {
    return (oper1->GetLsn() < oper2->GetLsn());
}

class SharedResourceFilterOperation : public SharedResourceOperation
{
public:
    SharedResourceFilterOperation() {}
    SharedResourceFilterOperation(const SharedResourceOperationType oper_type,
                                  const SharedResourceId& resource) {
        SetLsn(INVALID_LSN);
        SetType(oper_type);
        SetResource(resource);
    }

    uint32_t GetEncodeLength() const override;
    uint32_t EncodeToBuf(char* buf) override;
    static SharedResourceFilterOperation* DecodeFrom(const LogRecord& record);

    void SetResource(const SharedResourceId& resource) { resource_ = resource; }
    const SharedResourceId& GetResource() const { return resource_; }

    ~SharedResourceFilterOperation() {}

    SharedResourceFilterOperation& operator = (const SharedResourceFilterOperation &ref_oper) {
        SetLsn(ref_oper.GetLsn());
        SetType(ref_oper.GetType());
        SetResource(ref_oper.GetResource());
        return *this;
    }

    bool operator == (const SharedResourceFilterOperation& rhs) {
        return ((GetLsn() == rhs.GetLsn()) &&
                (GetType() == rhs.GetType()) &&
                EqualResourceId(GetResource(), rhs.GetResource()));
    }

private:
    SharedResourceId resource_;

};

class SharedResourceReferenceOperation : public SharedResourceOperation
{
public:
    SharedResourceReferenceOperation() {}
    SharedResourceReferenceOperation(const SharedResourceOperationType oper_type,
                                     const SharedResourceId& resource,
                                     const uint64_t& parent_chunk_id,
                                     const uint64_t& child_chunk_id) {
        SetLsn(INVALID_LSN);
        SetType(oper_type);
        SetResource(resource);
        SetParentChunkId(parent_chunk_id);
        SetChildChunkId(child_chunk_id);
    }

    uint32_t GetEncodeLength() const override;
    uint32_t EncodeToBuf(char* buf) override;
    static SharedResourceReferenceOperation* DecodeFrom(const LogRecord& record);

    void SetResource(const SharedResourceId& resource) { resource_ = resource; }
    const SharedResourceId& GetResource() const { return resource_; }

    void SetParentChunkId(const uint64_t& parent_chunk_id) { parent_chunk_id_ = parent_chunk_id; }
    const uint64_t& GetParentChunkId() const { return parent_chunk_id_; }

    void SetChildChunkId(const uint64_t& child_chunk_id) { child_chunk_id_ = child_chunk_id; }
    const uint64_t& GetChildChunkId() const { return child_chunk_id_; }

    SharedResourceReferenceOperation& operator = (const SharedResourceReferenceOperation &ref_oper) {
        SetLsn(ref_oper.GetLsn());
        SetType(ref_oper.GetType());
        SetResource(ref_oper.GetResource());
        SetParentChunkId(ref_oper.GetParentChunkId());
        SetChildChunkId(ref_oper.GetChildChunkId());
        return *this;
    }
    bool operator == (const SharedResourceReferenceOperation& rhs) {
        return ((GetLsn() == rhs.GetLsn()) &&
                (GetType() == rhs.GetType()) &&
                EqualResourceId(GetResource(), rhs.GetResource()) &&
                (GetParentChunkId() == rhs.GetParentChunkId()) &&
                (GetChildChunkId() == rhs.GetChildChunkId()));
    }

//private:
    SharedResourceId resource_;
    uint64_t parent_chunk_id_;
    uint64_t child_chunk_id_;

};

class ProcessedLSNOperation : public SharedResourceOperation
{
public:
    ProcessedLSNOperation() { SetType(SharedResourceOperationType::Processed); }
    ProcessedLSNOperation(const uint64_t processedLSN) {
        SetType(SharedResourceOperationType::Processed);
        SetProcessedLSN(processedLSN);
    }

    uint32_t GetEncodeLength() const override;
    uint32_t EncodeToBuf(char* buf) override;
    static ProcessedLSNOperation* DecodeFrom(const LogRecord& record);

    void SetProcessedLSN(uint64_t lsn) { processed_lsn_ = lsn; }
    uint64_t GetProcessedLSN() const { return processed_lsn_; }

    ProcessedLSNOperation& operator = (const ProcessedLSNOperation &ref_oper) {
        SetLsn(ref_oper.GetLsn());
        SetType(ref_oper.GetType());
        SetProcessedLSN(ref_oper.GetProcessedLSN());
        return *this;
    }
    bool operator == (const ProcessedLSNOperation& rhs) {
        return ((GetLsn() == rhs.GetLsn()) &&
                (GetType() == rhs.GetType()) &&
                (GetProcessedLSN() == rhs.GetProcessedLSN()));
    }

private:
    uint64_t processed_lsn_;

};

#define KEY_FIELD_NAME  "Key"
#define KEY_BSON(v)     BSON(KEY_FIELD_NAME << (v))
class SplitDescription
{
public:
    uint64_t parentChunkId = INVALID_CHUNK_ID;
    uint64_t childChunkId = INVALID_CHUNK_ID;
    BSONObj originalKeyLow = KEY_BSON("");
    BSONObj originalKeyHigh = KEY_BSON("");
    BSONObj splitPointKey = KEY_BSON("");
    std::string rightDbPathWithRootPlog = "";

public:
    SplitDescription() {}
    SplitDescription(uint64_t parent_chunk_id,
                     uint64_t child_chunk_id,
                     const BSONObj& key_low,
                     const BSONObj& key_high,
                     const BSONObj& split_point,
                     const std::string& right_db_path) {
        parentChunkId = parent_chunk_id;
        childChunkId = child_chunk_id;
        originalKeyLow = key_low;
        originalKeyHigh = key_high;
        splitPointKey = split_point;
        rightDbPathWithRootPlog = right_db_path;
    }

    SplitDescription(const uint64_t& parent_chunk_id,
                     const uint64_t& child_chunk_id,
                     const std::string& key_low,
                     const std::string& key_high,
                     const std::string& split_point,
                     const std::string& right_db_path) {
        parentChunkId = parent_chunk_id;
        childChunkId = child_chunk_id;
        originalKeyLow = KEY_BSON(key_low);
        originalKeyHigh = KEY_BSON(key_high);
        splitPointKey = KEY_BSON(split_point);
        rightDbPathWithRootPlog = right_db_path;
    }

    const uint64_t& GetParentChunkId() { return parentChunkId; }
    const uint64_t& GetChildChunkId() { return childChunkId; }

    mongo::ChunkMetadata::KeyRange GetKeyRange() const {
        return mongo::ChunkMetadata::KeyRange(originalKeyLow, originalKeyHigh);
    }
    mongo::ChunkMetadata::KeyRange GetLeftKeyRange() const {
        return mongo::ChunkMetadata::KeyRange(originalKeyLow, splitPointKey);
    }

    uint32_t GetEncodeLength() const;
    uint32_t EncodeToBuf(char* buf);
    uint32_t DecodeFromBuf(const char* buf);

    // Encode a bson object into the buffer
    static inline uint32_t EncodeBsonToBuf(const BSONObj& obj, char* buf) {
        return EncodeStringToBuf(tojson(obj), buf);
    }
    // Decode a bson object from the buffer
    static inline uint32_t DecodeBsonFromBuf(BSONObj& obj, const char* buf) {
        std::string str;
        uint32_t len = DecodeStringFromBuf(str, buf);
        obj = fromjson(str);
        return len;
    }
    // Get the encode length of a bson object.
    static inline uint32_t GetBsonEncodeLength(const BSONObj& obj) {
        return (tojson(obj).length() + 1);
    }

    bool operator == (const SplitDescription& rhs) {
        return ((tojson(originalKeyLow) == tojson(rhs.originalKeyLow)) &&
                (tojson(originalKeyHigh) == tojson(rhs.originalKeyHigh)) &&
                (tojson(splitPointKey) == tojson(rhs.splitPointKey)) &&
                (rightDbPathWithRootPlog == rhs.rightDbPathWithRootPlog) &&
                (parentChunkId == rhs.parentChunkId) &&
                (childChunkId == rhs.childChunkId));
    }

};

#define INVALID_SPLIT_ID    INVALID_LSN
class SplitOperation : public SharedResourceOperation
{
public:
    SplitOperation() { }
    SplitOperation(const SharedResourceOperationType oper_type,
                   const uint64_t split_id,
                   std::shared_ptr<SplitDescription> description) {
        SetLsn(INVALID_LSN);
        SetType(oper_type);
        SetId(split_id);
        SetDescription(description);
    }
    SplitOperation(const uint64_t lsn,
                   const SharedResourceOperationType oper_type,
                   const uint64_t split_id,
                   std::shared_ptr<SplitDescription> description) {
        SetLsn(lsn);
        SetType(oper_type);
        SetId(split_id);
        SetDescription(description);
    }

    uint32_t GetEncodeLength() const override;
    uint32_t EncodeToBuf(char* buf) override;
    static SplitOperation* DecodeFrom(const LogRecord& record);

    void SetDescription(std::shared_ptr<SplitDescription> description) { description_ = description; }
    std::shared_ptr<SplitDescription> GetDescription() const { return description_; }

    void SetId(const uint64_t id) { id_ = id; }
    const uint64_t GetId() const { return id_; }

    SplitOperation& operator = (const SplitOperation &ref_oper) {
        SetLsn(ref_oper.GetLsn());
        SetType(ref_oper.GetType());
        SetId(ref_oper.GetId());
        SetDescription(ref_oper.GetDescription());
        return *this;
    }
    bool operator == (const SplitOperation& rhs) {
        return ((GetLsn() == rhs.GetLsn()) &&
                (GetType() == rhs.GetType()) &&
                (GetId() == rhs.GetId()) &&
                ((*description_) == (*rhs.description_)));
    }

    static const SharedResourceOperationType GetOperTypeOfLogRecord(const LogRecord& record);

private:
    uint64_t id_ = INVALID_SPLIT_ID;
    std::shared_ptr<SplitDescription> description_ = nullptr;

};

typedef std::vector<SharedResourceId> SharedResourceIds;

enum class SplitState
{
    SplitState_None = 0,
    SplitState_Start,
    SplitState_Success,
    SplitState_Fault
};

class SharedResourceOperationLogRecordProvider;
class SplitContext
{
private:
    // The split id just is the LSN of the log of StartSplit and is written in the log
    // of CommitSplit or RollbackSplit. And now its sole purpose is to make it efficiently
    // to match CommitSplit or RollbakSplit with SplitStart.
    uint64_t id_ = INVALID_SPLIT_ID;
    SplitState state_ = SplitState::SplitState_None;

    std::shared_ptr<SplitDescription> description_ = nullptr;
    //std::shared_ptr<SharedResourceOperationLogRecordProvider> record_provider_;
    SharedResourceIds shared_resources_;

public:
    // Set and get the the split ID which just is the LSN of StartSplit.
    void SetId(const uint64_t id) { id_ = id; }
    const uint64_t GetId() const { return id_; }
    // Set and get the the LSN of StartSplit.
    void SetLsn(const uint64_t id) { id_ = id; }
    const uint64_t GetLsn() const { return id_; }

    void Copy(const SplitContext* split_ctx) {
        SetId(split_ctx->GetId());
        SetDescription(split_ctx->GetDescription());
        shared_resources_.clear();
        shared_resources_.insert(shared_resources_.end(),
                                 split_ctx->shared_resources_.begin(),
                                 split_ctx->shared_resources_.end());
    }
    void SetDescription(const std::shared_ptr<SplitDescription> description);
    const std::shared_ptr<SplitDescription> GetDescription() const
    {
        return description_;
    }

    // Save the shared resources into the split context for SharedResourceManager.
    void SortSharedResources() {
        std::sort(shared_resources_.begin(), shared_resources_.end(), CompareResourceId);
    }
    void RegisterSharedResources(const SharedResourceIds& resources) {
        // Save the shared resources.
        shared_resources_.clear();
        shared_resources_.insert(shared_resources_.end(), resources.begin(), resources.end());
        // Sort the shared resources.
        SortSharedResources();
    }

    // Add shared resources in batch.
    void ReplaySharedResource(const SharedResourceId& resource)
    {
        // Append the shared resource.
        shared_resources_.insert(shared_resources_.end(), resource);
    }

    rocksdb::Status Finalize(SharedResourceOperationLogRecordProvider* newChunkProvider) {
        //Todo: what is the purpose of this function?
        return rocksdb::Status::OK();
    }

    const SharedResourceIds& GetSharedResources() const { return shared_resources_; }
    void ClearSharedResources() { shared_resources_.clear(); }

    void SetStateToNone() { state_ = SplitState::SplitState_None; }
    void SetStateToStart() { state_ = SplitState::SplitState_Start; }
    void SetStateToSuccess() { state_ = SplitState::SplitState_Success; }
    void SetStateToFault() { state_ = SplitState::SplitState_Fault; }
    const bool IsStateNone() const { return (state_ == SplitState::SplitState_None); }
    const bool IsStateStart() const { return (state_ == SplitState::SplitState_Start); }
    const bool IsStateSuccess() const { return (state_ == SplitState::SplitState_Success); }
    const bool IsStateFault() const { return (state_ == SplitState::SplitState_Fault); }
    const SplitState GetState() const { return state_; }
};

typedef std::vector<LogRecord>::iterator LogRecordIterator;
class LogRecordVector
{
private:
    std::vector<LogRecord> log_records_;

public:
    ~LogRecordVector() { clear(); }

    // Get log records vector.
    std::vector<LogRecord>& GetLogRecords() { return log_records_; }

    // Release all the buffer bonded with all the records, then clear this vector.
    void clear() {
        for (auto record = log_records_.begin(); record != log_records_.end(); record++) {
            if (!record->GetSlice().empty()) {
                delete record->GetData();
                rocksdb::Slice& slice = const_cast<rocksdb::Slice&>(record->GetSlice());
                slice.clear();
            }
        }
        log_records_.clear();
    }

    const size_t size() const { return log_records_.size(); }
    const LogRecordIterator begin() { return log_records_.begin(); }
    const LogRecordIterator end() { return log_records_.end(); }
    bool empty() const { return log_records_.empty(); }

    // Append a log record to this vector.
    void push_back(const LogRecord record) {
        log_records_.push_back(record);
    }

    LogRecord front() { return log_records_.front(); }
};


class LogRecordProvider : public ILogRecordProvider
{
public:
    void SetLogWriter(ILogRecordWriter* writer) { writer_ = writer; }
    ILogRecordWriter* GetLogWriter() { return writer_; }
private:
    ILogRecordWriter* writer_ = nullptr;
};

enum class SharedResourceDebugMode
{
    None = 0,
    RunLog,
    MemLog,
    Both
};

//
//  Shared resource manager: has the functionality to check that resource is shared
//  For that MaaS can save the highest SN of child SST file of last split.
//  Index Layer can save list of PLogs
//
class SharedResourceManager : public LogRecordProvider
{
private:
    // The provider for writing shared resources log.
    SharedResourceOperationLogRecordProvider* share_res_provider_;

    std::vector<SharedResourceId> shared_resources_;    // Shared resources vector that is always sorted.
    std::vector<SharedResourceId> shared_resources_rmv_;// Temporary vector for saving the shared resources to be
                                                        // removed during the process of replaying log records.
    // Manage shared resources in memory.
    void ApplyTemporarySharedResources();
    void RemoveSharedResources_Method1(SharedResourceIds& resources_rmv);
    void RemoveSharedResources_Method2(SharedResourceIds& resources_rmv);
    void RemoveSharedResources(SharedResourceIds& resources_rmv);

    // Prepare shared resources filter log.
    rocksdb::Status PrepareFilterLog(LogRecordVector& log_records,
                                     const SharedResourceOperationType oper_type,
                                     const SharedResourceId& resource);

    // Check whether the appoint resource is shared.
    bool IsResourceShared(const SharedResourceId resource) {
        // Because the vector is sorted, binary search is supported.
        auto it = std::lower_bound(shared_resources_.begin(), shared_resources_.end(), resource, CompareResourceId);
        return ((it != shared_resources_.end()) && EqualResourceId(resource, (*it)));
    }

    void SetSharedResourcesRemoveResultToFail(std::vector<SharedResourceRemoveDescription>& list);
    rocksdb::Status OnResourceDeletion(LogRecordVector& log_records,
                                       std::vector<SharedResourceId>& resources,
                                       std::vector<SharedResourceRemoveDescription>::iterator& resource_it);

public:
    virtual rocksdb::Status OnResourcesDeletion(std::vector<SharedResourceRemoveDescription>& list);

    // Add shared resources into filter list.
    virtual void AddSharedResources(const SharedResourceIds& resources);

    // Prepare shared resources filter logs.
    virtual rocksdb::Status PrepareFilterLog(LogRecordVector& log_records,
                                     const SharedResourceOperationType oper_type,
                                     const std::vector<SharedResourceId>& resources);

    void SetLogRecordProvider(SharedResourceOperationLogRecordProvider* provider) {
        share_res_provider_ = provider;
    }

    //
    //  ILogRecordProvider implementation
    //
    rocksdb::Status InitializationBegin(LogReplayOrder replayOrder, std::vector<LogRecordType>& supportedRecordTypes) override
    {
        supportedRecordTypes.push_back(LogRecordType::SharedResourceFilterLog);
        return rocksdb::Status::OK();
    }
    virtual rocksdb::Status ReplayLogRecord(const LogRecord& record, bool& needMoreRecords);
    virtual rocksdb::Status InitializationEnd(ILogRecordWriter& logWriter) override;
    rocksdb::Status RewriteImportantFailedLog() override { return rocksdb::Status::OK(); }

    //
    // Write Checkpoint
    //
    void CheckpointBegin() override { }
    virtual rocksdb::Status WriteCheckpoint() override;
    void CheckpointEnd() override { }

    // It simply returns fixed value because there is only one instance now.
    std::string GetName() override { return "SharedResourceManager_000"; }

    // Clear filters in memory. This method is only used for the test of replaying log.
    void Reset() {
        shared_resources_.clear();
    }

    const std::vector<SharedResourceId>& GetSharedResources() { return shared_resources_; }
    const std::vector<SharedResourceId>& GetSharedResourcesRmv() { return shared_resources_rmv_; }

    virtual bool CheckSharedResource(const SharedResourceId &id, std::string &path){return false;}
    virtual bool getSharedResourcePath(const SharedResourceId &id, std::string &path) {return false;}

};

typedef std::vector<SharedResourceOperation*>::iterator SharedResourceOperationIterator;
class SharedResourceOperationVector
{
private:
    std::vector<SharedResourceOperation*> operations_;

    // Generate a copy of the input operation instance.
    SharedResourceOperation* NewOperation(const SharedResourceOperation& oper) const {
        switch (oper.GetType()) {
        case SharedResourceOperationType::AddReference:
        case SharedResourceOperationType::RemoveReference:
            {
            const SharedResourceReferenceOperation* ref_oper
                = reinterpret_cast<const SharedResourceReferenceOperation*>(&oper);
            SharedResourceReferenceOperation* new_oper
                = new SharedResourceReferenceOperation(*ref_oper);
            return (reinterpret_cast<SharedResourceOperation*>(new_oper));
            }
        case SharedResourceOperationType::StartSplit:   // Now only StartSplit operation is possiblely recorded in memory.
        case SharedResourceOperationType::CommitSplit:
        case SharedResourceOperationType::RollbackSplit:
            {
            const SplitOperation* ref_oper = reinterpret_cast<const SplitOperation*>(&oper);
            SplitOperation* new_oper = new SplitOperation(*ref_oper);
            return (reinterpret_cast<SharedResourceOperation*>(new_oper));
            }
        case SharedResourceOperationType::Processed:    // Now Processed operation always will not be recorded in memory.
            {
            const ProcessedLSNOperation* ref_oper = reinterpret_cast<const ProcessedLSNOperation*>(&oper);
            ProcessedLSNOperation* new_oper = new ProcessedLSNOperation(*ref_oper);
            return (reinterpret_cast<SharedResourceOperation*>(new_oper));
            }
        default:
            assert(false);
            return nullptr;
        }
        return nullptr;
    }

public:
    ~SharedResourceOperationVector() { clear(); }

    void reset() { operations_.clear(); }

    void clear() {
        for (auto oper : operations_) {
            delete oper;
        }
        operations_.clear();
    }

    const size_t size() const { return operations_.size(); }
    const SharedResourceOperationIterator begin() { return operations_.begin(); }
    const SharedResourceOperationIterator end() { return operations_.end(); }

    // Append the input operation instance to the tail of this vector.
    void push_back(SharedResourceOperation* oper) { operations_.push_back(oper); }
    // Generate a copy of the input operation instance and append the new one to the tail of this vector.
    void push_back(const SharedResourceOperation& ref_oper) {
        SharedResourceOperation* oper = NewOperation(ref_oper);
        operations_.push_back(oper);
    }
    // Append all the operation instances of the input vector to the tail of this vector, and reset the
    // input vector to avoid repeated release of these operation instances.
    void push_back(SharedResourceOperationVector& vec) {
        operations_.insert(operations_.end(), vec.begin(), vec.end());
        vec.reset(); // !!! After that, the input vector has been reset.
    }

    // Insert all the operations within the appoint range in another vector. just insert the pointers to the
    // instances but copies of the instances.
    void insert(SharedResourceOperationIterator first, SharedResourceOperationIterator last) {
        operations_.insert(operations_.end(), first, last);
    }

    // Erase the operation at the appointed position.
    void erase(SharedResourceOperationIterator first) { operations_.erase(first); };
    // Erase all the operations within the appoint range.
    void erase(SharedResourceOperationIterator first, SharedResourceOperationIterator last) {
        operations_.erase(first, last);
    }

    // Find the operation with the appointed LSN.
    const SharedResourceOperationIterator find(const uint64_t lsn) {
        SharedResourceOperation ref_oper(lsn);
        auto oper_it = std::lower_bound(operations_.begin(), operations_.end(), &ref_oper, CompareLSN);
        if ((oper_it != operations_.end()) && ((*oper_it)->GetLsn() == lsn)) {
            return oper_it;
        }
        return operations_.end();
    }

    // Remove all the operations with LSN not-greater than the processed LSN.
    void RemoveProcessedOperations(const uint64_t processedLSN) {
        auto oper_it = operations_.begin();
        for (; oper_it != operations_.end(); oper_it++) {
            if ((*oper_it)->GetLsn() > processedLSN) {
                // Break if meet the first unprocessed operation.
                break;
            }
            // Release the processed operation.
            delete (*oper_it);
            *oper_it = nullptr;
        }
        // Remove all the pointers to the processed operations.
        operations_.erase(operations_.begin(), oper_it);
    }

};

//
//  Provider which is responsible for
//
class SharedResourceOperationLogRecordProvider : public LogRecordProvider
{
private:
    // The key range of current chunk.
    mongo::ChunkMetadata::KeyRange key_range_;
    // A context for saving the information of the ongoing or latest completed split.
    SplitContext split_ctx_;
    // A pointer to shared resources manager.
    SharedResourceManager* share_res_manager_;
    // A vector for saving all the unprocessed share resource operations.
    SharedResourceOperationVector operations_;

    // If 'is_splitting_' is true, it means that StartSplit is logged but neither RollbackSplit
    // nor CommitSplit is logged. That is, the split has started but not ended.
    bool is_splitting_ = false; // check SplitStart and SplitCommit error

    // If fail to write the log of the operation of CommitSplit or RollbackSplit, we will record
    // the operation type as the variable 'failed_split_end_oper_' in memory. After that we can
    // sitll treat it as a success. And when we receive any new log request later, the LogStore
    // will rewrite the split end log indicated by 'failed_split_end_oper_' at first by calling
    // the function RewriteImportantFailedLog() before write the new one.
    // On the case that the component shutdown and 'failed_split_end_oper_' is missing from the
    // memory, during the process of ASSIGN, we will replay the StartSplit log and check it with
    // the chunk meta data from the config server to confirm that the related split transaction 
    // is successful or not. So we can recover 'failed_split_end_oper_'.
    SharedResourceOperationType failed_split_end_oper_ = SharedResourceOperationType::None;

    // Anytime we receive a ProcessedLSN operation, we save it in 'processed_lsn_', and later we
    // will write a log of it. If the log is written failed, we will record the event by setting 
    // the variable 'processed_lsn_failed_flag_' to TRUE. After that we can still treat it as a
    // success. And when we receive any new log request later, LogStore will rewrite the failed
    // log at first by calling the function RewriteImportantFailedLog() before write the new one.
    // On the case that the component shutdown and 'processed_lsn_failed_flag_' is missing from 
    // memory, there will be no recovery for it, which will cause some plogs be recycled agained.
    // Because GC function is idempotent, so it will not have impact on GC function. But it will 
    // possible have some impact on GC performance.
    // If several Processed logs are written failed continuously, the maximum processed lsn will
    // be record in "failed_processed_lsn_".
    bool processed_lsn_failed_flag_ = false;
    uint64_t processed_lsn_ = INVALID_LSN;

    // 'split_oper_to_write_' is used to avoid writting CommitSplit or RollbackSplit repeatedly.
    SharedResourceOperationType split_oper_to_write_ = SharedResourceOperationType::None;

    // Output the operations in an appointed range.
    void GetOperationsByRange(
            uint32_t max_size,
            std::vector<SharedResourceReferenceOperation>& records,
            const SharedResourceOperationIterator& start_it,
            const SharedResourceOperationIterator& end_it);
    // Replay split log
    rocksdb::Status ReplayStartSplitLog(const LogRecord& record);
    rocksdb::Status ReplayCommitSplitLog(const LogRecord& record);
    rocksdb::Status ReplayRollbackSplitLog(const LogRecord& record);
    // Replay reference log
    rocksdb::Status ReplayReferenceLog(const LogRecord& record);
    // Replay processed log
    rocksdb::Status ReplayProcessedLog(const LogRecord& record);
    // Remove processed operations from memory.
    void RemoveProcessedOperations(SharedResourceOperationVector& operations, uint64_t processedLSN);
    rocksdb::Status RemoveStartSplitOperation();
    rocksdb::Status RollbackSplitOperations();
    // Write the log of split operation.
    rocksdb::Status LogSplitState(SharedResourceOperationType oper_type,
                                  uint64_t split_id,
                                  std::shared_ptr<SplitDescription> description);

	rocksdb::Status RewriteSplitEndLog();
	
public:
    SharedResourceOperationLogRecordProvider() {}

    SharedResourceOperationLogRecordProvider(ChunkMetadata* chunk_meta_data) {
        if (chunk_meta_data != nullptr) {
            key_range_ = chunk_meta_data->GetKeyRange();
        }else {
            key_range_ = mongo::ChunkMetadata::KeyRange(KEY_BSON(""), KEY_BSON(""));
        }
    }

    // Write all logs of all shared resource operations of the split to right chunk.
    rocksdb::Status WriteSharedResourceLogToRight(SharedResourceOperationLogRecordProvider* right_provider);

    //
    //  Load-balancing transaction log related functions
    //  Is used by GC manager: GC manager periodically asks for batch of unprocessed log records calling
    //  function GetSharedResourceLogRecords. If there are some records that GC manager can process,
    //  provider will return this messages at "unprocessedRecords" list. When GC manager finished processing
    //  some messages from the batch it can set the lowest LSN of processed messages using function
    //  SetLastProcessedSharedResourceLSN, so with the next call GetSharedResourceLogRecords only records
    //  having higher LSN will be returned.
    //
    void GetUnprocessedOperations(uint32_t maxBatchMaxSize,
                                  std::vector<SharedResourceReferenceOperation>& unprocessedRecords);
    rocksdb::Status SetLastProcessedOperationLSN(uint64_t processedLSN);
    // Called by PlogEnv during SplitDB() to register shared plogs.
    rocksdb::Status RegisterSharedResources(const SharedResourceIds& resources);

    //
    //  Split related functions
    //
    rocksdb::Status StartSplit(std::shared_ptr<SplitDescription> description);
    rocksdb::Status RollbackSplit();
    rocksdb::Status CommitSplit();
    // Return information about the latest split which is started or ended.
    SplitContext* GetSplitContext() { return &split_ctx_; }

    // Get the pointer to the share resource operation vecotr.
    SharedResourceOperationVector& GetSharedResourceOperationVector() {
        return operations_;
    }

    void SetSharedResourceManager(SharedResourceManager* manager) { share_res_manager_ = manager; }

    //
    //  ILogRecordProvider implementation
    //
    rocksdb::Status InitializationBegin(LogReplayOrder replayOrder, std::vector<LogRecordType>& supportedRecordTypes) override
    {
        supportedRecordTypes.push_back(LogRecordType::LoadBalancingTransactionLog);
        return rocksdb::Status::OK();
    }

    rocksdb::Status ReplayLogRecord(const LogRecord& record, bool& needMoreRecords) override;

    rocksdb::Status InitializationEnd(ILogRecordWriter& logWriter) override;

    rocksdb::Status RewriteImportantFailedLog() override;

    //
    // Write Checkpoint
    //
    void CheckpointBegin() override { }
    rocksdb::Status WriteCheckpoint() override;
    void CheckpointEnd() override { }

    // It simply returns fixed value because there is only one instance now.
    std::string GetName() override { return "SharedResourceOperationLogRecordProvider_000"; }

    // Write log records in batch.
    rocksdb::Status WriteRecord(LogRecord& log_record) {
        rocksdb::Status s = GetLogWriter()->WriteRecord(log_record);
        return s;
    }
    rocksdb::Status WriteRecords(LogRecordVector& log_records) {
        rocksdb::Status s = GetLogWriter()->WriteRecord(log_records.GetLogRecords());
        return s;
    }

    // SharedResources [ SharedResourceId ]
    rocksdb::Status PrepareReferenceLog(LogRecordVector& log_records,
                                        const SharedResourceOperationType oper_type,
                                        const SharedResourceId& resource,
                                        const uint64_t& parent_chunk_id,
                                        const uint64_t& child_chunk_id);
    rocksdb::Status PrepareReferenceLog(LogRecordVector& log_records,
                                        const SharedResourceOperationType oper_type,
                                        const std::vector<SharedResourceId>& resources,
                                        const uint64_t& parent_chunk_id,
                                        const uint64_t& child_chunk_id);
    // SplitState [ StartSplit CommitSplit RollbackSplit ]
    // add currentSplitID to check SplitStart and SplitCommit error
    rocksdb::Status PrepareSplitLog(LogRecordVector& log_records,
                                    const SharedResourceOperationType oper_type,
                                    const uint64_t split_id,
                                    std::shared_ptr<SplitDescription> description);
    // ProcessedLSN:don't write into <SharedResourceOperation> and record processedLSN only.
    rocksdb::Status LogProcessedLSN();
    // Append shared resource reference operation(s).
    void AppendSharedResourceReferenceOperation(LogRecordVector& log_records);

    // Clear filters in memory. This method is only used for the test of replaying log.
    void Reset() {
        // clear operation list
        operations_.clear();
        // clear split context
        GetSplitContext()->SetId(INVALID_SPLIT_ID);
        GetSplitContext()->SetDescription(nullptr);
        GetSplitContext()->SetStateToNone();
    }

    SharedResourceOperationVector* GetOperations() { return &operations_; }

};

// Share resource memory log.
class SharedResourceMemLog : public SharedResourceOperation
{
public:
    SharedResourceMemLog() {}
    SharedResourceMemLog(const SharedResourceOperationType oper_type) {
        SetType(oper_type);
    }
    ~SharedResourceMemLog() {}

    void SetType(const SharedResourceOperationType type) { type_ = type; }
    const SharedResourceOperationType GetType() const { return type_; }

    void SetSharedFlag(bool shared_flag) { shared_flag_ = shared_flag; }
    const bool GetSharedFlag() const { return shared_flag_; }

    void SetResource(const SharedResourceId& resource) { resource_ = resource; }
    const SharedResourceId& GetResource() const { return resource_; }

    void SetProcessedLSN(uint64_t lsn) { processed_lsn_ = lsn; }
    uint64_t GetProcessedLSN() const { return processed_lsn_; }

    void SetSplitId(const uint64_t split_id) { split_id_ = split_id; }
    const uint64_t GetSplitId() const { return split_id_; }

    void SetTime() {
        time(&tm_);
        struct timeval tv;
        gettimeofday(&tv, NULL);
        tm_usec_ = tv.tv_usec;
    }

    void DumpSharedResourceRecord(std::ofstream& dump_stream);

private:
    SharedResourceOperationType type_ = SharedResourceOperationType::None;
    bool shared_flag_ = true;
    struct {
        time_t tm_;
        uint64_t tm_usec_;
    };
    union {
        SharedResourceId resource_;
        uint64_t processed_lsn_;
        uint64_t split_id_;
    };

    std::string GetTimeStr() {
        char buffer[32];
        strftime(buffer, 20, "%Y-%m-%d %H:%M:%S", std::localtime(&tm_));
        sprintf(buffer+19, ".%06lu", tm_usec_);
        return buffer;
    }

    void DumpResource(std::ofstream& dump_stream);
    void DumpRmvFilter(std::ofstream& dump_stream);
    void DumpSplit(std::ofstream& dump_stream);
    void DumpProcessed(std::ofstream& dump_stream);
    void DumpOthers(std::ofstream& dump_stream);
};

// Loop vector for share resource memory log.
typedef std::vector<SharedResourceMemLog>::iterator SharedResourceMemLogIterator;
class SharedResourceMemLogVector {
private:
    std::vector<SharedResourceMemLog> mem_logs_;
    SharedResourceMemLogIterator begin_;
    SharedResourceMemLogIterator end_;
    uint32_t size_;
    bool init_flag_ = false;
public:
    void init(size_t size) {
        mem_logs_.resize(size, SharedResourceMemLog());
        begin_ = mem_logs_.begin();
        end_ = mem_logs_.begin();
        size_ = 0;
        init_flag_ = true;
    }
    const SharedResourceMemLogIterator begin() {
        return begin_;
    }
    const SharedResourceMemLogIterator end() {
        return end_;
    }
    void inc(SharedResourceMemLogIterator& it) {
        it++;
        if (it == mem_logs_.end()) {
            it = mem_logs_.begin();
        }
    }
    void append(SharedResourceMemLog& mem_log) {
        assert(init_flag_);
        *end_ = mem_log;
        end_->SetTime();
        inc(end_);
        if ((size_+1) < mem_logs_.size()) {
            size_++;
        }else {
            inc(begin_);
        }
    }
};

class SharedResourceModule : public SharedResource {
public:
    SharedResourceModule() {}
    ~SharedResourceModule(); //todo

    // Init module.
    virtual rocksdb::Status Init(rocksdb::Env* env,
                                 const std::string& db_path,
                                 ChunkMetadata* chunk_meta_data);

    // Shared resource interface for rocksdb & plog_env.

    virtual int RegisterSharedResource(std::vector<SharedResourceId>& resource_id_list) override;
    virtual int RemoveSharedResource(std::vector<SharedResourceRemoveDescription>& list) override;
    virtual bool CheckSharedResource(const SharedResourceId &id, std::string &path) override{return true;}

    // Shared resource interface for mongo-rocks.
    rocksdb::Status StartSplit(std::shared_ptr<SplitDescription> description);
    rocksdb::Status RollbackSplit();
    rocksdb::Status CommitSplit();
    bool IsSplitStateFault() { return log_provider_->GetSplitContext()->IsStateFault(); }
    std::string GetRightDbPath() { return log_provider_->GetSplitContext()->GetDescription()->rightDbPathWithRootPlog; }
    rocksdb::Status WriteSharedResourceLogToRight(SharedResourceModule& right_module);

    // Shared resource interface for global GC.
    virtual void GetUnprocessedOperations(uint32_t maxBatchMaxSize, std::vector<SharedResourceReferenceOperation>& unprocessedRecords);
    rocksdb::Status SetLastProcessedOperationLSN(uint64_t processedLSN);

    // Get shared resources log provider.
    SharedResourceOperationLogRecordProvider* GetLogProvider() { return log_provider_.get(); }
    SharedResourceManager* GetFilterManager() { return filter_manager_.get(); }

    // MemLog methods.
    void MemLogRegisterSharedResources(std::vector<SharedResourceId>& resources);
    void MemLogRemoveSharedResource(std::vector<SharedResourceRemoveDescription>& list);
    void MemLogStartSplit();
    void MemLogAddFilters(const std::vector<SharedResourceId>& resources);
    void MemLogCommitSplit();
    void MemLogRollbackSplit();
    void MemLogGetUnprocessed(std::vector<SharedResourceReferenceOperation>& unprocessedRecords, uint32_t old_size);
    void MemLogSetProcessed(uint64_t processedLSN);
    void MemLogOperation(const SharedResourceOperation* oper);
    void MemLogOperations(SharedResourceOperationVector& operations);
    void MemLogFilters(const std::vector<SharedResourceId>& resources);
    void MemLogReplayLog();
    void DumpSharedResourceList(const char* history_path);
    SharedResourceMemLogVector& GetMemLogs() { return mem_logs_; }
    static bool RunLogIsEnable() {
        return ((debug_mode_ == SharedResourceDebugMode::RunLog) || (debug_mode_ == SharedResourceDebugMode::Both));
    }
    static bool MemLogIsEnable() {
        return ((debug_mode_ == SharedResourceDebugMode::MemLog) || (debug_mode_ == SharedResourceDebugMode::Both));
    }

protected:
    // Memory log.
    SharedResourceMemLogVector mem_logs_;

    // log provider and filter manager.
    std::unique_ptr<SharedResourceOperationLogRecordProvider> log_provider_ = nullptr;
    std::unique_ptr<SharedResourceManager> filter_manager_ = nullptr;

	// init flag.
	bool init_flag_ = false;

private:
    // log storage.
    std::unique_ptr<TransLogRecordStore> log_store_;

    // read&write lock.
    stdx::mutex rw_mutex_;

    // Check debug mode.
    static const SharedResourceDebugMode debug_mode_ = SharedResourceDebugMode::Both;

};


}   //  namespace TransactionLog

}   //  namespace mongo



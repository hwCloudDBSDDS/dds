
#pragma once



namespace mongo
{

namespace TransactionLog
{

uint64_t lsn_stub = 0;
static inline uint64_t GetLsnStub() {
    return lsn_stub;
}
static inline uint64_t IncLsnStub() {
    lsn_stub++;
    return lsn_stub;
}
static inline void ResetLsnStub() {
    lsn_stub = 0;
}

static inline void CopyBuf(uint8_t* buf1, uint32_t len1, const uint8_t* buf2, uint32_t len2) {
    uint32_t i = 0;
    for (; ((i < len1) && (i < len2)); i++) {
        buf1[len1-(i+1)] = buf2[len2-(i+1)];
    }
    for (; i < len1; i++) {
        buf1[len1-(i+1)] = 0;
    }
}

static inline const SharedResourceId Uint64ToSharedResourceId(const uint64_t id) {
    SharedResourceId resource;
    uint32_t len1 = sizeof(resource);
    uint32_t len2 = sizeof(id);
    uint8_t* buf1 = reinterpret_cast<uint8_t*>(&resource);
    const uint8_t* buf2 = reinterpret_cast<const uint8_t*>(&id);
    CopyBuf(buf1,len1,buf2,len2);
    return resource;
}

static inline const uint64_t SharedResourceIdToUint64(const SharedResourceId& resource) {
    uint64_t id;
    uint32_t len1 = sizeof(id);
    uint32_t len2 = sizeof(resource);
    uint8_t* buf1 = reinterpret_cast<uint8_t*>(&id);
    const uint8_t* buf2 = reinterpret_cast<const uint8_t*>(&resource);
    CopyBuf(buf1,len1,buf2,len2);
    return id;
}

void PrintLogRecord(const LogRecord& record, uint32_t index) {
    uint64_t lsn = record.GetLsn();
    LogRecordType log_type = record.GetType();
    SharedResourceOperationType oper_type = SplitOperation::GetOperTypeOfLogRecord(record);
    std::string oper_type_name = GetOperTypeName(oper_type);
    const SharedResourceFilterOperation* filter_oper;
    const SharedResourceReferenceOperation* reference_oper;
    const ProcessedLSNOperation* processed_oper;
    const SplitOperation* split_oper;

    switch (oper_type) {
    case SharedResourceOperationType::AddFilter:
        filter_oper = SharedResourceFilterOperation::DecodeFrom(record);
        printf("----[%u] LSN(%lu) LogType(%d) OperType(%s) ResourceId(%lu) \n",
               index, lsn, log_type, oper_type_name.c_str(), SharedResourceIdToUint64(filter_oper->GetResource()));
        delete filter_oper;
        break;
    case SharedResourceOperationType::RemoveFilter:
        filter_oper = SharedResourceFilterOperation::DecodeFrom(record);
        printf("----[%u] LSN(%lu) LogType(%d) OperType(%s) ResourceId(%lu) \n",
               index, lsn, log_type, oper_type_name.c_str(), SharedResourceIdToUint64(filter_oper->GetResource()));
        delete filter_oper;
        break;
    case SharedResourceOperationType::AddReference:
        reference_oper = SharedResourceReferenceOperation::DecodeFrom(record);
        printf("----[%u] LSN(%lu) LogType(%d) OperType(%s) ResourceId(%lu) \n",
               index, lsn, log_type, oper_type_name.c_str(), SharedResourceIdToUint64(reference_oper->GetResource()));
        delete reference_oper;
        break;
    case SharedResourceOperationType::RemoveReference:
        reference_oper = SharedResourceReferenceOperation::DecodeFrom(record);
        printf("----[%u] LSN(%lu) LogType(%d) OperType(%s) ResourceId(%lu) \n",
               index, lsn, log_type, oper_type_name.c_str(), SharedResourceIdToUint64(reference_oper->GetResource()));
        delete reference_oper;
        break;
    case SharedResourceOperationType::Processed:
        processed_oper = ProcessedLSNOperation::DecodeFrom(record);
        printf("----[%u] LSN(%lu) LogType(%d) OperType(%s) ProcessedLsn(%lu) \n",
               index, lsn, log_type, oper_type_name.c_str(), processed_oper->GetProcessedLSN());
        delete processed_oper;
        break;
    case SharedResourceOperationType::StartSplit:
        split_oper = SplitOperation::DecodeFrom(record);
        printf("----[%u] LSN(%lu) LogType(%d) OperType(%s) SplitID(%lu) \n",
               index, lsn, log_type, oper_type_name.c_str(), split_oper->GetId());
        delete split_oper;
        break;
    case SharedResourceOperationType::CommitSplit:
        split_oper = SplitOperation::DecodeFrom(record);
        printf("----[%u] LSN(%lu) LogType(%d) OperType(%s) SplitID(%lu) \n",
               index, lsn, log_type, oper_type_name.c_str(), split_oper->GetId());
        delete split_oper;
        break;
    case SharedResourceOperationType::RollbackSplit:
        split_oper = SplitOperation::DecodeFrom(record);
        printf("----[%u] LSN(%lu) LogType(%d) OperType(%s) SplitID(%lu) \n",
               index, lsn, log_type, oper_type_name.c_str(), split_oper->GetId());
        delete split_oper;
        break;
    default:
        printf("----[%u] !!! Error operation type (%d) ---- \n", index, static_cast<int>(oper_type));
        break;
    }
}

void PrintOpeation(const SharedResourceOperation* oper, uint32_t index) {
    uint64_t lsn = oper->GetLsn();
    SharedResourceOperationType oper_type = oper->GetType();
    std::string oper_type_name = GetOperTypeName(oper_type);
    const SharedResourceFilterOperation* filter_oper;
    const SharedResourceReferenceOperation* reference_oper;
    const ProcessedLSNOperation* processed_oper;
    const SplitOperation* split_oper;

    switch (oper_type) {
    case SharedResourceOperationType::AddFilter:
        filter_oper = reinterpret_cast<const SharedResourceFilterOperation*>(oper);
        printf("----[%u] LSN(%lu) OperType(%s) ResourceId(%lu) \n",
               index, lsn, oper_type_name.c_str(), SharedResourceIdToUint64(filter_oper->GetResource()));
        break;
    case SharedResourceOperationType::RemoveFilter:
        filter_oper = reinterpret_cast<const SharedResourceFilterOperation*>(oper);
        printf("----[%u] LSN(%lu) OperType(%s) ResourceId(%lu) \n",
               index, lsn, oper_type_name.c_str(), SharedResourceIdToUint64(filter_oper->GetResource()));
        break;
    case SharedResourceOperationType::AddReference:
        reference_oper = reinterpret_cast<const SharedResourceReferenceOperation*>(oper);
        printf("----[%u] LSN(%lu) OperType(%s) ResourceId(%lu) \n",
               index, lsn, oper_type_name.c_str(), SharedResourceIdToUint64(reference_oper->GetResource()));
        break;
    case SharedResourceOperationType::RemoveReference:
        reference_oper = reinterpret_cast<const SharedResourceReferenceOperation*>(oper);
        printf("----[%u] LSN(%lu) OperType(%s) ResourceId(%lu) \n",
               index, lsn, oper_type_name.c_str(), SharedResourceIdToUint64(reference_oper->GetResource()));
        break;
    case SharedResourceOperationType::Processed:
        processed_oper = reinterpret_cast<const ProcessedLSNOperation*>(oper);
        printf("----[%u] LSN(%lu) OperType(%s) ProcessedLsn(%lu) \n",
               index, lsn, oper_type_name.c_str(), processed_oper->GetProcessedLSN());
        break;
    case SharedResourceOperationType::StartSplit:
        split_oper = reinterpret_cast<const SplitOperation*>(oper);
        printf("----[%u] LSN(%lu) OperType(%s) SplitID(%lu) \n",
               index, lsn, oper_type_name.c_str(), split_oper->GetId());
        break;
    case SharedResourceOperationType::CommitSplit:
        split_oper = reinterpret_cast<const SplitOperation*>(oper);
        printf("----[%u] LSN(%lu) OperType(%s) SplitID(%lu) \n",
               index, lsn, oper_type_name.c_str(), split_oper->GetId());
        break;
    case SharedResourceOperationType::RollbackSplit:
        split_oper = reinterpret_cast<const SplitOperation*>(oper);
        printf("----[%u] LSN(%lu) OperType(%s) SplitID(%lu) \n",
               index, lsn, oper_type_name.c_str(), split_oper->GetId());
        break;
    default:
        printf("----[%u] !!! Error operation type (%d) ---- \n", index, static_cast<int>(oper_type));
        break;
    }
}

void PrintOperations(SharedResourceOperationLogRecordProvider* log_provider, std::string header)
{
    printf("---- %s: begin of provider operations:\n", header.c_str());
    SharedResourceOperationVector* operations = log_provider->GetOperations();
    uint32_t count = 0;
    for (auto oper_it = operations->begin(); oper_it != operations->end(); oper_it++) {
        count++;
        if ((count <= 101) || (0 == (count % 200))) {
            PrintOpeation(*oper_it, (count-1));
        }
    }
    printf("---- %s: end of provider operations. TotalNum(%lu)\n", header.c_str(), operations->size());
}

void PrintFilters(SharedResourceManager* filter_manager, std::string header)
{
    printf("---- %s: begin of filter resources:\n", header.c_str());
    const SharedResourceIds& resources = filter_manager->GetSharedResources();
    uint32_t count = 0;
    for (auto resource : resources) {
        count++;
        if ((count <= 101) || (0 == (count % 200))) {
            printf("      [%u] resourceID(%lu)\n", (count-1), SharedResourceIdToUint64(resource));
        }
    }
    printf("---- %s: end of filter resources. TotalNum(%lu)\n", header.c_str(), resources.size());
}

uint32_t GetOperationNumber(SharedResourceOperationLogRecordProvider* log_provider) {
    SharedResourceOperationVector* operations = log_provider->GetOperations();
    return operations->size();
}

uint32_t GetFilterNumber(SharedResourceManager* filter_manager) {
    const SharedResourceIds& resources = filter_manager->GetSharedResources();
    return resources.size();
}


class LogRecordVector_Stub : public LogRecordVector
{
public:
    // Only for test, Copy and append the input record instance to the tail of this vector.
    void copy_push_back(const LogRecord& ref_record) {
        int size = ref_record.GetSize();
        char *data = new char[size];
        memcpy(data, ref_record.GetData(), size);
        LogRecord record(ref_record.GetType(), data, size);
        record.SetLsn(ref_record.GetLsn());
        push_back(record);
    }
};

//
//  Interface which reprensents write functionality of Log Record Store.
//  The instance of this interface will be provided to Log Record Provider during initialization.
//
class LogRecordWriter_Stub : public ILogRecordWriter
{
private:
    LogRecordVector_Stub* log_records_;
    bool fault_ingest_flag_ = false;

    ILogRecordProvider* provider_ = nullptr;
    std::vector<LogRecordType> log_types_;

public:
    LogRecordWriter_Stub() { }
    LogRecordWriter_Stub(ILogRecordProvider* provider) : provider_(provider) { }
    ~LogRecordWriter_Stub() { }

    void SetLogRecords(LogRecordVector_Stub* log_records) { log_records_ = log_records; }

    void SetProvider(ILogRecordProvider* provider) { provider_ = provider; }
    ILogRecordProvider* GetProvider() const { return provider_; }

    void SetLogTypes(std::vector<LogRecordType> log_types) {
        log_types_.clear();
        log_types_.insert(log_types_.end(), log_types.begin(), log_types.end());
    }
    std::vector<LogRecordType>& GetLogTypes() { return log_types_; }

    void reset() { log_types_.clear(); }


    bool IsMe(const LogRecordType type) {
        for (auto t : log_types_) {
            if (t == type) {
                return true;
            }
        }
        return false;
    }

    // Write mutilpue records in one write
    rocksdb::Status WriteRecord(std::vector<LogRecord>& records) override {
        if ((provider_== nullptr) || fault_ingest_flag_) {
            return rocksdb::Status::Aborted();
        }
        // Write the log failed last time before write current logs
        rocksdb::Status s = provider_->RewriteImportantFailedLog();
        if (!s.ok()) {
            return s;
        }
        // Write current logs
        for (auto it = records.begin(); it != records.end(); it++) {
            it->SetLsn(IncLsnStub());
            log_records_->copy_push_back(*it);
        }
        return rocksdb::Status::OK();
    };

    // Write one record in one write
    rocksdb::Status WriteRecord(LogRecord& record) override {
        if ((provider_== nullptr) || fault_ingest_flag_) {
            return rocksdb::Status::Aborted();
        }
        // Write the log failed last time before write current log
        rocksdb::Status s = provider_->RewriteImportantFailedLog();
        if (!s.ok()) {
            return s;
        }
        // Write current logs
        record.SetLsn(IncLsnStub());
        log_records_->copy_push_back(record);
        return rocksdb::Status::OK();
    }

    // Ingest or clear fault
    void IngestFault() { fault_ingest_flag_ = true; }
    void ClearFault() { fault_ingest_flag_ = false; }

};

class LogRecordStore_Stub : public ILogRecordStore
{
private:
    std::vector<LogRecordWriter_Stub*> writers_;
    LogRecordVector_Stub log_records_;
    bool fault_ingest_flag_ = false;
    bool printf_flag_ = false;

public:
    LogRecordStore_Stub() { }
    ~LogRecordStore_Stub() { ClearWriters(); }

    void SetPrintFlag(bool printf_flag) { printf_flag_ = printf_flag; }
    bool GetPrintFlag() { return printf_flag_; }

    rocksdb::Status RegisterProvider(ILogRecordProvider& provider) {
        LogRecordWriter_Stub* writer = new LogRecordWriter_Stub(&provider);
        writer->SetLogRecords(&log_records_);
        writers_.push_back(writer);
        return rocksdb::Status::OK();
    }

    // Replay the logs
    void ReplayLogRecord() {
        if (printf_flag_) { printf("-- begin of ReplayLogRecord \n"); }
        uint64_t max_lsn = 0;
        bool needMoreRecords = false;
        uint32_t count = 0;
        for (auto record : log_records_) {
            if (max_lsn < record.GetLsn()) {
                max_lsn = record.GetLsn();
            }
            for (auto writer : writers_) {
                if (!writer->IsMe(record.GetType())) {
                    continue;
                }
                if (printf_flag_) {
                    count++;
                    if ((count <= 101) || (0 == (count % 200))) {
                        PrintLogRecord(record, (count-1));
                    }
                }
                writer->GetProvider()->ReplayLogRecord(record, needMoreRecords);
                if (!needMoreRecords) {
                    break;
                }
            }
            if (!needMoreRecords) {
                break;
            }
        }
        if (max_lsn > GetLsnStub()) {
            printf("!!!!! LSN Error: %lu > %lu\n", max_lsn, GetLsnStub());
        }
        if (printf_flag_) { printf("-- end of ReplayLogRecord. max_lsn(%lu) \n", max_lsn); }
    }

    // Init log storage
    void Init() {
        for (auto writer : writers_) {
            writer->GetLogTypes().clear();
            writer->GetProvider()->InitializationBegin(
                LogReplayOrder::Forward, writer->GetLogTypes());
        }
        ReplayLogRecord();
        for (auto writer : writers_) {
            writer->GetProvider()->InitializationEnd(*writer);
        }
    }

    void Reset() {
        log_records_.clear();
        fault_ingest_flag_ = false;
        printf_flag_ = false;
    }

    void InitLogTypes() {
        for (auto writer : writers_) {
            writer->GetLogTypes().clear();
            writer->GetProvider()->InitializationBegin(
                LogReplayOrder::Forward, writer->GetLogTypes());
        }
    }

    void ClearWriters() {
        for (auto writer : writers_) {
            delete writer;
        }
        writers_.clear();
    }

    // Get the number of log records related to the appointed provider.
    const uint32_t GetLogRecordsNumber() const {
        return log_records_.size();
    }

    LogRecordVector_Stub& GetLogRecords() { return log_records_; }

    rocksdb::Status WriteCheckPoint() {
        rocksdb::Status s;
        Reset();
        for (auto writer : writers_) {
            s = writer->GetProvider()->WriteCheckpoint();
            if (!s.ok()) {
                return s;
            }
        }
        return rocksdb::Status::OK();
    }

    // Check log record number in log storage.
    bool CheckLogRecordNumber(const uint32_t log_record_num,
                              const uint32_t filter_num,
                              const uint32_t loadbalacnce_num) {
        uint32_t _filter_num = 0;
        uint32_t _loadbalacnce_num = 0;
        uint32_t _log_record_num = 0;
        LogRecordVector_Stub& log_records = GetLogRecords();
        uint32_t count = 0;
        for (auto record : log_records) {
            if (printf_flag_) {
                PrintLogRecord(record, count++);
            }
            switch (record.GetType()) {
            case LogRecordType::LoadBalancingTransactionLog:
                _loadbalacnce_num++;
                _log_record_num++;
                break;
            case LogRecordType::SharedResourceFilterLog:
                _filter_num++;
                _log_record_num++;
                break;
            default:
                // Invalid log record type.
                return false;
            }
        }
        return ((GetLogRecordsNumber() == log_record_num)
                && (_log_record_num == log_record_num)
                && (_filter_num == filter_num)
                && (_loadbalacnce_num == loadbalacnce_num));
    }

};

class SharedResourceModule_Stub : public SharedResourceModule {
public:
    SharedResourceModule_Stub() {}
    virtual ~SharedResourceModule_Stub() {}

    // Init module.
    rocksdb::Status Init(ChunkMetadata* chunk_meta_data = nullptr);

    // Check log record number in log storage.
    const uint32_t GetLogRecordsNumber() const { return log_store_->GetLogRecordsNumber(); }
    bool CheckLogRecordNumber(const uint32_t log_record_num,
                              const uint32_t filter_num,
                              const uint32_t loadbalacnce_num) {
        return log_store_->CheckLogRecordNumber(log_record_num, filter_num, loadbalacnce_num);
    }
    // Check operation number in memory.
    bool CheckOperationNumber(const uint32_t filter_num, const uint32_t loadbalacnce_num) {
        if (printf_flag_) {
            PrintFilters(filter_manager_.get(),  "");
            PrintOperations(log_provider_.get(),  "");
        }
        return ((filter_num == GetFilterNumber(filter_manager_.get()))
                && (loadbalacnce_num == GetOperationNumber(log_provider_.get())));
    };

    void SetPrintFlag(bool printf_flag) {
        printf_flag_ = printf_flag;
        log_store_->SetPrintFlag(printf_flag_);
    }
    bool GetPrintFlag() { return printf_flag_; }

    // Simulate the steps of shared resource operations.
    void Step_SplitSuccess(std::shared_ptr<SplitDescription> description,
                           uint64_t* register_list,
                           uint32_t size,
                           SharedResourceModule_Stub& right_module);
    void Step_SplitRollback(std::shared_ptr<SplitDescription> description,
                            uint64_t* register_list,
                            uint32_t size);
    void Step_SplitMissingEndState(std::shared_ptr<SplitDescription> description,
                           uint64_t* register_list,
                           uint32_t size,
                           SharedResourceModule_Stub& right_module);
    int Step_RemoveResources(uint64_t* delete_list, uint32_t size);
    void Step_GetUnprocessed(uint32_t batch_max_size);
    rocksdb::Status Step_SetProcessed(uint64_t processed_lsn);

private:
    std::unique_ptr<LogRecordStore_Stub> log_store_;
    bool printf_flag_ = false;
};


}   //  namespace TransactionLog

}   //  namespace mongo



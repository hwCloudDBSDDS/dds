
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

#include "mongo/unittest/unittest.h"
#include "ChunkMetadata.h"
#include <mongo/bson/json.h>
#include "../GlobalConfig.h"

//#include "../../../../common/include/error_code.h"
//#include "../../../../common/plogclient_stub/plog_client_x.h"
#include "i_log_record_store.h"
#include "TransLogRecordStore.h"
#include "i_shared_resource_manager.h"
#include "i_shared_resource_manager_test.h"

namespace mongo
{

namespace TransactionLog
{

bool CheckRemoveResult(const SharedResourceRemoveDescription& rmv_desc,
                       const bool shared_flag,
                       const int remove_result) {
    return ((rmv_desc.shared_flag == shared_flag) && (rmv_desc.remove_result == remove_result));
}
bool CheckRemoveResult(const std::vector<SharedResourceRemoveDescription>& rmv_desc_list,
                       const uint64_t id,
                       const bool shared_flag,
                       const int remove_result) {
    SharedResourceId resource = Uint64ToSharedResourceId(id);
    for (auto desc : rmv_desc_list) {
        if (EqualResourceId(resource, desc.id)) {
            return CheckRemoveResult(desc, shared_flag, remove_result);
        }
    }
    return false;
}
void AddRemoveDescription(std::vector<SharedResourceRemoveDescription>& rmv_desc_list,
                          const uint64_t id,
                          const bool shared_flag,
                          const int remove_result) {
    SharedResourceRemoveDescription rmv_desc = {Uint64ToSharedResourceId(id), shared_flag, remove_result};
    rmv_desc_list.push_back(rmv_desc);
}
void PrepareSharedResourceIds(SharedResourceIds& resources, uint64_t* res_array, uint32_t size) {
    for (uint32_t i=0; i<size; i++) {
        resources.push_back(Uint64ToSharedResourceId(res_array[i]));
    }
}


//
// Methods of SharedResourceModule_Stub
//

// Init the stub of shared resource module.
rocksdb::Status SharedResourceModule_Stub::Init(ChunkMetadata* chunk_meta_data)
{
    bool log_store_init_flag = false;

    // Create store, provider, manager
    if (log_store_ == nullptr) {
        log_store_init_flag = true;
        log_store_.reset(new LogRecordStore_Stub());
    }
    else {
        log_store_->ClearWriters();
    }
    log_provider_.reset(new SharedResourceOperationLogRecordProvider(chunk_meta_data));
    filter_manager_.reset(new SharedResourceManager());

    // Reigist providers
    rocksdb::Status s = log_store_->RegisterProvider(*filter_manager_);
    if (!s.ok()) {
        printf("[TransactionLog] Register log provider faild: %s\n", s.ToString().c_str());
        return s;
    }
    s = log_store_->RegisterProvider(*log_provider_);
    if (!s.ok()) {
        printf("[TransactionLog] Register filter manager faild: %s\n", s.ToString().c_str());
        return s;
    }

    // Associate provider and manager with each other.
    log_provider_->SetSharedResourceManager(filter_manager_.get());
    filter_manager_->SetLogRecordProvider(log_provider_.get());

    // Init log storage. (Include of the process of replaying log)
    log_store_->Init();

    // Init memory log.
    mem_logs_.init(8*1024);
    MemLogReplayLog();
    init_flag_ = true;

    printf("[TransactionLog] Init shared resource module successfully.\n");
    return rocksdb::Status::OK();
}

void SharedResourceModule_Stub::Step_SplitSuccess(std::shared_ptr<SplitDescription> description,
                                                  uint64_t* register_list,
                                                  uint32_t size,
                                                  SharedResourceModule_Stub& right_module) {
    int ret = RET_OK;
    rocksdb::Status s;
// 1 Start split.
    // 1.1) Start split.
    s = StartSplit(description);
    ASSERT_TRUE(s.ok());
    // 1.2) Register shared resources during the split.
    SharedResourceIds resources;
    PrepareSharedResourceIds(resources, register_list, size);
    ret = RegisterSharedResource(resources);
    ASSERT_TRUE(ret==RET_OK);
// 2 Write shared resource reference logs to right side.
    right_module.Init();
    s = WriteSharedResourceLogToRight(right_module);
    ASSERT_TRUE(s.ok());
    right_module.Init(); // Replay log
// 3 Commit split.
    s = CommitSplit();
    ASSERT_TRUE(s.ok());
}
void SharedResourceModule_Stub::Step_SplitRollback(std::shared_ptr<SplitDescription> description,
                                                   uint64_t* register_list,
                                                   uint32_t size) {
    int ret = RET_OK;
    rocksdb::Status s;
// 1 Start split.
    // 1.1) Start split.
    s = StartSplit(description);
    ASSERT_TRUE(s.ok());
    // 1.2) Register shared resources during the split.
    SharedResourceIds resources;
    PrepareSharedResourceIds(resources, register_list, size);
    ret = RegisterSharedResource(resources);
    ASSERT_TRUE(ret==RET_OK);
// 2 Write shared resource reference logs to right side.
    SharedResourceModule_Stub right_module;
    right_module.Init();
    s = WriteSharedResourceLogToRight(right_module);
    ASSERT_TRUE(s.ok());
// 3 Commit split.
    s = RollbackSplit();
    ASSERT_TRUE(s.ok());
}
void SharedResourceModule_Stub::Step_SplitMissingEndState(std::shared_ptr<SplitDescription> description,
                                                  uint64_t* register_list,
                                                  uint32_t size,
                                                  SharedResourceModule_Stub& right_module) {
    int ret = RET_OK;
    rocksdb::Status s;
// 1 Start split.
    // 1.1) Start split.
    s = StartSplit(description);
    ASSERT_TRUE(s.ok());
    // 1.2) Register shared resources during the split.
    SharedResourceIds resources;
    PrepareSharedResourceIds(resources, register_list, size);
    ret = RegisterSharedResource(resources);
    ASSERT_TRUE(ret==RET_OK);
// 2 Write shared resource reference logs to right side.
    right_module.Init();
    s = WriteSharedResourceLogToRight(right_module);
    ASSERT_TRUE(s.ok());
    right_module.Init(); // Replay log.
// 3 Simulate missing split end state.
}

int SharedResourceModule_Stub::Step_RemoveResources(uint64_t* delete_list, uint32_t size) {
    std::vector<SharedResourceRemoveDescription> remove_list;
    for (uint32_t i=0; i<size; i++) {
        AddRemoveDescription(remove_list, delete_list[i], true, RET_ERR);
    }
    return RemoveSharedResource(remove_list);
}
void SharedResourceModule_Stub::Step_GetUnprocessed(uint32_t batch_max_size) {
    std::vector<SharedResourceReferenceOperation> unprocessed_operations;
    GetUnprocessedOperations(batch_max_size, unprocessed_operations);
    return;
}
rocksdb::Status SharedResourceModule_Stub::Step_SetProcessed(uint64_t processed_lsn) {
    return SetLastProcessedOperationLSN(processed_lsn);
}


//
// Testcases:
//

// Test encoding and decoding SharedResourceFilterOperation
TEST(SharedResourceFilterOperation, EncodeDecode)
{
    char* buf = nullptr;
    uint32_t buf_size = 0;

    srand((unsigned)time(NULL));

    SharedResourceFilterOperation oper;
    oper.SetType(SharedResourceOperationType::AddFilter);
    oper.SetResource(Uint64ToSharedResourceId(rand()));
    // 1) test EncodeTo()
    buf_size = oper.EncodeTo(buf);
    ASSERT_EQ(oper.GetEncodeLength(), buf_size);
    // 2) test DecodeFrom()
    LogRecord record(LogRecordType::SharedResourceFilterLog, rocksdb::Slice(buf, buf_size));
    record.SetLsn(1234);
    oper.SetLsn(1234);
    SharedResourceFilterOperation* oper2 = SharedResourceFilterOperation::DecodeFrom(record);
    ASSERT_TRUE((*oper2) == oper);
    delete buf;
    delete oper2;
}

// Test encoding and decoding SharedResourceReferenceOperation
TEST(SharedResourceReferenceOperation, EncodeDecode)
{
    char* buf = nullptr;
    uint32_t buf_size = 0;

    srand((unsigned)time(NULL));

    SharedResourceReferenceOperation oper;
    oper.SetType(SharedResourceOperationType::AddFilter);
    oper.SetResource(Uint64ToSharedResourceId(rand()));
    oper.SetParentChunkId(rand());
    oper.SetChildChunkId(rand());
    // 1) test EncodeTo()
    buf_size = oper.EncodeTo(buf);
    ASSERT_EQ(oper.GetEncodeLength(), buf_size);
    // 2) test DecodeFrom()
    LogRecord record(LogRecordType::LoadBalancingTransactionLog, rocksdb::Slice(buf, buf_size));
    record.SetLsn(1234);
    oper.SetLsn(1234);
    SharedResourceReferenceOperation* oper2 = SharedResourceReferenceOperation::DecodeFrom(record);
    ASSERT_TRUE((*oper2) == oper);
    delete buf;
    delete oper2;
}

// Test encoding and decoding SetDescription
TEST(SplitDescription, EncodeDecode)
{
    SplitDescription description1;
    SplitDescription description2;
    description1.parentChunkId = rand();
    description1.childChunkId = rand();
    description1.originalKeyLow = KEY_BSON("00000");
    description1.originalKeyHigh = KEY_BSON("12345");
    description1.splitPointKey = KEY_BSON("10000");
    description1.rightDbPathWithRootPlog = "11111";

    int buf_size = description1.GetEncodeLength();
    char* buf = new char[buf_size];
    description1.EncodeToBuf(buf);
    description2.DecodeFromBuf(buf);
    ASSERT_TRUE(description1 == description2);
}

// Test encoding and decoding SplitOperation
TEST(SplitOperation, EncodeDecode)
{
    char* buf = nullptr;
    uint32_t buf_size = 0;

    srand((unsigned)time(NULL));
    // EncodeTo
    std::shared_ptr<SplitDescription> description = std::make_shared<SplitDescription>();
    SplitOperation oper;
    oper.SetType(SharedResourceOperationType::AddFilter);
    oper.SetId(rand());
    oper.SetDescription(description);
    buf_size = oper.EncodeTo(buf);
    // DecodeFrom
    LogRecord record(LogRecordType::LoadBalancingTransactionLog, rocksdb::Slice(buf, buf_size));
    record.SetLsn(1234);
    oper.SetLsn(1234);
    SplitOperation* oper2 = SplitOperation::DecodeFrom(record);
    ASSERT_TRUE((*oper2) == oper);
    delete buf;
    delete oper2;
}

// Test encoding and decoding ProcessedLSNOperation
TEST(ProcessedLSNOperation, EncodeDecode)
{
    char* buf = nullptr;
    uint32_t buf_size = 0;

    srand((unsigned)time(NULL));

    ProcessedLSNOperation oper;
    oper.SetType(SharedResourceOperationType::AddFilter);
    oper.SetProcessedLSN(rand());
    // 1) test EncodeTo()
    buf_size = oper.EncodeTo(buf);
    ASSERT_EQ(oper.GetEncodeLength(), buf_size);
    // 2) test DecodeFrom()
    LogRecord record(LogRecordType::LoadBalancingTransactionLog, rocksdb::Slice(buf, buf_size));
    record.SetLsn(1234);
    oper.SetLsn(1234);
    ProcessedLSNOperation* oper2 = ProcessedLSNOperation::DecodeFrom(record);
    ASSERT_TRUE((*oper2) == oper);
    delete buf;
    delete oper2;
}

// Test a successful split transaction.
TEST(SharedResource, SplitTransactionOK)
{
    // Init the stub of shared resource module.
    SharedResourceModule_Stub shared_resource_module;
    shared_resource_module.Init();
    ResetLsnStub();

    int ret = RET_OK;
    rocksdb::Status s;
    std::vector<SharedResourceId> register_list;
    std::vector<SharedResourceRemoveDescription> remove_list;
// 1 Before the split
    // 1.1) Deleting non-shared resource is permitted before the split.
    AddRemoveDescription(remove_list, 5, true, RET_ERR);
    ret = shared_resource_module.RemoveSharedResource(remove_list);
    ASSERT_TRUE(ret == RET_OK);
    ASSERT_TRUE(CheckRemoveResult(remove_list, 5, false, RET_OK));
    // 1.2) Getting unprocessed operations before the split. (No unprocessed operations.)
    std::vector<SharedResourceReferenceOperation> unprocessed_operations;
    uint32_t batch_max_size = 5;
    shared_resource_module.GetUnprocessedOperations(batch_max_size, unprocessed_operations);
    ASSERT_TRUE(unprocessed_operations.empty());
    ASSERT_TRUE(shared_resource_module.CheckLogRecordNumber(0,0,0));
    ASSERT_TRUE(shared_resource_module.CheckOperationNumber(0,0));
// 2 Start split.
    std::shared_ptr<SplitDescription> description = std::make_shared<SplitDescription>();
    s = shared_resource_module.StartSplit(description);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(shared_resource_module.CheckLogRecordNumber(1,0,1));
    ASSERT_TRUE(shared_resource_module.CheckOperationNumber(0,1));
    // 2.2) Register shared resources during the split.
    SharedResourceIds resources;
    resources.push_back(Uint64ToSharedResourceId(20));
    resources.push_back(Uint64ToSharedResourceId(11));
    resources.push_back(Uint64ToSharedResourceId(18));
    resources.push_back(Uint64ToSharedResourceId(13));
    resources.push_back(Uint64ToSharedResourceId(16));
    resources.push_back(Uint64ToSharedResourceId(15));
    resources.push_back(Uint64ToSharedResourceId(14));
    resources.push_back(Uint64ToSharedResourceId(17));
    resources.push_back(Uint64ToSharedResourceId(12));
    resources.push_back(Uint64ToSharedResourceId(19));
    ret = shared_resource_module.RegisterSharedResource(resources);
    ASSERT_TRUE(ret==RET_OK);
    ASSERT_TRUE(shared_resource_module.CheckLogRecordNumber(11,0,11));
    ASSERT_TRUE(shared_resource_module.CheckOperationNumber(0,11));
    // 2.3) It is not permitted to delete shared resource that dosen't belongs to current split.
    remove_list.clear();
    AddRemoveDescription(remove_list, 1, true, RET_ERR);
    AddRemoveDescription(remove_list, 9, true, RET_ERR);
    AddRemoveDescription(remove_list, 21, true, RET_ERR);
    AddRemoveDescription(remove_list, 10, true, RET_ERR);
    AddRemoveDescription(remove_list, 20, false, RET_ERR);
    AddRemoveDescription(remove_list, 15, false, RET_ERR);
    AddRemoveDescription(remove_list, 11, false, RET_ERR);
    ret = shared_resource_module.RemoveSharedResource(remove_list);
    ASSERT_TRUE(ret==RET_ERR);
    ASSERT_TRUE(CheckRemoveResult(remove_list, 1, true, RET_ERR));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 9, true, RET_ERR));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 21, true, RET_ERR));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 10, true, RET_ERR));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 20, false, RET_ERR));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 15, false, RET_ERR));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 11, false, RET_ERR));
    // 2.4) Getting unprocessed operations during the split. (No unprocessed operations.)
    unprocessed_operations.clear();
    batch_max_size = 5;
    shared_resource_module.GetUnprocessedOperations(batch_max_size, unprocessed_operations);
    ASSERT_TRUE(unprocessed_operations.empty());
    ASSERT_TRUE(shared_resource_module.CheckLogRecordNumber(11,0,11));
    ASSERT_TRUE(shared_resource_module.CheckOperationNumber(0,11));
// 3 Write shared resource reference logs to right side.
    SharedResourceModule_Stub right_module;
    right_module.Init();
    s = shared_resource_module.WriteSharedResourceLogToRight(right_module);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(right_module.CheckLogRecordNumber(20,10,10));
// 4 Commit split.
    s = shared_resource_module.CommitSplit();
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(shared_resource_module.CheckLogRecordNumber(22,10,12));
    ASSERT_TRUE(shared_resource_module.CheckOperationNumber(10,10)); // StartSplit and CommitSplit in pair will be deleted.
// 5 After split.
    // 4.1) It is permitted to delete shared resource after split.
    remove_list.clear();
    AddRemoveDescription(remove_list, 15, false, RET_ERR);
    AddRemoveDescription(remove_list, 20, false, RET_ERR);
    AddRemoveDescription(remove_list, 10, true, RET_ERR);
    AddRemoveDescription(remove_list, 11, false, RET_ERR);
    AddRemoveDescription(remove_list, 21, true, RET_ERR);
    ret = shared_resource_module.RemoveSharedResource(remove_list);
    ASSERT_TRUE(ret==RET_OK);
    ASSERT_TRUE(CheckRemoveResult(remove_list, 15, true, RET_OK));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 20, true, RET_OK));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 10, false, RET_OK));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 11, true, RET_OK));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 21, false, RET_OK));
    ASSERT_TRUE(shared_resource_module.CheckLogRecordNumber(28,13,15));
    ASSERT_TRUE(shared_resource_module.CheckOperationNumber(7,13));
    // 4.3) Getting unprocessed operations after the split.
    //      (Some unprocessed operations have been generated by the above split.)
    unprocessed_operations.clear();
    batch_max_size = 5;
    shared_resource_module.GetUnprocessedOperations(batch_max_size, unprocessed_operations);
    ASSERT_TRUE(batch_max_size == unprocessed_operations.size());
    // 4.4) Set processed LSN.
    s = shared_resource_module.SetLastProcessedOperationLSN(6);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(shared_resource_module.CheckLogRecordNumber(29,13,16));
    ASSERT_TRUE(shared_resource_module.CheckOperationNumber(7,8));
// 6 Replay log.
    shared_resource_module.Init();
    ASSERT_TRUE(shared_resource_module.CheckLogRecordNumber(29,13,16));
    ASSERT_TRUE(shared_resource_module.CheckOperationNumber(7,8));

}

// Test a failed split transaction.
TEST(SharedResource, SplitTransactionRollback)
{
    // Init the stub of shared resource module.
    SharedResourceModule_Stub shared_resource_module;
    shared_resource_module.Init();
    ResetLsnStub();

    int ret = RET_OK;
    rocksdb::Status s;
    std::vector<SharedResourceId> register_list;
    std::vector<SharedResourceRemoveDescription> remove_list;
// 1 Start split.
    std::shared_ptr<SplitDescription> description = std::make_shared<SplitDescription>();
    s = shared_resource_module.StartSplit(description);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(shared_resource_module.CheckLogRecordNumber(1,0,1));
    ASSERT_TRUE(shared_resource_module.CheckOperationNumber(0,1));
    // 2.2) Register shared resources during the split.
    SharedResourceIds resources;
    resources.push_back(Uint64ToSharedResourceId(20));
    resources.push_back(Uint64ToSharedResourceId(11));
    resources.push_back(Uint64ToSharedResourceId(18));
    resources.push_back(Uint64ToSharedResourceId(13));
    resources.push_back(Uint64ToSharedResourceId(16));
    resources.push_back(Uint64ToSharedResourceId(15));
    resources.push_back(Uint64ToSharedResourceId(14));
    resources.push_back(Uint64ToSharedResourceId(17));
    resources.push_back(Uint64ToSharedResourceId(12));
    resources.push_back(Uint64ToSharedResourceId(19));
    ret = shared_resource_module.RegisterSharedResource(resources);
    ASSERT_TRUE(ret==RET_OK);
    ASSERT_TRUE(shared_resource_module.CheckLogRecordNumber(11,0,11));
    ASSERT_TRUE(shared_resource_module.CheckOperationNumber(0,11));
    // 2.3) It is not permitted to delete shared resource that dosen't belongs to current split.
    remove_list.clear();
    AddRemoveDescription(remove_list, 1, true, RET_ERR);
    AddRemoveDescription(remove_list, 9, true, RET_ERR);
    AddRemoveDescription(remove_list, 21, true, RET_ERR);
    AddRemoveDescription(remove_list, 10, true, RET_ERR);
    AddRemoveDescription(remove_list, 20, false, RET_ERR);
    AddRemoveDescription(remove_list, 15, false, RET_ERR);
    AddRemoveDescription(remove_list, 11, false, RET_ERR);
    ret = shared_resource_module.RemoveSharedResource(remove_list);
    ASSERT_TRUE(ret==RET_ERR);
    ASSERT_TRUE(CheckRemoveResult(remove_list, 1, true, RET_ERR));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 9, true, RET_ERR));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 21, true, RET_ERR));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 10, true, RET_ERR));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 20, false, RET_ERR));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 15, false, RET_ERR));
    ASSERT_TRUE(CheckRemoveResult(remove_list, 11, false, RET_ERR));
    // 2.4) Getting unprocessed operations during the split. (No unprocessed operations.)
    std::vector<SharedResourceReferenceOperation> unprocessed_operations;
    uint32_t batch_max_size = 5;
    shared_resource_module.GetUnprocessedOperations(batch_max_size, unprocessed_operations);
    ASSERT_TRUE(unprocessed_operations.empty());
    ASSERT_TRUE(shared_resource_module.CheckLogRecordNumber(11,0,11));
    ASSERT_TRUE(shared_resource_module.CheckOperationNumber(0,11));
// 2 Write shared resource reference logs to right side.
    SharedResourceModule_Stub right_module;
    right_module.Init();
    s = shared_resource_module.WriteSharedResourceLogToRight(right_module);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(right_module.CheckLogRecordNumber(20,10,10));
// 3 Rollback split.
    s = shared_resource_module.RollbackSplit();
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(shared_resource_module.CheckLogRecordNumber(12,0,12));
    ASSERT_TRUE(shared_resource_module.CheckOperationNumber(0,0));
// 4 Replay log.
    shared_resource_module.Init();
    ASSERT_TRUE(shared_resource_module.CheckLogRecordNumber(12,0,12));
    ASSERT_TRUE(shared_resource_module.CheckOperationNumber(0,0));

}

// Test more than one split transactions of the left chunk.
TEST(SharedResource, SplitTransactionMultiOK)
{
    // Init the stub of shared resource module.
    SharedResourceModule_Stub left_module;
    SharedResourceModule_Stub right_module1;
    SharedResourceModule_Stub right_module2;
    left_module.Init();
    ResetLsnStub();

    // Succeed to do the first split of the left chunk.
    std::shared_ptr<SplitDescription> description1 = std::make_shared<SplitDescription>(1,2,"", "", "a", "");
    uint64_t register_list1[] = {20,1,18,3,16,5,14,7,12,9,10,11,8,13,6,15,4,17,2,19};
    uint32_t size = sizeof(register_list1) / sizeof(uint64_t);
    left_module.Step_SplitSuccess(description1, register_list1, size, right_module1);
    uint64_t delete_list1[] = {16,11,3,6, 1000};
    size = sizeof(delete_list1) / sizeof(uint64_t);
    left_module.Step_RemoveResources(delete_list1, size);
    left_module.Step_GetUnprocessed(7);
    left_module.Step_SetProcessed(8);

    // Failed to do the second split of the left chunk.
    std::shared_ptr<SplitDescription> description2 = std::make_shared<SplitDescription>(1,3,"", "a", "M", "");
    uint64_t register_list2[] = {20,11,18,13,16,15,14,17,12,19,  // the part same as that of the first split
                                 27,28,21,22,24,23,26,25,30,29}; // the part different from the first split
    size = sizeof(register_list2) / sizeof(uint64_t);
    left_module.Step_SplitRollback(description2, register_list2, size);

    // Succeed to retry the second split of the left chunk.
    size = sizeof(register_list2) / sizeof(uint64_t);
    left_module.Step_SplitSuccess(description2, register_list2, size, right_module2);
    uint64_t delete_list2[] = {1,7,10,8,5,  // Some IDs only appeared in the first split.
                               20,13,15,12, // Some IDs appeared in both splites.
                               28,23,25,26, // Some IDs only appeared in the second split.
                               1000,2000};  // Some IDs didn't appear in either split.
    size = sizeof(delete_list2) / sizeof(uint64_t);
    left_module.Step_RemoveResources(delete_list2, size);
    left_module.Step_GetUnprocessed(7);
    left_module.Step_SetProcessed(15);

    //Check result.
    /***************************************************************************
    -------------------------------------------------------------------------
    Log index    OperType             Filter          Oper
                                    log/memory     log/memory
    -------------------------------------------------------------------------
    0            StartSplit
    1~20         AddRef
    21~40        AddFilter
    41           CommitSplit
    42~49        RmvFilter + RmvRef
    50           Processed(8)         24/16          27/16

    51           StartSplit
    52~71        AddRef
    72           RollbackSplit        0/0            22/0

    73           StartSplit
    74~93        AddRef
    94~113       AddFilter
    114          CommitSplit
    115~140      RmvFilter + RmvRef
    141          Processed(15)        33/7           36/27
    -------------------------------------------------------------------------
                 142                  57/23          85/43
    -------------------------------------------------------------------------
    ***************************************************************************/
    ASSERT_TRUE(left_module.CheckLogRecordNumber(142,57,85));
    ASSERT_TRUE(left_module.CheckOperationNumber(23,43));
    //Replay log and check result.
    left_module.Init();
    ASSERT_TRUE(left_module.CheckLogRecordNumber(142,57,85));
    ASSERT_TRUE(left_module.CheckOperationNumber(23,43));

}

// Test recover the missing split end state, include of CommitSplit or RollbackSplit.
TEST(SharedResource, SplitTransactionRollbackRecovery)
{
    // Init the stub of shared resource module.
    SharedResourceModule_Stub left_module;
    SharedResourceModule_Stub right_module;
    ChunkMetadata chunk_meta_data;
    ChunkType& chunk_type = const_cast<ChunkType&>(chunk_meta_data.GetChunk());
    chunk_type.setMin(KEY_BSON(""));
    chunk_type.setMax(KEY_BSON(""));
    left_module.Init(&chunk_meta_data);
    ResetLsnStub();

    // Do a split missing the end state.
    std::shared_ptr<SplitDescription> description = std::make_shared<SplitDescription>(1,2,"", "", "a", "");
    uint64_t register_list[] = {20,1,18,3,16,5,14,7,12,9,10,11,8,13,6,15,4,17,2,19};
    uint32_t size = sizeof(register_list) / sizeof(uint64_t);
    left_module.Step_SplitMissingEndState(description, register_list, size, right_module);

    // Unchanged key range means the split is not successful.
    // Replay log.
    left_module.Init();
    ASSERT_TRUE(left_module.CheckLogRecordNumber(21,0,21));
    ASSERT_TRUE(left_module.CheckOperationNumber(0,0));

    // Next operation tirgger recovery of the split end state.
    left_module.SetLastProcessedOperationLSN(6);

    // Check result.
    ASSERT_TRUE(left_module.CheckLogRecordNumber(23,0,23));
    ASSERT_TRUE(left_module.CheckOperationNumber(0,0));
    // Replay log and check result.
    left_module.Init();
    ASSERT_TRUE(left_module.CheckLogRecordNumber(23,0,23));
    ASSERT_TRUE(left_module.CheckOperationNumber(0,0));
}

// Test recover the missing split end state, include of CommitSplit or RollbackSplit.
TEST(SharedResource, SplitTransactionCommitRecovery)
{
    // Init the stub of shared resource module.
    SharedResourceModule_Stub left_module;
    SharedResourceModule_Stub right_module;
    ChunkMetadata chunk_meta_data;
    ChunkType& chunk_type = const_cast<ChunkType&>(chunk_meta_data.GetChunk());
    chunk_type.setMin(KEY_BSON(""));
    chunk_type.setMax(KEY_BSON(""));
    left_module.Init(&chunk_meta_data);
    ResetLsnStub();

    // Do a split missing the end state.
    std::shared_ptr<SplitDescription> description = std::make_shared<SplitDescription>(1,2,"", "", "a", "");
    uint64_t register_list[] = {20,1,18,3,16,5,14,7,12,9,10,11,8,13,6,15,4,17,2,19};
    uint32_t size = sizeof(register_list) / sizeof(uint64_t);
    left_module.Step_SplitMissingEndState(description, register_list, size, right_module);

    // New key range means the split is successful.
    chunk_type.setMin(KEY_BSON(""));
    chunk_type.setMax(KEY_BSON("a"));
    // Replay log.
    left_module.Init(&chunk_meta_data);
    ASSERT_TRUE(left_module.CheckLogRecordNumber(21,0,21));
    ASSERT_TRUE(left_module.CheckOperationNumber(20,20));

    // Next operation tirgger recovery of the split end state.
    uint64_t delete_list1[] = {16,11,3,6, 1000};
    size = sizeof(delete_list1) / sizeof(uint64_t);
    left_module.Step_RemoveResources(delete_list1, size);

    // Check result.
    ASSERT_TRUE(left_module.CheckLogRecordNumber(50,24,26));
    ASSERT_TRUE(left_module.CheckOperationNumber(16,24));
    // Replay log and check result.
    left_module.Init();
    ASSERT_TRUE(left_module.CheckLogRecordNumber(50,24,26));
    ASSERT_TRUE(left_module.CheckOperationNumber(16,24));

}

TEST(SharedResourceMemLog, TwoSplites)
{
    // Init the stub of shared resource module.
    SharedResourceModule_Stub left_module;
    SharedResourceModule_Stub right_module1;
    SharedResourceModule_Stub right_module2;
    left_module.Init();
    ResetLsnStub();

    // Succeed to do the first split of the left chunk.
    std::shared_ptr<SplitDescription> description1 = std::make_shared<SplitDescription>(1,2,"", "", "a", "");
    uint64_t register_list1[] = {20,1,18,3,16,5,14,7,12,9,10,11,8,13,6,15,4,17,2,19};
    uint32_t size = sizeof(register_list1) / sizeof(uint64_t);
    left_module.Step_SplitSuccess(description1, register_list1, size, right_module1);
    uint64_t delete_list1[] = {16,11,3,6, 1000};
    size = sizeof(delete_list1) / sizeof(uint64_t);
    left_module.Step_RemoveResources(delete_list1, size);
    left_module.Step_GetUnprocessed(7);
    left_module.Step_SetProcessed(8);

    // Failed to do the second split of the left chunk.
    std::shared_ptr<SplitDescription> description2 = std::make_shared<SplitDescription>(1,3,"", "a", "M", "");
    uint64_t register_list2[] = {20,11,18,13,16,15,14,17,12,19,  // the part same as that of the first split
                                 27,28,21,22,24,23,26,25,30,29}; // the part different from the first split
    size = sizeof(register_list2) / sizeof(uint64_t);
    left_module.Step_SplitRollback(description2, register_list2, size);

    // Succeed to retry the second split of the left chunk.
    size = sizeof(register_list2) / sizeof(uint64_t);
    left_module.Step_SplitSuccess(description2, register_list2, size, right_module2);
    uint64_t delete_list2[] = {1,7,10,8,5,  // Some IDs only appeared in the first split.
                               20,13,15,12, // Some IDs appeared in both splites.
                               28,23,25,26, // Some IDs only appeared in the second split.
                               1000,2000};  // Some IDs didn't appear in either split.
    size = sizeof(delete_list2) / sizeof(uint64_t);
    left_module.Step_RemoveResources(delete_list2, size);
    left_module.Step_GetUnprocessed(7);
    left_module.Step_SetProcessed(15);
    ASSERT_TRUE(left_module.CheckLogRecordNumber(142,57,85));
    ASSERT_TRUE(left_module.CheckOperationNumber(23,43));

    left_module.DumpSharedResourceList("/CloudBuild/test/memlog_out");

    left_module.Init();
    left_module.DumpSharedResourceList("/CloudBuild/test/memlog_out2");
}

TEST(SharedResourceMemLog, Perfermance)
{
    SharedResourceModule_Stub left_module;
    left_module.Init();
    SharedResourceMemLogVector& mem_logs = left_module.GetMemLogs();

    // loop of writing SharedResourceMemLog.
    for (uint64_t i = 0; i < (16*1024); i++) {
        SharedResourceMemLog mem_log(SharedResourceOperationType::RemoveFilter);
        mem_log.SetResource(Uint64ToSharedResourceId(i+1));
        mem_logs.append(mem_log);
    }

    // output all SharedResourceMemLog.
    std::ofstream perf_out("/CloudBuild/test/perf_out");
    perf_out << "---- Begin at " << GetCurrentTimeStr() << std::endl;
    for (auto mem_log = mem_logs.begin(); mem_log != mem_logs.end(); mem_logs.inc(mem_log)) {
        mem_log->DumpSharedResourceRecord(perf_out);
    }
    perf_out << "---- End at   " << GetCurrentTimeStr() << std::endl;

    perf_out << "---- CompareTest Begin at " << GetCurrentTimeStr() << std::endl;
    uint64_t vvv = 1;
    for (auto mem_log = mem_logs.begin(); mem_log != mem_logs.end(); mem_logs.inc(mem_log)) {
        perf_out << "[RmvFilter & RmvRef] lsn:" << vvv \
                 << ", type:" << "RmvFilter" \
                 << ", resource:(" << vvv << "," << 0U \
                 << ", shared:" << true << "\n";
    }
    perf_out << "---- CompareTest End at   " << GetCurrentTimeStr() << std::endl;    perf_out.close();
}


#define  SECOND_VS_MICROSECOND  1000000
int CompareTimeVal(struct timeval tv1, struct timeval tv2) {
    if (tv1.tv_sec > tv2.tv_sec) {
        return 1;
    }else if (tv1.tv_sec < tv2.tv_sec) {
        return -1;
    }else if (tv1.tv_usec > tv2.tv_usec) {
        return 1;
    }else if (tv1.tv_usec < tv2.tv_usec) {
        return -1;
    }else {
        return 0;
    }
}
struct timeval Duration_Sub(struct timeval tv1, struct timeval tv2) {
    if (CompareTimeVal(tv1, tv2) < 0) {
        return Duration_Sub(tv2, tv1);
    }

    struct timeval tv = {0,0};
    tv.tv_sec = tv1.tv_sec - tv2.tv_sec;
    if (tv1.tv_usec >= tv2.tv_usec) {
        tv.tv_usec = tv1.tv_usec - tv2.tv_usec;
    }else {
        tv.tv_sec--;
        tv.tv_usec = (tv1.tv_usec + SECOND_VS_MICROSECOND) - tv2.tv_usec;
    }
    return tv;
}
struct timeval Duration_Add(struct timeval tv1, struct timeval tv2) {
    struct timeval tv = {0,0};
    tv.tv_sec = tv1.tv_sec + tv2.tv_sec;
    tv.tv_usec = tv1.tv_usec + tv2.tv_usec;
    if (tv.tv_usec >= SECOND_VS_MICROSECOND) {
        tv.tv_sec++;
        tv.tv_usec -= SECOND_VS_MICROSECOND;
    }
    return tv;
}
struct timeval Duration_Avg(struct timeval tv, uint32_t num) {
    struct timeval tv_avg = tv;
    tv_avg.tv_sec = tv.tv_sec / num;
    time_t diff = tv.tv_sec % num;
    tv_avg.tv_usec = ((diff * SECOND_VS_MICROSECOND) + tv.tv_usec) / num;
    return tv_avg;
}

void RemoveSharedResources_Method0(SharedResourceIds& resources, SharedResourceIds& resources_rmv) {
    std::vector<SharedResourceId>::iterator it;
    for (auto resource : resources_rmv) {
        it = std::lower_bound(resources.begin(), resources.end(), resource, CompareResourceId);
        if (it == resources.end() || (!EqualResourceId((*it), resource))) {
            // Never!
            assert(false);
        }
        resources.erase(it);
    }
}
void RemoveSharedResources_Method1(SharedResourceIds& resources, SharedResourceIds& resources_rmv) {
    std::vector<SharedResourceId>::iterator it;
    uint32_t it_idx = 0;
    std::sort(resources_rmv.begin(), resources_rmv.end(), CompareResourceId);
    for (auto resource : resources_rmv) {
        it = std::lower_bound((resources.begin()+it_idx), resources.end(), resource, CompareResourceId);
        it_idx = (it - resources.begin());
        if (it == resources.end() || (!EqualResourceId((*it), resource))) {
            // Never!
            assert(false);
        }
        resources.erase(it);
    }
}
void RemoveSharedResources_Method2(SharedResourceIds& resources, SharedResourceIds& resources_rmv) {
    std::vector<SharedResourceId>::iterator prev = resources.begin();
    std::vector<SharedResourceId>::iterator next = resources.end();
    SharedResourceIds resources_result;

    // Sort the shared resources to be deleted in order that the following algorithm can work well.
    std::sort(resources_rmv.begin(), resources_rmv.end(), CompareResourceId);

    // The logic before calling this function make it sure that all the resources in 'resources_rmv' must be in
    // 'shared_resources_'.
    resources_result.reserve(resources.size() - resources_rmv.size());

    // Skip the resources to be removed one by one, and copy all the remained resources to the 'resources_result'.
    for (auto resource : resources_rmv) {
        next = std::lower_bound(prev, resources.end(), resource, CompareResourceId);
        if (next == resources.end() || (!EqualResourceId((*next), resource))) {
            // Never!
            assert(false);
        }
        // Copy the segment between two adjacent resources to be removed to the tail of the temporary vector.
        if (next != prev) {
            resources_result.insert(resources_result.end(), prev, next);
        }
        prev = (next + 1);
    }
    if (prev != resources.end()) {
        // Copy the last segment to the tail of 'resources_result'.
        resources_result.insert(resources_result.end(), prev, resources.end());
    }

    // Save the result after remove.
    resources.swap(resources_result);
}
TEST(SharedResourcePerfermance, Remove)
{
    const uint32_t total_count = 10;
    struct timeval tv_begin;
    struct timeval tv_end;
    struct timeval tv_duration;
    const uint32_t base_num_array[] = {50000, 20000, 10000, 5000, 2000, 1000, 900, 800, 700, 600, 500, 200, 100, 50, 20, 10, 5, 2, 1};
    const uint32_t base_num = sizeof(base_num_array) / sizeof(uint32_t);
    const uint32_t rmv_size_array[] = {500,200,100,50,20,10,7,6,5,2,1};
    const uint32_t case_num = sizeof(rmv_size_array) / sizeof(uint32_t);
    struct timeval tv_total0[case_num];
    struct timeval tv_total1[case_num];
    struct timeval tv_total2[case_num];

    for (uint32_t base_idx = 0; base_idx < base_num; base_idx++) {
        const uint32_t stable_num = base_num_array[base_idx];

        for (uint32_t case_idx = 0; case_idx < case_num; case_idx++) {
            const uint32_t rmv_size = rmv_size_array[case_idx];
            if (rmv_size > stable_num) {
                printf("\n---- Case%u: Remove %u from %u : Ignore.",
                    case_idx, rmv_size_array[case_idx], stable_num);
                continue;
            }

            tv_total0[case_idx].tv_sec = 0;
            tv_total0[case_idx].tv_usec = 0;
            tv_total1[case_idx].tv_sec = 0;
            tv_total1[case_idx].tv_usec = 0;
            tv_total2[case_idx].tv_sec = 0;
            tv_total2[case_idx].tv_usec = 0;

            for (uint32_t count = 0; count < total_count; count++) {
                // Prepare base elements.
                SharedResourceIds resources0;
                SharedResourceIds resources1;
                SharedResourceIds resources2;
                for (uint32_t i = 0; i < stable_num; i++) {
                    SharedResourceId resource = Uint64ToSharedResourceId(i);
                    resources0.insert(resources0.end(), resource);
                }
                std::sort(resources0.begin(), resources0.end(), CompareResourceId);
                resources1.insert(resources1.end(), resources0.begin(), resources0.end());
                resources2.insert(resources2.end(), resources0.begin(), resources0.end());

                // Prepare the elements to be removed.
                SharedResourceIds resources_rmv0;
                SharedResourceIds resources_rmv1;
                SharedResourceIds resources_rmv2;
                const uint32_t step = stable_num/rmv_size;
                const uint32_t middle = (step - (step/2));
                for (uint32_t i = rmv_size; i > 0; i--) {
                    SharedResourceId resource = Uint64ToSharedResourceId((i*step) - middle);
                    resources_rmv0.insert(resources_rmv0.end(), resource);
                }
                resources_rmv1.insert(resources_rmv1.end(), resources_rmv0.begin(), resources_rmv0.end());
                resources_rmv2.insert(resources_rmv2.end(), resources_rmv0.begin(), resources_rmv0.end());

                uint32_t old_size = 0;
                uint32_t rmv_num = 0;

                // Case0: removed one by one by normal method.
                gettimeofday(&tv_begin, NULL);
                old_size = resources0.size();
                RemoveSharedResources_Method1(resources0, resources_rmv0);
                rmv_num = old_size - resources0.size();
                if (rmv_num != rmv_size) {
                    printf("\n-------- Case%u.0: Remove %u from %u : abnormal, rmv_num(%u) != rmv_size(%u)",
                        case_idx, rmv_size_array[case_idx], stable_num, rmv_num, rmv_size);
                }
                gettimeofday(&tv_end, NULL);
                tv_duration = Duration_Sub(tv_end, tv_begin);
                tv_total0[case_idx] = Duration_Add(tv_total0[case_idx], tv_duration);

                // Case1: removed one by one by normal method.
                gettimeofday(&tv_begin, NULL);
                old_size = resources1.size();
                RemoveSharedResources_Method0(resources1, resources_rmv1);
                rmv_num = old_size - resources1.size();
                if (rmv_num != rmv_size) {
                    printf("\n-------- Case%u.1: Remove %u from %u : abnormal, rmv_num(%u) != rmv_size(%u)",
                        case_idx, rmv_size_array[case_idx], stable_num, rmv_num, rmv_size);
                }
                gettimeofday(&tv_end, NULL);
                tv_duration = Duration_Sub(tv_end, tv_begin);
                tv_total1[case_idx] = Duration_Add(tv_total1[case_idx], tv_duration);

                // Case2: removed one by one by sorting and moving elements.
                gettimeofday(&tv_begin, NULL);
                old_size = resources2.size();
                RemoveSharedResources_Method2(resources2, resources_rmv2);
                rmv_num = old_size - resources2.size();
                if (rmv_num != rmv_size) {
                    printf("\n-------- Case%u.2: Remove %u from %u : abnormal, rmv_num(%u) != rmv_size(%u)",
                        case_idx, rmv_size_array[case_idx], stable_num, rmv_num, rmv_size);
                }
                gettimeofday(&tv_end, NULL);
                tv_duration = Duration_Sub(tv_end, tv_begin);
                tv_total2[case_idx] = Duration_Add(tv_total2[case_idx], tv_duration);
            }

            printf("\n---- Case%u: Remove %u from %u : The total duration of %d times is (%ld.%06ld, %ld.%06ld, %ld.%06ld).",
                case_idx, rmv_size_array[case_idx], stable_num, total_count,
                tv_total0[case_idx].tv_sec, tv_total0[case_idx].tv_usec,
                tv_total1[case_idx].tv_sec, tv_total1[case_idx].tv_usec,
                tv_total2[case_idx].tv_sec, tv_total2[case_idx].tv_usec);
        }
        printf("\n");
    }
}
TEST(SharedResourcePerfermance, RemoveStartSplit)
{
    std::vector<SharedResourceOperation*> vec;
    vec.resize(2000);    
}


}   //  namespace TransactionLog

}   //  namespace mongo



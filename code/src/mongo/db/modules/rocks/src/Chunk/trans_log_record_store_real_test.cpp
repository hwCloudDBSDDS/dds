
#include "TransLogRecordStore.h"
#include "mongo/unittest/unittest.h"
//#include "../../../public_inc/plog_client.h"
#include "/usr2/DFV-Rocks/util/random.h"
#include "i_shared_resource_manager.h"
//#include "../../../../common/include/error_code.h"

namespace mongo {
    namespace TransactionLog {

#define RET_OK (0)
#define RET_ERR (-1)

        // static bool BaseModuleInitFlag = false;
        static bool BaseModuleExitFlag = false;
        // db_pat h must end with "/plogcnt0"
        // use env plog

        std::unique_ptr<TransLogRecordStore> CreateRecordStore(std::string& chunk_id,
                                                               std::string& db_path,
                                                               rocksdb::Env** plog_env) {
            // rocksdb::PlogEnvOptions    env_options;
            // PlogClientStubOptions plog_client_options;

            // plog_client_options.db_path.clear();
            // plog_client_options.db_path = db_path + chunk_id;
            // plog_client_options.plog_path = plog_client_options.db_path;
            // plog_client_options.plog_append_no_sync = true;  //single host test , no need fsync
            // for plog append data  to speedup
            // int ret_code = libclient_init_x(nullptr, plog_client_options);
            // if (0 != ret_code) {
            //  fprintf(stderr, "Ret(%d).plog client init fail!\n", ret_code);
            //  exit(1);
            //}

            // if (!BaseModuleInitFlag) {
            //    if(BaseModuleInitial() != 0){
            //        fprintf(stderr, "Init BaseModule failed!\n");
            //        exit(1);
            //    }
            //    BaseModuleInitFlag = true;
            //}

            // env_options.specify_root_plog = false;
            // env_options.db_path.clear();
            // env_options.db_path = db_path;
            // env_options.db_path += chunk_id;
            // env_options.db_path += "/plogcnt0";

            // rocksdb::Status s = rocksdb::NewPlogEnv(env_options, plog_env);
            // if (!s.ok()) {
            //  fprintf(stderr, "create plog Env fail: %s\n", s.ToString().c_str());
            //  exit(1);
            //}

            // std::string db_path_tmp = db_path + chunk_id;

            // std::unique_ptr<TransLogRecordStore> trans_log_Record_store(new
            // TransLogRecordStore(*plog_env, db_path));

            // return trans_log_Record_store;
            return nullptr;
        }

        const SharedResourceId GetSharedResourceId(const uint64_t id) {
            SharedResourceId plog_id;
            uint32_t len1 = sizeof(plog_id);
            uint32_t len2 = sizeof(id);
            uint8_t* buf1 = reinterpret_cast<uint8_t*>(&plog_id);
            const uint8_t* buf2 = reinterpret_cast<const uint8_t*>(&id);
            uint32_t i = 0;
            for (; ((i < len1) && (i < len2)); i++) {
                buf1[len1 - (i + 1)] = buf2[len2 - (i + 1)];
            }
            for (; i < len1; i++) {
                buf1[len1 - (i + 1)] = 0;
            }
            return plog_id;
        }
        bool CheckQueryResult(const SharedResourceDescription& rmv_desc, const bool shared_flag) {
            return (rmv_desc.shared_flag == shared_flag);
        }
        bool CheckQueryResult(const std::vector<SharedResourceDescription>& rmv_desc_list,
                              const uint64_t id, const bool shared_flag) {
            SharedResourceId resource = GetSharedResourceId(id);
            for (auto desc : rmv_desc_list) {
                if (EqualResourceId(resource, desc.id)) {
                    return CheckQueryResult(desc, shared_flag);
                }
            }
            return false;
        }
        void AddQueryDescription(std::vector<SharedResourceDescription>& query_desc_list,
                                 const uint64_t id, const bool shared_flag) {
            SharedResourceDescription query_desc = {GetSharedResourceId(id), shared_flag};
            query_desc_list.push_back(query_desc);
        }
        void AddRemoveDescription(std::vector<SharedResourceId>& rmv_list, const uint64_t id) {
            rmv_list.push_back(GetSharedResourceId(id));
        }

        // env clean
        static const char* clean_cmd1 = "rm -rf /CloudBuild/transLogRecStore1/chunk1/translog/*";
        static const char* clean_cmd2 = "rm -rf /CloudBuild/transLogRecStore1/chunk2/translog/*";
        static const char* clean_cmd3 = "rm -rf /CloudBuild/transLogRecStore1/chunk3/translog/*";
        void PosixEnvClean() {
            int ret_code = 0;
            ret_code = system(clean_cmd1) + system(clean_cmd2) + system(clean_cmd3);
            if (0 != ret_code) {
                fprintf(stderr, "PosixEnvClean cmd exec fail");
            }
            fprintf(stderr, "PosixEnvClean cmd exec success");
        }

        // env clean
        static const char* clean_cmd = "rm -rf /CloudBuild/transLogRecStore/*";
        void PlogEnvClean() {
            int ret_code = 0;
            ret_code = system(clean_cmd);
            if (0 != ret_code) {
                fprintf(stderr, "PlogEnvClean cmd exec fail");
            }
            fprintf(stderr, "PlogEnvClean cmd exec success");
        }

        void DestroyEnv(rocksdb::Env* plog_env) {
            if (nullptr != plog_env) {
                delete plog_env;
            }

            // PlogEnvClean();
            // libclient_exit_x(nullptr);
            // if(BaseModuleExitFlag){
            //    BaseModuleDestroy();
            //}
        }

        void GetRandomString(rocksdb::Random* rnd, int length, rocksdb::Slice& key) {
            char base[63] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            int32_t number = 0;
            char* data = const_cast<char*>(key.data());
            // key begin with "abcdefghijklmnopqrstuvwxyz"
            number = rnd->Uniform(26);
            data[0] = base[number];

            for (int i = 0; i < length; i++) {
                number = rnd->Uniform(62);
                data[i] = base[number];
            }
            return;
        }

        rocksdb::Slice AllocateKey(std::unique_ptr<const char[]>* key_guard, int key_size) {
            char* data = new char[key_size];
            const char* const_data = data;
            key_guard->reset(const_data);
            return rocksdb::Slice(key_guard->get(), key_size);
        }

        // batchwrite by posix env
        TEST(TransLogRecordStoreTest, TransLogRecordStore_BatchWritePosixEnv) {
            rocksdb::Status s;
            std::vector<LogRecord> recordVec;
            std::string chunk_path = "chunk1";
            std::string chunk_path2 = "chunk2";
            std::string chunk_path3 = "chunk3";
            std::string db_path = "/CloudBuild/transLogRecStore1/";
            std::string path_tmp = db_path + chunk_path;
            std::string path_tmp2 = db_path + chunk_path2;
            std::string path_tmp3 = db_path + chunk_path3;

            rocksdb::Env* env = rocksdb::Env::Default();
            std::shared_ptr<SplitDescription> description = std::make_shared<SplitDescription>();

            // env clean
            PosixEnvClean();

            std::unique_ptr<TransLogRecordStore> trans_log_record_store(
                new TransLogRecordStore(env, path_tmp));
            // 1 Before the split
            SharedResourceOperationLogRecordProvider sro_provider;
            SharedResourceManager filter_manager;
            filter_manager.SetLogRecordProvider(&sro_provider);
            sro_provider.SetSharedResourceManager(&filter_manager);
            s = trans_log_record_store->RegisterProvider(sro_provider);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store->RegisterProvider(filter_manager);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store->Init();  // Init and replay empty logs
            ASSERT_TRUE(s.ok());

            std::vector<SharedResourceDescription> query_list;
            query_list.clear();
            AddQueryDescription(query_list, 5, true);
            s = filter_manager.OnResourcesQuery(query_list);
            ASSERT_TRUE(s.ok());
            ASSERT_TRUE(CheckQueryResult(query_list, 5, false));

            std::vector<SharedResourceId> rmv_list;
            rmv_list.clear();
            AddRemoveDescription(rmv_list, 5);
            s = filter_manager.OnResourcesDeletion(rmv_list);
            ASSERT_TRUE(s.ok());

            // 2 Start split.
            s = sro_provider.StartSplit(description);
            ASSERT_TRUE(s.ok());

            SharedResourceIds resources;
            int i = 0;
            int num = 30000;
            for (i = 0; i < num; i++) {
                resources.push_back(GetSharedResourceId(i));
            }

            s = sro_provider.RegisterSharedResources(resources);
            ASSERT_TRUE(s.ok());

            query_list.clear();
            AddQueryDescription(query_list, 21, false);
            AddQueryDescription(query_list, 20, false);
            AddQueryDescription(query_list, 15, false);
            s = filter_manager.OnResourcesQuery(query_list);
            ASSERT_TRUE(!s.ok());

            rmv_list.clear();
            AddRemoveDescription(rmv_list, 21);
            AddRemoveDescription(rmv_list, 20);
            AddRemoveDescription(rmv_list, 15);
            s = filter_manager.OnResourcesDeletion(rmv_list);
            ASSERT_TRUE(!s.ok());

            fprintf(stderr, "complete write (%d) record\r\n", num);

            // 3 Write shared resource reference logs to right side.
            // Prepare the instances of the classes to be tested.
            SharedResourceOperationLogRecordProvider right_sro_provider;
            SharedResourceManager right_filter_manager;
            std::unique_ptr<TransLogRecordStore> right_trans_log_record_store(
                new TransLogRecordStore(env, path_tmp2));
            right_filter_manager.SetLogRecordProvider(&right_sro_provider);
            right_sro_provider.SetSharedResourceManager(&right_filter_manager);
            right_trans_log_record_store->RegisterProvider(right_sro_provider);
            right_trans_log_record_store->RegisterProvider(right_filter_manager);
            right_trans_log_record_store->Init();  // Init and replay empty logs
            s = sro_provider.WriteSharedResourceLogToRight(&right_sro_provider);
            ASSERT_TRUE(s.ok());

            // 4 Commit split
            s = sro_provider.CommitSplit();
            ASSERT_TRUE(s.ok());
            // 5 After split.
            // 5.1) It is permitted to delete shared resource that dosen't belongs to current split.

            query_list.clear();
            AddQueryDescription(query_list, 100, false);
            AddQueryDescription(query_list, 86, false);
            s = filter_manager.OnResourcesQuery(query_list);
            ASSERT_TRUE(s.ok());
            ASSERT_TRUE(CheckQueryResult(query_list, 100, true));
            ASSERT_TRUE(CheckQueryResult(query_list, 86, true));

            rmv_list.clear();
            AddRemoveDescription(rmv_list, 100);
            AddRemoveDescription(rmv_list, 86);
            s = filter_manager.OnResourcesDeletion(rmv_list);
            ASSERT_TRUE(s.ok());

            s = sro_provider.SetLastProcessedOperationLSN(6);
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "complete CommitSplit\r\n");

            // 6 Replay log.
            // 6.1 clear mem
            sro_provider.Reset();
            filter_manager.Reset();
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store.reset();

            right_sro_provider.Reset();
            right_filter_manager.Reset();
            // close all file
            s = right_trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            right_trans_log_record_store.reset();

            // 6.2 replay left
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_test_left(
                new TransLogRecordStore(env, path_tmp));
            // init
            SharedResourceOperationLogRecordProvider sro_provider_test_left;
            SharedResourceManager filter_manager_test_left;
            filter_manager_test_left.SetLogRecordProvider(&sro_provider_test_left);
            sro_provider_test_left.SetSharedResourceManager(&filter_manager_test_left);
            s = trans_log_record_store_test_left->RegisterProvider(sro_provider_test_left);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store_test_left->RegisterProvider(filter_manager_test_left);
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "Reset and create chunk1\r\n");

            // replay translog
            s = trans_log_record_store_test_left->Init();
            ASSERT_TRUE(s.ok());
            sro_provider_test_left.Reset();
            filter_manager_test_left.Reset();
            // close all file
            s = trans_log_record_store_test_left->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store_test_left.reset();

            // 6.2 replay right
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_test_right(
                new TransLogRecordStore(env, path_tmp2));
            // init
            SharedResourceOperationLogRecordProvider sro_provider_test_right;
            SharedResourceManager filter_manager_test_right;
            filter_manager_test_right.SetLogRecordProvider(&sro_provider_test_right);
            sro_provider_test_right.SetSharedResourceManager(&filter_manager_test_right);
            s = trans_log_record_store_test_right->RegisterProvider(sro_provider_test_right);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store_test_right->RegisterProvider(filter_manager_test_right);
            ASSERT_TRUE(s.ok());

            // replay translog
            s = trans_log_record_store_test_right->Init();
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "Reset and create chunk2\r\n");

            // 7 split again
            std::shared_ptr<SplitDescription> description1 = std::make_shared<SplitDescription>();
            s = sro_provider_test_right.StartSplit(description1);
            ASSERT_TRUE(s.ok());

            // 7.1 Write shared resource reference logs to right side.
            SharedResourceOperationLogRecordProvider provider_test;
            SharedResourceManager filter_manager_test;
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_test(
                new TransLogRecordStore(env, path_tmp3));
            filter_manager_test.SetLogRecordProvider(&provider_test);
            provider_test.SetSharedResourceManager(&filter_manager_test);
            trans_log_record_store_test->RegisterProvider(provider_test);
            trans_log_record_store_test->RegisterProvider(filter_manager_test);
            trans_log_record_store_test->Init();  // Init and replay empty logs
            s = sro_provider_test_right.WriteSharedResourceLogToRight(&provider_test);
            ASSERT_TRUE(s.ok());

            s = sro_provider_test_right.CommitSplit();
            ASSERT_TRUE(s.ok());

            sro_provider_test_right.Reset();
            filter_manager_test_right.Reset();
            // close all file
            s = trans_log_record_store_test_right->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store_test_right.reset();

            provider_test.Reset();
            filter_manager_test.Reset();
            s = trans_log_record_store_test->CloseAllWriteFile();
            trans_log_record_store_test.reset();
            ASSERT_TRUE(s.ok());

            // 7.2 replay right
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_1(
                new TransLogRecordStore(env, path_tmp3));
            // init
            SharedResourceOperationLogRecordProvider sro_provider_1;
            SharedResourceManager filter_manager_1;
            filter_manager_1.SetLogRecordProvider(&sro_provider_1);
            sro_provider_1.SetSharedResourceManager(&filter_manager_1);
            s = trans_log_record_store_1->RegisterProvider(sro_provider_1);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store_1->RegisterProvider(filter_manager_1);
            ASSERT_TRUE(s.ok());

            // replay translog
            s = trans_log_record_store_1->Init();
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "Reset and create chunk3\r\n");
        }

        // singlewrite by posix env
        TEST(TransLogRecordStoreTest, TransLogRecordStore_SingleWritePosixEnv) {
            rocksdb::Status s;
            std::vector<LogRecord> recordVec;
            std::string chunk_path = "chunk1";
            std::string chunk_path2 = "chunk2";
            std::string chunk_path3 = "chunk3";
            std::string db_path = "/CloudBuild/transLogRecStore1/";
            std::string path_tmp = db_path + chunk_path;
            std::string path_tmp2 = db_path + chunk_path2;
            std::string path_tmp3 = db_path + chunk_path3;

            rocksdb::Env* env = rocksdb::Env::Default();
            std::shared_ptr<SplitDescription> description = std::make_shared<SplitDescription>();

            // env clean
            PosixEnvClean();

            std::unique_ptr<TransLogRecordStore> trans_log_record_store(
                new TransLogRecordStore(env, path_tmp));
            // 1 Before the split
            SharedResourceOperationLogRecordProvider sro_provider;
            SharedResourceManager filter_manager;
            filter_manager.SetLogRecordProvider(&sro_provider);
            sro_provider.SetSharedResourceManager(&filter_manager);
            s = trans_log_record_store->RegisterProvider(sro_provider);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store->RegisterProvider(filter_manager);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store->Init();  // Init and replay empty logs
            ASSERT_TRUE(s.ok());

            std::vector<SharedResourceDescription> query_list;
            query_list.clear();
            AddQueryDescription(query_list, 5, true);
            s = filter_manager.OnResourcesQuery(query_list);
            ASSERT_TRUE(s.ok());
            ASSERT_TRUE(CheckQueryResult(query_list, 5, false));

            std::vector<SharedResourceId> rmv_list;
            rmv_list.clear();
            AddRemoveDescription(rmv_list, 5);
            s = filter_manager.OnResourcesDeletion(rmv_list);
            ASSERT_TRUE(s.ok());

            // 2 Start split.
            s = sro_provider.StartSplit(description);
            ASSERT_TRUE(s.ok());

            SharedResourceIds resources;
            int i = 0;
            int num = 2;
            for (i = 0; i < num; i++) {
                resources.push_back(GetSharedResourceId(i));
            }

            s = sro_provider.RegisterSharedResources(resources);
            ASSERT_TRUE(s.ok());

            query_list.clear();
            AddQueryDescription(query_list, 21, false);
            AddQueryDescription(query_list, 20, false);
            AddQueryDescription(query_list, 15, false);
            s = filter_manager.OnResourcesQuery(query_list);
            ASSERT_TRUE(!s.ok());

            rmv_list.clear();
            AddRemoveDescription(rmv_list, 21);
            AddRemoveDescription(rmv_list, 20);
            AddRemoveDescription(rmv_list, 15);
            s = filter_manager.OnResourcesDeletion(rmv_list);
            ASSERT_TRUE(!s.ok());

            fprintf(stderr, "complete write (%d) record\r\n", num);

            // 3 Write shared resource reference logs to right side.
            // Prepare the instances of the classes to be tested.
            SharedResourceOperationLogRecordProvider right_sro_provider;
            SharedResourceManager right_filter_manager;
            std::unique_ptr<TransLogRecordStore> right_trans_log_record_store(
                new TransLogRecordStore(env, path_tmp2));
            right_filter_manager.SetLogRecordProvider(&right_sro_provider);
            right_sro_provider.SetSharedResourceManager(&right_filter_manager);
            right_trans_log_record_store->RegisterProvider(right_sro_provider);
            right_trans_log_record_store->RegisterProvider(right_filter_manager);
            right_trans_log_record_store->Init();  // Init and replay empty logs
            s = sro_provider.WriteSharedResourceLogToRight(&right_sro_provider);
            ASSERT_TRUE(s.ok());

            // 4 Commit split
            s = sro_provider.CommitSplit();
            ASSERT_TRUE(s.ok());
            // 5 After split.
            // 5.1) It is permitted to delete shared resource that dosen't belongs to current split.

            query_list.clear();
            AddQueryDescription(query_list, 1, false);
            s = filter_manager.OnResourcesQuery(query_list);
            ASSERT_TRUE(s.ok());
            ASSERT_TRUE(CheckQueryResult(query_list, 1, true));

            rmv_list.clear();
            AddRemoveDescription(rmv_list, 1);
            s = filter_manager.OnResourcesDeletion(rmv_list);
            ASSERT_TRUE(s.ok());

            s = sro_provider.SetLastProcessedOperationLSN(6);
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "complete CommitSplit\r\n");

            // 6 Replay log.
            // 6.1 clear mem
            sro_provider.Reset();
            filter_manager.Reset();
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store.reset();

            right_sro_provider.Reset();
            right_filter_manager.Reset();
            // close all file
            s = right_trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            right_trans_log_record_store.reset();

            // 6.2 replay left
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_test_left(
                new TransLogRecordStore(env, path_tmp));
            // init
            SharedResourceOperationLogRecordProvider sro_provider_test_left;
            SharedResourceManager filter_manager_test_left;
            filter_manager_test_left.SetLogRecordProvider(&sro_provider_test_left);
            sro_provider_test_left.SetSharedResourceManager(&filter_manager_test_left);
            s = trans_log_record_store_test_left->RegisterProvider(sro_provider_test_left);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store_test_left->RegisterProvider(filter_manager_test_left);
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "Reset and create chunk1\r\n");

            // replay translog
            s = trans_log_record_store_test_left->Init();
            ASSERT_TRUE(s.ok());
            sro_provider_test_left.Reset();
            filter_manager_test_left.Reset();
            // close all file
            s = trans_log_record_store_test_left->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store_test_left.reset();

            // 6.2 replay right
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_test_right(
                new TransLogRecordStore(env, path_tmp2));
            // init
            SharedResourceOperationLogRecordProvider sro_provider_test_right;
            SharedResourceManager filter_manager_test_right;
            filter_manager_test_right.SetLogRecordProvider(&sro_provider_test_right);
            sro_provider_test_right.SetSharedResourceManager(&filter_manager_test_right);
            s = trans_log_record_store_test_right->RegisterProvider(sro_provider_test_right);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store_test_right->RegisterProvider(filter_manager_test_right);
            ASSERT_TRUE(s.ok());

            // replay translog
            s = trans_log_record_store_test_right->Init();
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "Reset and create chunk2\r\n");

            // 7 split again
            std::shared_ptr<SplitDescription> description1 = std::make_shared<SplitDescription>();
            s = sro_provider_test_right.StartSplit(description1);
            ASSERT_TRUE(s.ok());

            // 7.1 Write shared resource reference logs to right side.
            SharedResourceOperationLogRecordProvider provider_test;
            SharedResourceManager filter_manager_test;
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_test(
                new TransLogRecordStore(env, path_tmp3));
            filter_manager_test.SetLogRecordProvider(&provider_test);
            provider_test.SetSharedResourceManager(&filter_manager_test);
            trans_log_record_store_test->RegisterProvider(provider_test);
            trans_log_record_store_test->RegisterProvider(filter_manager_test);
            trans_log_record_store_test->Init();  // Init and replay empty logs
            s = sro_provider_test_right.WriteSharedResourceLogToRight(&provider_test);
            ASSERT_TRUE(s.ok());

            s = sro_provider_test_right.CommitSplit();
            ASSERT_TRUE(s.ok());

            sro_provider_test_right.Reset();
            filter_manager_test_right.Reset();
            // close all file
            s = trans_log_record_store_test_right->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store_test_right.reset();

            provider_test.Reset();
            filter_manager_test.Reset();
            s = trans_log_record_store_test->CloseAllWriteFile();
            trans_log_record_store_test.reset();
            ASSERT_TRUE(s.ok());

            // 7.2 replay right
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_1(
                new TransLogRecordStore(env, path_tmp3));
            // init
            SharedResourceOperationLogRecordProvider sro_provider_1;
            SharedResourceManager filter_manager_1;
            filter_manager_1.SetLogRecordProvider(&sro_provider_1);
            sro_provider_1.SetSharedResourceManager(&filter_manager_1);
            s = trans_log_record_store_1->RegisterProvider(sro_provider_1);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store_1->RegisterProvider(filter_manager_1);
            ASSERT_TRUE(s.ok());

            // replay translog
            s = trans_log_record_store_1->Init();
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "Reset and create chunk3\r\n");
        }

        // batchwrite by plog env
        TEST(TransLogRecordStoreTest, TransLogRecordStore_BatchWrite_PlogEnv) {
            rocksdb::Status s;
            std::vector<LogRecord> recordVec;
            std::string chunk_path = "chunk1";
            std::string chunk_path2 = "chunk2";
            std::string chunk_path3 = "chunk3";
            std::string db_path = "/CloudBuild/transLogRecStore/";

            rocksdb::Env* env = nullptr;
            std::shared_ptr<SplitDescription> description = std::make_shared<SplitDescription>();

            // env clean
            PlogEnvClean();

            std::unique_ptr<TransLogRecordStore> trans_log_record_store =
                CreateRecordStore(chunk_path, db_path, &env);
            // 1 Before the split
            SharedResourceOperationLogRecordProvider sro_provider;
            SharedResourceManager filter_manager;
            filter_manager.SetLogRecordProvider(&sro_provider);
            sro_provider.SetSharedResourceManager(&filter_manager);
            s = trans_log_record_store->RegisterProvider(sro_provider);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store->RegisterProvider(filter_manager);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store->Init();  // Init and replay empty logs
            ASSERT_TRUE(s.ok());

            std::vector<SharedResourceDescription> query_list;
            query_list.clear();
            AddQueryDescription(query_list, 5, true);
            s = filter_manager.OnResourcesQuery(query_list);
            ASSERT_TRUE(s.ok());
            ASSERT_TRUE(CheckQueryResult(query_list, 5, false));

            std::vector<SharedResourceId> rmv_list;
            rmv_list.clear();
            s = filter_manager.OnResourcesDeletion(rmv_list);
            ASSERT_TRUE(s.ok());

            // 2 Start split.
            s = sro_provider.StartSplit(description);
            ASSERT_TRUE(s.ok());

            SharedResourceIds resources;
            int i = 0;
            int num = 30000;
            for (i = 0; i < num; i++) {
                resources.push_back(GetSharedResourceId(i));
            }

            s = sro_provider.RegisterSharedResources(resources);
            ASSERT_TRUE(s.ok());

            query_list.clear();
            AddQueryDescription(query_list, 21, false);
            AddQueryDescription(query_list, 20, false);
            AddQueryDescription(query_list, 15, false);
            s = filter_manager.OnResourcesQuery(query_list);
            ASSERT_TRUE(!s.ok());

            rmv_list.clear();
            AddRemoveDescription(rmv_list, 21);
            AddRemoveDescription(rmv_list, 20);
            AddRemoveDescription(rmv_list, 15);
            s = filter_manager.OnResourcesDeletion(rmv_list);
            ASSERT_TRUE(!s.ok());

            fprintf(stderr, "complete write (%d) record\r\n", num);

            // 3 Write shared resource reference logs to right side.
            // Prepare the instances of the classes to be tested.
            SharedResourceOperationLogRecordProvider right_sro_provider;
            SharedResourceManager right_filter_manager;
            std::unique_ptr<TransLogRecordStore> right_trans_log_record_store =
                CreateRecordStore(chunk_path2, db_path, &env);
            right_filter_manager.SetLogRecordProvider(&right_sro_provider);
            right_sro_provider.SetSharedResourceManager(&right_filter_manager);
            right_trans_log_record_store->RegisterProvider(right_sro_provider);
            right_trans_log_record_store->RegisterProvider(right_filter_manager);
            right_trans_log_record_store->Init();  // Init and replay empty logs
            s = sro_provider.WriteSharedResourceLogToRight(&right_sro_provider);
            ASSERT_TRUE(s.ok());

            // 4 Commit split
            s = sro_provider.CommitSplit();
            ASSERT_TRUE(s.ok());
            // 5 After split.
            // 5.1) It is permitted to delete shared resource that dosen't belongs to current split.

            query_list.clear();
            AddQueryDescription(query_list, 100, false);
            AddQueryDescription(query_list, 86, false);
            s = filter_manager.OnResourcesQuery(query_list);
            ASSERT_TRUE(s.ok());
            ASSERT_TRUE(CheckQueryResult(query_list, 100, true));
            ASSERT_TRUE(CheckQueryResult(query_list, 86, true));

            rmv_list.clear();
            AddRemoveDescription(rmv_list, 100);
            AddRemoveDescription(rmv_list, 86);
            s = filter_manager.OnResourcesDeletion(rmv_list);
            ASSERT_TRUE(s.ok());

            s = sro_provider.SetLastProcessedOperationLSN(6);
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "complete CommitSplit\r\n");

            // 6 Replay log.
            // 6.1 clear mem
            sro_provider.Reset();
            filter_manager.Reset();
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store.reset();

            right_sro_provider.Reset();
            right_filter_manager.Reset();
            // close all file
            s = right_trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            right_trans_log_record_store.reset();

            // 6.2 replay left
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_test_left =
                CreateRecordStore(chunk_path, db_path, &env);
            // init
            SharedResourceOperationLogRecordProvider sro_provider_test_left;
            SharedResourceManager filter_manager_test_left;
            filter_manager_test_left.SetLogRecordProvider(&sro_provider_test_left);
            sro_provider_test_left.SetSharedResourceManager(&filter_manager_test_left);
            s = trans_log_record_store_test_left->RegisterProvider(sro_provider_test_left);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store_test_left->RegisterProvider(filter_manager_test_left);
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "Reset and create chunk1\r\n");

            // replay translog
            s = trans_log_record_store_test_left->Init();
            ASSERT_TRUE(s.ok());
            sro_provider_test_left.Reset();
            filter_manager_test_left.Reset();
            // close all file
            s = trans_log_record_store_test_left->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store_test_left.reset();

            // 6.2 replay right
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_test_right =
                CreateRecordStore(chunk_path2, db_path, &env);
            // init
            SharedResourceOperationLogRecordProvider sro_provider_test_right;
            SharedResourceManager filter_manager_test_right;
            filter_manager_test_right.SetLogRecordProvider(&sro_provider_test_right);
            sro_provider_test_right.SetSharedResourceManager(&filter_manager_test_right);
            s = trans_log_record_store_test_right->RegisterProvider(sro_provider_test_right);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store_test_right->RegisterProvider(filter_manager_test_right);
            ASSERT_TRUE(s.ok());

            // replay translog
            s = trans_log_record_store_test_right->Init();
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "Reset and create chunk2\r\n");

            // 7 split again
            std::shared_ptr<SplitDescription> description1 = std::make_shared<SplitDescription>();
            s = sro_provider_test_right.StartSplit(description1);
            ASSERT_TRUE(s.ok());

            resources.clear();
            for (i = 0; i < num; i++) {
                resources.push_back(GetSharedResourceId(i));
            }

            s = sro_provider_test_right.RegisterSharedResources(resources);
            ASSERT_TRUE(s.ok());

            // 7.1 Write shared resource reference logs to right side.
            SharedResourceOperationLogRecordProvider provider_test;
            SharedResourceManager filter_manager_test;
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_test =
                CreateRecordStore(chunk_path3, db_path, &env);
            filter_manager_test.SetLogRecordProvider(&provider_test);
            provider_test.SetSharedResourceManager(&filter_manager_test);
            trans_log_record_store_test->RegisterProvider(provider_test);
            trans_log_record_store_test->RegisterProvider(filter_manager_test);
            trans_log_record_store_test->Init();  // Init and replay empty logs
            s = sro_provider_test_right.WriteSharedResourceLogToRight(&provider_test);
            ASSERT_TRUE(s.ok());

            s = sro_provider_test_right.CommitSplit();
            ASSERT_TRUE(s.ok());

            sro_provider_test_right.Reset();
            filter_manager_test_right.Reset();
            // close all file
            s = trans_log_record_store_test_right->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store_test_right.reset();

            provider_test.Reset();
            filter_manager_test.Reset();
            // close all file
            s = trans_log_record_store_test->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store_test.reset();

            // 7.2 replay right
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_1 =
                CreateRecordStore(chunk_path3, db_path, &env);
            // init
            SharedResourceOperationLogRecordProvider sro_provider_1;
            SharedResourceManager filter_manager_1;
            filter_manager_1.SetLogRecordProvider(&sro_provider_1);
            sro_provider_1.SetSharedResourceManager(&filter_manager_1);
            s = trans_log_record_store_1->RegisterProvider(sro_provider_1);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store_1->RegisterProvider(filter_manager_1);
            ASSERT_TRUE(s.ok());

            // replay translog
            s = trans_log_record_store_1->Init();
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "Reset and create chunk3\r\n");

            // clear plog env
            DestroyEnv(env);
        }

        // singlewrite by plog env
        TEST(TransLogRecordStoreTest, TransLogRecordStore_SingleWrite_PlogEnv) {
            rocksdb::Status s;
            std::vector<LogRecord> recordVec;
            std::string chunk_path = "chunk1";
            std::string chunk_path2 = "chunk2";
            std::string chunk_path3 = "chunk3";
            std::string db_path = "/CloudBuild/transLogRecStore/";

            rocksdb::Env* env = nullptr;
            std::shared_ptr<SplitDescription> description = std::make_shared<SplitDescription>();

            // env clean
            PlogEnvClean();

            std::unique_ptr<TransLogRecordStore> trans_log_record_store =
                CreateRecordStore(chunk_path, db_path, &env);
            // 1 Before the split
            SharedResourceOperationLogRecordProvider sro_provider;
            SharedResourceManager filter_manager;
            filter_manager.SetLogRecordProvider(&sro_provider);
            sro_provider.SetSharedResourceManager(&filter_manager);
            s = trans_log_record_store->RegisterProvider(sro_provider);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store->RegisterProvider(filter_manager);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store->Init();  // Init and replay empty logs
            ASSERT_TRUE(s.ok());

            std::vector<SharedResourceDescription> query_list;
            query_list.clear();
            AddQueryDescription(query_list, 5, true);
            s = filter_manager.OnResourcesQuery(query_list);
            ASSERT_TRUE(s.ok());
            ASSERT_TRUE(CheckQueryResult(query_list, 5, false));

            std::vector<SharedResourceId> rmv_list;
            rmv_list.clear();
            s = filter_manager.OnResourcesDeletion(rmv_list);
            ASSERT_TRUE(s.ok());

            // 2 Start split.
            s = sro_provider.StartSplit(description);
            ASSERT_TRUE(s.ok());

            SharedResourceIds resources;
            int i = 0;
            int num = 2;
            for (i = 0; i < num; i++) {
                resources.push_back(GetSharedResourceId(i));
            }

            s = sro_provider.RegisterSharedResources(resources);
            ASSERT_TRUE(s.ok());

            query_list.clear();
            AddQueryDescription(query_list, 21, false);
            AddQueryDescription(query_list, 20, false);
            AddQueryDescription(query_list, 15, false);
            s = filter_manager.OnResourcesQuery(query_list);
            ASSERT_TRUE(!s.ok());

            rmv_list.clear();
            AddRemoveDescription(rmv_list, 21);
            AddRemoveDescription(rmv_list, 20);
            AddRemoveDescription(rmv_list, 15);
            s = filter_manager.OnResourcesDeletion(rmv_list);
            ASSERT_TRUE(!s.ok());

            fprintf(stderr, "complete write (%d) record\r\n", num);

            // 3 Write shared resource reference logs to right side.
            // Prepare the instances of the classes to be tested.
            SharedResourceOperationLogRecordProvider right_sro_provider;
            SharedResourceManager right_filter_manager;
            std::unique_ptr<TransLogRecordStore> right_trans_log_record_store =
                CreateRecordStore(chunk_path2, db_path, &env);
            right_filter_manager.SetLogRecordProvider(&right_sro_provider);
            right_sro_provider.SetSharedResourceManager(&right_filter_manager);
            right_trans_log_record_store->RegisterProvider(right_sro_provider);
            right_trans_log_record_store->RegisterProvider(right_filter_manager);
            right_trans_log_record_store->Init();  // Init and replay empty logs
            s = sro_provider.WriteSharedResourceLogToRight(&right_sro_provider);
            ASSERT_TRUE(s.ok());

            // 4 Commit split
            s = sro_provider.CommitSplit();
            ASSERT_TRUE(s.ok());
            // 5 After split.
            // 5.1) It is permitted to delete shared resource that dosen't belongs to current split.

            query_list.clear();
            AddQueryDescription(query_list, 1, false);
            s = filter_manager.OnResourcesQuery(query_list);
            ASSERT_TRUE(s.ok());
            ASSERT_TRUE(CheckQueryResult(query_list, 1, true));

            rmv_list.clear();
            AddRemoveDescription(rmv_list, 1);
            s = filter_manager.OnResourcesDeletion(rmv_list);
            ASSERT_TRUE(s.ok());

            s = sro_provider.SetLastProcessedOperationLSN(6);
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "complete CommitSplit\r\n");

            // 6 Replay log.
            // 6.1 clear mem
            sro_provider.Reset();
            filter_manager.Reset();
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store.reset();

            right_sro_provider.Reset();
            right_filter_manager.Reset();
            // close all file
            s = right_trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            right_trans_log_record_store.reset();

            // 6.2 replay left
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_test_left =
                CreateRecordStore(chunk_path, db_path, &env);
            // init
            SharedResourceOperationLogRecordProvider sro_provider_test_left;
            SharedResourceManager filter_manager_test_left;
            filter_manager_test_left.SetLogRecordProvider(&sro_provider_test_left);
            sro_provider_test_left.SetSharedResourceManager(&filter_manager_test_left);
            s = trans_log_record_store_test_left->RegisterProvider(sro_provider_test_left);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store_test_left->RegisterProvider(filter_manager_test_left);
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "Reset and create chunk1\r\n");

            // replay translog
            s = trans_log_record_store_test_left->Init();
            ASSERT_TRUE(s.ok());
            sro_provider_test_left.Reset();
            filter_manager_test_left.Reset();
            // close all file
            s = trans_log_record_store_test_left->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store_test_left.reset();

            // 6.2 replay right
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_test_right =
                CreateRecordStore(chunk_path2, db_path, &env);
            // init
            SharedResourceOperationLogRecordProvider sro_provider_test_right;
            SharedResourceManager filter_manager_test_right;
            filter_manager_test_right.SetLogRecordProvider(&sro_provider_test_right);
            sro_provider_test_right.SetSharedResourceManager(&filter_manager_test_right);
            s = trans_log_record_store_test_right->RegisterProvider(sro_provider_test_right);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store_test_right->RegisterProvider(filter_manager_test_right);
            ASSERT_TRUE(s.ok());

            // replay translog
            s = trans_log_record_store_test_right->Init();
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "Reset and create chunk2\r\n");

            // 7 split again
            std::shared_ptr<SplitDescription> description1 = std::make_shared<SplitDescription>();
            s = sro_provider_test_right.StartSplit(description1);
            ASSERT_TRUE(s.ok());

            resources.clear();
            for (i = 0; i < num; i++) {
                resources.push_back(GetSharedResourceId(i));
            }

            s = sro_provider_test_right.RegisterSharedResources(resources);
            ASSERT_TRUE(s.ok());

            // 7.1 Write shared resource reference logs to right side.
            SharedResourceOperationLogRecordProvider provider_test;
            SharedResourceManager filter_manager_test;
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_test =
                CreateRecordStore(chunk_path3, db_path, &env);
            filter_manager_test.SetLogRecordProvider(&provider_test);
            provider_test.SetSharedResourceManager(&filter_manager_test);
            trans_log_record_store_test->RegisterProvider(provider_test);
            trans_log_record_store_test->RegisterProvider(filter_manager_test);
            trans_log_record_store_test->Init();  // Init and replay empty logs
            s = sro_provider_test_right.WriteSharedResourceLogToRight(&provider_test);
            ASSERT_TRUE(s.ok());

            s = sro_provider_test_right.CommitSplit();
            ASSERT_TRUE(s.ok());

            sro_provider_test_right.Reset();
            filter_manager_test_right.Reset();
            // close all file
            s = trans_log_record_store_test_right->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store_test_right.reset();

            provider_test.Reset();
            filter_manager_test.Reset();
            // close all file
            s = trans_log_record_store_test->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            trans_log_record_store_test.reset();

            // 7.2 replay right
            std::unique_ptr<TransLogRecordStore> trans_log_record_store_1 =
                CreateRecordStore(chunk_path3, db_path, &env);
            // init
            SharedResourceOperationLogRecordProvider sro_provider_1;
            SharedResourceManager filter_manager_1;
            filter_manager_1.SetLogRecordProvider(&sro_provider_1);
            sro_provider_1.SetSharedResourceManager(&filter_manager_1);
            s = trans_log_record_store_1->RegisterProvider(sro_provider_1);
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store_1->RegisterProvider(filter_manager_1);
            ASSERT_TRUE(s.ok());

            // replay translog
            s = trans_log_record_store_1->Init();
            ASSERT_TRUE(s.ok());

            fprintf(stderr, "Reset and create chunk3\r\n");

            BaseModuleExitFlag = true;
            // clear plog env
            DestroyEnv(env);
        }
    }
}

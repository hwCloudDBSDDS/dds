
#include "TransLogRecordStore.h"
#include "mongo/unittest/unittest.h"
//#include "../../../public_inc/plog_client.h"
#include "/usr2/DFV-Rocks/util/random.h"

namespace mongo {
    namespace TransactionLog {

        class ILogRecordProviderTest : public ILogRecordProvider {
        public:
            ILogRecordProviderTest(std::string& provider_name) { provider_name_ = provider_name; }

            ~ILogRecordProviderTest() {
                const char* buf = nullptr;
                for (size_t i = 0; i < written_record_.size(); i++) {
                    buf = written_record_[i].GetData();
                    delete buf;
                }
                written_record_.clear();
            }

            //
            //  ILogRecordProvider implementation
            //
            rocksdb::Status InitializationBegin(LogReplayOrder replayOrder,
                                                std::vector<LogRecordType>& supportedRecordTypes) {
                supportedRecordTypes.push_back(LogRecordType::LoadBalancingTransactionLog);
                return rocksdb::Status::OK();
            }

            rocksdb::Status ReplayLogRecord(const LogRecord& record, bool& needMoreRecords) {
                bool isFind = false;
                auto iter = written_record_.begin();
                for (; iter != written_record_.end(); iter++) {
                    if (*iter == record) {
                        isFind = true;
                        break;
                    }
                }
                if (!isFind) {
                    return rocksdb::Status::NotFound();
                }
                needMoreRecords = needMoreRecords_;
                return rocksdb::Status::OK();
            }

            void SetNeedMoreRecordState(bool needMoreRecord) { needMoreRecords_ = needMoreRecord; }

            rocksdb::Status InitializationEnd(ILogRecordWriter& logWriter) {
                log_writer_ = &logWriter;
                return rocksdb::Status::OK();
            }

            //
            //  Called when IRecordStore decided to checkpoint current state of providers.
            //  Poviders can use this function to block writes to IRecordStore before checkpoint is
            //  finished.
            //  This function can be ommited if provider are able to aggregate records saved during
            //  checkpointing.
            //  In this case provider don't need to block writes, since it can merge them with
            //  checkpoint.
            //
            void CheckpointBegin(){};

            //
            //  This function is called between CheckpointBegin and CheckpointEnd
            //  In this function provider should save it's' state: aggregated representation of all
            //  valid records. if checkpoint succed for all providers, records written before
            //  CheckpointBegin
            //  will be discarded and never be replayed through ReplayLogRecord.
            //
            rocksdb::Status WriteCheckpoint() {
                return log_writer_->WriteRecord(written_record_, true);
            }

            //
            //  Called after all providers saved their checkpoints.
            //
            void CheckpointEnd() {}

            //
            //  Called before record store is closed. After that, it's not allowed to use writer.
            //
            void UnloadProvider() {}

            //
            // get provider name
            //
            std::string GetName() { return provider_name_; }

            rocksdb::Status WriteRecord(std::vector<LogRecord>& record) {
                rocksdb::Status s = log_writer_->WriteRecord(record, true);
                if (s.ok()) {
                    char* buf = nullptr;
                    for (size_t i = 0; i < record.size(); i++) {
                        buf = new char[record[i].GetSize()];
                        if (nullptr == buf) {
                            return rocksdb::Status::NoSpace();
                        }
                        memcpy(buf, record[i].GetData(), record[i].GetSize());
                        LogRecord record_tmp(record[i].GetType(), buf, record[i].GetSize());
                        written_record_.push_back(record_tmp);
                    }
                }
                return s;
            }

            rocksdb::Status WriteRecord(LogRecord& record) {
                rocksdb::Status s = log_writer_->WriteRecord(record, true);
                if (s.ok()) {
                    char* buf = new char[record.GetSize()];
                    if (nullptr == buf) {
                        return rocksdb::Status::NoSpace();
                    }
                    memcpy(buf, record.GetData(), record.GetSize());
                    LogRecord record_tmp(record.GetType(), buf, record.GetSize());
                    if (written_record_.size() < 1024) {
                        written_record_.push_back(record_tmp);
                    }
                }
                return s;
            };

        private:
            bool needMoreRecords_;
            ILogRecordWriter* log_writer_;
            std::string provider_name_;
            std::vector<LogRecord> written_record_;
        };

        // static bool BaseModuleInitFlag = false;
        static bool BaseModuleExitFlag = false;
        // db_pat h must end with "/plogcnt0"
        // use env plog
        std::unique_ptr<TransLogRecordStore> CreateRecordStore(std::string& chunk_id,
                                                               std::string& db_path,
                                                               rocksdb::Env** plog_env) {
            //            rocksdb::PlogEnvOptions    env_options;
            // PlogClientStubOptions plog_client_options;

            // plog_client_options.db_path.clear();
            // plog_client_options.db_path = db_path + chunk_id;
            // plog_client_options.plog_path = plog_client_options.db_path;
            // plog_client_options.plog_append_no_sync = true;//single host test , no need fsync for
            // plog append data  to speedup
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
            // TransLogRecordStore(*plog_env, db_path_tmp));

            // return trans_log_Record_store;
            return nullptr;
        }

        // env clean
        static const char* clean_cmd1 = "rm -rf /CloudBuild/transLogRecStore/chunk1/translog/*";
        static const char* clean_cmd2 = "rm -rf /CloudBuild/transLogRecStore/chunk2/translog/*";
        void EnvClean() {
            int ret_code = system(clean_cmd1) + system(clean_cmd2);
            if (0 != ret_code) {
                fprintf(stderr, "clean cmd exec fail");
            }
            fprintf(stderr, "clean cmd exec success");
        }

        void DestroyEnv(rocksdb::Env* plog_env) {
            if (nullptr != plog_env) {
                delete plog_env;
            }

            // EnvClean();
            // libclient_exit_x(nullptr);
            // if(BaseModuleExitFlag){
            //    BaseModuleDestroy();
            //}
        }

        void GetRandomString(rocksdb::Random* rnd, int length, rocksdb::Slice& key) {
            char base[63] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            int32_t number = 0;
            int i;
            char* data = const_cast<char*>(key.data());
            // key begin with "abcdefghijklmnopqrstuvwxyz"
            number = rnd->Uniform(26);
            data[0] = base[number];

            for (i = 1; i < length; i++) {
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

        // singlewrite by posix env
        TEST(TransLogRecordStoreTest, TransLogRecordStore_SingleWritePosixEnv) {
            rocksdb::Status s;
            std::vector<LogRecord> recordVec;
            std::string chunk_path = "chunk1";
            std::string db_path = "/CloudBuild/transLogRecStore/";
            std::string path_tmp = db_path + chunk_path;
            rocksdb::Env* env = rocksdb::Env::Default();

            // env clean
            EnvClean();

            std::unique_ptr<TransLogRecordStore> trans_log_record_store(
                new TransLogRecordStore(env, path_tmp));
            std::unique_ptr<ILogRecordProviderTest> provider(new ILogRecordProviderTest(db_path));
            s = trans_log_record_store->RegisterProvider(*(provider.get()));
            ASSERT_TRUE(s.ok());
            // produce random string
            int key_size = 64;
            std::unique_ptr<const char[]> key_guard;
            rocksdb::Slice buffer = AllocateKey(&key_guard, key_size);
            if (nullptr == buffer.data()) {
                s = rocksdb::Status::NoSpace("new random buffer fail");
                ASSERT_TRUE(s.ok());
            }
            rocksdb::Random rnd(301);
            GetRandomString(&rnd, buffer.size(), buffer);

            LogRecord record(LogRecordType::LoadBalancingTransactionLog, buffer);

            // initilizaitonEnd return provider
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());
            // Write logs
            s = provider->WriteRecord(record);
            ASSERT_TRUE(s.ok());
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            // set needMoreRecords true
            provider->SetNeedMoreRecordState(true);
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            // replay translog
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());
        }

        // singlewrite by plog env
        TEST(TransLogRecordStoreTest, TransLogRecordStore_SingleWritePlogEnv) {
            rocksdb::Status s;
            std::vector<LogRecord> recordVec;
            std::string chunk_path = "chunk1";
            std::string db_path = "/CloudBuild/transLogRecStore/";
            std::string path_tmp = db_path + chunk_path;
            rocksdb::Env* env = nullptr;

            // env clean
            EnvClean();

            std::unique_ptr<TransLogRecordStore> trans_log_record_store =
                CreateRecordStore(chunk_path, db_path, &env);
            std::unique_ptr<ILogRecordProviderTest> provider(new ILogRecordProviderTest(db_path));
            s = trans_log_record_store->RegisterProvider(*(provider.get()));
            ASSERT_TRUE(s.ok());
            // produce random string
            int key_size = 64;
            std::unique_ptr<const char[]> key_guard;
            rocksdb::Slice buffer = AllocateKey(&key_guard, key_size);
            if (nullptr == buffer.data()) {
                s = rocksdb::Status::NoSpace("new random buffer fail");
                ASSERT_TRUE(s.ok());
            }
            rocksdb::Random rnd(301);
            GetRandomString(&rnd, buffer.size(), buffer);

            LogRecord record(LogRecordType::LoadBalancingTransactionLog, buffer);

            // initilizaitonEnd return provider
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());
            // Write logs
            s = provider->WriteRecord(record);
            ASSERT_TRUE(s.ok());
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            // set needMoreRecords true
            provider->SetNeedMoreRecordState(true);
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            // replay translog
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());

            DestroyEnv(env);
        }

        // batchwrite by posix env
        TEST(TransLogRecordStoreTest, TransLogRecordStore_BatchWritePosixEnv) {
            rocksdb::Status s;
            std::vector<LogRecord> recordVec;
            std::string chunk_path = "chunk1";
            std::string db_path = "/CloudBuild/transLogRecStore/";
            std::string path_tmp = db_path + chunk_path;

            rocksdb::Env* env = rocksdb::Env::Default();
            // batch write num
            size_t i;
            size_t num = 1000;
            // env clean
            EnvClean();
            std::unique_ptr<TransLogRecordStore> trans_log_record_store(
                new TransLogRecordStore(env, path_tmp));
            std::unique_ptr<ILogRecordProviderTest> provider(new ILogRecordProviderTest(db_path));

            s = trans_log_record_store->RegisterProvider(*(provider.get()));
            ASSERT_TRUE(s.ok());
            // produce random string
            int key_size = 64;
            std::unique_ptr<const char[]> key_guard;
            rocksdb::Slice buffer = AllocateKey(&key_guard, key_size);
            if (nullptr == buffer.data()) {
                s = rocksdb::Status::NoSpace("new random buffer fail");
                ASSERT_TRUE(s.ok());
            }
            rocksdb::Random rnd(301);
            // initilizaitonEnd return provider
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());
            for (i = 0; i < num; i++) {
                recordVec.push_back(LogRecord(LogRecordType::LoadBalancingTransactionLog, buffer));
                GetRandomString(&rnd, key_size, buffer);
            }
            // Write logs
            s = provider->WriteRecord(recordVec);
            ASSERT_TRUE(s.ok());
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            // set needMoreRecords true
            provider->SetNeedMoreRecordState(true);
            // replay translog
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());
        }

        // batchwrite by plog env
        TEST(TransLogRecordStoreTest, TransLogRecordStore_BatchWritePlogEnv) {
            rocksdb::Status s;
            std::vector<LogRecord> recordVec;
            std::string chunk_path = "chunk1";
            std::string db_path = "/CloudBuild/transLogRecStore/";
            std::string path_tmp = db_path + chunk_path;

            rocksdb::Env* env = nullptr;
            // batch write num
            size_t i;
            size_t num = 1000;
            // env clean
            EnvClean();

            std::unique_ptr<TransLogRecordStore> trans_log_record_store =
                CreateRecordStore(chunk_path, db_path, &env);
            std::unique_ptr<ILogRecordProviderTest> provider(new ILogRecordProviderTest(db_path));

            s = trans_log_record_store->RegisterProvider(*(provider.get()));
            ASSERT_TRUE(s.ok());
            // produce random string
            int key_size = 64;
            std::unique_ptr<const char[]> key_guard;
            rocksdb::Slice buffer = AllocateKey(&key_guard, key_size);
            if (nullptr == buffer.data()) {
                s = rocksdb::Status::NoSpace("new random buffer fail");
                ASSERT_TRUE(s.ok());
            }
            rocksdb::Random rnd(301);
            // initilizaitonEnd return provider
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());
            for (i = 0; i < num; i++) {
                recordVec.push_back(LogRecord(LogRecordType::LoadBalancingTransactionLog, buffer));
                GetRandomString(&rnd, key_size, buffer);
            }
            // Write logs
            s = provider->WriteRecord(recordVec);
            ASSERT_TRUE(s.ok());
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            // set needMoreRecords true
            provider->SetNeedMoreRecordState(true);
            // replay translog
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());

            DestroyEnv(env);
        }

        // batchwrite by posix env and set needMoreRecord to be false
        TEST(TransLogRecordStoreTest, TransLogRecordStore_BatchWrite_needMore_PosixEnv) {
            rocksdb::Status s;
            std::vector<LogRecord> recordVec;
            std::string chunk_path = "chunk1";
            std::string db_path = "/CloudBuild/transLogRecStore/";
            std::string path_tmp = db_path + chunk_path;
            rocksdb::Env* env = rocksdb::Env::Default();

            // batch write num
            size_t i;
            size_t num = 100;
            // env clean
            EnvClean();

            std::unique_ptr<TransLogRecordStore> trans_log_record_store(
                new TransLogRecordStore(env, path_tmp));
            std::unique_ptr<ILogRecordProviderTest> provider(new ILogRecordProviderTest(db_path));
            s = trans_log_record_store->RegisterProvider(*(provider.get()));
            ASSERT_TRUE(s.ok());

            // produce random string
            int key_size = 64;
            std::unique_ptr<const char[]> key_guard;
            rocksdb::Slice buffer = AllocateKey(&key_guard, key_size);
            if (nullptr == buffer.data()) {
                s = rocksdb::Status::NoSpace("new random buffer fail");
                ASSERT_TRUE(s.ok());
            }
            rocksdb::Random rnd(301);

            // initilizaitonEnd return provider
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());
            for (i = 0; i < num; i++) {
                recordVec.push_back(LogRecord(LogRecordType::LoadBalancingTransactionLog, buffer));
                GetRandomString(&rnd, key_size, buffer);
            }
            // Write logs
            s = provider->WriteRecord(recordVec);
            ASSERT_TRUE(s.ok());
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            // set needMoreRecords false
            provider->SetNeedMoreRecordState(false);
            // TODO: Add record replay num check after set false, should be zero
            // replay translog
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());
        }

        // batchwrite by plog env and set needMoreRecord to be false
        TEST(TransLogRecordStoreTest, TransLogRecordStore_BatchWrite_needMore_PlogEnv) {
            rocksdb::Status s;
            std::vector<LogRecord> recordVec;
            std::string chunk_path = "chunk1";
            std::string db_path = "/CloudBuild/transLogRecStore/";
            std::string path_tmp = db_path + chunk_path;
            rocksdb::Env* env = nullptr;

            // batch write num
            size_t i;
            size_t num = 100;
            // env clean
            EnvClean();

            std::unique_ptr<TransLogRecordStore> trans_log_record_store =
                CreateRecordStore(chunk_path, db_path, &env);
            std::unique_ptr<ILogRecordProviderTest> provider(new ILogRecordProviderTest(db_path));
            s = trans_log_record_store->RegisterProvider(*(provider.get()));
            ASSERT_TRUE(s.ok());

            // produce random string
            int key_size = 64;
            std::unique_ptr<const char[]> key_guard;
            rocksdb::Slice buffer = AllocateKey(&key_guard, key_size);
            if (nullptr == buffer.data()) {
                s = rocksdb::Status::NoSpace("new random buffer fail");
                ASSERT_TRUE(s.ok());
            }
            rocksdb::Random rnd(301);

            // initilizaitonEnd return provider
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());
            for (i = 0; i < num; i++) {
                recordVec.push_back(LogRecord(LogRecordType::LoadBalancingTransactionLog, buffer));
                GetRandomString(&rnd, key_size, buffer);
            }
            // Write logs
            s = provider->WriteRecord(recordVec);
            ASSERT_TRUE(s.ok());
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            // set needMoreRecords false
            provider->SetNeedMoreRecordState(false);
            // TODO: Add record replay num check after set false, should be zero
            // replay translog
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());

            DestroyEnv(env);
        }

        // batchwrite by posix env and add more provider
        TEST(TransLogRecordStoreTest, TransLogRecordStore_BatchWrite_MoreProvider_PosixEnv) {
            rocksdb::Status s;
            std::vector<LogRecord> recordVec;
            std::string chunk_path = "chunk1";
            std::string db_path = "/CloudBuild/transLogRecStore/";
            std::string db_path2 = "/CloudBuild/transLogRecStore2/";
            std::string path_tmp = db_path + chunk_path;
            rocksdb::Env* env = rocksdb::Env::Default();

            // batch write num
            size_t i;
            size_t num = 100;

            // env clean
            EnvClean();

            std::unique_ptr<TransLogRecordStore> trans_log_record_store(
                new TransLogRecordStore(env, path_tmp));
            std::unique_ptr<ILogRecordProviderTest> provider(new ILogRecordProviderTest(db_path));
            std::unique_ptr<ILogRecordProviderTest> provider2(new ILogRecordProviderTest(db_path2));

            s = trans_log_record_store->RegisterProvider(*(provider.get()));
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store->RegisterProvider(*(provider2.get()));
            ASSERT_TRUE(s.ok());

            // produce random string
            int key_size = 64;
            std::unique_ptr<const char[]> key_guard;
            rocksdb::Slice buffer = AllocateKey(&key_guard, key_size);
            if (nullptr == buffer.data()) {
                s = rocksdb::Status::NoSpace("new random buffer fail");
                ASSERT_TRUE(s.ok());
            }
            rocksdb::Random rnd(301);

            // initilizaitonEnd return provider
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());
            for (i = 0; i < num; i++) {
                recordVec.push_back(LogRecord(LogRecordType::LoadBalancingTransactionLog, buffer));
                GetRandomString(&rnd, key_size, buffer);
            }
            // Write logs
            s = provider->WriteRecord(recordVec);
            ASSERT_TRUE(s.ok());
            s = provider2->WriteRecord(recordVec);
            ASSERT_TRUE(s.ok());
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            // set needMoreRecords false
            provider->SetNeedMoreRecordState(true);
            provider2->SetNeedMoreRecordState(true);
            // replay translog
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());
        }

        // batchwrite by plog env and add more provider
        TEST(TransLogRecordStoreTest, TransLogRecordStore_BatchWrite_MoreProvider_PlogEnv) {
            rocksdb::Status s;
            std::vector<LogRecord> recordVec;
            std::string chunk_path = "chunk1";
            std::string db_path = "/CloudBuild/transLogRecStore/";
            std::string db_path2 = "/CloudBuild/transLogRecStore2/";
            std::string path_tmp = db_path + chunk_path;
            rocksdb::Env* env = nullptr;

            // batch write num
            size_t i;
            size_t num = 100;

            // env clean
            EnvClean();

            std::unique_ptr<TransLogRecordStore> trans_log_record_store =
                CreateRecordStore(chunk_path, db_path, &env);
            std::unique_ptr<ILogRecordProviderTest> provider(new ILogRecordProviderTest(db_path));
            std::unique_ptr<ILogRecordProviderTest> provider2(new ILogRecordProviderTest(db_path2));

            s = trans_log_record_store->RegisterProvider(*(provider.get()));
            ASSERT_TRUE(s.ok());
            s = trans_log_record_store->RegisterProvider(*(provider2.get()));
            ASSERT_TRUE(s.ok());

            // produce random string
            int key_size = 64;
            std::unique_ptr<const char[]> key_guard;
            rocksdb::Slice buffer = AllocateKey(&key_guard, key_size);
            if (nullptr == buffer.data()) {
                s = rocksdb::Status::NoSpace("new random buffer fail");
                ASSERT_TRUE(s.ok());
            }
            rocksdb::Random rnd(301);

            // initilizaitonEnd return provider
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());
            for (i = 0; i < num; i++) {
                recordVec.push_back(LogRecord(LogRecordType::LoadBalancingTransactionLog, buffer));
                GetRandomString(&rnd, key_size, buffer);
            }
            // Write logs
            s = provider->WriteRecord(recordVec);
            ASSERT_TRUE(s.ok());
            s = provider2->WriteRecord(recordVec);
            ASSERT_TRUE(s.ok());
            // close all file
            s = trans_log_record_store->CloseAllWriteFile();
            ASSERT_TRUE(s.ok());
            // set needMoreRecords false
            provider->SetNeedMoreRecordState(true);
            provider2->SetNeedMoreRecordState(true);
            // replay translog
            s = trans_log_record_store->Init();
            ASSERT_TRUE(s.ok());

            DestroyEnv(env);
        }

        // checkpointtest by posix env
        TEST(TransLogRecordStoreTest, checkpointtest_PosixEnv) {
            std::string db_path = "/CloudBuild/transLogRecStore/";
            rocksdb::Env* env = rocksdb::Env::Default();
            rocksdb::Status s;
            std::string chunk_path("chunk1");
            uint32_t kv_num = 1024;

            // env clean
            EnvClean();

            std::unique_ptr<TransLogRecordStore> trans_log_Record_store =
                CreateRecordStore(chunk_path, db_path, &env);

            std::unique_ptr<ILogRecordProviderTest> provider(new ILogRecordProviderTest(db_path));

            s = trans_log_Record_store->RegisterProvider(*(provider.get()));
            ASSERT_TRUE(s.ok());

            char buf[1024] =
                "this is LogRecordType::ShareResourceLogthis is LogRecordType::ShareResourceLog"
                "this is LogRecordType::ShareResourceLogthis is "
                "LogRecordType::ShareResourceLogthis is LogRecordType::ShareResourceLog"
                "this is LogRecordType::ShareResourceLogthis is LogRecordType::ShareResourceLog"
                "this is LogRecordType::ShareResourceLogthis is "
                "LogRecordType::ShareResourceLogthis is LogRecordType::ShareResourceLog"
                "this is LogRecordType::ShareResourceLogthis is LogRecordType::ShareResourceLog"
                "std::unique_ptr<ILogRecordProviderTest> provider(new "
                "ILogRecordProviderTest(db_path))"
                "std::unique_ptr<ILogRecordProviderTest> provider(new "
                "ILogRecordProviderTest(db_path))";
            LogRecord record(LogRecordType::LoadBalancingTransactionLog, buf, strlen(buf));

            // replay translog
            s = trans_log_Record_store->Init();
            ASSERT_TRUE(s.ok());

            for (size_t i = 0; i < kv_num; i++) {
                // Write logs
                s = provider->WriteRecord(record);
                ASSERT_TRUE(s.ok());
            }

            trans_log_Record_store->CloseAllWriteFile();

            // replay translog
            s = trans_log_Record_store->Init();
            ASSERT_TRUE(s.ok());

            DestroyEnv(env);
        }

        TEST(TransLogRecordStoreTest, checkpointtest_PlogEnv) {
            std::string db_path = "/CloudBuild/transLogRecStore/";
            rocksdb::Env* env = nullptr;
            rocksdb::Status s;
            std::string chunk_path("chunk1");
            uint32_t kv_num = 1024;

            // env clean
            EnvClean();

            std::unique_ptr<TransLogRecordStore> trans_log_Record_store =
                CreateRecordStore(chunk_path, db_path, &env);

            std::unique_ptr<ILogRecordProviderTest> provider(new ILogRecordProviderTest(db_path));

            s = trans_log_Record_store->RegisterProvider(*(provider.get()));
            ASSERT_TRUE(s.ok());

            char buf[1024] =
                "this is LogRecordType::ShareResourceLogthis is LogRecordType::ShareResourceLog"
                "this is LogRecordType::ShareResourceLogthis is "
                "LogRecordType::ShareResourceLogthis is LogRecordType::ShareResourceLog"
                "this is LogRecordType::ShareResourceLogthis is LogRecordType::ShareResourceLog"
                "this is LogRecordType::ShareResourceLogthis is "
                "LogRecordType::ShareResourceLogthis is LogRecordType::ShareResourceLog"
                "this is LogRecordType::ShareResourceLogthis is LogRecordType::ShareResourceLog"
                "std::unique_ptr<ILogRecordProviderTest> provider(new "
                "ILogRecordProviderTest(db_path))"
                "std::unique_ptr<ILogRecordProviderTest> provider(new "
                "ILogRecordProviderTest(db_path))";
            LogRecord record(LogRecordType::LoadBalancingTransactionLog, buf, strlen(buf));

            // replay translog
            s = trans_log_Record_store->Init();
            ASSERT_TRUE(s.ok());

            for (size_t i = 0; i < kv_num; i++) {
                // Write logs
                s = provider->WriteRecord(record);
                ASSERT_TRUE(s.ok());
            }

            trans_log_Record_store->CloseAllWriteFile();

            // replay translog
            s = trans_log_Record_store->Init();
            ASSERT_TRUE(s.ok());
            BaseModuleExitFlag = true;
            DestroyEnv(env);
        }
    }
}

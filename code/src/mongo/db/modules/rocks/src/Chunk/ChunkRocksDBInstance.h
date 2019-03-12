#pragma once

#include <mongo/s/confirm_split_request.h>
#include <mongo/stdx/condition_variable.h>
#include <mongo/stdx/memory.h>
#include <mongo/stdx/mutex.h>
#include <rocksdb/cache.h>
#include <rocksdb/env.h>
#include "ChunkCompactionFilter.h"
#include "ChunkMetadata.h"
#include "IndexedRecordStorage.h"

#include "../rocks_compaction_scheduler.h"
#include "../rocks_counter_manager.h"
#include "../rocks_durability_manager.h"
#include "../rocks_recovery_unit.h"
#include "TransLogRecordStore.h"
#include "chunk_jounal_flush.h"
#include "i_log_record_store.h"
#include "i_shared_resource_manager.h"
#include "mongo/util/util_extend/GlobalConfig.h"
#include "mongo/util/util_extend/config_reader.h"

namespace mongo {
    class RocksJournalFlusher;
    class RocksRecoveryUnit;
    class GCManager;
    class ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage;
    class ChunkRocksDBInstance;
}

namespace mongo {

    //
    //  Helper class which obtains from IIndexedRecordStorage and stores the list of column family
    //  decriptors
    //
    class ChunkColumnFamilyDescriptors {
        std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilyDescriptors;

    public:
        ChunkColumnFamilyDescriptors() {}

        void Init(const IIndexedRecordStorage& recordStorage, const rocksdb::Options& options, 
                                                const ChunkRocksDBInstance* dbInstance);

        const std::vector<rocksdb::ColumnFamilyDescriptor>& Get() {
            return columnFamilyDescriptors;
        }
    };


    ///////////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Class describes RocksDB instance related to chunk and information associated with it.
    //
    class ChunkRocksDBInstance {
    public:
        ChunkRocksDBInstance(const std::string& path, ChunkJounalFlush* flusher);
        ~ChunkRocksDBInstance();
        ChunkMetadata* GetChunkMetadata();
        const ChunkMetadata* GetChunkMetadata() const;
        void DumpSharedResourceStats(const char* path);

        void stopBGThread(void);

    protected:
        static constexpr const char* const metadataFileName = "chunk.metadata";

        std::unique_ptr<ChunkMetadata> metadata;

        std::unique_ptr<IIndexedRecordStorage> indexedRecordStorage;

        std::unique_ptr<IIndexedRecordStorage> indexedRecordStorageInPreperSplit;

        ChunkColumnFamilyDescriptors columnFamilyDescriptors;
        std::vector<rocksdb::ColumnFamilyHandle*> columnFamilyHandles;

        std::unique_ptr<rocksdb::DB> dbInstance;

        // CounterManages manages counters like numRecords and dataSize for record stores
        std::unique_ptr<mongo::RocksCounterManager> _counterManager;
        std::unique_ptr<mongo::RocksCompactionScheduler> _compactionScheduler;
        std::unique_ptr<mongo::RocksDurabilityManager> _durabilityManager;
        std::unique_ptr<mongo::RocksJournalFlusher>
            _journalFlusher;  // Depends on _durabilityManager

        // can't use unique_ptr for env, because we can't free default::Env
        rocksdb::Env* _rightEnv;
        rocksdb::Env* _leftEnv;

        std::shared_ptr<TransactionLog::SharedResourceModule> _shared_resource_module;

        // This is for concurrency control
        mongo::RocksTransactionEngine _transactionEngine;

        bool durable = true;

        const uint64_t system_chunkID = 0;
        uint64_t _letChunkID;
        uint64_t _rightChunkID;
        std::string _dbPath;

        std::shared_ptr<rocksdb::Logger> _rocksdbLogger;

        ChunkJounalFlush* _chunkFlush;

        std::unique_ptr<ChunkMetadata> _rightMeta;

    protected:
        static rocksdb::Status LoadMetadata(rocksdb::Env& fileSystem,
                                            const std::string& chukMetaPath,
                                            std::unique_ptr<ChunkMetadata>& metadata);
        static rocksdb::Status SaveMetadata(rocksdb::Env& fileSystem,
                                            const std::string& chukMetaPath,
                                            const std::unique_ptr<ChunkMetadata>& metadata,
                                            bool fileExist = true);

        rocksdb::Status OpenStorage(const rocksdb::Options& options, const std::string& dbPath);

        void PostInit(const rocksdb::Options& options);  //  Do final initialization

        rocksdb::Status saveRightChunkMeta(const BSONObj& splitPoint,
                                           const std::string& rightDBPath,
                                           const std::string& rightChunkId,
                                           std::unique_ptr<ChunkMetadata>& rightMeta);

        rocksdb::Status initRightPlogEnv(const std::string& rightDBPath,
                                         bool is_in_rollback = false);

        rocksdb::Status rePlayTransactionLog();

        rocksdb::Status writeRightTranasctionLog(const std::string& rightDBPath,
                                                 rocksdb::Env* rightEnv, ChunkMetadata* rightMeta);

        Status getSplitPointInfo(const std::string& prefix, const ChunkType& chunk,
                                 BSONObj& splitPoint);

        void blockRightWrite(const BSONObj& splitPoint, const ChunkType& chunk, uint64_t r_datasize,
                             uint64_t r_numrecord);

    public:
        rocksdb::Logger* getLogger() const {
            if (_rocksdbLogger) {
                return _rocksdbLogger.get();
            } else {
                return nullptr;
            }
        }

        rocksdb::Status initTransactionLogModule(const std::string& dbPathWithoutPlog);

        const ChunkMetadata& GetMetadata() const { return *metadata.get(); }

        void setChunkVersion(const mongo::ChunkVersion& chunkVersion) {
            metadata->setChunkVersion(chunkVersion);
        }

        rocksdb::DB* GetDB() { return dbInstance.get(); }

        const rocksdb::DB* GetDB() const { return dbInstance.get(); }

        mongo::RocksCounterManager* GetCounterManager() { return _counterManager.get(); }

        mongo::RocksCompactionScheduler* GetCompactionScheduler() {
            return _compactionScheduler.get();
        }

        mongo::RocksDurabilityManager* GetDurabilityManager() { return _durabilityManager.get(); }

        mongo::RocksTransactionEngine* GetTransactionEngine() { return &_transactionEngine; }

        bool IsDurable() const  //  Durable means we need to write WAL
        {
            return durable;
        }

        IIndexedRecordStorage& GetIndexedRecordStorage() { return *indexedRecordStorage.get(); }

        IIndexedRecordStorage* GetIndexedRecordStoragePointer() {
            return indexedRecordStorage.get();
        }

        IIndexedRecordStorage* GetIndexedRecordStorageInPreperSplit() {
            return indexedRecordStorageInPreperSplit.get();
        }

        const IChunkColumnFamilyDescription* getColumnFamilyDescriptionInPreperSplit(
            const std::string& name) const;

        static constexpr int dataColumnFamilyId =
            0;  // Default column family which contains documents (not index)
        static constexpr int indexColumnFamilyId =
            1;  // Index column family which contains all indexes

        rocksdb::ColumnFamilyHandle* GetColumnFamily(int columnFamilyId) {
            invariant(columnFamilyId >= 0 && (size_t)columnFamilyId < columnFamilyHandles.size());
            return columnFamilyHandles[columnFamilyId];
        }

        std::vector<rocksdb::ColumnFamilyHandle*> GetColumnFamily() {
            return columnFamilyHandles;
        }

        //
        //  Open chunk's RocksDB instance. Is called from MongoD on Shard Server
        //
        rocksdb::Status Open(const rocksdb::Options& options, const std::string& dbPath,
                             bool durable, const CollectionOptions* collOptions);

        //
        //  Create first chunk of the collection. Called from Config Server before assigning
        //  this chunk to be served to Shard Server
        //
        // update Metadata for createIndex()
        rocksdb::Status UpdateMetadata(const std::string& chukMetaPath,
                                       const std::unique_ptr<ChunkMetadata>& metadata);
        rocksdb::Status UpdateMetadata(const ChunkType& chunk);

        static rocksdb::Status Create(const rocksdb::Options& options, const std::string& dbPath,
                                      ChunkRocksDBInstance* chunkDB,
                                      std::unique_ptr<ChunkMetadata>&& metadata);

        //
        //  Obtain RocksRecoveryUnit from transaction and set correct RocksDB into it
        //
        RocksRecoveryUnit* GetRocksRecoveryUnit(OperationContext* opCtx) const;

        Status preSplit(const std::string& rightDbpathWithPlog, const std::string& rightChunkId,
                        const std::string& prefix, const ChunkType& chunk, BSONObj& splitPoint);

        Status split(const std::string& rightDbpathWithPlog, const BSONObj& splitPoint,
                     const ChunkType& chunk, uint64_t r_datasize, uint64_t r_numrecord);

        Status rollbackPreSplit(OperationContext* txn, const std::string& rightDbpathWithPlog);

        rocksdb::Status confirmSplit(const ConfirmSplitRequest& request);

        void setLeftChunkID(const std::string& chunkID) {
            _letChunkID = strtoull(chunkID.c_str(), nullptr, 16);
        }

        uint64_t getLeftChunkID() const { return _letChunkID; }

        void setRightChunkID(const std::string& chunkID) {
            _rightChunkID = strtoull(chunkID.c_str(), nullptr, 16);
        }

        std::string getPath() const;
        std::string getDbPath() const;
        std::string getChunkMetaPath() const;
        std::string getTransLogPath() const;

        static std::string getDbPath(const std::string& dbpath);
        static std::string getChunkMetaPath(const std::string& dbpath);
        static std::string getTransLogPath(const std::string& dbpath);

        rocksdb::Status createChunkDir(const std::string& dbPath);

        rocksdb::Status createChildChunkDir(const std::string& dbPath);

        bool isSystemChunk() const { return (_letChunkID == system_chunkID); }

        uint64_t getRightChunkID() const { return _rightChunkID; }

        std::shared_ptr<TransactionLog::SharedResourceModule> GetSharedResourceModule() {
            return _shared_resource_module;
        }

        bool bockingReadIOofRight() const { return _transactionEngine.bockingReadIOofRight(); }
        std::unordered_set<uint32_t> getDroppedPrefixes() const;
        void updateDroppedPrefixes(const std::unordered_set<uint32_t>& prefix_set);

        Status compact(void);
     
        class SplitInfo {
        public:
            SplitInfo(const DbInstanceOption& option, const ChunkMetadata& metadata);
            std::vector<const rocksdb::IKeyRangeChecker*> getDescriptions() const;
            const DbInstanceOption& getDBInstanceOption() const { return options; }

        private:
            DbInstanceOption options;
            std::unique_ptr<ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage>
                indexColumnFamilyStorge;
        };

        std::unique_ptr<SplitInfo> _splitInfo;

    public:
        stdx::condition_variable _duringSplitCondVar;
        stdx::condition_variable _outsideOfSplitCondVar;
        stdx::mutex _mutex;

        // set of all prefixes that are deleted. we delete them in the background thread
        mutable stdx::mutex _droppedPrefixesMutex;
        std::unordered_set<uint32_t> _droppedPrefixes;

    };  //  ChunkRocksDBInstance

    class IdxPrefixDeletingCompactionFilter : public rocksdb::CompactionFilter {
    public:
        explicit IdxPrefixDeletingCompactionFilter(std::unordered_set<uint32_t> droppedPrefixes)
            : _droppedPrefixes(std::move(droppedPrefixes)),
              _prefixCache(0),
              _droppedCache(false) {}

        // we encode prefixes in big endian because we want to quickly jump to the max prefix
        // (iter->SeekToLast())
        bool extractIdxPrefix(const rocksdb::Slice& slice, uint32_t* prefix) const {
            if (slice.size() < sizeof(uint32_t)) {
                return false;
            }
            *prefix = endian::bigToNative(*reinterpret_cast<const uint32_t*>(slice.data()));
            return true;
        }

        // filter is not called from multiple threads simultaneously
        virtual bool Filter(int level, const rocksdb::Slice& key,
                            const rocksdb::Slice& existing_value, std::string* new_value,
                            bool* value_changed) const ;

        virtual const char* Name() const { return "IdxPrefixDeletingCompactionFilter"; }

    private:
        std::unordered_set<uint32_t> _droppedPrefixes;
        mutable uint32_t _prefixCache;
        mutable bool _droppedCache;
    };

    class IdxPrefixDeletingCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
    public:
        explicit IdxPrefixDeletingCompactionFilterFactory(const ChunkRocksDBInstance* chunkDbInstance)
            : _chunk_db_instance(chunkDbInstance) {}

        virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
            const rocksdb::CompactionFilter::Context& context) override ;

        virtual const char* Name() const override {
            return "IdxPrefixDeletingCompactionFilterFactory";
        }

    private:
        const ChunkRocksDBInstance* _chunk_db_instance;
    };

};  //  namespace mongo

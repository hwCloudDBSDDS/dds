#pragma once

#include "ChunkMetadata.h"
#include "IndexedRecordStorage.h"
#include "ChunkCompactionFilter.h"
#include <rocksdb/env.h>
#include <mongo/stdx/memory.h>
#include <mongo/s/confirm_split_request.h>
#include <mongo/stdx/condition_variable.h>
#include <mongo/stdx/mutex.h>

#include "../rocks_durability_manager.h"
#include "../rocks_counter_manager.h"
#include "../rocks_compaction_scheduler.h"
#include "../rocks_recovery_unit.h"
#include "TransLogRecordStore.h"
#include "i_shared_resource_manager.h"
#include "i_log_record_store.h"
#include "../GlobalConfig.h"


namespace mongo
{
    class RocksJournalFlusher;
    class RocksRecoveryUnit;
}

namespace mongo
{

//
//  Helper class which obtains from IIndexedRecordStorage and stores the list of column family decriptors
//
class ChunkColumnFamilyDescriptors
{
    std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilyDescriptors;

public:

    ChunkColumnFamilyDescriptors()
    {}

    ChunkColumnFamilyDescriptors(const IIndexedRecordStorage& recordStorage, const rocksdb::Options& options)
    {
        Init(recordStorage, options);
    }

    void Init(const IIndexedRecordStorage& recordStorage, const rocksdb::Options& options)
    {
        auto compactionFilterFactory = std::make_shared<ChunkCompactionFilterFactory>(
                    recordStorage,
                    options.compaction_filter_factory
                );

        for(const auto& columnFamilyDescription : recordStorage.GetColumnFamilyDescriptions())
        {
            rocksdb::ColumnFamilyOptions columnFamilyOptions(options);

            // Set compaction factory, which will remove records which don't belong to chunk after the split
            columnFamilyOptions.compaction_filter_factory = compactionFilterFactory;

            // Set filter which will filter out records during the read to return only records belonging to chunk
            if (GLOBAL_CONFIG_GET(enableAutoChunkSplit)) {
                columnFamilyOptions.range_checker = columnFamilyDescription;
            }
            // Set extractor which will return part of the key to create bloom filter on it
            if(GLOBAL_CONFIG_GET(UsePrefixBloomFilter))
                columnFamilyOptions.prefix_extractor = columnFamilyDescription->GetPrefixExtractor();

            columnFamilyDescriptors.emplace_back(
                std::string(columnFamilyDescription->GetName()),
                columnFamilyOptions
                );
        }
    }

    const std::vector<rocksdb::ColumnFamilyDescriptor>& Get()
    {
        return columnFamilyDescriptors;
    }
};


///////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Class describes RocksDB instance related to chunk and information associated with it.
//
class ChunkRocksDBInstance
{
public:
    ChunkRocksDBInstance(const std::string& path);
    ~ChunkRocksDBInstance();
    ChunkMetadata* GetChunkMetadata();
    void DumpSharedResourceStats(const char* path);

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
    std::unique_ptr<mongo::RocksJournalFlusher> _journalFlusher;  // Depends on _durabilityManager

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

protected:
    static rocksdb::Status LoadMetadata(rocksdb::Env& fileSystem, 
                                        const std::string& chukMetaPath,
                                        std::unique_ptr<ChunkMetadata>& metadata);
    static rocksdb::Status SaveMetadata(rocksdb::Env& fileSystem,
                                        const std::string& chukMetaPath, 
                                        const std::unique_ptr<ChunkMetadata>& metadata);

    rocksdb::Status OpenStorage(const rocksdb::Options& options, const std::string& dbPath);

    void PostInit(const rocksdb::Options& options);   //  Do final initialization

    rocksdb::Status saveRightChunkMeta(const BSONObj& splitPoint,
                                       const std::string& rightDBPath,
                                       const std::string& rightChunkId,
                                       std::unique_ptr<ChunkMetadata>& rightMeta);

    rocksdb::Status initRightPlogEnv(const std::string& rightDBPath, bool is_in_rollback=false);

    rocksdb::Status rePlayTransactionLog();

    rocksdb::Status writeRightTranasctionLog(const std::string& rightDBPath,
                                             rocksdb::Env* rightEnv,
                                             ChunkMetadata* rightMeta);

public:

    rocksdb::Logger* getLogger() const {
        if (_rocksdbLogger) {
            return _rocksdbLogger.get();
        } else {
            return nullptr;
        }
    }

    rocksdb::Status initTransactionLogModule(const std::string& dbPathWithoutPlog);

    const ChunkMetadata& GetMetadata() const
    {
        return *metadata.get();
    }

    void setChunkVersion(const mongo::ChunkVersion& chunkVersion) {
        metadata->setChunkVersion(chunkVersion);
    }

    rocksdb::DB* GetDB()
    {
        return dbInstance.get();
    }

    const rocksdb::DB* GetDB() const
    {
        return dbInstance.get();
    }

    rocksdb::Env* getLeftEnv() const {
        return _leftEnv;
    }

    mongo::RocksCounterManager* GetCounterManager()
    {
        return _counterManager.get();
    }

    mongo::RocksCompactionScheduler* GetCompactionScheduler()
    {
        return _compactionScheduler.get();
    }

    mongo::RocksDurabilityManager* GetDurabilityManager()
    {
        return _durabilityManager.get();
    }

    mongo::RocksTransactionEngine* GetTransactionEngine()
    {
        return &_transactionEngine;
    }

    bool IsDurable() const  //  Durable means we need to write WAL
    {
        return durable;
    }

    IIndexedRecordStorage& GetIndexedRecordStorage()
    {
        return *indexedRecordStorage.get();
    }

    IIndexedRecordStorage* GetIndexedRecordStorageInPreperSplit()
    {
        return indexedRecordStorageInPreperSplit.get();
    }

    const IChunkColumnFamilyDescription* getColumnFamilyDescriptionInPreperSplit(const std::string& name) const;

    static constexpr int dataColumnFamilyId = 0;    // Default column family which contains documents (not index)

    rocksdb::ColumnFamilyHandle* GetColumnFamily(int columnFamilyId)
    {
        invariant(columnFamilyId >= 0 && (size_t)columnFamilyId < columnFamilyHandles.size());
        return columnFamilyHandles[columnFamilyId];
    }

    //
    //  Open chunk's RocksDB instance. Is called from MongoD on Shard Server
    //
    rocksdb::Status Open(const rocksdb::Options& options, const std::string& dbPath, bool durable);

    //
    //  Create first chunk of the collection. Called from Config Server before assigning
    //  this chunk to be served to Shard Server
    //
    rocksdb::Status UpdateMetadata(const rocksdb::Options& options, 
                                   const std::string& chukMetaPath, 
                                   const std::unique_ptr<ChunkMetadata>& metadata);
    static rocksdb::Status Create(
        const rocksdb::Options& options,
        const std::string& dbPath,
        std::unique_ptr<ChunkMetadata>&& metadata,
        std::unique_ptr<ChunkRocksDBInstance>& chunkDB
        );

    //
    //  Obtain RocksRecoveryUnit from transaction and set correct RocksDB into it
    //
    RocksRecoveryUnit* GetRocksRecoveryUnit(OperationContext* opCtx) const;

    Status split(const std::string& rightDbpathWithPlog,  const std::string& rightChunkId,
                 const std::string& prefix, BSONObj& splitPoint, const ChunkType& chunk, 
                  uint64_t r_datasize, uint64_t r_numrecord);

    rocksdb::Status confirmSplit(const ConfirmSplitRequest& request);

    void setLeftChunkID(const std::string& chunkID) {
       _letChunkID = strtoull(chunkID.c_str(), nullptr, 16);
    }

    uint64_t getLeftChunkID() const {
        return _letChunkID;
    }

    void setRightChunkID(const std::string& chunkID) {
        _rightChunkID = strtoull(chunkID.c_str(), nullptr, 16);
    }
    
    std::string getDbPath() const; 
    std::string getChunkMetaPath() const;
    static std::string getChunkMetaPath(const std::string& dbpath);
    std::string getTransLogPath() const;

    bool isSystemChunk() const {
        return (_letChunkID == system_chunkID);
    }

    uint64_t getRightChunkID() const {
        return _rightChunkID;
    }

    std::shared_ptr<TransactionLog::SharedResourceModule> GetSharedResourceModule(){
        return _shared_resource_module;
    }

    bool bockingReadIOofRight() const {
        return _transactionEngine.bockingReadIOofRight();
    }

public:
    stdx::condition_variable _duringSplitCondVar;
    stdx::condition_variable _outsideOfSplitCondVar;
    stdx::mutex _mutex;

};  //  ChunkRocksDBInstance

};  //  namespace mongo


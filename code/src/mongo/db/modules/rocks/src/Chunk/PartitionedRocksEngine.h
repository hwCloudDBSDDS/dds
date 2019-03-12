
#pragma once
#include <mongo/stdx/memory.h>
#include <map>
#include "../rocks_engine.h"
#include "../rocks_index.h"
#include "i_shared_resource_manager.h"

namespace mongo {
    class GCClient;
    //
    //  This engine creates separate RocksDB instance for each collection or chunk
    //
    class PartitionedRocksEngine : public mongo::RocksEngine {
        MONGO_DISALLOW_COPYING(PartitionedRocksEngine);

    public:
        PartitionedRocksEngine(const std::string& path, bool durable, int formatVersion);
        ~PartitionedRocksEngine();

        virtual std::unique_ptr<mongo::RocksRecordStore> populateRecordStore(
            mongo::StringData ns, mongo::StringData ident, const std::string& prefix,
            const mongo::CollectionOptions& options) override;

        virtual mongo::Status createSortedDataInterface(
            mongo::OperationContext* opCtx, mongo::StringData ident,
            const mongo::IndexDescriptor* desc) override;

        virtual mongo::RocksIndexBase* populateIndex(StringData ident, const IndexDescriptor* desc,
                                                     const std::string& prefix,
                                                     BSONObj&& config) override;

        virtual Status createRecordStore(OperationContext* opCtx, StringData ns, StringData ident,
                                         const CollectionOptions& options) override;

        virtual RecoveryUnit* newRecoveryUnit() override;

        //dropped is true when in removing index case
        virtual Status dropIdent(OperationContext* opCtx, StringData ident, StringData ns=StringData()) override;

        void SetLastProcessedOperationLSN(std::string& lastProcessedChunkNs,
                                          uint64_t lastProcessLSN);

        bool GetSharedResourcesOperations(
            uint64_t& chunkID,
            std::vector<mongo::TransactionLog::SharedResourceReferenceOperation>& shareResources,
            int maxBatchMaxSize, std::string& lastProcessedChunkNs, std::string& collIdent);

        void GetChunkMetadata(std::string chunkId,
                              std::map<std::string, std::string>& chunkMetadataMap,
                              bool getALL = false);
        void GetDbStats(std::string chunkId, std::map<std::string, std::string>& dbStats,
                        bool getAll = false);
        void DumpSharedResourceStats(std::string chunkId, const char* path, bool getALL = false);
        void GetShardId(ShardId& shardId);

        virtual Status postInitRecordStore(StringData ns, StringData ident,
                                           RecordStore* recordStore) override;
        virtual Status updateChunkMetadata(OperationContext* opCtx, mongo::StringData ident,
                                           BSONArray& indexes, RecordStore* recordStore) override;

        virtual void setGCInfo(const std::string& ns, const std::string& dataPath) override;

        virtual Status destroyRocksDB(const std::string& nss) override;

        virtual Status reNameNss(const std::string& srcName, const std::string& destName) override;
        virtual Status OpenDB(const std::string& path, ChunkRocksDBInstance* db,
                             const CollectionOptions* options = nullptr) const override;;

        Status addRocksDB(const std::string& nss, std::unique_ptr<ChunkRocksDBInstance>);

        ChunkRocksDBInstance* getRocksDB(const std::string& nss);

        virtual void getTransactionEngineStat(long long& numKey, long long& numActiveSnapshot)override;

    protected:
        Status _createIdent(StringData ident, BSONObjBuilder* configBuilder, int64_t prefix,
                            const std::string& db_path);
        static const ChunkRocksDBInstance& GetDBInstance(const mongo::IndexDescriptor* desc);
        Status createOrLoadDB(const mongo::CollectionOptions& options,
                              std::unique_ptr<ChunkRocksDBInstance>& db);

        Status LoadDB(const mongo::CollectionOptions& options,
                      std::unique_ptr<ChunkRocksDBInstance>& dbInstance);
        Status CreateDB(const mongo::CollectionOptions& options,
                        std::unique_ptr<mongo::ChunkMetadata> chunkMetadata,
                        std::unique_ptr<ChunkRocksDBInstance>& dbInstance);

        virtual bool pauseGC(void) override;
        virtual void stopGC(void) override;
        virtual bool continueGC(void) override;
        virtual Status openChunkDbInstance(OperationContext* txn, StringData ns,
                                           const mongo::CollectionOptions& options) override;

        void genTransactionEngineStat(RocksTransactionEngine* engine, 
                                      BSONArrayBuilder& builder,
                                      long long& numKey, long long& numActiveSnapshot, 
                                      long long& unCommited, long long& sortSnapshot);

        // This is for gc client
        std::unique_ptr<mongo::GCClient> _gcClient;
        std::unique_ptr<mongo::GCManager> _gcManager;

        std::map<std::string, std::unique_ptr<ChunkRocksDBInstance> > _dbInstances;
        stdx::mutex _dbInstancesMutex;

        std::unique_ptr<ChunkJounalFlush> _chunkFlush;
    };
}

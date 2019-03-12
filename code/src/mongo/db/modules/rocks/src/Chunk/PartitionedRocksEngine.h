
#pragma once
#include <mongo/stdx/memory.h>
#include <map>
#include "../rocks_engine.h"
#include "../rocks_index.h"
#include "i_shared_resource_manager.h"
#include "../gc_manager.h"

namespace mongo
{

//
//  This engine creates separate RocksDB instance for each collection or chunk
//
class PartitionedRocksEngine : public mongo::RocksEngine
{
    MONGO_DISALLOW_COPYING(PartitionedRocksEngine);
public:
    PartitionedRocksEngine(const std::string& path, bool durable, int formatVersion);
    ~PartitionedRocksEngine();

    virtual void Init() override;

    virtual std::unique_ptr<mongo::RocksRecordStore> populateRecordStore(mongo::StringData ns, mongo::StringData ident,
                const std::string& prefix, const mongo::CollectionOptions& options) override;

    virtual mongo::Status createSortedDataInterface(mongo::OperationContext* opCtx, mongo::StringData ident,
                                                const mongo::IndexDescriptor* desc) override;

    virtual mongo::RocksIndexBase* populateIndex(
                    StringData ident,
                    const IndexDescriptor* desc,
                    const std::string& prefix,
                    BSONObj&& config
                    ) override;

    virtual void setStorageEngineLogLevel(int level) override;

    virtual Status createRecordStore(OperationContext* opCtx, StringData ns, StringData ident,
                                          		const CollectionOptions& options) override;

    virtual RecoveryUnit* newRecoveryUnit() override;

    virtual Status dropIdent(OperationContext* opCtx, StringData ident) override;

    virtual mongo::Status dropUserCollections(OperationContext* opCtx, StringData ident) override;
    virtual mongo::Status offloadUserCollections(OperationContext* opCtx, StringData ident) override;

    bool GetSharedResourcesOperations(uint64_t& ChunkID, std::shared_ptr<mongo::TransactionLog::SharedResourceModule> provider, 
                                        std::vector<mongo::TransactionLog::SharedResourceReferenceOperation>& shareResources, 
                                        int maxBatchMaxSize);
    void GetChunkMetadata(std::string chunkId, std::map<std::string, std::string>& chunkMetadataMap, bool getALL = false);
    void GetDbStats(std::string chunkId, std::map<std::string, std::string>& dbStats, bool getAll = false);
    void DumpSharedResourceStats(std::string chunkId, const char* path, bool getALL = false);
    void GetShardId(ShardId& shardId);

    virtual Status postInitRecordStore(StringData ns, StringData ident, 
                                       const CollectionOptions& options,
                                       RecordStore* recordStore) override;
    virtual Status updateChunkMetadata(OperationContext* opCtx,mongo::StringData ident,BSONArray& indexes,RecordStore* recordStore) override;

protected:
    Status _createIdent(StringData ident, BSONObjBuilder* configBuilder, int64_t prefix, const std::string & db_path);
    static const ChunkRocksDBInstance& GetDBInstance(const mongo::IndexDescriptor* desc);
    Status createOrLoadDB(const mongo::StringData& ident,
                          const mongo::CollectionOptions& options, 
                          std::unique_ptr<ChunkRocksDBInstance>& db);

    Status LoadDB(mongo::StringData ident, std::unique_ptr<ChunkRocksDBInstance>& dbInstance);
    Status CreateDB(mongo::StringData ident, 
                    std::unique_ptr<mongo::ChunkMetadata> chunkMetadata, 
                    std::unique_ptr<ChunkRocksDBInstance>& dbInstance);

    Status deleteCollectionIdent(OperationContext* opCtx, 
                                 const StringData& ident, 
                                 bool deleteFromDisk);
};

}


#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "PartitionedRocksEngine.h"

#include <rocksdb/cache.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/table.h>
#include <rocksdb/convenience.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/env.h>

#include "../rocks_recovery_unit.h"
#include "ChunkRocksRecordStore.h"
#include "ChunkMetadata.h"
#include <mongo/db/index/index_descriptor.h>
#include "../rocks_index.h"
#include "../rocks_global_options.h"
#include "../rocks_util.h"
#include "../GlobalConfig.h"
#include "../rocks_engine.h"
#include "mongo/util/log.h"
#include "chunk_rocks_index.h"
#include "../gc_manager.h"
#include "../more_db_recovery_unit.h"
#include "mongo/s/catalog/type_collection.h"
#include "../mongo_rocks_result_handling.h"

namespace mongo
{

std::string encodePrefix(uint32_t prefix) {
    uint32_t bigEndianPrefix = endian::nativeToBig(prefix);
    return std::string(reinterpret_cast<const char*>(&bigEndianPrefix), sizeof(uint32_t));
}

PartitionedRocksEngine::PartitionedRocksEngine(const std::string& path, bool durable, int formatVersion)
        : mongo::RocksEngine(path, durable, formatVersion)
{

}

PartitionedRocksEngine::~PartitionedRocksEngine(){
    //GCManager::Get().Stop();
}


void PartitionedRocksEngine::Init()
{
    //
    //  TODO: Need to have in-memory DB instance for metadata (getMetadataDB())
    //
    mongo::RocksEngine::Init();
    log() << "[MongoRocks] PartitionedRocksEngine initialized";
    //GCManager::Get().setEngine(this);
    //GCManager::Get().Start();
}

Status PartitionedRocksEngine::createOrLoadDB(const mongo::StringData& ident,
                                             const mongo::CollectionOptions& options, 
                                             std::unique_ptr<ChunkRocksDBInstance>& db)
{
    if (options.toCreate) {
        log() << "[assign] PartitionedRocksEngine::createOrLoadDB()-> createDB";
        IndexedRecordStorageId storageId = (IndexedRecordStorageId)GLOBAL_CONFIG_GET(DefaultStorageTypeId);
        auto chunkMetadata = stdx::make_unique<ChunkMetadata>(storageId, options.chunk, options.collection);
        log() << "[assign] PartitionedRocksEngine::createOrLoadDB()-> chunkMeta 1: " << chunkMetadata->ToString();
        invariantOK(CreateDB(ident, std::move(chunkMetadata), db));  //  TODO: don't need to crash on failure, just break assign
        db->setLeftChunkID(options.chunk.getName());
    } else {
        log() << "[assign] PartitionedRocksEngine::createOrLoadDB()-> loadDB";
        invariantOK(LoadDB(ident, db));
        db->setChunkVersion(options.chunk.getVersion()); //not shoud persist
        log() << "[assign] PartitionedRocksEngine::createOrLoadDB()-> chunkMeta 2: " << db->GetMetadata().ToString();
        db->setLeftChunkID(options.chunk.getName());

        options.collection = db->GetMetadata().GetCollection();
        options.chunk = db->GetMetadata().GetChunk();
    }

    return Status::OK();
}

std::unique_ptr<mongo::RocksRecordStore> PartitionedRocksEngine::populateRecordStore(
    mongo::StringData ns,
    mongo::StringData ident,
    const std::string& prefix,
    const mongo::CollectionOptions& options)
{
    NamespaceString nss(ns);
    log() << "[assignChunk] PartitionedRocksEngine::populateRecordStore ns: " << nss << "; options: "
        << options.toBSON().toString() << "; chunk: " << options.chunk.toString() << ";    collection :"
        << options.collection.toString();
    if(IsSystemCollection(ns))
    {
         //  For metadata currently do the same thing as before - use single DB
         return mongo::RocksEngine::populateRecordStore(ns, ident, prefix, options);
    }

    //std::unique_ptr<ChunkRocksDBInstance> db;
    //if(!nss.isChunk()) {
    //    log() << "[assignChunk] PartitionedRocksEngine::createOrLoadDB ";
    //    createOrLoadDB(ident, options, db);
    //}
    
    return options.capped ?
        mongo::stdx::make_unique<ChunkRocksRecordStore>(
                     ns, ident, nullptr, prefix,
                     true, options.cappedSize ? options.cappedSize : 4096,  // default size
                     options.cappedMaxDocs ? options.cappedMaxDocs : -1) :
        mongo::stdx::make_unique<ChunkRocksRecordStore>(ns, ident, nullptr, prefix);
}

Status PartitionedRocksEngine::updateChunkMetadata(OperationContext* opCtx,
                                                   mongo::StringData ident,
                                                   BSONArray& indexes,
                                                   RecordStore* recordStore){
    
    mongo::BSONObj config = _getIdentConfig(ident);
    std::string path = config.getStringField("dbPath");
    ChunkRocksRecordStore* chunkRecordStore = static_cast<ChunkRocksRecordStore *> (recordStore);
    ChunkRocksDBInstance &db = chunkRecordStore->GetChunkDBInstance();
    ChunkMetadata *meta = db.GetChunkMetadata();
    std::string refinedPath = path;
    if(IsPLogUsed()) {    //TODO remove after the dbpath is nolonger used by RocksDB
        refinedPath.assign(path, 0, path.find_last_of('/'));
    }
    
    CollectionType collection = meta->GetCollection();
    collection.setIndex(indexes);
    auto chunkMetadata = stdx::make_unique<ChunkMetadata>(meta->GetIndexedRecordStorageId(),meta->GetChunk(),collection);
    db.UpdateMetadata(_options(path),db.getChunkMetaPath(refinedPath),std::move(chunkMetadata));
    return Status::OK();
}

Status PartitionedRocksEngine::postInitRecordStore(
    mongo::StringData ns,
    mongo::StringData ident,
    const mongo::CollectionOptions& options,
    RecordStore* recordStore)
{
    log() << "[assignChunk] << PartitionedRocksEngine::postInitRecordStore ns: " << ns;
    invariant (!IsSystemCollection(ns));
    std::unique_ptr<ChunkRocksDBInstance> db;
    createOrLoadDB(ident, options, db);

    //std::string pathWithPLog = _getIdentConfig(ident).getStringField("dbPath");
    //std::string pathWithoutPLog = pathWithPLog;
    //pathWithoutPLog.assign(pathWithoutPLog, 0, pathWithoutPLog.find_last_of('/'));
    //invariantRocksOKWithNoCore(db->initTransactionLogModule(pathWithoutPLog));

    ChunkRocksRecordStore* chunkRecordStore = static_cast<ChunkRocksRecordStore *> (recordStore);
    chunkRecordStore->PostInit(std::move(db));
    return Status::OK();
}

// non public api
Status PartitionedRocksEngine::_createIdent(StringData ident, BSONObjBuilder* configBuilder, int64_t prefix, const std::string & dbPath) {
    BSONObj config;

    {
        stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
        if (_identMap.find(ident) != _identMap.end()) {
            // already exists
            return Status::OK();
        }

        if (prefix == 0){
            prefix = ++_maxPrefix;
        }

        configBuilder->append("prefix", static_cast<int32_t>(prefix));
        if (!dbPath.empty()){
            configBuilder->append("dbPath", dbPath);
        } else {
            //for test 
            std::string path = _path;
            char prefix_str[sizeof(prefix) + 1] = {0};
            sprintf(prefix_str, "%ld", prefix);
            path = path + "/" + prefix_str;
            configBuilder->append("dbPath", path);
        }

        config = std::move(configBuilder->obj());
        _identMap[ident] = config.copy();
    }

    BSONObjBuilder builder;

    auto s = getMetadataDB()->Put(rocksdb::WriteOptions(), kMetadataPrefix + ident.toString(),
                      rocksdb::Slice(config.objdata(), config.objsize()));

    if (s.ok()) {
        // As an optimization, add a key <prefix> to the DB
        std::string encodedPrefix(encodePrefix(prefix));
        s = getMetadataDB()->Put(rocksdb::WriteOptions(), encodedPrefix, rocksdb::Slice());
    }

    return rocksToMongoStatus(s);
}

Status PartitionedRocksEngine::createSortedDataInterface(OperationContext* opCtx, StringData ident, const IndexDescriptor* desc)
{
    BSONObjBuilder configBuilder;
    // let index add its own config things
    //RocksIndexBase::generateConfig(&configBuilder, _formatVersion);
    return _createIdent(ident, &configBuilder, desc->getPrefix(), "");
}

Status PartitionedRocksEngine::createRecordStore(OperationContext* opCtx, StringData ns, StringData ident, const CollectionOptions& options)
{
    log() << "PartitionedRocksEngine::createRecordStore()-> dbPath: " << options.dbPath;
    BSONObjBuilder configBuilder;
    MDB_RIF(_createIdent(ident, &configBuilder, options.prefix, options.dbPath));
    if (NamespaceString::oplog(ns))
    {
        _oplogIdent = ident.toString();
        // oplog needs two prefixes, so we also reserve the next one
        uint64_t oplogTrackerPrefix = 0;
        {
            stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
            oplogTrackerPrefix = ++_maxPrefix;
        }
        // we also need to write out the new prefix to the database. this is just an
        // optimization
        std::string encodedPrefix(encodePrefix(oplogTrackerPrefix));
        MDB_RIF(getMetadataDB()->Put(rocksdb::WriteOptions(), encodedPrefix, rocksdb::Slice()));
    }

    return mongo::Status::OK();
}

mongo::RocksIndexBase* PartitionedRocksEngine::populateIndex(
    mongo::StringData ident,
    const mongo::IndexDescriptor* desc,
    const std::string& prefix,
    mongo::BSONObj&& config
    )
{
    NamespaceString nss = desc->getCollection()->ns();
    if(IsSystemCollection(StringData(nss.ns())))
        return RocksEngine::populateIndex(ident, desc, prefix, std::move(config));

    ChunkRocksDBInstance& db = (ChunkRocksDBInstance&)GetDBInstance(desc);
    const ChunkMetadata& metadata = db.GetMetadata();

    //  For MongoRocksStandard we can use standard MongoRocks classes
    if(metadata.GetIndexedRecordStorageId() == IndexedRecordStorageId::MongoRocksStandard)
        return RocksEngine::populateIndex(&db, ident.toString(), desc, prefix, std::move(config));

    //  We need to use our own implementations, use assign index desc, todo get from chunkmeta
    //const IndexDefinition* indexDefinition = metadata.GetIndex(desc->getPrefix());
    const IndexDefinition indexDefinition(desc->getPrefix(),desc->infoObj());
    LOG(1)<<"PartitionedRocksEngine::populateIndex --prefix:"<< desc->getPrefix()
          <<"--desc:"<<desc->toString()<<"-- metadata:" <<metadata.ToString();
    
    std::unique_ptr<IIndexRecordFormatter> indexFormatter = db.GetIndexedRecordStorage().GetSecondaryIndexFormatter(indexDefinition);
    return desc->unique() ?
        (RocksIndexBase*)new ChunkRocksUniqueIndex(db, std::move(indexFormatter)) :
        (RocksIndexBase*)new ChunkRocksStandardIndex(db, std::move(indexFormatter));
}

const ChunkRocksDBInstance& PartitionedRocksEngine::GetDBInstance(const mongo::IndexDescriptor* desc)
{
    const mongo::Collection* collection = desc->getCollection();
    ChunkRocksRecordStore* recordStore = (ChunkRocksRecordStore*)collection->getRecordStore();
    return recordStore->GetChunkDBInstance();
}

mongo::Status PartitionedRocksEngine::LoadDB(mongo::StringData ident, std::unique_ptr<ChunkRocksDBInstance>& dbInstance)
{
    mongo::BSONObj config = _getIdentConfig(ident);
    std::string path = config.getStringField("dbPath");

    log() << "PartitionedRocksEngine::LoadDB(): path: " << path; 
    MDB_RET(OpenDB(path, dbInstance));
}

mongo::Status PartitionedRocksEngine::CreateDB(mongo::StringData ident, std::unique_ptr<mongo::ChunkMetadata> chunkMetadata, std::unique_ptr<ChunkRocksDBInstance>& dbInstance)
{
    mongo::BSONObj config = _getIdentConfig(ident);
    std::string path = config.getStringField("dbPath");
    std::unique_ptr<mongo::ChunkRocksDBInstance> db;

    std::string refinedPath = path;
    if(IsPLogUsed()) {    //TODO remove after the dbpath is nolonger used by RocksDB
        refinedPath.assign(path, 0, path.find_last_of('/'));
    } 

    log() << "PartitionedRocksEngine::CreateDB()-> path: " << path << "; refinedPath: " << refinedPath;
    MDB_RIF(mongo::ChunkRocksDBInstance::Create(_options(path), refinedPath, std::move(chunkMetadata), db));
    dbInstance = std::move(db);
    return mongo::Status::OK();
}

RecoveryUnit* PartitionedRocksEngine::newRecoveryUnit() {
    bool useMoreDB =  ClusterRole::ShardServer == serverGlobalParams.clusterRole 
        || ClusterRole::None == serverGlobalParams.clusterRole;

    if (useMoreDB) {
        return new MoreDbRocksRecoveryUnit(&_snapshotManager, _db.get());
    } else {
        return RocksEngine::newRecoveryUnit();
    }
}

Status PartitionedRocksEngine::dropIdent(OperationContext* opCtx, StringData ident)
{
    stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
    _identMap.erase(ident);

    return Status::OK();
}

bool PartitionedRocksEngine::GetSharedResourcesOperations(uint64_t& ChunkID, std::shared_ptr<TransactionLog::SharedResourceModule> provider,
                                            std::vector<TransactionLog::SharedResourceReferenceOperation>& shareResources,
                                            int maxBatchMaxSize){
    stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);

    for (auto recordStoreEntry: _identCollectionMap){
        provider = recordStoreEntry.second->GetChunkDBInstance().GetSharedResourceModule();
        ChunkID = recordStoreEntry.second->GetChunkDBInstance().getLeftChunkID();
        if (provider){
            provider->GetUnprocessedOperations(maxBatchMaxSize, shareResources);
        }
        if (shareResources.size() > 0){
            return true;
        }
    }

    return false;
}

void PartitionedRocksEngine::GetChunkMetadata(std::string chunkId, std::map<std::string, std::string>& chunkMetadataMap, bool getALL)
{
    stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);

    chunkMetadataMap.clear();
    if (getALL) {
        // Get all chunk metadata
        for (auto recordStoreEntry: _identCollectionMap) {
            auto chunkMetadata = recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata(); 
            if (chunkMetadata){
                chunkMetadataMap[(chunkMetadata->GetChunk()).getName()] = tojson(chunkMetadata->ToBSON(), Strict, true);
            }
        }
    } else {
        // Get specific chunk metadata
        for (auto recordStoreEntry: _identCollectionMap) {
            auto chunkMetadata = recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata(); 
            if (chunkMetadata && (chunkMetadata->GetChunk()).getName() == chunkId) {
                chunkMetadataMap[(chunkMetadata->GetChunk()).getName()] = tojson(chunkMetadata->ToBSON(), Strict, true);
            }
        }
    }
}

void PartitionedRocksEngine::GetDbStats(std::string chunkId, std::map<std::string, std::string>& dbStatsMap, bool getALL)
{
    stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
    std::string statsString;

    dbStatsMap.clear();
    if (getALL) {
        // Get all db stats
        for (auto recordStoreEntry: _identCollectionMap) {
            auto chunkMetadata = recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata(); 
            if (recordStoreEntry.second->GetChunkDBInstance().GetDB()->GetProperty("rocksdb.stats", &statsString)) {
                dbStatsMap[(chunkMetadata->GetChunk()).getName()] = statsString;
            }
        }
    } else {
        // Get db stats corresponding to a specific chunk
        for (auto recordStoreEntry: _identCollectionMap) {
            auto chunkMetadata = recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata(); 
            if (chunkMetadata && (chunkMetadata->GetChunk()).getName() == chunkId) {
                if (recordStoreEntry.second->GetChunkDBInstance().GetDB()->GetProperty("rocksdb.stats", &statsString)) { 
                    dbStatsMap[(chunkMetadata->GetChunk()).getName()] = statsString;
                }
            }
        }
    }
}

void PartitionedRocksEngine::DumpSharedResourceStats(std::string chunkId, const char* path, bool getALL)
{
    stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);

    if (getALL) {
        // Dump all chunk shared resources stats
        for (auto recordStoreEntry: _identCollectionMap) {
            auto chunkMetadata = recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata(); 
            if (chunkMetadata){
                recordStoreEntry.second->GetChunkDBInstance().DumpSharedResourceStats(path);
            }
        }
    } else {
        // Dump shared resource stats corresponding to a specific chunk
        for (auto recordStoreEntry: _identCollectionMap) {
            auto chunkMetadata = recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata(); 
            if (chunkMetadata && (chunkMetadata->GetChunk()).getName() == chunkId) {
                recordStoreEntry.second->GetChunkDBInstance().DumpSharedResourceStats(path);
            }
        }
    }
}

void PartitionedRocksEngine::GetShardId(ShardId& shardId)
{
    stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
    for (auto recordStoreEntry: _identCollectionMap) {
        auto chunkMetadata = recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata(); 
        if (chunkMetadata){
            shardId = (chunkMetadata->GetChunk()).getShard();
            return;
        }
    }
}

mongo::Status PartitionedRocksEngine::dropUserCollections(OperationContext* opCtx, StringData ident){
     return deleteCollectionIdent(opCtx, ident, true);
}

mongo::Status PartitionedRocksEngine:: offloadUserCollections(OperationContext* opCtx, StringData ident){
    return deleteCollectionIdent(opCtx, ident, false);
}

Status PartitionedRocksEngine::deleteCollectionIdent(OperationContext* opCtx, 
                                                     const StringData& ident, 
                                                     bool deleteFromDisk)
{
    rocksdb::WriteBatch wb;
    wb.Delete(kMetadataPrefix + ident.toString());
    rocksdb::WriteOptions syncOptions;
    syncOptions.sync = true;
    MDB_RIF(getMetadataDB()->Write(syncOptions, &wb));

    {
        stdx::lock_guard<stdx::mutex>lk(_identObjectMapMutex);
        _identCollectionMap.erase(ident);
    }

    std::string path = _getIdentConfig(ident).getStringField("dbPath");

    {
        stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
        _identMap.erase(ident);
    }

    if (deleteFromDisk) {
        MDB_RIF( deleteDir(path) );
        log()<<"[PartitionedRocksEngine::dropUserCollections] end dbpath: " << path;
    }

    return Status::OK();
}

void PartitionedRocksEngine::setStorageEngineLogLevel(int level) 
{
    rocksdb::InfoLogLevel rlevel;
    switch(level)
    {
    case 0:
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
        rlevel = rocksdb::InfoLogLevel::DEBUG_LEVEL;
        break;
    case -1:
        rlevel = rocksdb::InfoLogLevel::INFO_LEVEL;
        break;
    case -2:
        rlevel = rocksdb::InfoLogLevel::WARN_LEVEL;
        break;
    case -3:
        rlevel = rocksdb::InfoLogLevel::ERROR_LEVEL;
        break;
    case -4:
        rlevel = rocksdb::InfoLogLevel::FATAL_LEVEL;
        break;
    default:
        rlevel = rocksdb::InfoLogLevel::HEADER_LEVEL;
    }

    index_LOG(0) << "[setLog] PartitionedRocksEngine::setStorageEngineLogLevel newLevel: " << level;

    ResultHandling::MongoRocksLoggerForChunk::setLogLevel(rlevel);

    stdx::lock_guard<stdx::mutex>lk(_identObjectMapMutex);
    for (auto it=_identCollectionMap.begin(); it!=_identCollectionMap.end(); ++it) {
        if (it->second->GetChunkDBInstance().getLogger()) {
            it->second->GetChunkDBInstance().getLogger()->SetInfoLogLevel(rlevel);
        }
    }
}

}

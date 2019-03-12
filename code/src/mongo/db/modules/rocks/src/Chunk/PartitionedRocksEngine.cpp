
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage



#include "PartitionedRocksEngine.h"

#include <rocksdb/cache.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/comparator.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/experimental.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include <mongo/db/index/index_descriptor.h>
#include "../gc_client.h"
#include "../gc_manager.h"
#include "../mongo_rocks_result_handling.h"
#include "../more_db_recovery_unit.h"
#include "../rocks_engine.h"
#include "../rocks_global_options.h"
#include "../rocks_index.h"
#include "../rocks_recovery_unit.h"
#include "../rocks_util.h"
#include "ChunkMetadata.h"
#include "ChunkRocksRecordStore.h"
#include "chunk_rocks_index.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/util_extend/GlobalConfig.h"
#include "mongo/db/server_options.h"
#include "mongo/db/repl/replication_coordinator_global.h"

namespace mongo {
    class GCClient;

    PartitionedRocksEngine::PartitionedRocksEngine(const std::string& path, bool durable,
                                                   int formatVersion)
        : mongo::RocksEngine(path, durable, formatVersion) {
        if (GLOBAL_CONFIG_GET(enableGlobalGC) && 
             ClusterRole::ShardServer == serverGlobalParams.clusterRole) {
            _gcManager.reset(new GCManager());
            _gcManager->start();
            _gcClient.reset(new GCClient(this));
            _gcClient->start();
        } else {
            index_warning() << "[initMongoRocks] disable GC!";
        }

        invariant(serverGlobalParams.clusterRole != ClusterRole::ConfigServer);

        int flushNum = ConfigReader::getInstance()->getDecimalNumber<int>("PublicOptions",
                                                                          "flush_threads_num");
        if (0 == flushNum) {
            ProcessInfo pi;
            flushNum = pi.getNumCores();
        }

        _chunkFlush.reset(new ChunkJounalFlush(flushNum));
    }

    PartitionedRocksEngine::~PartitionedRocksEngine() {
        if (_gcClient) {
            invariant(!_gcClient->isRuning());
        }

        if (_gcManager) {
            invariant(!_gcManager->isRuning());
        }

        if (_chunkFlush) {
            _chunkFlush.reset();
        }

        _dbInstances.clear();
    }

    Status PartitionedRocksEngine::createOrLoadDB(const mongo::CollectionOptions& options,
                                                  std::unique_ptr<ChunkRocksDBInstance>& db) {
        if (options.toCreate) {
            IndexedRecordStorageId storageId;
            if (CollectionType::TableType::kNonShard == options.collection.getTabType()) {
                // non-sharded collection use MongoRocksStandard to keep natural sort.
                storageId = IndexedRecordStorageId::MongoRocksStandard;
            } else {
                // sharded collection use config type
                storageId = IndexedRecordStorageId::ShardKeyPrefixedDataWithStandardIndexColumnFamily;
            }
            auto chunkMetadata =
                stdx::make_unique<ChunkMetadata>(storageId, options.chunk, options.collection);

            invariantOK(CreateDB(options, std::move(chunkMetadata),
                                 db));  //  TODO: don't need to crash on failure, just break assign
            db->setLeftChunkID(options.chunk.getName());
        } else {
            invariantOK(LoadDB(options, db));
            db->setChunkVersion(options.chunk.getVersion());  // not shoud persist
            db->setLeftChunkID(options.chunk.getName());

            options.collection = db->GetMetadata().GetCollection();
            options.chunk = db->GetMetadata().GetChunk();
        }

        return Status::OK();
    }

    std::unique_ptr<mongo::RocksRecordStore> PartitionedRocksEngine::populateRecordStore(
        mongo::StringData ns, mongo::StringData ident, const std::string& prefix,
        const mongo::CollectionOptions& options) {
        NamespaceString nss(ns);
        index_log() << "[assignChunk]  ns: " << nss;
        if (IsSystemCollection(ns)) {
            //  For metadata currently do the same thing as before - use single DB
            return mongo::RocksEngine::populateRecordStore(ns, ident, prefix, options);
        }
        // std::unique_ptr<ChunkRocksDBInstance> db;
        // if(!nss.isChunk()) {
        //    log() << "[assignChunk] PartitionedRocksEngine::createOrLoadDB ";
        //    createOrLoadDB(ident, options, db);
        //}

        return options.capped
                   ? mongo::stdx::make_unique<ChunkRocksRecordStore>(
                         ns, ident, nullptr, prefix, true,
                         options.cappedSize ? options.cappedSize : 4096,  // default size
                         options.cappedMaxDocs ? options.cappedMaxDocs : -1)
                   : mongo::stdx::make_unique<ChunkRocksRecordStore>(ns, ident, nullptr, prefix);
    }

    Status PartitionedRocksEngine::updateChunkMetadata(OperationContext* opCtx,
                                                       mongo::StringData ident, BSONArray& indexes,
                                                       RecordStore* recordStore) {
        mongo::BSONObj config = _getIdentConfig(ident);
        std::string path = config.getStringField("dbPath");
        ChunkRocksRecordStore* chunkRecordStore = static_cast<ChunkRocksRecordStore*>(recordStore);
        ChunkRocksDBInstance& db = chunkRecordStore->GetChunkDBInstance();
        ChunkMetadata* meta = db.GetChunkMetadata();
        std::string refinedPath = path;

        CollectionType collection = meta->GetCollection();
        collection.setIndex(indexes);
        auto chunkMetadata = stdx::make_unique<ChunkMetadata>(meta->GetIndexedRecordStorageId(),
                                                              meta->GetChunk(), collection);
        db.UpdateMetadata(db.getChunkMetaPath(refinedPath), std::move(chunkMetadata));
        return Status::OK();
    }

    Status PartitionedRocksEngine::postInitRecordStore(mongo::StringData ns,
                                                       mongo::StringData ident,
                                                       RecordStore* recordStore) {
        index_log() << "[assignChunk] postInitRecordStore ns: " << ns << "; ident: " << ident;
        // for rename8.js witch want to rename from others to "system.users"
        // the createUser command may have problem,if we change IsSystemCollection like
        // "system.profile".
        NamespaceString nss(ns);
        invariant(!IsSystemCollection(ns));
        /*std::unique_ptr<ChunkRocksDBInstance> db;
        createOrLoadDB(ident, options, db);*/

        // std::string pathWithPLog = _getIdentConfig(ident).getStringField("dbPath");
        // std::string pathWithoutPLog = pathWithPLog;
        // pathWithoutPLog.assign(pathWithoutPLog, 0, pathWithoutPLog.find_last_of('/'));
        // invariantRocksOKWithNoCore(db->initTransactionLogModule(pathWithoutPLog));

        ChunkRocksRecordStore* chunkRecordStore = static_cast<ChunkRocksRecordStore*>(recordStore);
        auto chunkDbInstance = getRocksDB(ns.toString());
        chunkRecordStore->PostInit(chunkDbInstance);

        // addRocksDB(nss.toString(), std::move(chunkDbInstance));

        {
            stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
            _identCollectionMap[ident] = chunkRecordStore;
        }

        return Status::OK();
    }

    // non public api
    Status PartitionedRocksEngine::_createIdent(StringData ident, BSONObjBuilder* configBuilder,
                                                int64_t prefix, const std::string& dbPath) {
        BSONObj config;

        {
            stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
            if (_identMap.find(ident) != _identMap.end()) {
                // already exists
                return Status::OK();
            }

            if (prefix == 0) {
                prefix = ++_maxPrefix;
            }

            configBuilder->append("prefix", static_cast<int32_t>(prefix));
            if (!dbPath.empty()) {
                configBuilder->append("dbPath", dbPath);
            } else {
                // for test
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

        auto db = getMetadataDB();
        if (db == nullptr) {
            index_err() << "getMetadataDB failed.";
            return Status(ErrorCodes::InternalError, "getMetadataDB failed.");
        }

        auto s = db->Put(rocksdb::WriteOptions(), kMetadataPrefix + ident.toString(),
                         rocksdb::Slice(config.objdata(), config.objsize()));

        if (s.ok()) {
            // As an optimization, add a key <prefix> to the DB
            std::string encodedPrefix(encodePrefix(prefix));
            s = getMetadataDB()->Put(rocksdb::WriteOptions(), encodedPrefix, rocksdb::Slice());
        }

        return rocksToMongoStatus(s);
    }

    Status PartitionedRocksEngine::createSortedDataInterface(OperationContext* opCtx,
                                                             StringData ident,
                                                             const IndexDescriptor* desc) {
        BSONObjBuilder configBuilder;
        // let index add its own config things
        MongoRocksIndexBase::generateConfig(&configBuilder, _formatVersion);
        return _createIdent(ident, &configBuilder, desc->getPrefix(), "");
    }

    Status PartitionedRocksEngine::createRecordStore(OperationContext* opCtx, StringData ns,
                                                     StringData ident,
                                                     const CollectionOptions& options) {
        index_log() << "[assignChunk] createRecordStore()-> dbPath: " << options.dbPath
                    << "; ident: " << ident;
        BSONObjBuilder configBuilder;
        MDB_RIF(_createIdent(ident, &configBuilder, options.prefix, options.dbPath));
        if (NamespaceString::oplog(ns)) {
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
            auto db = getMetadataDB();
            if (db == nullptr) {
                index_err() << "[assignChunk] getMetadataDB error!";
                return Status(ErrorCodes::InternalError, "getMetadataDB failed.");
            }
            MDB_RIF(db->Put(rocksdb::WriteOptions(), encodedPrefix, rocksdb::Slice()));
        }

        return mongo::Status::OK();
    }

    mongo::RocksIndexBase* PartitionedRocksEngine::populateIndex(mongo::StringData ident,
                                                                 const mongo::IndexDescriptor* desc,
                                                                 const std::string& prefix,
                                                                 mongo::BSONObj&& config) {
        NamespaceString nss = desc->getCollection()->ns();
        if (IsSystemCollection(StringData(nss.ns())))
            return RocksEngine::populateIndex(ident, desc, prefix, std::move(config));
        ChunkRocksDBInstance& db = (ChunkRocksDBInstance&)GetDBInstance(desc);
        const ChunkMetadata& metadata = db.GetMetadata();

        //  For MongoRocksStandard we can use standard MongoRocks classes
        if (metadata.GetIndexedRecordStorageId() == IndexedRecordStorageId::MongoRocksStandard)
            return RocksEngine::populateIndex(&db, ident.toString(), desc, prefix,
                                              std::move(config));

        //  We need to use our own implementations, use assign index desc, todo get from chunkmeta
        // const IndexDefinition* indexDefinition = metadata.GetIndex(desc->getPrefix());
        const IndexDefinition indexDefinition(desc->getPrefix(), desc->infoObj());

        std::unique_ptr<IIndexRecordFormatter> indexFormatter =
            db.GetIndexedRecordStorage().GetSecondaryIndexFormatter(indexDefinition);
        return desc->unique()
                   ? (RocksIndexBase*)new ChunkRocksUniqueIndex(db, std::move(indexFormatter))
                   : (RocksIndexBase*)new ChunkRocksStandardIndex(db, std::move(indexFormatter));
    }

    const ChunkRocksDBInstance& PartitionedRocksEngine::GetDBInstance(
        const mongo::IndexDescriptor* desc) {
        const mongo::Collection* collection = desc->getCollection();
        ChunkRocksRecordStore* recordStore = (ChunkRocksRecordStore*)collection->getRecordStore();
        return recordStore->GetChunkDBInstance();
    }

    mongo::Status PartitionedRocksEngine::LoadDB(
        const mongo::CollectionOptions& options,
        std::unique_ptr<ChunkRocksDBInstance>& dbInstance) {
        std::string path = options.chunk.getRootFolder();
        dbInstance.reset(new mongo::ChunkRocksDBInstance(path, _chunkFlush.get()));
        MDB_RET(OpenDB(path, dbInstance.get(), &options));
    }

    mongo::Status PartitionedRocksEngine::CreateDB(
        const mongo::CollectionOptions& options,
        std::unique_ptr<mongo::ChunkMetadata> chunkMetadata,
        std::unique_ptr<ChunkRocksDBInstance>& dbInstance) {
        std::string path = options.chunk.getRootFolder();
        index_log() << "[assignChunk] CreateDB new DB path: " << path;
        dbInstance = stdx::make_unique<ChunkRocksDBInstance>(path, _chunkFlush.get());
        MDB_RIF(mongo::ChunkRocksDBInstance::Create(_options(path), path, dbInstance.get(),
                                                    std::move(chunkMetadata)));
        return mongo::Status::OK();
    }

    RecoveryUnit* PartitionedRocksEngine::newRecoveryUnit() {
        bool useMoreDB = ClusterRole::ShardServer == serverGlobalParams.clusterRole ||
                         ClusterRole::None == serverGlobalParams.clusterRole;

        if (useMoreDB) {
            return new MoreDbRocksRecoveryUnit(&_snapshotManager, _db.get());
        } else {
            return RocksEngine::newRecoveryUnit();
        }
    }

    // ns is not empty only in user db remove index case on shard server 
    Status PartitionedRocksEngine::dropIdent(OperationContext* opCtx, StringData ident, StringData ns) {
        //case1: single db mode, call RocksEngine::dropIdent
        //case2: multi db mode but not remove index, only erase from _identMap
        //case3: user table remove index operation, need to manage dropped prefix.
        //case4: remove index for system collction in multi db mode not to deal dropped prefix.
        auto replMode = repl::getGlobalReplicationCoordinator()->getReplicationMode();
        bool useMultiInstance = GLOBAL_CONFIG_GET(UseMultiRocksDBInstanceEngine) &&
                                (serverGlobalParams.clusterRole == ClusterRole::ShardServer ||
                                (ClusterRole::None == serverGlobalParams.clusterRole 
                                && replMode != repl::ReplicationCoordinator::Mode::modeReplSet));
        NamespaceString nss(ns);
        if(!useMultiInstance){
            //single db mode
            return mongo::RocksEngine::dropIdent(opCtx, ident);
        }else{
            //user chunk db instance.
            if(!ns.empty() && !nss.isSystemCollection()){
                ChunkRocksDBInstance* dbInstancePtr = getRocksDB(ns.toString());
                uint32_t int_prefix;
                auto cfg = _getIdentConfig(ident);
                // calculate which prefixes we need to drop
                std::vector<std::string> prefixesToDrop;
                prefixesToDrop.push_back(_extractPrefix(cfg));

                int_prefix = cfg.getField("prefix").numberInt();
                std::unordered_set<uint32_t> droppedPrefixesSet;
                droppedPrefixesSet.insert(int_prefix);
                //update  dropped prefix for chunkRocksDBinstance.
                dbInstancePtr->updateDroppedPrefixes(droppedPrefixesSet);
                // Suggest compaction for the prefixes that we need to drop, So that
                // we free space as fast as possible.
                for (auto& prefix : prefixesToDrop) {
                    /*Status DeleteFilesInRange(ColumnFamilyHandle* column_family,
                            const Slice* begin, const Slice* end);*/
                      
                    std::string end_prefix_str = rocksGetNextPrefix(prefix);
                    rocksdb::Slice start_prefix = prefix;
                    rocksdb::Slice end_prefix = end_prefix_str;
                    rocksdb::ColumnFamilyHandle* cfh;
                    if(IndexedRecordStorageId::MongoRocksStandard == 
                            dbInstancePtr->GetChunkMetadata()->GetIndexedRecordStorageId()){
                        cfh = dbInstancePtr->GetColumnFamily(ChunkRocksDBInstance::dataColumnFamilyId);
                    }else {
                        cfh = dbInstancePtr->GetColumnFamily(ChunkRocksDBInstance::indexColumnFamilyId);
                    }
                    auto s_1 = rocksdb::DeleteFilesInRange(dbInstancePtr->GetDB(), cfh, &start_prefix, &end_prefix);
                    auto s_2 = rocksdb::experimental::SuggestCompactRange(dbInstancePtr->GetDB(), cfh,&start_prefix, &end_prefix);
                    index_log()<<"dropIdent prefix: "<<int_prefix <<",DeleteFilesInRange status: "<< s_1.ToString()
                               <<", SuggestCompactRange status: "<< s_2.ToString();
                    if (!s_1.ok() || !s_2.ok() ) {
                        index_err() <<"dropIdent prefix: "<<int_prefix <<",DeleteFilesInRange status: "<< s_1.ToString()
                               <<", SuggestCompactRange status: "<< s_2.ToString();
                    }
                }
                //update chunk meta
                ChunkMetadata* meta = dbInstancePtr->GetChunkMetadata();
                CollectionType collection = meta->GetCollection();
                collection.setDroppedPrefixes(dbInstancePtr->getDroppedPrefixes());
                auto chunkMetadata = stdx::make_unique<ChunkMetadata>(meta->GetIndexedRecordStorageId(),
                                                              meta->GetChunk(), collection);
                std::string path = meta->GetChunk().getRootFolder();
                index_log()<<"dropIdent prefix: "<<int_prefix <<"save meta : "<< collection<<", path: "<<path;
                dbInstancePtr->UpdateMetadata(dbInstancePtr->getChunkMetaPath(path), std::move(chunkMetadata));
            }

            stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
            _identMap.erase(ident);
        }

        return mongo::Status::OK();
    }

    void PartitionedRocksEngine::SetLastProcessedOperationLSN(std::string& lastProcessedChunkNs,
                                                              uint64_t lastProcessLSN) {
        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);

        for (auto recordStoreEntry : _identCollectionMap) {
            auto chunkMetadata = recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata();

            if (chunkMetadata && Status::OK() == (chunkMetadata->GetChunk()).validate()) {
                auto fullChunkNs = chunkMetadata->GetChunk().getFullNs();

                if (fullChunkNs == lastProcessedChunkNs) {
                    auto provider =
                        recordStoreEntry.second->GetChunkDBInstance().GetSharedResourceModule();
                    provider->SetLastProcessedOperationLSN(lastProcessLSN);
                    break;
                }
            }
        }
    }

    bool PartitionedRocksEngine::GetSharedResourcesOperations(
        uint64_t& chunkId,
        std::vector<TransactionLog::SharedResourceReferenceOperation>& shareResources,
        int maxBatchMaxSize, std::string& lastProcessedChunkNs, std::string& collIdent) {
        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
        StringMap<RocksRecordStore *>::iterator lastProcessedIter, startProcessingIter;

        index_LOG(1) << "[GC Client] Begin GetSharedResourcesOperations lastProcessedChunkNs("
                     << lastProcessedChunkNs << ")";
        // Decide the startProcessingIter which is the next iter of lastProcessedIter
        {
            // Find the lastProcessedIter
            if (lastProcessedChunkNs != "") {
                for (lastProcessedIter = _identCollectionMap.begin();
                     lastProcessedIter != _identCollectionMap.end(); lastProcessedIter++) {
                    auto chunkMetadata =
                        lastProcessedIter->second->GetChunkDBInstance().GetChunkMetadata();
                    if (chunkMetadata && Status::OK() == (chunkMetadata->GetChunk()).validate()) {
                        auto fullChunkNs = chunkMetadata->GetChunk().getFullNs();

                        if (fullChunkNs == lastProcessedChunkNs) {
                            break;
                        }
                    }
                }
            } else {
                lastProcessedIter = _identCollectionMap.end();
            }

            if (lastProcessedIter == _identCollectionMap.end()) {
                // If not found, set the startProcessingIter to the begin location
                index_LOG(1) << "[GC Client] GetSharedResourcesOperations startProcessingIter = "
                                "_identCollectionMap.begin()";
                startProcessingIter = _identCollectionMap.begin();
            } else {
                // If found, set the startProcessingIter to next iter of lastProcessedIter
                index_LOG(1) << "[GC Client] GetSharedResourcesOperations startProcessingIter = ++ "
                                "lastProcessedIter";
                startProcessingIter = ++lastProcessedIter;
            }
        }

        // Scan from startProcessingIter to end iter
        for (auto iter = startProcessingIter; iter != _identCollectionMap.end(); iter++) {
            auto chunkMetadata = iter->second->GetChunkDBInstance().GetChunkMetadata();
            if (chunkMetadata && Status::OK() == (chunkMetadata->GetChunk()).validate()) {
                auto fullChunkNs = chunkMetadata->GetChunk().getFullNs();
                index_LOG(1) << "[GC Client] GetSharedResourcesOperations 1_fullChunkNs("
                             << fullChunkNs << ")";

                auto provider = iter->second->GetChunkDBInstance().GetSharedResourceModule();
                chunkId = iter->second->GetChunkDBInstance().getLeftChunkID();
                if (provider) {
                    provider->GetUnprocessedOperations(maxBatchMaxSize, shareResources);
                }
                if (shareResources.size() > 0) {
                    lastProcessedChunkNs = fullChunkNs;
                    collIdent.clear();
                    collIdent = chunkMetadata->GetCollection().getIdent();
                    index_LOG(1) << "[GC Client] End GetSharedResourcesOperations ident : "
                                 << collIdent << " 1_lastProcessedChunkNs(" << lastProcessedChunkNs
                                 << ")";
                    if (collIdent.empty()) {
                        index_err() << "ident empty chunkMeta : " << chunkMetadata->ToString();
                        return false;
                    }
                    return true;
                }
            }
        }

        // Scan from begin iter to startProcessingIter
        for (auto iter = _identCollectionMap.begin(); iter != startProcessingIter; iter++) {
            auto chunkMetadata = iter->second->GetChunkDBInstance().GetChunkMetadata();
            if (chunkMetadata && Status::OK() == (chunkMetadata->GetChunk()).validate()) {
                auto fullChunkNs = chunkMetadata->GetChunk().getFullNs();
                index_LOG(1) << "[GC Client] GetSharedResourcesOperations 2_fullChunkNs("
                             << fullChunkNs << ")";

                auto provider = iter->second->GetChunkDBInstance().GetSharedResourceModule();
                chunkId = iter->second->GetChunkDBInstance().getLeftChunkID();
                if (provider) {
                    provider->GetUnprocessedOperations(maxBatchMaxSize, shareResources);
                }
                if (shareResources.size() > 0) {
                    lastProcessedChunkNs = fullChunkNs;
                    collIdent = chunkMetadata->GetCollection().getIdent();
                    index_LOG(1) << "[GC Client] End GetSharedResourcesOperations ident: "
                                 << collIdent << "; 2_lastProcessedChunkNs(" << lastProcessedChunkNs
                                 << ")";
                    return true;
                }
            }
        }
        lastProcessedChunkNs = "";
        index_LOG(1) << "[GC Client] End GetSharedResourcesOperations 3_lastProcessedChunkNs("
                     << lastProcessedChunkNs << ")";

        return false;
    }

    void PartitionedRocksEngine::GetChunkMetadata(
        std::string chunkId, std::map<std::string, std::string>& chunkMetadataMap, bool getALL) {
        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);

        chunkMetadataMap.clear();
        if (getALL) {
            // Get all chunk metadata
            for (auto recordStoreEntry : _identCollectionMap) {
                auto chunkMetadata =
                    recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata();
                if (chunkMetadata) {
                    chunkMetadataMap[(chunkMetadata->GetChunk()).getName()] =
                        tojson(chunkMetadata->ToBSON(), Strict, true);
                }
            }
        } else {
            // Get specific chunk metadata
            for (auto recordStoreEntry : _identCollectionMap) {
                auto chunkMetadata =
                    recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata();
                if (chunkMetadata && (chunkMetadata->GetChunk()).getName() == chunkId) {
                    chunkMetadataMap[(chunkMetadata->GetChunk()).getName()] =
                        tojson(chunkMetadata->ToBSON(), Strict, true);
                }
            }
        }
    }

    void PartitionedRocksEngine::GetDbStats(std::string chunkId,
                                            std::map<std::string, std::string>& dbStatsMap,
                                            bool getALL) {
        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
        std::string statsString;

        dbStatsMap.clear();
        if (getALL) {
            // Get all db stats
            for (auto recordStoreEntry : _identCollectionMap) {
                auto chunkMetadata =
                    recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata();
                if (recordStoreEntry.second->GetChunkDBInstance().GetDB()->GetProperty(
                        "rocksdb.stats", &statsString)) {
                    dbStatsMap[(chunkMetadata->GetChunk()).getName()] = statsString;
                }
            }
        } else {
            // Get db stats corresponding to a specific chunk
            for (auto recordStoreEntry : _identCollectionMap) {
                auto chunkMetadata =
                    recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata();
                if (chunkMetadata && (chunkMetadata->GetChunk()).getName() == chunkId) {
                    if (recordStoreEntry.second->GetChunkDBInstance().GetDB()->GetProperty(
                            "rocksdb.stats", &statsString)) {
                        dbStatsMap[(chunkMetadata->GetChunk()).getName()] = statsString;
                    }
                }
            }
        }
    }

    void PartitionedRocksEngine::DumpSharedResourceStats(std::string chunkId, const char* path,
                                                         bool getALL) {
        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);

        if (getALL) {
            // Dump all chunk shared resources stats
            for (auto recordStoreEntry : _identCollectionMap) {
                auto chunkMetadata =
                    recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata();
                if (chunkMetadata) {
                    recordStoreEntry.second->GetChunkDBInstance().DumpSharedResourceStats(path);
                }
            }
        } else {
            // Dump shared resource stats corresponding to a specific chunk
            for (auto recordStoreEntry : _identCollectionMap) {
                auto chunkMetadata =
                    recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata();
                if (chunkMetadata && (chunkMetadata->GetChunk()).getName() == chunkId) {
                    recordStoreEntry.second->GetChunkDBInstance().DumpSharedResourceStats(path);
                }
            }
        }
    }

    void PartitionedRocksEngine::GetShardId(ShardId& shardId) {
        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
        for (auto recordStoreEntry : _identCollectionMap) {
            auto chunkMetadata = recordStoreEntry.second->GetChunkDBInstance().GetChunkMetadata();
            if (chunkMetadata) {
                shardId = (chunkMetadata->GetChunk()).getShard();
                return;
            }
        }
    }

    bool PartitionedRocksEngine::pauseGC(void) {
        if (_gcClient.get()) {
            if (!_gcClient->pause()) {
                return false;
            }
        }

        if (_gcManager.get()) {
            if (!_gcManager->pause()) {
                return false;
            }
        }

        return true;
    }

    void PartitionedRocksEngine::stopGC(void) {
        if (_gcClient.get()) {
            _gcClient->stop();
        }

        if (_gcManager.get()) {
            _gcManager->stop();
        }
    }

    bool PartitionedRocksEngine::continueGC(void) {
        bool suc = true;
        if (_gcClient.get()) {
            if (!_gcClient->continues()) {
                suc = false;
                // nothing;
            }
        }

        if (_gcManager.get()) {
            if (!_gcManager->continues()) {
                return false;
            }
        }

        return suc;
    }

    void PartitionedRocksEngine::setGCInfo(const std::string& ns, const std::string& dataPath) {
        invariant(_gcManager);
        _gcManager->setGCInfo(ns, dataPath);
    }

    Status PartitionedRocksEngine::destroyRocksDB(const std::string& nss) {
        index_log() << "[offloadChunk] nss: " << nss;
        invariant(!(NamespaceString(nss).isSystemCollection()));
        stdx::lock_guard<stdx::mutex> lk(_dbInstancesMutex);
        auto it = _dbInstances.find(nss);
        if (it != _dbInstances.end()) {
            _dbInstances.erase(it);
        }
        return Status::OK();
    }

    Status PartitionedRocksEngine::addRocksDB(const std::string& nss,
                                              std::unique_ptr<ChunkRocksDBInstance> dbInstance) {
        index_log() << "[assignChunk] nss: " << nss;
        invariant(!(NamespaceString(nss).isSystemCollection()));
        stdx::lock_guard<stdx::mutex> lk(_dbInstancesMutex);
        auto it = _dbInstances.find(nss);
        invariant(it == _dbInstances.end());
        _dbInstances[nss] = std::move(dbInstance);
        return Status::OK();
    }

    Status PartitionedRocksEngine::reNameNss(const std::string& srcName,
                                             const std::string& destName) {
        index_log() << "[renameCollection] srcNss: " << srcName << "; destName: " << destName;
        invariant(!(NamespaceString(srcName).isSystemCollection()));
        invariant(!(NamespaceString(destName).isSystemCollection()));
        stdx::lock_guard<stdx::mutex> lk(_dbInstancesMutex);
        auto it = _dbInstances.find(srcName);
        invariant(it != _dbInstances.end());
        auto destit = _dbInstances.find(destName);
        if (destit != _dbInstances.end()) {
            _dbInstances.erase(destit);
        }

        auto dbinstance = std::move(it->second);
        _dbInstances.erase(it);
        _dbInstances[destName] = std::move(dbinstance);
        return Status::OK();
    }

    ChunkRocksDBInstance* PartitionedRocksEngine::getRocksDB(const std::string& nss) {
        index_log() << "[assignChunk] ns: " << nss;
        stdx::lock_guard<stdx::mutex> lk(_dbInstancesMutex);
        auto it = _dbInstances.find(nss);
        invariant(it != _dbInstances.end());
        return it->second.get();
    }

    Status PartitionedRocksEngine::openChunkDbInstance(OperationContext* txn, StringData ns,
                                                       const mongo::CollectionOptions& options) {
        index_log() << "[assignChunk] ns: " << ns;
        std::unique_ptr<ChunkRocksDBInstance> db;
        createOrLoadDB(options, db);
        addRocksDB(ns.toString(), std::move(db));
        return Status::OK();
    }

    void PartitionedRocksEngine::genTransactionEngineStat(RocksTransactionEngine* engine,
                                  BSONArrayBuilder& builder,
                                  long long& numKey, long long& numActiveSnapshot, 
                                  long long& unCommited, long long& sortSnapshot) {
        long long _numKey = static_cast<long long>(engine->numKeysTracked());
        long long _numActiveSnapshot = static_cast<long long>(engine->numActiveSnapshots());
        long long _unCommited = static_cast<long long>(engine->numUnCommitedTracked());
        long long _sortSnapshot = static_cast<long long>(engine->numSortSnapshots());

        builder.append(_numKey);
        builder.append(_numActiveSnapshot);
        builder.append(_unCommited);
        builder.append(_sortSnapshot);

        numKey += _numKey;
        numActiveSnapshot += _numActiveSnapshot;
        unCommited += _unCommited;
        sortSnapshot += _sortSnapshot;
    }

    void PartitionedRocksEngine::getTransactionEngineStat(long long& numKey, long long& numActiveSnapshot) {
        stdx::lock_guard<stdx::mutex> lk(_dbInstancesMutex);
        BSONObjBuilder bob;
        long long unCommited = 0;
        long long sortSnapshot = 0;
        for(auto it = _dbInstances.begin(); it!=_dbInstances.end(); it++) {
            BSONArrayBuilder a;
            invariant(!it->second->isSystemChunk());
            genTransactionEngineStat(it->second->GetTransactionEngine(), a, numKey, numActiveSnapshot, unCommited, sortSnapshot);
            bob.appendArray(it->second->GetMetadata().GetChunk().getFullNs(), a.arr());
        }

        BSONArrayBuilder a;
        genTransactionEngineStat(getTransactionEngine(), a, numKey, numActiveSnapshot, unCommited, sortSnapshot);
        bob.appendArray("systemDB", a.arr());
        
        index_LOG(1) << "rocksTransactionEngine unCommited num: " << unCommited << "; sortSnapshot num : "
            << sortSnapshot << "; stat: " << bob.obj();
    }
    Status PartitionedRocksEngine::OpenDB(const std::string& path, ChunkRocksDBInstance* dbInstance,
                                      const CollectionOptions* options) const {
        index_log()<<"this is user db.";
        std::string pathWithoutPlog = path;
        if (pathWithoutPlog.find("plogcnt") != std::string::npos) {
            pathWithoutPlog.assign(pathWithoutPlog, 0, pathWithoutPlog.find_last_of('/'));
        }
        MDB_RIF(dbInstance->Open(_options(path), pathWithoutPlog, _durable, options));

        return mongo::Status::OK();
    }
}

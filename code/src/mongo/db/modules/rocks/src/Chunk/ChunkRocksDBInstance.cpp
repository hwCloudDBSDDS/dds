#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage



#include <mongo/util/background.h>
#include <mongo/util/log.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string>
#include "../gc_common.h"
#include "../gc_manager.h"
#include "../more_db_counter_manager.h"
#include "../rocks_engine.h"
#include "../rocks_engine.h"
#include "../rocks_global_options.h"
#include "../rocks_record_store.h"
#include "ChunkRocksDBInstance.h"
#include "ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage.h"
#include "maas_shared_resource_manager.h"
#include "mongo/platform/basic.h"
#include "mongo/util/duration.h"
#include "mongo/util/time_support.h"
#include "mongo/db/server_options.h"
#include "mongo/db/repl/replication_coordinator_global.h"

namespace mongo {
    using namespace TransactionLog;

    class RocksJournalFlusher : public mongo::BackgroundJob {
    public:
        explicit RocksJournalFlusher(RocksDurabilityManager* durabilityManager)
            : BackgroundJob(false /* deleteSelf */), _durabilityManager(durabilityManager) {}

        virtual std::string name() const { return "RocksJournalFlusher"; }

        virtual void run() {
            Client::initThread(name().c_str());

            mongo::log() << "[MongoRocks] Starting " << name() << " thread";

            while (!_shuttingDown.load()) {
                try {
                    _durabilityManager->waitUntilDurable(false);
                } catch (const UserException& e) {
                    invariant(e.getCode() == ErrorCodes::ShutdownInProgress);
                }

                int ms = storageGlobalParams.journalCommitIntervalMs;
                if (!ms) {
                    ms = 100;
                }

                sleepmillis(ms);
            }
            mongo::log() << "[MongoRocks] Stopping " << name() << " thread";
        }

        void shutdown() {
            _shuttingDown.store(true);
            wait();
        }

    private:
        RocksDurabilityManager* _durabilityManager;  // not owned
        std::atomic<bool> _shuttingDown{false};      // NOLINT
    };

    ChunkRocksDBInstance::ChunkRocksDBInstance(const std::string& path, ChunkJounalFlush* flusher)
        : _chunkFlush(flusher) {
        _rightEnv = nullptr;
        _leftEnv = nullptr;
        _letChunkID = system_chunkID;
        _rightChunkID = system_chunkID;
        _dbPath = path;
        index_log() << "ChunkRocksDBInstance::ChunkRocksDBInstance() dbpath: " << path;
    }

    ChunkRocksDBInstance::~ChunkRocksDBInstance() {
        if (_counterManager) {
            _counterManager->des_sync();
        }

        //  Call resets to make sure we clean in a right order
        if (_journalFlusher) {
            _journalFlusher->shutdown();
            _journalFlusher.reset();
        }

        if (_chunkFlush && !isSystemChunk()) {
            _chunkFlush->delDurableManager(_durabilityManager.get());
        }

        _durabilityManager.reset();
        _counterManager.reset();
        _compactionScheduler.reset();

        auto status = dbInstance->SyncWAL();
        if (!status.ok()) {
            index_err() << "SyncWAL fail: " << status.ToString();
        }

        rocksdb::FlushOptions option;
        option.wait = true;
        for (auto cfHandle : columnFamilyHandles) {
            status = dbInstance->Flush(option, cfHandle);
            if (!status.ok()) {
                index_err() << "Flush fail: " << status.ToString();
            }
        }

        for (auto handle : columnFamilyHandles) {
            if (handle == nullptr || handle == dbInstance->DefaultColumnFamily()) {
                continue;
            } else {
                delete handle;
            }
        }

        {
            Date_t initExecTime = Date_t::now();
            index_log() << "[offloadChunk] shardSvr delete rocksdb start nss: " << _rightChunkID;
            dbInstance.reset();
            index_log() << "[offloadChunk] shardSvr delete rocksdb end nss: " << _rightChunkID
                        << "; used Time(ms): " << (Date_t::now() - initExecTime);
        }

        // IndexedRecordStorage is used by compaction, thus it should be released only after DB is
        // closed
        indexedRecordStorage.reset();

        _shared_resource_module.reset();
        metadata.reset();

        if (_rightEnv) {
            delete _rightEnv;
            _rightEnv = nullptr;
        }

        if (_leftEnv) {
            delete _leftEnv;
            _leftEnv = nullptr;
        }
    }

    void ChunkRocksDBInstance::stopBGThread(void) {
        _durabilityManager->setJournalListener(nullptr);
    }

    ChunkMetadata* ChunkRocksDBInstance::GetChunkMetadata() { return metadata.get(); }
    const ChunkMetadata* ChunkRocksDBInstance::GetChunkMetadata() const { return metadata.get(); }

    void ChunkRocksDBInstance::DumpSharedResourceStats(const char* path) {
        if (_shared_resource_module) {
            _shared_resource_module->DumpSharedResourceList(path);
        }
    }

    rocksdb::Status ChunkRocksDBInstance::LoadMetadata(rocksdb::Env& fileSystem,
                                                       const std::string& chukMetaPath,
                                                       std::unique_ptr<ChunkMetadata>& metadata) {
        uint64_t fileSize = 0;

        // get chunkmetadata full name

        std::string db_path = chukMetaPath + "/" + metadataFileName;

        //
        //  For now if we don't find metadata just treat it as standard mongodb collection
        //
        rocksdb::Status status = fileSystem.GetFileSize(db_path, &fileSize);
        if (!status.ok() || fileSize == 0) {
            index_warning() << "ChunkRocksDBInstance::LoadMetadata metadataFileName:" << db_path
                            << ", fileSize:" << fileSize << "; status: " << status.ToString();
            metadata = mongo::stdx::make_unique<ChunkMetadata>();
            return rocksdb::Status::OK();
        }

        auto buffer = mongo::stdx::make_unique<char[]>(fileSize);

        std::unique_ptr<rocksdb::SequentialFile> metadataFile;
        RES_RIF(fileSystem.NewSequentialFile(db_path, &metadataFile, rocksdb::EnvOptions()));

        rocksdb::Slice data;
        RES_RIF(metadataFile->Read(fileSize, &data, buffer.get()));

        RES_RET(ChunkMetadata::Parse(data, metadata));
    }

    rocksdb::Status ChunkRocksDBInstance::SaveMetadata(
        rocksdb::Env& fileSystem, const std::string& chukMetaPath,
        const std::unique_ptr<ChunkMetadata>& metadata, bool fileExist) {
        std::string chunkMetaFile = chukMetaPath + "/" + metadataFileName;
        std::string chunkMetaFileB = chukMetaPath + "/" + metadataFileName + "B";
        std::string chunkfile = fileExist ? chunkMetaFileB : chunkMetaFile;
        std::unique_ptr<rocksdb::WritableFile> metadataFile;
        RES_RIF(fileSystem.NewWritableFile(chunkfile, &metadataFile, rocksdb::EnvOptions()));

        mongo::BSONObj bson = metadata->ToBSON();
        std::string data_str = bson.toString();
        RES_RIF(metadataFile->Append(rocksdb::Slice(bson.objdata(), bson.objsize())));
		metadataFile->Sync();
        metadataFile.reset();

        if (fileExist) {
            RES_RIF(fileSystem.RenameFile(chunkMetaFileB, chunkMetaFile));
        }
        return rocksdb::Status::OK();
    }

    void ChunkRocksDBInstance::PostInit(
        const rocksdb::Options& options)  //  Do final initialization
    {
        if (options.info_log) {
            auto logger =
                dynamic_cast<ResultHandling::MongoRocksLoggerForChunk*>(options.info_log.get());
            _rocksdbLogger = options.info_log;
            if (logger != nullptr) logger->SetTag(metadata->GetChunk().getID());
        }
        //  use muli-instance record store only on Shard Server or single shard.
        auto replMode = repl::getGlobalReplicationCoordinator()->getReplicationMode();
        bool useMultiInstance = GLOBAL_CONFIG_GET(UseMultiRocksDBInstanceEngine) &&
                                (serverGlobalParams.clusterRole == ClusterRole::ShardServer ||
                                (ClusterRole::None == serverGlobalParams.clusterRole 
                                && replMode != repl::ReplicationCoordinator::Mode::modeReplSet));
        bool non_sys_chunk = (metadata->GetChunk().getID().size() > 0 &&
                              metadata->GetChunk().getName() != system_chunkID);
        _compactionScheduler.reset(new mongo::RocksCompactionScheduler(GetDB()));
        _durabilityManager.reset(new mongo::RocksDurabilityManager(GetDB(), durable, GetColumnFamily()));

        if (useMultiInstance) {
            _counterManager.reset(new MoreDBCounterManager(
                GetDB(), mongo::rocksGlobalOptions.crashSafeCounters,
                GetColumnFamily(indexedRecordStorage->GetMetadataFamilyId())));
            if (durable) {   
                if (non_sys_chunk) {
                    invariant(_chunkFlush);
                    _chunkFlush->addDurableManager(_durabilityManager.get());
                } else {
                    _journalFlusher =
                        stdx::make_unique<mongo::RocksJournalFlusher>(_durabilityManager.get());
                    _journalFlusher->go();
                }
            }
        } else {
            _counterManager.reset(new mongo::RocksCounterManager(
                GetDB(), mongo::rocksGlobalOptions.crashSafeCounters,
                GetColumnFamily(indexedRecordStorage->GetMetadataFamilyId())));
            if (durable) {
                _journalFlusher =
                    stdx::make_unique<mongo::RocksJournalFlusher>(_durabilityManager.get());
                _journalFlusher->go();
            }
        }
        
        //
        //  If this chunk belongs to gc.references, start a new gc manager thread
        //
        if (GLOBAL_CONFIG_GET(enableGlobalGC)) {
            if (metadata && Status::OK() == (metadata->GetChunk()).validate()) {
                std::string fullChunkNs = metadata->GetChunk().getFullNs();

                index_LOG(2) << "[GC Manager] ChunkType::NS(" << metadata->GetChunk().getNS()
                             << ") ns_with_chunkID(" << fullChunkNs << ")";

                if (GcRefNs == metadata->GetChunk().getNS()) {
                    if (getGlobalServiceContext() &&
                        getGlobalServiceContext()->getGlobalStorageEngine()) {
                        getGlobalServiceContext()->getGlobalStorageEngine()->setGCInfo(
                            fullChunkNs, metadata->GetChunk().getChunkDataPath());
                    } else {
                        index_err() << "serverContext is NULL!";
                    }
                }
            }
        }
    }

    std::string ChunkRocksDBInstance::getPath() const { return _dbPath; }

    std::string ChunkRocksDBInstance::getDbPath() const { return getDbPath(_dbPath); }

    std::string ChunkRocksDBInstance::getChunkMetaPath() const { return getChunkMetaPath(_dbPath); }

    std::string ChunkRocksDBInstance::getTransLogPath() const { return getTransLogPath(_dbPath); }

    std::string ChunkRocksDBInstance::getDbPath(const std::string& dbpath) {
        return dbpath + ROCKSDB_PATH;
    }

    std::string ChunkRocksDBInstance::getChunkMetaPath(const std::string& dbpath) {
        return dbpath + CHUNK_META_PATH;
    }

    std::string ChunkRocksDBInstance::getTransLogPath(const std::string& dbpath) {
        return dbpath + TRANS_LOG_PATH;
    }

    //
    //  Initialize storage and open RocksDB
    //
    rocksdb::Status ChunkRocksDBInstance::OpenStorage(const rocksdb::Options& options,
                                                      const std::string& dbPath) {
        _leftEnv = options.env;

        //
        //  Create storage based on metadata settings
        //
        RES_RIF(IIndexedRecordStorage::Create(*metadata.get(), indexedRecordStorage));

        //
        //  Populate descriptors for column families, setting compaction filters
        //
        columnFamilyDescriptors.Init(*indexedRecordStorage.get(), options, this);

        if (metadata->GetChunk().getID().size() > 0 &&
            metadata->GetChunk().getName() != system_chunkID) {
            invariantRocksOKWithNoCore(initTransactionLogModule(dbPath));
        }

        std::string rocksdb_path = getDbPath();
        rocksdb::Options newOptions = options;
        newOptions.instance_option.instance_path = rocksdb_path;
        newOptions.instance_option.shared_checker = _shared_resource_module;

        //
        //  Open RocksDB instance
        //
        rocksdb::DB* db = nullptr;
        {
            Date_t initExecTime = Date_t::now();
            for (auto cfDes : columnFamilyDescriptors.Get()) {
                index_LOG(1) << "cf name: " << cfDes.name
                             << "; write buf size: " << cfDes.options.write_buffer_size
                             << "; write buf num: " << cfDes.options.max_write_buffer_number;
            }
            index_log() << "rocksdb::Open start nss: " << rocksdb_path;
            RES_RIF(rocksdb::DB::Open(newOptions, rocksdb_path, columnFamilyDescriptors.Get(),
                                      &columnFamilyHandles, &db));
            index_log() << "rocksdb::Open end nss: " << rocksdb_path
                        << "; used Time(ms): " << (Date_t::now() - initExecTime);
        }

        invariant(columnFamilyHandles.size() == columnFamilyDescriptors.Get().size());
        dbInstance.reset(db);

        //
        //  Initialize required managers, schedulers and background jobs
        //
        {
            PostInit(options);
        }

        return rocksdb::Status::OK();
    }

    //
    //  Open chunk's RocksDB instance. Is called from MongoD on Shard Server
    //
    rocksdb::Status ChunkRocksDBInstance::Open(const rocksdb::Options& options,
                                               const std::string& dbPath, bool _durable,
                                               const CollectionOptions* colllOptions) {
        durable = _durable;
        _leftEnv = options.env;
        //
        //  First we need to load metadata from our environment
        //
        {
            RES_RIF(LoadMetadata(*options.env, getChunkMetaPath(), metadata));

            // update chunkmeta if the key range is inconsistent with config server
            if (colllOptions) {
                RES_RIF(UpdateMetadata(colllOptions->chunk));
            }
            //update mem index dropped prefixes set.
            CollectionType coll = metadata->GetCollection();
            index_log()<<"ChunkRocksDBInstance open coll: "<< coll;
            updateDroppedPrefixes(coll.getDroppedPrefixes());
        }

        //
        //  Initialize storage and open RocksDB
        //
        RES_RIF(OpenStorage(options, dbPath));

        //  TODO: should be removed from here
        RES_RIF(IIndexedRecordStorage::Create(*metadata.get(), indexedRecordStorageInPreperSplit));

        return rocksdb::Status::OK();
    }

    rocksdb::Status ChunkRocksDBInstance::UpdateMetadata(const ChunkType& chunkFromConfig) {
        // check the chunk meta between the one stored on disk and the one in assign command
        if ((chunkFromConfig.getMin().toString() != metadata->GetChunk().getMin().toString()) ||
            (chunkFromConfig.getMax().toString() != metadata->GetChunk().getMax().toString())) {
            metadata->SetKeyRange(chunkFromConfig.getMin(), chunkFromConfig.getMax());
            std::string chukMetaPath = chunkFromConfig.getRootFolder() + CHUNK_META_PATH;
            RES_RET(SaveMetadata(*_leftEnv, chukMetaPath, metadata));
        }

        return rocksdb::Status::OK();
    }

    rocksdb::Status ChunkRocksDBInstance::UpdateMetadata(
        const std::string& chukMetaPath, const std::unique_ptr<ChunkMetadata>& metaData) {
        metadata->setCollection(metaData->GetCollection());
        index_log()<<" collection: "<< metadata->GetCollection();
        RES_RIF(SaveMetadata(*_leftEnv, chukMetaPath, metaData));
        return rocksdb::Status::OK();
    }

    rocksdb::Status saveCollectionName(rocksdb::Env* fileSystem, const std::string& dbPath,
                                       const NamespaceString& nss) {
        std::string file = dbPath + "/" + nss.nsFilteredOutChunkId();
        index_LOG(2) << "[assignChunk]  dbpath: " << dbPath << "; nss: " << nss
                     << "; filepath: " << file;

        std::unique_ptr<rocksdb::WritableFile> metadataFile;
        RES_RIF(fileSystem->NewWritableFile(file, &metadataFile, rocksdb::EnvOptions()));
        RES_RIF(metadataFile->Append(
            rocksdb::Slice(nss.nsFilteredOutChunkId().c_str(), nss.nsFilteredOutChunkId().size())));
        metadataFile->Flush();
        metadataFile->Close();
        return rocksdb::Status::OK();
    }

    rocksdb::Status ChunkRocksDBInstance::createChunkDir(const std::string& dbPath) {
        createChildChunkDir(dbPath);
        std::string dataPath = getDbPath(dbPath);
        RES_RIF(createDir(dataPath));
        return rocksdb::Status::OK();
    }

    rocksdb::Status ChunkRocksDBInstance::createChildChunkDir(const std::string& dbPath) {
        std::string chunkMetaPath = getChunkMetaPath(dbPath);
        std::string tranLogPath = getTransLogPath(dbPath);
        index_log() << "[assignChunk] create New chunk dbPath: " << dbPath
                    << "; metaData path: " << chunkMetaPath << "; tranLogPath: " << tranLogPath;

        RES_RIF(createDir(dbPath));
        RES_RIF(createDir(chunkMetaPath));
        RES_RIF(createDir(tranLogPath));
        return rocksdb::Status::OK();
    }

    rocksdb::Status ChunkRocksDBInstance::Create(const rocksdb::Options& options,
                                                 const std::string& dbPath,
                                                 ChunkRocksDBInstance* instance,
                                                 std::unique_ptr<ChunkMetadata>&& metadata) {
        instance->createChunkDir(dbPath);

        //
        //  Save metadata
        //
        {
            RES_RIF(SaveMetadata(*options.env, getChunkMetaPath(dbPath), metadata, false));
        }

        instance->metadata = std::move(metadata);
        //
        //  Initialize storage and create RocksDB
        //
        rocksdb::Options dbOptions(options);
        dbOptions.create_missing_column_families = true;  //  Create column families that are missed
        RES_RIF(instance->OpenStorage(dbOptions, dbPath));

        //  TODO: should be removed from here
        RES_RIF(IIndexedRecordStorage::Create(*instance->metadata.get(),
                                              instance->indexedRecordStorageInPreperSplit));

        {
            saveCollectionName(options.env, dbPath,
                               instance->GetChunkMetadata()->GetCollection().getNs());
        }

        return rocksdb::Status::OK();
    }

    mongo::RocksRecoveryUnit* ChunkRocksDBInstance::GetRocksRecoveryUnit(
        mongo::OperationContext* txn) const {
        RocksRecoveryUnit* ru = mongo::RocksRecoveryUnit::getRocksRecoveryUnit(txn);

        //  Make sure correct DB is associated with RecoveryUnit
        if (ru->_db == this) {
            return ru;
        }

        ru->_db = (ChunkRocksDBInstance*)this;
        ru->transaction()->SetTransactionEngine(ru->_db->GetTransactionEngine());

        return ru;
    }

    rocksdb::Status ChunkRocksDBInstance::saveRightChunkMeta(
        const BSONObj& splitPoint, const std::string& rightDBPath, const std::string& rightChunkId,
        std::unique_ptr<ChunkMetadata>& rightMeta) {
        mongo::ChunkType chunkType = metadata->GetChunk();
        chunkType.setMin(splitPoint);
        chunkType.setRootFolder(rightDBPath);
        chunkType.setID(rightChunkId);

        mongo::CollectionType ctype = metadata->GetCollection();
        ctype.setNs(NamespaceString(chunkType.getFullNs()));
        // other field

        rightMeta.reset(new ChunkMetadata(metadata->GetIndexedRecordStorageId(), chunkType, ctype));

        std::string chunkMetaPath = rightDBPath + CHUNK_META_PATH;
        return SaveMetadata(*_rightEnv, chunkMetaPath, rightMeta, false);
    }

    rocksdb::Status ChunkRocksDBInstance::initRightPlogEnv(const std::string& rightDBPath,
                                                           bool is_in_rollback) {
        _rightEnv = initEnv();
        return rocksdb::Status::OK();
    }

    rocksdb::Status ChunkRocksDBInstance::writeRightTranasctionLog(const std::string& rightDBPath,
                                                                   rocksdb::Env* rightEnv,
                                                                   ChunkMetadata* rightMeta) {
        rocksdb::Status status;
        // SharedResourceModule right_module;
        MaaSSharedResourceModule right_module;
        status = right_module.Init(rightEnv, rightDBPath, rightMeta);
        if (!status.ok()) {
            return status;
        }
        status = _shared_resource_module->WriteSharedResourceLogToRight(right_module);
        if (!status.ok()) {
            return status;
        }

        return rocksdb::Status::OK();
    }

    Status ChunkRocksDBInstance::getSplitPointInfo(const std::string& prefix,
                                                   const ChunkType& chunk, BSONObj& splitPoint) {
        uint64_t left_datasize = 0, left_numrecord = 0;
        uint64_t right_datasize = 0, right_numrecord = 0;
      
        std::string splitPointStr;
        if (!splitPoint.isEmpty()) {
            mongo::RecordId recordid;
            GetIndexedRecordStorage().GetRecordId(splitPoint, recordid);
            auto splitPointKey = RocksRecordStore::_makePrefixedKey(prefix, recordid);
            rocksdb::Status status = dbInstance->GetSplitInfoBySplitPoint(splitPointKey, left_datasize,
                                                          left_numrecord, right_datasize,
                                                          right_numrecord);
            if (!status.ok()) {
                index_log() << "GetSplitInfoBySplitPoint error: " << status.ToString() << "; chunk: "
                    << chunk << "; splitPoint: " << splitPoint;
                return Status(ErrorCodes::CannotSplit, "GetSplitInfoBySplitPoint error!");
            }
            splitPointStr = splitPoint.toString();
        } else {
            rocksdb::Status r_status = dbInstance->GetSplitPointBySize(
                splitPointStr, left_datasize, left_numrecord, right_datasize, right_numrecord);
            {
                std::vector<rocksdb::LiveFileMetaData> v_live_file_meta;
                dbInstance->GetLiveFilesMetaData(&v_live_file_meta);
                auto L1_file_num = 0;
                auto L1_file_size = 0;
                std::string file_meta;
                auto num = 0;
                for (auto file : v_live_file_meta) {
                    if (num > 10) break;
                    if (1 == file.level &&
                        rocksdb::kDefaultColumnFamilyName == file.column_family_name) {
                        num++;
                        L1_file_num++;
                        L1_file_size += file.size;

                        BSONObj l_bson_key;
                        std::string s_key_str = file.smallestkey;
                        s_key_str = s_key_str.substr(prefix.size());
                        MDB_RIF(GetIndexedRecordStorage().GetBSONFromPrimaryKey(rocksdb::Slice(s_key_str), true,
                                                                                true, l_bson_key));
                        BSONObj h_bson_key;
                        std::string m_key_str = file.largestkey;
                        m_key_str = m_key_str.substr(prefix.size());
                        MDB_RIF(GetIndexedRecordStorage().GetBSONFromPrimaryKey(rocksdb::Slice(m_key_str), true,
                                                                                true, h_bson_key));
                        file_meta +=
                            str::stream()
                            << ", cf: " << file.column_family_name << ", level: " << file.level
                            << ", name: " << file.name << ", size(bytes): " << file.size
                            << ", db_path: " << file.db_path << ", l_key: " << l_bson_key
                            << ", h_key: " << h_bson_key << ", b_compact: " << file.being_compacted;
                    }
                }
                std::string errmsg =
                    str::stream() << "GetSplitPointBySize, rocskdb status: " << r_status.ToString()
                                  << ", data_cf L1_data_file_num: " << L1_file_num
                                  << ", data_cf L1_data_file_size bytes: " << L1_file_size
                                  << file_meta;
                index_log() << "[splitChunk] GetSplitPointBySize info: " << errmsg;
                if (!r_status.ok()) {
                    return Status(ErrorCodes::CannotSplit, errmsg);
                }
            }

            // remove prefix
            if (splitPointStr.size() <= prefix.size()) {
                std::string errmsg = str::stream() << "splitPoint: " << splitPointStr
                                                   << " is invalid.";

                index_err() << "[splitChunk] error: " << errmsg;
                return Status(ErrorCodes::CannotSplit, errmsg);
            }
            splitPointStr = splitPointStr.substr(prefix.size());

            BSONObj splitPointBson;
            MDB_RIF(GetIndexedRecordStorage().GetBSONFromPrimaryKey(rocksdb::Slice(splitPointStr),
                                                                    true, true, splitPointBson));

            splitPoint = splitPointBson;
        }

        // Check if splitPoint is valid
        if (splitPoint.isEmpty() || splitPoint.woCompare(chunk.getMin()) <= 0 ||
            splitPoint.woCompare(chunk.getMax()) >= 0) {
            index_err() << "[splitChunk] splitPoint (" << splitPoint << ") is invalid.";
            std::string errmsg = str::stream() << "splitPoint: " << splitPoint
                                               << ", lowKey: " << chunk.getMin()
                                               << ", highKey: " << chunk.getMax();
            return Status(ErrorCodes::CannotSplit, errmsg);
        }

        index_log() << "[splitChunk]  left_datasize: " << left_datasize
                    << "; left_numrecord: " << left_numrecord
                    << "; right_datasize: " << right_numrecord;

        return Status::OK();
    }

    void ChunkRocksDBInstance::blockRightWrite(const BSONObj& splitPoint, const ChunkType& chunk,
                                               uint64_t r_datasize, uint64_t r_numrecord) {
        Date_t initExecTime = Date_t::now();
        index_log() << "[splitChunk] shardSvr blockIO start nss: " << chunk.getNS();
        const mongo::Milliseconds kSplitBlockIoInterval(10);
        while (_transactionEngine.getCountOfActiveWriteBatchesDuringSplit() > 0) {
            stdx::unique_lock<stdx::mutex> lock(_mutex);
            _duringSplitCondVar.wait_for(lock, kSplitBlockIoInterval.toSteadyDuration(), [&] {
                return _transactionEngine.getCountOfActiveWriteBatchesDuringSplit() == 0;
            });
        }

        // block write io of right db
        GetIndexedRecordStorageInPreperSplit()->SetKeyRange(
            ChunkMetadata::KeyRange(metadata->GetKeyRange().GetKeyLow(), splitPoint));
        _transactionEngine.setSplitIsOnGoing(true);

        // update the recordNum & DataSize for right chunk.
        GetCounterManager()->updateCounter(
            GetIndexedRecordStorageInPreperSplit()->GetRightMetadataRecordKey("datasize"),
            r_datasize, nullptr, true);
        GetCounterManager()->updateCounter(
            GetIndexedRecordStorageInPreperSplit()->GetRightMetadataRecordKey("numrecords"),
            r_numrecord, nullptr, true);

        while (_transactionEngine.getCountOfActiveWriteBatchesOutsideOfSplit() > 0) {
            stdx::unique_lock<stdx::mutex> outSideOfSplitLock(_mutex);
            _outsideOfSplitCondVar.wait_for(
                outSideOfSplitLock, kSplitBlockIoInterval.toSteadyDuration(), [&] {
                    return _transactionEngine.getCountOfActiveWriteBatchesOutsideOfSplit() == 0;
                });
        }

        index_log() << "[splitChunk] shardSvr blockIO end nss: " << chunk.getNS()
                    << "; used Time(ms): " << (Date_t::now() - initExecTime);
    }

    // 1. getSplitInfo(splitPoint, dataSize, numRecords);
    // 2. new right env
    // 3. write startSplit to transaction Log;
    // 4  save right chunkMetaData;
    // 5. preperSplit;
    // 6. block write IO of right DB
    // 7. rocksdb::split(stop compaction);
    // 8   write Transaction Log of right db
    // 9   block read IO of right db
    Status ChunkRocksDBInstance::preSplit(const std::string& rightDbpathWithPlog,
                                          const std::string& rightChunkId,
                                          const std::string& prefix, const ChunkType& chunk,
                                          BSONObj& splitPoint) {
        index_log() << "[splitChunk] right_dbpath:" << rightDbpathWithPlog
                    << "; chunkId: " << rightChunkId;

        auto status1 = getSplitPointInfo(prefix, chunk, splitPoint);
        if (!status1.isOK()) {
            index_err() << "[splitChunk] getSplitPointInfo error: " << status1.toString();
            std::string errmsg = str::stream() << "getSplitPointInfo failed, status:"
                                               << status1.toString();
            return status1;
        }
        auto status = initRightPlogEnv(rightDbpathWithPlog);

        if (!status.ok()) {
            index_err() << "[splitChunk] initRightPlogEnv error: " << (int)(status.subcode())
                        << status.ToString();
            std::string errmsg = str::stream() << "initRightPlogEnv failed, rocskdb status:"
                                               << status.ToString();
            return Status(ErrorCodes::InternalError, errmsg);
        }

        createChildChunkDir(rightDbpathWithPlog);

        index_log() << "[splitChunk] _shared_resource_module->StartSplit() parent chunkId: "
                    << getLeftChunkID() << "; childChunkId: " << getRightChunkID();
        auto splitInfo = std::make_shared<TransactionLog::SplitDescription>(
            getLeftChunkID(), getRightChunkID(), metadata->GetKeyRange().GetKeyLow(),
            metadata->GetKeyRange().GetKeyHigh(), splitPoint, rightDbpathWithPlog);
        status = _shared_resource_module->StartSplit(splitInfo);
        if (!status.ok()) {
            index_err() << "[splitChunk] write StartSplit error: " << status.ToString();
            return rocksToMongoStatus(status);
        }

        index_log() << "ChunkRocksDBInstance::split()-> saveChunkMeta start: ";

        status = saveRightChunkMeta(splitPoint, rightDbpathWithPlog, rightChunkId, _rightMeta);
        if (!status.ok()) {
            index_err() << "[splitChunk] saveRightChunkMeta fail: " << status.ToString();
            invariantRocksOKWithNoCore(_shared_resource_module->RollbackSplit());
            deleteDir(rightDbpathWithPlog);
            return rocksToMongoStatus(status);
        }

        index_log() << "ChunkRocksDBInstance::split()-> saveChunkMeta end splitDescription: ";

        _splitInfo.reset(new SplitInfo(
            {rightDbpathWithPlog + ROCKSDB_PATH, nullptr, "", rightChunkId}, *(_rightMeta.get())));

        Date_t initExecTime = Date_t::now();
        index_log() << "[splitChunk] shardSvr prepareSplitDB start nss: " << chunk.getNS();
        status = dbInstance->PrepareSplitDb(_rightEnv, _splitInfo->getDescriptions(),
                                            _splitInfo->getDBInstanceOption());
        index_log() << "[splitChunk] shardSvr prepareSplitDB end nss: " << chunk.getNS()
                    << "; used Time(ms): " << (Date_t::now() - initExecTime);
        if (!status.ok()) {
            index_err() << "[splitChunk] prepareSplit error: " << status.ToString();
            invariantRocksOKWithNoCore(_shared_resource_module->RollbackSplit());
            deleteDir(rightDbpathWithPlog);
            return rocksToMongoStatus(status);
        }
        return Status::OK();
    }

    Status ChunkRocksDBInstance::split(const std::string& rightDbpathWithPlog,
                                       const BSONObj& splitPoint, const ChunkType& chunk,
                                       uint64_t r_datasize, uint64_t r_numrecord) {
        blockRightWrite(splitPoint, chunk, r_datasize, r_numrecord);

        Date_t initExecTime = Date_t::now();
        index_log() << "[splitChunk] shardSvr rocksdb::splitDb start nss: " << chunk.getNS();
        auto status = dbInstance->SplitDb(_rightEnv);
        index_log() << "[splitChunk] shardSvr rocksdb::splitDb end nss: " << chunk.getNS()
                    << "; used Time(ms): " << (Date_t::now() - initExecTime);
        if (!status.ok()) {
            index_err() << "[splitChunk] SplitDb error: " << status.ToString();
            GetIndexedRecordStorageInPreperSplit()->SetKeyRange(metadata->GetKeyRange());
            _transactionEngine.setSplitIsOnGoing(false);
            invariantRocksOK(dbInstance->RollbackSplit(_rightEnv));
            invariantRocksOKWithNoCore(_shared_resource_module->RollbackSplit());
            deleteDir(rightDbpathWithPlog);
            return rocksToMongoStatus(status);
        }

        status = writeRightTranasctionLog(rightDbpathWithPlog, _rightEnv, _rightMeta.get());
        if (!status.ok()) {
            index_err() << "[splitChunk] writeRightTranasctionLog error: " << status.ToString();
            GetIndexedRecordStorageInPreperSplit()->SetKeyRange(metadata->GetKeyRange());
            _transactionEngine.setSplitIsOnGoing(false);
            invariantRocksOK(dbInstance->RollbackSplit(_rightEnv));
            invariantRocksOKWithNoCore(_shared_resource_module->RollbackSplit());
            deleteDir(rightDbpathWithPlog);
            return rocksToMongoStatus(status);
        }

        // block read io of right db
        _transactionEngine.setBlockReadIOofRight(true);
        // GetIndexedRecordStorageInPreperSplit()->SetKeyRange(ChunkMetadata::KeyRange(metadata->GetKeyRange().GetKeyLow(),
        // splitPoint));

        index_log() << "[splitChunk] SplitDb suc: ";
        return Status::OK();
    }

    Status ChunkRocksDBInstance::rollbackPreSplit(OperationContext* txn,
                                                  const std::string& rightDbpathWithPlog) {
        GetIndexedRecordStorageInPreperSplit()->SetKeyRange(metadata->GetKeyRange());
        _transactionEngine.setSplitIsOnGoing(false);
        invariantRocksOK(dbInstance->RollbackSplit(_rightEnv));
        delete _rightEnv;
        _rightEnv = nullptr;
        invariantRocksOKWithNoCore(_shared_resource_module->RollbackSplit());
        deleteDir(rightDbpathWithPlog);
        return Status::OK();
    }

    // 1. update left ChunkMetaData
    // 2. update read filter(compaction, rocksdb read,
    // 3  rocksdb::comfirm (start compaction);
    // 4  write split commit to left transaction log
    // 5  unblock write IO of right db
    // 6  unblock read IO of right db
    rocksdb::Status ChunkRocksDBInstance::confirmSplit(const ConfirmSplitRequest& request) {
        rocksdb::Status status;
        if (request.splitSuccess()) {
            index_log() << "[comfirmSplit] split success ";
            std::unique_ptr<ChunkMetadata> chunkMeta(
                new ChunkMetadata(metadata->GetIndexedRecordStorageId(), request.getChunk(),
                                  request.getCollection()));
            status = SaveMetadata(*_leftEnv, getChunkMetaPath(), chunkMeta);
            if (!status.ok()) {
                index_err() << "[comfirmSplit] saveMetadata error: " << status.ToString();
                return status;
            }

            metadata = std::move(chunkMeta);

            // ChunkMetadata::KeyRange keyRange(metadata->GetKeyRange().GetKeyLow(), splitPoint);
            GetIndexedRecordStorage().SetKeyRange(metadata->GetKeyRange());

            invariantRocksOKWithNoCore(dbInstance->ConfirmSplitSuccess(_rightEnv));
            index_log() << "[comfirmSplit] start s_shared_resource_module->CommitSplit()";

            status = _shared_resource_module->CommitSplit();
            if (!status.ok()) {
                index_err() << "[comfirmSplit] _shared_resource_module->CommitSplit error: "
                            << status.ToString();
                return status;
            }
            index_log() << "[comfirmSplit] suc";
        } else {
            index_log() << "[comfirmSplit] split fail ";
            if (nullptr == _rightEnv) {  // shardSvr crash
                // delete
                std::string right_dbpath = _shared_resource_module->GetRightDbPath();
                status = initRightPlogEnv(right_dbpath);
                if (!status.ok()) {
                    index_err() << "[comfirmSplit] initRightPlogEnv fail: " << status.ToString();
                    return status;
                }
            }

            invariantRocksOKWithNoCore(dbInstance->RollbackSplit(_rightEnv));
            invariantRocksOKWithNoCore(_shared_resource_module->RollbackSplit());

            GetIndexedRecordStorageInPreperSplit()->SetKeyRange(metadata->GetKeyRange());

            // unblock read IO of rigth db
            _transactionEngine.setBlockReadIOofRight(false);
        }

        // unblock  wirte IO of rigth db
        _transactionEngine.setSplitIsOnGoing(false);
        return rocksdb::Status::OK();
    }

    rocksdb::Status ChunkRocksDBInstance::initTransactionLogModule(
        const std::string& dbPathWithoutPlog) {
        //_shared_resource_module.reset(new SharedResourceModule);
        _shared_resource_module.reset(new MaaSSharedResourceModule());
        rocksdb::Status status =
            _shared_resource_module->Init(_leftEnv, dbPathWithoutPlog, metadata.get());
        if (!status.ok()) {
            index_err() << "[transLog] Init ShardResourceModeul error: " << status.ToString();
            return status;
        }

        status = rePlayTransactionLog();
        if (!status.ok()) {
            index_err() << "[transLog] rePlayTransactionLog() error: " << status.ToString();
            return status;
        }

        return rocksdb::Status::OK();
    }

    rocksdb::Status ChunkRocksDBInstance::rePlayTransactionLog() {
        if (_shared_resource_module->IsSplitStateFault()) {
            std::string right_dbpath = _shared_resource_module->GetRightDbPath();
            invariant(!right_dbpath.empty());
            rocksdb::Status status = initRightPlogEnv(right_dbpath);
            if (!status.ok()) {
                index_err() << "[transLog] initRightPlogEnv fail: " << status.ToString();
                return status;
            }

            // invariantRocksOKWithNoCore(dbInstance->RollbackSplit(_rightEnv));
            index_warning() << "[transLog] splitChunk faild! rightdbpath: " << right_dbpath;
            // deleteDir(right_dbpath);// remove dbpath of right
        }
        return rocksdb::Status::OK();
    }

    const IChunkColumnFamilyDescription*
    ChunkRocksDBInstance::getColumnFamilyDescriptionInPreperSplit(const std::string& name) const {
        auto cfds = indexedRecordStorageInPreperSplit->GetColumnFamilyDescriptions();
        for (auto cfd : cfds) {
            if (cfd->GetName() == name) {
                return cfd;
            }
        }

        return nullptr;
    }
    std::unordered_set<uint32_t> ChunkRocksDBInstance::getDroppedPrefixes() const {
        stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
        // this will copy the set. that way compaction filter has its own copy and doesn't need to
        // worry about thread safety
        return _droppedPrefixes;
    }
    void ChunkRocksDBInstance::updateDroppedPrefixes(const std::unordered_set<uint32_t>& prefix_set){
        for(auto prefix : prefix_set){
             stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
             index_log()<<"update dropped index prefix :" << prefix;
            _droppedPrefixes.insert(prefix);
        }
        return;
    }

    ChunkRocksDBInstance::SplitInfo::SplitInfo(const DbInstanceOption& option,
                                               const ChunkMetadata& metadata)
        : options(option) {
        indexColumnFamilyStorge.reset(
            new ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage(metadata));
    }

    std::vector<const rocksdb::IKeyRangeChecker*> ChunkRocksDBInstance::SplitInfo::getDescriptions()
        const {
        auto cfds = indexColumnFamilyStorge->GetColumnFamilyDescriptions();
        std::vector<const rocksdb::IKeyRangeChecker*> cfDescriptions;
        for (auto cfd : cfds) {
            cfDescriptions.push_back(cfd);
        }
        return cfDescriptions;
    }

    void ChunkColumnFamilyDescriptors::Init(const IIndexedRecordStorage& recordStorage, const rocksdb::Options& options, 
              const ChunkRocksDBInstance* dbInstance) {
        std::shared_ptr<ChunkCompactionFilterFactory> chkCompactionFilterFactory;

        if(!options.compaction_filter_factory){
            auto idxFactoryToCheck = std::make_shared<mongo::IdxPrefixDeletingCompactionFilterFactory>( dbInstance );
            chkCompactionFilterFactory = std::make_shared<ChunkCompactionFilterFactory>(recordStorage, idxFactoryToCheck);
        }else{
            chkCompactionFilterFactory = std::make_shared<ChunkCompactionFilterFactory>(
                                                  recordStorage, options.compaction_filter_factory);
        }

        for (const auto& columnFamilyDescription : recordStorage.GetColumnFamilyDescriptions()) {
            rocksdb::ColumnFamilyOptions columnFamilyOptions(options);
            if (nullptr == columnFamilyDescription->GetName()) {
                continue;
            }
            if (strcmp(columnFamilyDescription->GetName(), "index") != 0) {
                columnFamilyOptions.enable_row_cache = false;
                columnFamilyOptions.write_buffer_size =
                    ConfigReader::getInstance()->getDecimalNumber<int>(
                        "CFOptions", "data_write_buffer_size");
                columnFamilyOptions.max_write_buffer_number =
                    ConfigReader::getInstance()->getDecimalNumber<int>(
                        "CFOptions", "data_max_write_buffer_number");
            } else {
                columnFamilyOptions.write_buffer_size =
                    ConfigReader::getInstance()->getDecimalNumber<int>(
                        "CFOptions", "index_write_buffer_size");
                columnFamilyOptions.max_write_buffer_number =
                    ConfigReader::getInstance()->getDecimalNumber<int>(
                        "CFOptions", "index_max_write_buffer_number");
            }
    
            //1.  sys db use PrefixDeletingCompactionFilterFactory
            //2.  user db index cf use IdxPrefixDeletingCompactionFilterFactory
            //3.  user db data cf not use filter factory.
            if(1 == recordStorage.GetColumnFamilyDescriptions().size()||
                (2 == recordStorage.GetColumnFamilyDescriptions().size()
                     && 0 == strcmp(columnFamilyDescription->GetName(), "index"))){
                columnFamilyOptions.compaction_filter_factory = chkCompactionFilterFactory;
            }
            // Set filter which will filter out records during the read to return only records
            // belonging to chunk
            if (GLOBAL_CONFIG_GET(enableAutoChunkSplit)) {
                columnFamilyOptions.range_checker = columnFamilyDescription;
            }

            if (CollectionType::TableType::kSharded == dbInstance->GetChunkMetadata()->GetCollection().getTabType()) {
                index_log() << "open shard Collection: metadate: " << dbInstance->GetChunkMetadata()->ToString();
                columnFamilyOptions.num_levels = 3;
                columnFamilyOptions.max_bytes_for_level_base = 640 * 1024 * 1024;
            } else {
                columnFamilyOptions.num_levels = 5;
                columnFamilyOptions.max_bytes_for_level_base = 256 * 1024 * 1024;
                index_log() << "open nonShard Collection type: " << (int)(dbInstance->GetChunkMetadata()->GetCollection().getTabType()) <<
                    ": metadate: " << dbInstance->GetChunkMetadata()->ToString();
            }
    
            columnFamilyDescriptors.emplace_back(
                std::string(columnFamilyDescription->GetName()), columnFamilyOptions);
        }
    }
    bool IdxPrefixDeletingCompactionFilter::Filter(int level, const rocksdb::Slice& key,
                            const rocksdb::Slice& existing_value, std::string* new_value,
                            bool* value_changed) const {
            uint32_t prefix = 0;
            if (!extractIdxPrefix(key, &prefix)) {
                return false;
            }
            if (prefix == _prefixCache) {
                return _droppedCache;
            }
            _prefixCache = prefix;
            _droppedCache = _droppedPrefixes.find(prefix) != _droppedPrefixes.end();
            index_LOG(2)<<"prefix: "<<prefix <<", _droppedCache: "<<_droppedCache;
            return _droppedCache;
    }
    std::unique_ptr<rocksdb::CompactionFilter> IdxPrefixDeletingCompactionFilterFactory::CreateCompactionFilter(
        const rocksdb::CompactionFilter::Context& context)  {
        //MongoRocksStandard has one column family, need filter for dropped index yet.
        //if (context.column_family_id == 0) return nullptr;

        auto droppedPrefixes = _chunk_db_instance->getDroppedPrefixes();
        index_LOG(2)<<"CreateCompactionFilter prefixes size: "<< droppedPrefixes.size(); 
        if (droppedPrefixes.size() == 0) {
            // no compaction filter needed
            return std::unique_ptr<rocksdb::CompactionFilter>(nullptr);
        } else {
            return std::unique_ptr<rocksdb::CompactionFilter>(
                new IdxPrefixDeletingCompactionFilter(std::move(droppedPrefixes)));
        }
    }

    Status ChunkRocksDBInstance::compact(void) {
        for (auto cfhandle : columnFamilyHandles)
        {
            auto s = dbInstance->CompactRange(cfhandle, nullptr, nullptr);
            if (!s.ok()) {
                index_err() << "compactRange error: " << s.ToString() << "; handle: " << cfhandle->GetName();
                index_err() << "; db: " << GetMetadata().ToString();
            }
        }
        return Status::OK();        
    }

}  //  namespace mongo

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include "ChunkRocksDBInstance.h"
#include <mongo/util/background.h>
#include "../rocks_global_options.h"
#include "../rocks_engine.h"
#include "mongo/platform/basic.h"
#include "mongo/util/log.h"
#include "../more_db_counter_manager.h"
#include "mongo/util/duration.h"
#include "../rocks_record_store.h"
#include "ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage.h"
#include "maas_shared_resource_manager.h"
#include "../rocks_engine.h"

namespace mongo
{
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
}

namespace mongo
{

ChunkRocksDBInstance::ChunkRocksDBInstance(const std::string& path)
{
    _rightEnv = nullptr;
    _leftEnv = nullptr;    
    _letChunkID = system_chunkID;
    _rightChunkID = system_chunkID;
    _dbPath = path;
    log() << "ChunkRocksDBInstance::ChunkRocksDBInstance() dbpath: " << path;
}

ChunkRocksDBInstance::~ChunkRocksDBInstance()
{
    //  Call resets to make sure we clean in a right order
    if (_journalFlusher) {
        _journalFlusher->shutdown();
        _journalFlusher.reset();
    }
    _durabilityManager.reset();

    if(_counterManager)
        _counterManager->sync();

    _counterManager.reset();
    _compactionScheduler.reset();

    for(auto handle : columnFamilyHandles)
    {
        if(handle == nullptr || handle == dbInstance->DefaultColumnFamily())
            continue;

        delete handle;
    }

    Date_t initExecTime = Date_t::now();
    index_log() << "[offloadChunk] shardSvr delete rocksdb start nss: " << _rightChunkID;
    dbInstance.reset();
    index_log() << "[offloadChunk] shardSvr delete rocksdb end nss: " << _rightChunkID << "; used Time(ms): " <<
        (Date_t::now()-initExecTime);

    //db destructor must be before indexedRecordStorage destructor.
    indexedRecordStorage.reset();

    _shared_resource_module.reset();
    metadata.reset();

    if(_rightEnv) {
        delete _rightEnv;
        _rightEnv = nullptr;
    }

    if(_leftEnv) {
        delete _leftEnv;
        _leftEnv = nullptr;
    }
}

ChunkMetadata* ChunkRocksDBInstance::GetChunkMetadata()
{
    return metadata.get();
}

void ChunkRocksDBInstance::DumpSharedResourceStats(const char* path)
{
    if (_shared_resource_module) {
        _shared_resource_module->DumpSharedResourceList(path);
    }
}

rocksdb::Status ChunkRocksDBInstance::LoadMetadata(rocksdb::Env& fileSystem,
                                                   const std::string& chukMetaPath, 
                                                   std::unique_ptr<ChunkMetadata>& metadata)
{
    uint64_t fileSize = 0;

    // get chunkmetadata full name
    
    std::string db_path = chukMetaPath + "/" + metadataFileName;

    //
    //  For now if we don't find metadata just treat it as standard mongodb collection
    //
    rocksdb::Status status = fileSystem.GetFileSize(db_path, &fileSize);
    int retCode = status.code();
    log()<<"ChunkRocksDBInstance::LoadMetadata metadataFileName:" << db_path << ", retCode:" << retCode << ", fileSize:"<< fileSize;
    //int retCode = status.code();
    //log()<<"ChunkRocksDBInstance::LoadMetadata metadataFileName:" << db_path << ", retCode:" << retCode << ", fileSize:"<< fileSize;
    if(!status.ok() || fileSize==0)
    {
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


rocksdb::Status ChunkRocksDBInstance::SaveMetadata(rocksdb::Env& fileSystem, 
                                                   const std::string& chukMetaPath, 
                                                   const std::unique_ptr<ChunkMetadata>& metadata)
{   
    // get chunkmetadata full name
    std::string chunkMetaFile = chukMetaPath + "/" + metadataFileName;
    std::string chunkMetaFileB = chukMetaPath + "/" + metadataFileName + "B";

    RES_RIF(createDir(&fileSystem, chukMetaPath));
    
    std::unique_ptr<rocksdb::WritableFile> metadataFile;
    RES_RIF(fileSystem.NewWritableFile(chunkMetaFileB, &metadataFile, rocksdb::EnvOptions()));        

    mongo::BSONObj bson = metadata->ToBSON();
    std::string data_str = bson.toString();
    RES_RIF(metadataFile->Append(rocksdb::Slice(bson.objdata(), bson.objsize())));
    metadataFile->Sync();
    metadataFile.reset();
    RES_RIF(fileSystem.RenameFile(chunkMetaFileB,chunkMetaFile));

    return rocksdb::Status::OK();
}

void ChunkRocksDBInstance::PostInit(const rocksdb::Options& options)   //  Do final initialization
{
    if(options.info_log)
    {
        auto logger = dynamic_cast<ResultHandling::MongoRocksLoggerForChunk*>(options.info_log.get());
        _rocksdbLogger = options.info_log;
        if(logger != nullptr)
            logger->SetTag(metadata->GetChunk().getID());
    }

    //_leftEnv = options.env;

    bool useMoreDB =  ClusterRole::ShardServer == serverGlobalParams.clusterRole 
                      || ClusterRole::None == serverGlobalParams.clusterRole;

    bool non_sys_chunk = (metadata->GetChunk().getID().size() > 0 && metadata->GetChunk().getName() != system_chunkID);
    index_LOG(0)<<"ChunkRocksDBInstance::PostInit useMoreDB:"<<useMoreDB << ", non_sys_chunk:"<<non_sys_chunk
        << "; metadata: " << metadata->ToString() << "; ID: " << metadata->GetChunk().getID() << "; Name: "
        << metadata->GetChunk().getName();

    if ((useMoreDB/* && non_sys_chunk*/) || GLOBAL_CONFIG_GET(IsCoreTest)) {
        _counterManager.reset(new MoreDBCounterManager(GetDB(), 
                                  mongo::rocksGlobalOptions.crashSafeCounters,
                                  GetColumnFamily(indexedRecordStorage->GetMetadataFamilyId())));
    } else {
        _counterManager.reset(new mongo::RocksCounterManager(GetDB(),
                                  mongo::rocksGlobalOptions.crashSafeCounters,
                                  GetColumnFamily(indexedRecordStorage->GetMetadataFamilyId())));
    }
 
    _compactionScheduler.reset(new mongo::RocksCompactionScheduler(GetDB()));
    _durabilityManager.reset(new mongo::RocksDurabilityManager(GetDB(), durable));

    if (durable)
    {
        _journalFlusher = mongo::stdx::make_unique<mongo::RocksJournalFlusher>(_durabilityManager.get());
        _journalFlusher->go();
    }
}

std::string ChunkRocksDBInstance::getDbPath() const {
    return _dbPath + ROCKSDB_PATH;
}

std::string ChunkRocksDBInstance::getChunkMetaPath() const {
    return _dbPath + CHUNK_META_PATH;
}

std::string ChunkRocksDBInstance::getTransLogPath() const {
    return _dbPath + TRANS_LOG_PATH;
}


//
//  Initialize storage and open RocksDB
//
rocksdb::Status ChunkRocksDBInstance::OpenStorage(const rocksdb::Options& options, const std::string& dbPath)
{
    _leftEnv = options.env;
     RES_RIF(createDir(_leftEnv, dbPath));

    log() << "ChunkRocksDBInstance::OpenStorage()-> dbpath: " << dbPath << "; cf num : "
        << columnFamilyDescriptors.Get().size() << "; chunkMeta: " << metadata->ToString();
    //
    //  Create storage based on metadata settings
    //
    RES_RIF(IIndexedRecordStorage::Create(*metadata.get(), indexedRecordStorage));

    //
    //  Populate descriptors for column families, setting compaction filters
    //
    columnFamilyDescriptors.Init(*indexedRecordStorage.get(), options);

    if (metadata->GetChunk().getID().size() > 0 && metadata->GetChunk().getName() != system_chunkID) {
        if (GLOBAL_CONFIG_GET(enableAutoChunkSplit)) {
             invariantRocksOKWithNoCore(initTransactionLogModule(dbPath));
        }
    }
     
    std::string rocksdb_path = getDbPath();
    log() << "rocksdb path: " << rocksdb_path << "; dbPath: " << dbPath;

    RES_RIF(createDir(_leftEnv, rocksdb_path));

    log() << "ChunkRocksDBInstance::OpenStorage()->rocksdb_path: " << rocksdb_path << "; dbPath: " << dbPath;
    rocksdb::Options newOptions = options;
    newOptions.instance_option.instance_path = rocksdb_path;
    newOptions.instance_option.shared_checker = _shared_resource_module;
    //newOptions.instance_option.info_log = 
    //newOptions.instance_option.chunk_id = 

    //
    //  Open RocksDB instance
    //
    Date_t initExecTime = Date_t::now();
    index_log() << "[assignChunk] shardSvr rocksdb::Open start nss: " << rocksdb_path;
    rocksdb::DB* db = nullptr;
    RES_RIF(rocksdb::DB::Open(
        newOptions,
        rocksdb_path,
        columnFamilyDescriptors.Get(),
        &columnFamilyHandles,
        &db));
    index_log() << "[assignChunk] shardSvr rocksdb::Open end nss: " << rocksdb_path 
        << "; used Time(ms): "<< (Date_t::now()-initExecTime);
    invariant(columnFamilyHandles.size() == columnFamilyDescriptors.Get().size());
    dbInstance.reset(db);
    log() << "Opened Rocksdb for Path: {" << rocksdb_path << "} StorageId: " << (int)metadata->GetIndexedRecordStorageId()
        << " Chunk:" << metadata->GetChunk().getName();

    //
    //  Initialize required managers, schedulers and background jobs
    //
    PostInit(options);

    return rocksdb::Status::OK();
}

//
//  Open chunk's RocksDB instance. Is called from MongoD on Shard Server
//
rocksdb::Status ChunkRocksDBInstance::Open(const rocksdb::Options& options, const std::string& dbPath, bool _durable)
{
    durable = _durable;

    //
    //  First we need to load metadata from our environment
    //
    RES_RIF(LoadMetadata(*options.env, getChunkMetaPath(), metadata));

    //
    //  Initialize storage and open RocksDB
    //
    RES_RIF(OpenStorage(options, dbPath));

    //  TODO: should be removed from here
    RES_RIF(IIndexedRecordStorage::Create(*metadata.get(), indexedRecordStorageInPreperSplit));

    return rocksdb::Status::OK();
}

//
//  Create first chunk of the collection. Called from Config Server before assigning
//  this chunk to be served to Shard Server
//

std::string ChunkRocksDBInstance::getChunkMetaPath(const std::string& dbpath)
{
    return dbpath + CHUNK_META_PATH;
}

rocksdb::Status ChunkRocksDBInstance::UpdateMetadata(const rocksdb::Options& options,
                                                 const std::string& chukMetaPath,         
                                                 const std::unique_ptr<ChunkMetadata>& metaData){
   metadata->setCollection(metaData->GetCollection());
   RES_RIF(SaveMetadata(*options.env,chukMetaPath,metaData)); 
   return rocksdb::Status::OK(); 
}


rocksdb::Status ChunkRocksDBInstance::Create(
    const rocksdb::Options& options,
    const std::string& dbPath,
    std::unique_ptr<ChunkMetadata>&& metadata,
    std::unique_ptr<ChunkRocksDBInstance>& chunkDB
    )
{
    RES_RIF(createDir(options.env, dbPath));
    //
    //  Save metadata
    //

    RES_RIF(SaveMetadata(*options.env, getChunkMetaPath(dbPath), metadata));

    //
    //  Obtain storage
    //
    auto instance = mongo::stdx::make_unique<ChunkRocksDBInstance>(dbPath);
    instance->metadata = std::move(metadata);

    //
    //  Initialize storage and create RocksDB
    //
    rocksdb::Options dbOptions(options);
    dbOptions.create_missing_column_families = true;    //  Create column families that are missed
    RES_RIF(instance->OpenStorage(dbOptions, dbPath));


    //  TODO: should be removed from here
    RES_RIF(IIndexedRecordStorage::Create(*instance->metadata.get(), instance->indexedRecordStorageInPreperSplit));

    chunkDB = std::move(instance);

    return rocksdb::Status::OK();
}

mongo::RocksRecoveryUnit* ChunkRocksDBInstance::GetRocksRecoveryUnit(mongo::OperationContext* txn) const
{
    RocksRecoveryUnit* ru = mongo::RocksRecoveryUnit::getRocksRecoveryUnit(txn);

    //  Make sure correct DB is associated with RecoveryUnit
    if(ru->_db == this)
        return ru;

    ru->_db = (ChunkRocksDBInstance*)this;
    ru->transaction()->SetTransactionEngine(ru->_db->GetTransactionEngine());

    return ru;
}

rocksdb::Status ChunkRocksDBInstance::saveRightChunkMeta(const BSONObj& splitPoint,
                                                         const std::string& rightDBPath,
                                                         const std::string& rightChunkId,
                                                         std::unique_ptr<ChunkMetadata>& rightMeta)
{
    mongo::ChunkType chunkType =  metadata->GetChunk();
    chunkType.setMin(splitPoint);
    chunkType.setRootFolder(rightDBPath);
    chunkType.setID(rightChunkId);

    mongo::CollectionType ctype = metadata->GetCollection();
    //other field

    log() << "ChunkRocksDBInstance::saveRightChunkMeta()-> prarent chunkMeata: " << metadata->ToString();
    rightMeta.reset(new ChunkMetadata(metadata->GetIndexedRecordStorageId(), chunkType, ctype));
    log() << "ChunkRocksDBInstance::saveRightChunkMeta()-> right chunkMeata: " << rightMeta->ToString();
    log() << "ChunkRocksDBInstance::saveRightChunkMeta()-> rightDBPath: " << rightDBPath;
    std::string chunkMetaPath = rightDBPath + CHUNK_META_PATH;
    return SaveMetadata(*_rightEnv, chunkMetaPath , rightMeta);
}

rocksdb::Status ChunkRocksDBInstance::initRightPlogEnv(const std::string& rightDBPath, bool is_in_rollback)
{
    log() << "ChunkRocksDBInstance::initRightPlogEnv()-> right_dbpath: " << rightDBPath;

    //_rightEnv = rocksdb::Env::Default();
    rocksdb::NewHdfsEnv(&_rightEnv, GLOBAL_CONFIG_GET(hdfsUri), false);

    return rocksdb::Status::OK();
}

rocksdb::Status ChunkRocksDBInstance::writeRightTranasctionLog(const std::string& rightDBPath,
                                                               rocksdb::Env* rightEnv,
                                                               ChunkMetadata* rightMeta)
{
    log() << "ChunkRocksDBInstance::writeRightTranasctionLog()->rightDBPath: " << rightDBPath;
    rocksdb::Status status;
    //SharedResourceModule right_module;
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

//1. getSplitInfo(splitPoint, dataSize, numRecords);
//2. new right env
//3. write startSplit to transaction Log;
//4  save right chunkMetaData;
//5. preperSplit;
//6. block write IO of right DB
//7. rocksdb::split(stop compaction);
//8   write Transaction Log of right db
//9   block read IO of right db
Status ChunkRocksDBInstance::split(const std::string& rightDbpathWithPlog,
                                   const std::string& rightChunkId,
                                   const std::string& prefix,
                                   BSONObj& splitPoint,
                                   const ChunkType& chunk,
                                   uint64_t r_datasize,
                                   uint64_t r_numrecord)
{
    index_log() << "ChunkRocksDBInstance::split()-> right_dbpath:" << rightDbpathWithPlog << "; splitPoint: " << splitPoint
        << "; chunkId: " << rightChunkId;

    uint64_t left_datasize = 0, left_numrecord = 0;
    uint64_t right_datasize= 0, right_numrecord= 0;

    rocksdb::Status status;
    std::string splitPointStr;
    if (!splitPoint.isEmpty()) {
        mongo::RecordId recordid;
        GetIndexedRecordStorage().GetRecordId(splitPoint, recordid);
        rocksdb::Slice splitPointKey = RocksRecordStore::_makePrefixedKey(prefix, recordid);
        index_log() << "[splitChunk] ChunkRocksDBInstance::split(): RecordId: " << recordid.ToString()
            << "; splitPointkey: " << splitPointKey.ToString();
        status = dbInstance->GetSplitInfoBySplitPoint(splitPointKey.ToString(),
            left_datasize, left_numrecord, right_datasize, right_numrecord);
        splitPointStr = splitPoint.toString();
    }else{
        MDB_RIF(dbInstance->GetSplitPointBySize(splitPointStr,left_datasize, left_numrecord, right_datasize, right_numrecord));
        index_log() << "[splitChunk] ChunkRocksDBInstance::split(): splitPointStr: " << splitPointStr;

        //remove prefix
        if (splitPointStr.size() <= prefix.size()) {
            return Status(ErrorCodes::InternalError, "splitPoint is invalid");
        }
        splitPointStr = splitPointStr.substr(prefix.size());

        BSONObj splitPointBson;
        MDB_RIF(GetIndexedRecordStorage().GetBSONFromPrimaryKey(splitPointStr, true, true, splitPointBson));

        index_log() << "[splitChunk] ChunkRocksDBInstance::split(): splitPointBson: " << splitPointBson;
        splitPoint = splitPointBson;
    }

    // Check if splitPoint is valid
    if (splitPoint.isEmpty() ||
        splitPoint.woCompare(chunk.getMin()) <= 0 ||
        splitPoint.woCompare(chunk.getMax()) >= 0) {
        index_err() << "[splitChunk] splitPoint (" << splitPoint << ") is invalid";
        return Status(ErrorCodes::InternalError, "splitPoint is invalid");
    }

    index_log() << "ChunkRocksDBInstance::split()-> splitPoint: " << splitPoint << "; splitPointStr: " << splitPointStr
        <<"; left_datasize: " << left_datasize << "; left_numrecord: " << left_numrecord <<
        "; right_datasize: " << right_numrecord;

    index_log() << "ChunkRocksDBInstance::split()-> initRightPlogEnv: " ;
    status = initRightPlogEnv(rightDbpathWithPlog);
    if (!status.ok()) {
        index_log() << "ChunkRocksDBInstance::split()->genRightPlogEnv fail: " << (int)(status.subcode()) << status.ToString();
        return rocksToMongoStatus(status);
    }

    index_log() << "[splitChunk] _shared_resource_module->StartSplit() parent chunkId: " << getLeftChunkID()
        << "; childChunkId: " << getRightChunkID();
    auto splitInfo = std::make_shared<TransactionLog::SplitDescription>(getLeftChunkID(),
                                                                        getRightChunkID(),
                                                                        metadata->GetKeyRange().GetKeyLow(),
                                                                        metadata->GetKeyRange().GetKeyHigh(),
                                                                        splitPoint,
                                                                        rightDbpathWithPlog);
    status = _shared_resource_module->StartSplit(std::move(splitInfo));
    if (!status.ok()) {
        index_log() << "ChunkRocksDBInstance::split() write StartSplit fail: " <<  status.ToString();
        return rocksToMongoStatus(status);
    }

    std::unique_ptr<ChunkMetadata> rightMeta;
    status = saveRightChunkMeta(splitPoint, rightDbpathWithPlog, rightChunkId, rightMeta);
    if (!status.ok()) {
        index_log() << "ChunkRocksDBInstance::split() saveRightChunkMeta fail: " << status.ToString();
        invariantRocksOKWithNoCore(_shared_resource_module->RollbackSplit());
        deleteDir(rightDbpathWithPlog);
        return rocksToMongoStatus(status);
    }

    index_log() << "ChunkRocksDBInstance::split()-> splitDescription: " ;

    //GetIndexedRecordStorageInPreperSplit()->SetKeyRange(ChunkMetadata::KeyRange(metadata->GetKeyRange().GetKeyLow(), splitPoint));
    //std::vector<const rocksdb::IKeyRangeChecker*> CFDescriptions = { indexedRecordStorageInPreperSplit->GetColumnFamilyDescriptions()[0],
    //                                                                 indexedRecordStorageInPreperSplit->GetColumnFamilyDescriptions()[1] };

    ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage indexColumnFamilyStorge(*(rightMeta.get()));
    std::vector<const rocksdb::IKeyRangeChecker*> CFDescriptions = {indexColumnFamilyStorge.GetColumnFamilyDescriptions()[0],
                                                                    indexColumnFamilyStorge.GetColumnFamilyDescriptions()[1] };
    DbInstanceOption options = {rightDbpathWithPlog + ROCKSDB_PATH, nullptr, "", rightChunkId};
    Date_t initExecTime = Date_t::now();
    index_log() << "[splitChunk] shardSvr prepareSplitDB start nss: " << chunk.getNS(); 
    status = dbInstance->PrepareSplitDb(_rightEnv, CFDescriptions, options);
    index_log() << "[splitChunk] shardSvr prepareSplitDB end nss: " << chunk.getNS() <<
        "; used Time(ms): " << (Date_t::now()-initExecTime); 
    if (!status.ok()) {
        delete _rightEnv;
        _rightEnv = nullptr;
        index_log() << "ChunkRocksDBInstance::split()-> prepareSplit fail: " <<  status.ToString();
        invariantRocksOKWithNoCore(_shared_resource_module->RollbackSplit());
        deleteDir(rightDbpathWithPlog);
        return rocksToMongoStatus(status);
    }

    initExecTime = Date_t::now();
    index_log() << "[splitChunk] shardSvr blockIO start nss: " << chunk.getNS(); 
    const mongo::Milliseconds kSplitBlockIoInterval(10);
    while(_transactionEngine.getCountOfActiveWriteBatchesDuringSplit() > 0) {
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        _duringSplitCondVar.wait_for(lock, kSplitBlockIoInterval.toSystemDuration(),
            [&] {return _transactionEngine.getCountOfActiveWriteBatchesDuringSplit() == 0;});
    }

    //block write io of right db
    GetIndexedRecordStorageInPreperSplit()->SetKeyRange(ChunkMetadata::KeyRange(metadata->GetKeyRange().GetKeyLow(), splitPoint));
    _transactionEngine.setSplitIsOnGoing(true);
    index_log() << "ChunkRocksDBInstance::split()-> keyRange low: " << metadata->GetKeyRange().GetKeyLow() 
                << "; splitPoint: " << splitPoint;

    // update the recordNum & DataSize for right chunk.
    GetCounterManager()->updateCounter(GetIndexedRecordStorageInPreperSplit()->GetRightMetadataRecordKey("datasize"), r_datasize, nullptr, true);
    GetCounterManager()->updateCounter(GetIndexedRecordStorageInPreperSplit()->GetRightMetadataRecordKey("numrecords"), r_numrecord,nullptr, true);

    while(_transactionEngine.getCountOfActiveWriteBatchesOutsideOfSplit() > 0) {
        stdx::unique_lock<stdx::mutex> outSideOfSplitLock(_mutex);
        _outsideOfSplitCondVar.wait_for(outSideOfSplitLock, kSplitBlockIoInterval.toSystemDuration(),
            [&] {return _transactionEngine.getCountOfActiveWriteBatchesOutsideOfSplit() == 0;});
    }
    index_log() << "[splitChunk] shardSvr blockIO end nss: " << chunk.getNS() <<
        "; used Time(ms): " << (Date_t::now()-initExecTime);  

    initExecTime = Date_t::now();
    index_log() << "[splitChunk] shardSvr rocksdb::splitDb start nss: " << chunk.getNS(); 
    status = dbInstance->SplitDb(_rightEnv);
    index_log() << "[splitChunk] shardSvr rocksdb::splitDb end nss: " << chunk.getNS() <<
        "; used Time(ms): " << (Date_t::now()-initExecTime);  
    if (!status.ok()) {
        index_log() << "ChunkRocksDBInstance::split()-> SplitDb fail: " << status.ToString();
        GetIndexedRecordStorageInPreperSplit()->SetKeyRange(metadata->GetKeyRange());
        _transactionEngine.setSplitIsOnGoing(false);
        invariantRocksOK(dbInstance->RollbackSplit(_rightEnv));
        delete _rightEnv;
        _rightEnv = nullptr;
        invariantRocksOKWithNoCore(_shared_resource_module->RollbackSplit());
        deleteDir(rightDbpathWithPlog);
        return rocksToMongoStatus(status);
    }

    //std::string rightDBPath = rightDbpathWithPlog;
    //rightDBPath.assign(rightDBPath, 0, rightDBPath.find_last_of('/'));

    //log() << "ChunkRocksDBInstance::split()-> rightDbpathWithPlog: " <<  rightDbpathWithPlog <<
    //    "; rightDBPath: " << rightDBPath;
    status = writeRightTranasctionLog(rightDbpathWithPlog, _rightEnv, rightMeta.get());
    if (!status.ok()) {
        index_log() << "ChunkRocksDBInstance::split()-> writeRightTranasctionLog fail: " << status.ToString();
        GetIndexedRecordStorageInPreperSplit()->SetKeyRange(metadata->GetKeyRange());
        _transactionEngine.setSplitIsOnGoing(false);
        invariantRocksOK(dbInstance->RollbackSplit(_rightEnv));
        delete _rightEnv;
        _rightEnv = nullptr;
        invariantRocksOKWithNoCore(_shared_resource_module->RollbackSplit());
        deleteDir(rightDbpathWithPlog);
        return rocksToMongoStatus(status);
    }

    //block read io of right db
    _transactionEngine.setBlockReadIOofRight(true);
    //GetIndexedRecordStorageInPreperSplit()->SetKeyRange(ChunkMetadata::KeyRange(metadata->GetKeyRange().GetKeyLow(), splitPoint));

    index_log() << "ChunkRocksDBInstance::split()-> SplitDb suc: ";
    return Status::OK();
}

//1. update left ChunkMetaData
//2. update read filter(compaction, rocksdb read,
//3  rocksdb::comfirm (start compaction);
//4  write split commit to left transaction log
//5  unblock write IO of right db
//6  unblock read IO of right db
rocksdb::Status ChunkRocksDBInstance::confirmSplit(const ConfirmSplitRequest& request)
{
    rocksdb::Status status;
    if (request.splitSuccess()) {
        log() << "ChunkRocksDBInstance::comfirmSplit()-> split success ";
        std::unique_ptr<ChunkMetadata> chunkMeta(new ChunkMetadata(metadata->GetIndexedRecordStorageId(),
            request.getChunk(), request.getCollection()));
        status = SaveMetadata(*_leftEnv, getChunkMetaPath(), chunkMeta);
        if (!status.ok()) {
            log() << "ChunkRocksDBInstance::comfirmSplitSuccess() saveMetadata fail:";
            return status;
        }

        metadata = std::move(chunkMeta);

        //ChunkMetadata::KeyRange keyRange(metadata->GetKeyRange().GetKeyLow(), splitPoint);
        GetIndexedRecordStorage().SetKeyRange(metadata->GetKeyRange());
        log() << "ChunkRocksDBInstance::confirmSplit()-> keyLow: " << metadata->GetKeyRange().GetKeyLow()
            << "; keyHigh: " << metadata->GetKeyRange().GetKeyHigh();

        invariantRocksOKWithNoCore(dbInstance->ConfirmSplitSuccess(_rightEnv));
        log() << "ChunkRocksDBInstance::comfirmSplitSuccess() s_shared_resource_module->CommitSplit()";

        status = _shared_resource_module->CommitSplit();
        if (!status.ok()) {
            log() << "ChunkRocksDBInstance::comfirmSplitSuccess() _shared_resource_module->CommitSplit fail:";
            return status;
        }
       log() << "ChunkRocksDBInstance::comfirmSplitSuccess() suc";
    } else {
        log() << "ChunkRocksDBInstance::comfirmSplit()-> split fail ";
        if (nullptr == _rightEnv) { //shardSvr crash
            // delete
            std::string right_dbpath = _shared_resource_module->GetRightDbPath();
            status = initRightPlogEnv(right_dbpath, true);
            if (!status.ok()) {
                log() << "ChunkRocksDBInstance::confirmSplit()->genRightPlogEnv fail: " << status.ToString();
                return status;
            }
        }

        invariantRocksOKWithNoCore(dbInstance->RollbackSplit(_rightEnv));
        invariantRocksOKWithNoCore(_shared_resource_module->RollbackSplit());

        GetIndexedRecordStorageInPreperSplit()->SetKeyRange(metadata->GetKeyRange());

        //unblock read IO of rigth db
        _transactionEngine.setBlockReadIOofRight(false);
    }

    //unblock  wirte IO of rigth db
    _transactionEngine.setSplitIsOnGoing(false);
    delete _rightEnv;
    _rightEnv = nullptr;
    return rocksdb::Status::OK();
}

rocksdb::Status ChunkRocksDBInstance::initTransactionLogModule(const std::string& dbPathWithoutPlog)
{
    //_shared_resource_module.reset(new SharedResourceModule);
    _shared_resource_module.reset(new MaaSSharedResourceModule());
    rocksdb::Status status = _shared_resource_module->Init(_leftEnv, dbPathWithoutPlog, metadata.get());
    if (!status.ok()) {
        log() << "ChunkRocksDBInstance::initTransactionLogModule() Init ShardResourceModeul fail: " << status.ToString();
        return status;
    }

    status = rePlayTransactionLog();
    if (!status.ok()) {
        log() << "ChunkRocksDBInstance::initTransactionLogModule() rePlayTransactionLog(): " << status.ToString();
        return status;
    }

    return rocksdb::Status::OK();
}

rocksdb::Status ChunkRocksDBInstance::rePlayTransactionLog()
{
    if (_shared_resource_module->IsSplitStateFault()) {
        std::string right_dbpath = _shared_resource_module->GetRightDbPath();
        invariant(!right_dbpath.empty());
        rocksdb::Status status = initRightPlogEnv(right_dbpath, true);
        if (!status.ok()) {
            log() << "ChunkRocksDBInstance::confirmSplit()->genRightPlogEnv fail: " << status.ToString();
            return status;
        }

        //invariantRocksOKWithNoCore(dbInstance->RollbackSplit(_rightEnv));
        index_log() << "[assign] ChunkRocksDBInstance::rePlayTransactionLog(): rightdbpath: " << right_dbpath;
        //deleteDir(right_dbpath);//todo  remove dbpath of right
        delete _rightEnv;
        _rightEnv = nullptr;
    }
    return rocksdb::Status::OK();
}

const IChunkColumnFamilyDescription* ChunkRocksDBInstance::getColumnFamilyDescriptionInPreperSplit(const std::string& name) const{
    auto cfds = indexedRecordStorageInPreperSplit->GetColumnFamilyDescriptions();
    for (auto cfd : cfds) {
        if (cfd->GetName() == name) {
            return cfd;
        }
    }

    return nullptr;
}

}//  namespace mongo


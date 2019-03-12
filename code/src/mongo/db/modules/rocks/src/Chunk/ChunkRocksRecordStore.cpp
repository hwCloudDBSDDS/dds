#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "ChunkRocksRecordStore.h"
#include "ChunkRocksDBInstance.h"
#include <rocksdb/db.h>
#include <mongo/stdx/memory.h>
#include <mongo/bson/bson_comparator_interface_base.h>
#include <mongo/util/log.h>
#include <mongo/base/status.h>

namespace mongo {

ChunkRocksRecordStore::ChunkRocksRecordStore(
                StringData ns, StringData id,
                std::unique_ptr<ChunkRocksDBInstance> db,
                const std::string& prefix,
                bool isCapped, int64_t cappedMaxSize,
                int64_t cappedMaxDocs, CappedCallback* cappedDeleteCallback) : 
        RocksRecordStore(ns, id, db.get(),
            "9999",
            isCapped, cappedMaxSize,
            cappedMaxDocs, cappedDeleteCallback), chunkDBInstance(std::move(db))
{
    if (chunkDBInstance) {
        chunkDBInstance->GetIndexedRecordStorage().Init(*this);
    } else {
        log() << "ChunkRocksRecordStore::ChunkRocksRecordStore db is nullptr";
    }
}

void ChunkRocksRecordStore::PostInit(std::unique_ptr<ChunkRocksDBInstance> db)
{
    dbInstancePtr = db.get();
    _db = db->GetDB();
    _counterManager = db->GetCounterManager();

    _cappedVisibilityManager.reset((_isCapped || _isOplog) ? 
                     new CappedVisibilityManager(this, db->GetDurabilityManager())
                    : nullptr);

    chunkDBInstance = std::move(db);
    chunkDBInstance->GetIndexedRecordStorage().Init(*this);

    _dataSizeKey = chunkDBInstance->GetIndexedRecordStorage().GetMetadataRecordKey(_dataSizeMetadataRecordName);
    _numRecordsKey = chunkDBInstance->GetIndexedRecordStorage().GetMetadataRecordKey(_numRecordsMetadataRecordName);


    index_log() << " minkey is :" << dbInstancePtr->GetChunkMetadata()->GetChunk().getMin().getOwned().toString()
             << " _dataSizeKey is :" << _dataSizeKey
             << " _numRecordsKey is :" << _numRecordsKey;

    Init();
}

const char* ChunkRocksRecordStore::ChunkRocksRecordStore::name() const
{
    return "SeparateDBRocksRecordStore";
}

mongo::RecordId ChunkRocksRecordStore::_makeRecordId(const rocksdb::Slice& key) const
{
    mongo::RecordId recordId;
    chunkDBInstance->GetIndexedRecordStorage().ParseRecordKey(key, recordId);

    return recordId;
}

mongo::RecordId ChunkRocksRecordStore::generateRecordId(const rocksdb::Slice& data)
{
    mongo::RecordId recordId;    
    chunkDBInstance->GetIndexedRecordStorage().GetRecordId(BSONObj(data.data()), recordId);

    return recordId;
}

void ChunkRocksRecordStore::loadNextIdNum()
{
    //  Don't do anything. Particular storage need to load it when it decides that it makes sense.
}

RocksRecoveryUnit* ChunkRocksRecordStore::getRocksRecoveryUnit(OperationContext* txn) const
{
    //  Make sure correct DB is associated with RecoveryUnit
    return chunkDBInstance->GetRocksRecoveryUnit(txn);
}

//todo  May need to be moved to mongo::Collection
Status ChunkRocksRecordStore::validChunkMeta(const ChunkType& chunktype, bool isSplit) const {
    if(chunktype.getMin().woCompare(chunkDBInstance->GetMetadata().GetChunk().getMin()) != 0) {
        log() << "ChunkRocksRecordStore::validChunkMetaForSplit() minKey invalid, shard chunkType: " 
            << chunkDBInstance->GetMetadata().GetChunk() << "; config chunkType: " << chunktype;

        return Status(ErrorCodes::BadValue, "MinKey not EQ");
    }

    if(chunktype.getID() != chunkDBInstance->GetMetadata().GetChunk().getID()){
        log() << "ChunkRocksRecordStore::validChunkMetaForSplit() chunkid invalid, shard chunkType: " 
            << chunkDBInstance->GetMetadata().GetChunk() << "; config chunkType: " << chunktype;
        return Status(ErrorCodes::BadValue, "ChunkID not EQ");
    }

    if (isSplit) {
        if (!(chunktype.getVersion() == chunkDBInstance->GetMetadata().GetChunk().getVersion())) {
            log() << "ChunkRocksRecordStore::validChunkMetaForSplit() chunkversion invalid, shard chunkType: " 
                << chunkDBInstance->GetMetadata().GetChunk() << "; config chunkType: " << chunktype;
            return Status(ErrorCodes::BadValue, "ChunkVersion not EQ");
        }

        if(chunktype.getMax().woCompare(chunkDBInstance->GetMetadata().GetChunk().getMax()) != 0) {
            log() << "ChunkRocksRecordStore::validChunkMetaForSplit() maxKey invalid, shard chunkType: " 
                << chunkDBInstance->GetMetadata().GetChunk() << "; config chunkType: " << chunktype;
            return Status(ErrorCodes::BadValue, "MaxKey not EQ");
        }
    } else{
        if (!(chunktype.getVersion() <= chunkDBInstance->GetMetadata().GetChunk().getVersion())) {
            log() << "ChunkRocksRecordStore::validChunkMetaForSplit() chunkversion invalid, shard chunkType: " 
                << chunkDBInstance->GetMetadata().GetChunk() << "; config chunkType: " << chunktype;
            return Status(ErrorCodes::BadValue, "ChunkVersion not EQ");
        }

        if(chunktype.getMax().woCompare(chunkDBInstance->GetMetadata().GetChunk().getMax()) == 0) {
            log() << "ChunkRocksRecordStore::validChunkMetaForSplit() maxKey invalid, shard chunkType: " 
                << chunkDBInstance->GetMetadata().GetChunk() << "; config chunkType: " << chunktype;
            return Status(ErrorCodes::BadValue, "MaxKey not EQ");
        }
    }

    return Status::OK();
}

Status ChunkRocksRecordStore::split(OperationContext* txn, const SplitChunkReq& request,
                                    BSONObj& splitPoint) 
{
    if(!GLOBAL_CONFIG_GET(enableAutoChunkSplit)) {
        return Status(ErrorCodes::BadValue, "split chunk not allow");
    }

    Status status = validChunkMeta(request.getChunk(), true);

    //update right side data size and num records.
    r_data_size   =  dataSize(txn)/2;
    r_num_records =  numRecords(txn)/2 + numRecords(txn)%2;

    chunkDBInstance->setRightChunkID(request.getRightChunkName());
    status = chunkDBInstance->split(request.getRightDBPath(), request.getRightChunkId(),
             _prefix, splitPoint, request.getChunk(), r_data_size, r_num_records);

    index_log() << "[splitChunk] ChunkRocksRecordStore::split()"
                << ", p_chunk: "<< request.getName()
                << ", c_chunk: "<< request.getRightChunkName()
                << ", r_num_records: "<<r_num_records
                << ", r_data_size: "<< r_data_size
                << ", l_numRecords: "<< numRecords(txn)
                << ", l_dataSize: "<<dataSize(txn);

    log() << "ChunkRocksRecordStore::split() splitPoint: " << splitPoint ;
    return status;
}

Status ChunkRocksRecordStore::confirmSplit(OperationContext* txn,
                                           const ConfirmSplitRequest& request)
{
    Status status = validChunkMeta(request.getChunk(), false);
    //if (!status.isOK()){
    //    return status;
    //}
    auto r_status = rocksToMongoStatus(chunkDBInstance->confirmSplit(request));
    if(!r_status.isOK()){
        error()<<"ChunkRocksRecordStore::confirmSplit is not ok, status:"<<r_status;
        return r_status;
    }

    //update left side datasize and numrecords
    index_log() << "ChunkRocksRecordStore::confirmSplit before l_data_size:" << dataSize(txn) 
                << ", l_num_records:" << numRecords(txn) 
                << ", r_data_size:" << r_data_size 
                << ", r_num_records:" << r_num_records;
    if(request.splitSuccess())
    {
        //update memory statistics
        _numRecords.fetch_add(-r_num_records);
        _dataSize.fetch_add(-r_data_size);

        //write to disk
        _counterManager->updateCounter(_numRecordsKey, numRecords(txn), nullptr,true);
        _counterManager->updateCounter(_dataSizeKey, dataSize(txn), nullptr,true);
    }

    index_log() << "ChunkRocksRecordStore::confirmSplit after l_data_size:" << dataSize(txn) 
                << ", l_num_records:" << numRecords(txn);
    
    return r_status;
}

};    //  namespace mongo

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "ChunkRocksRecordStore.h"
#include <mongo/base/status.h>
#include <mongo/bson/bson_comparator_interface_base.h>
#include <mongo/stdx/memory.h>
#include <mongo/util/log.h>
#include <rocksdb/db.h>
#include "ChunkRocksDBInstance.h"

namespace mongo {

    ChunkRocksRecordStore::ChunkRocksRecordStore(StringData ns, StringData id,
                                                 ChunkRocksDBInstance* db,
                                                 const std::string& prefix, bool isCapped,
                                                 int64_t cappedMaxSize, int64_t cappedMaxDocs,
                                                 CappedCallback* cappedDeleteCallback)
        : RocksRecordStore(ns, id, db, "9999", isCapped, cappedMaxSize, cappedMaxDocs,
                           cappedDeleteCallback) {
        if (db) {
            db->GetIndexedRecordStorage().Init(*this);
        }
    }

    ChunkRocksRecordStore::~ChunkRocksRecordStore() {
        invariant(!(((NamespaceString)_ns).isSystemCollection()));
        if (dbInstancePtr) {
            dbInstancePtr->stopBGThread();
        }
    }

    void ChunkRocksRecordStore::PostInit(ChunkRocksDBInstance* db) {
        dbInstancePtr = db;
        _db = db->GetDB();
        _counterManager = db->GetCounterManager();

        _cappedVisibilityManager.reset(
            (_isCapped || _isOplog) ? new CappedVisibilityManager(this, db->GetDurabilityManager())
                                    : nullptr);

        dbInstancePtr->GetIndexedRecordStorage().Init(*this);

        _dataSizeKey = dbInstancePtr->GetIndexedRecordStorage().GetMetadataRecordKey(
            _dataSizeMetadataRecordName);
        _numRecordsKey = dbInstancePtr->GetIndexedRecordStorage().GetMetadataRecordKey(
            _numRecordsMetadataRecordName);

        Init();
    }

    const char* ChunkRocksRecordStore::ChunkRocksRecordStore::name() const {
        return "SeparateDBRocksRecordStore";
    }

    mongo::RecordId ChunkRocksRecordStore::_makeRecordId(const rocksdb::Slice& key) const {
        mongo::RecordId recordId;
        dbInstancePtr->GetIndexedRecordStorage().ParseRecordKey(key, recordId);
        return recordId;
    }

    mongo::RecordId ChunkRocksRecordStore::generateRecordId(const rocksdb::Slice& data) {
        mongo::RecordId recordId;
        dbInstancePtr->GetIndexedRecordStorage().GetRecordId(BSONObj(data.data()), recordId);
        return recordId;
    }

    void ChunkRocksRecordStore::loadNextIdNum() {
        //  Don't do anything. Particular storage need to load it when it decides that it makes
        //  sense.
    }


    // todo  May need to be moved to mongo::Collection
    Status ChunkRocksRecordStore::validChunkMeta(const ChunkType& chunktype, bool isSplit, bool needRollBack) const {
        if (chunktype.getMin().woCompare(dbInstancePtr->GetMetadata().GetChunk().getMin()) != 0) {
            index_err() << "[splitChunk] minKey invalid, shard chunkType: "
                        << dbInstancePtr->GetMetadata().GetChunk()
                        << "; config chunkType: " << chunktype;

            return Status(ErrorCodes::BadValue, "MinKey not EQ");
        }

        if (chunktype.getID() != dbInstancePtr->GetMetadata().GetChunk().getID()) {
            index_err() << "[splitChunk] chunkid invalid, shard chunkType: "
                        << dbInstancePtr->GetMetadata().GetChunk()
                        << "; config chunkType: " << chunktype;
            return Status(ErrorCodes::BadValue, "ChunkID not EQ");
        }

        if (isSplit) {
            /*if (!(chunktype.getVersion() == dbInstancePtr->GetMetadata().GetChunk().getVersion())) {
                index_err() << "[splitChunk] chunkversion invalid, shard chunkType: "
                            << dbInstancePtr->GetMetadata().GetChunk()
                            << "; config chunkType: " << chunktype;
                return Status(ErrorCodes::BadValue, "ChunkVersion not EQ");
            }*/

            if (chunktype.getMax().woCompare(dbInstancePtr->GetMetadata().GetChunk().getMax()) !=
                0) {
                index_err() << "[splitChunk] maxKey invalid, shard chunkType: "
                            << dbInstancePtr->GetMetadata().GetChunk()
                            << "; config chunkType: " << chunktype;
                return Status(ErrorCodes::BadValue, "MaxKey not EQ");
            }
        } else {
            /*if (!(chunktype.getVersion() <= dbInstancePtr->GetMetadata().GetChunk().getVersion())) {
                index_err() << "[splitChunk] chunkversion invalid, shard chunkType: "
                            << dbInstancePtr->GetMetadata().GetChunk()
                            << "; config chunkType: " << chunktype;
                return Status(ErrorCodes::BadValue, "ChunkVersion not EQ");
            }*/

            if (!needRollBack && chunktype.getMax().woCompare(dbInstancePtr->GetMetadata().GetChunk().getMax()) ==
                0) {
                index_err() << "[splitChunk] maxKey invalid, shard chunkType: "
                            << dbInstancePtr->GetMetadata().GetChunk()
                            << "; config chunkType: " << chunktype;
                return Status(ErrorCodes::BadValue, "split success and MaxKey should not EQ");
            }

            if (needRollBack && chunktype.getMax().woCompare(dbInstancePtr->GetMetadata().GetChunk().getMax()) !=
                0) {
               return Status(ErrorCodes::BadValue, "split failed and MaxKey should EQ"); 
            }
        }

        return Status::OK();
    }

    Status ChunkRocksRecordStore::preSplit(OperationContext* txn, const SplitChunkReq& request,
                                           BSONObj& splitPoint) {
        Status status = validChunkMeta(request.getChunk(), true);
	if (!status.isOK()) {
	    index_err() << "[splitChunk] preSplit failed, status: " << status.toString();
	    return status;
	}
        dbInstancePtr->setRightChunkID(request.getRightChunkName());
        status = dbInstancePtr->preSplit(request.getRightDBPath(), request.getRightChunkId(),
                                         _prefix, request.getChunk(), splitPoint);
        return status;
    }

    Status ChunkRocksRecordStore::split(OperationContext* txn, const SplitChunkReq& request,
                                        const BSONObj& splitPoint) {
        // update right side data size and num records.
        r_data_size = dataSize(txn) / 2;
        r_num_records = numRecords(txn) / 2 + numRecords(txn) % 2;

        Status status = dbInstancePtr->split(request.getRightDBPath(), splitPoint,
                                             request.getChunk(), r_data_size, r_num_records);

        index_LOG(0) << "[splitChunk] ChunkRocksRecordStore::split()"
                     << ", p_chunk: " << request.getName()
                     << ", c_chunk: " << request.getRightChunkName()
                     << ", r_num_records: " << r_num_records << ", r_data_size: " << r_data_size
                     << ", l_numRecords: " << numRecords(txn) << ", l_dataSize: " << dataSize(txn);

        return status;
    }

    Status ChunkRocksRecordStore::rollbackPreSplit(OperationContext* txn,
                                                   const SplitChunkReq& request) {
        return dbInstancePtr->rollbackPreSplit(txn, request.getRightDBPath());
    }

    Status ChunkRocksRecordStore::confirmSplit(OperationContext* txn,
                                               const ConfirmSplitRequest& request) {
        Status status = Status::OK();
        if (request.splitSuccess()) {
            status = validChunkMeta(request.getChunk(), false);
        } else {
            status = validChunkMeta(request.getChunk(), false, true);
        }

        if (!status.isOK()) {
	    index_err() << "[splitChunk] confirmSplit failed, status: " << status.toString();
            return status;
        }
        auto r_status = rocksToMongoStatus(dbInstancePtr->confirmSplit(request));
        if (!r_status.isOK()) {
            index_err() << "[confirmSplit] error, status: " << r_status;
            return r_status;
        }

        // update left side datasize and numrecords
        index_log() << "ChunkRocksRecordStore::confirmSplit before l_data_size:" << dataSize(txn)
                    << ", l_num_records:" << numRecords(txn) << ", r_data_size:" << r_data_size
                    << ", r_num_records:" << r_num_records;
        if (request.splitSuccess()) {
            // update memory statistics
            _numRecords.fetch_add(-r_num_records);
            _dataSize.fetch_add(-r_data_size);

            // write to disk
            _counterManager->updateCounter(_numRecordsKey, numRecords(txn), nullptr, true);
            _counterManager->updateCounter(_dataSizeKey, dataSize(txn), nullptr, true);
        }

        index_log() << "ChunkRocksRecordStore::confirmSplit after l_data_size:" << dataSize(txn)
                    << ", l_num_records:" << numRecords(txn);

        return r_status;
    }

};  //  namespace mongo

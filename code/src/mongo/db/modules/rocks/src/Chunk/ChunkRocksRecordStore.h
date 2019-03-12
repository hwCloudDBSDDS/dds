
#pragma once

#include <rocksdb/db.h>

#include "../rocks_record_store.h"

namespace mongo {
    class ChunkRocksDBInstance;

    class ChunkRocksRecordStore : public mongo::RocksRecordStore {
    public:
        ChunkRocksRecordStore(mongo::StringData ns, mongo::StringData id, ChunkRocksDBInstance* db,
                              const std::string& prefix, bool isCapped = false,
                              int64_t cappedMaxSize = -1, int64_t cappedMaxDocs = -1,
                              mongo::CappedCallback* cappedDeleteCallback = NULL);

        ~ChunkRocksRecordStore();

        //  Name of the store for debugging
        virtual const char* name() const override;

        //  Parse key slice and return record id
        virtual mongo::RecordId _makeRecordId(const rocksdb::Slice& key) const override;

        //  Generate RecordId from the data
        virtual mongo::RecordId generateRecordId(const rocksdb::Slice& data) override;

        //  Load auto-increment for RecordId - we just skip it in this class
        virtual void loadNextIdNum() override;

        virtual Status preSplit(OperationContext* txn, const SplitChunkReq& request,
                                BSONObj& splitPoint) override;

        virtual Status split(OperationContext* txn, const SplitChunkReq& request,
                             const BSONObj& splitPoint) override;

        virtual Status rollbackPreSplit(OperationContext* txn,
                                        const SplitChunkReq& request) override;

        virtual Status confirmSplit(OperationContext* txn,
                                    const ConfirmSplitRequest& request) override;

        void PostInit(ChunkRocksDBInstance* db);

    private:
        rocksdb::Status getSplitPoint(rocksdb::Slice& splitPoint) const;
        Status validChunkMeta(const ChunkType& chunktype, bool isSplit, bool needRollBack = false) const;

        long long r_data_size = 0;
        long long r_num_records = 0;
    };

}  //  namespace mongo

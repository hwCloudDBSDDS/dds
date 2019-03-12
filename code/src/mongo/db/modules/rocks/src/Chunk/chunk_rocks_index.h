
#pragma once

#include "IndexedRecordStorage.h"

#pragma once


namespace mongo {

class RocksRecoveryUnit;

class ChunkRocksIndexBase : public RocksIndexBase {
    MONGO_DISALLOW_COPYING(ChunkRocksIndexBase);
public:
    ChunkRocksIndexBase(mongo::ChunkRocksDBInstance& db, std::unique_ptr<IIndexRecordFormatter> recordFormatter);
    rocksdb::ColumnFamilyHandle* GetColumnFamily() const;
    IIndexRecordFormatter& GetIndexRecordFormatter() const;
    
    //  SortedDataInterface
    bool isEmpty(OperationContext* txn) override;    
    Status insert(OperationContext* txn, const BSONObj& key, const RecordId& loc, bool dupsAllowed) override;
    void unindex(OperationContext* txn, const BSONObj& key, const RecordId& loc, bool dupsAllowed) override;
    std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* txn, bool forward) const override;    

protected:
    //  onInsert allows to check and update inserted KV-pair. It will set empty key if insert is not required.
    virtual Status onInsert(OperationContext* txn, bool dupsAllowed, std::pair<std::string, std::string>& recordKeyValuePair) = 0;
    virtual Status finalizeUnindex(OperationContext* txn, const std::pair<std::string, std::string>& recordKeyValuePair, bool dupsAllowed) = 0;

    Status GenerateRecord(const BSONObj& key, const RecordId& loc, std::pair<std::string, std::string>& recordKeyValuePair);
    Status PrepareWrite(OperationContext* txn, const BSONObj& key, const RecordId& loc, std::pair<std::string, std::string>& recordKeyValuePair);

protected:    
    std::unique_ptr<IIndexRecordFormatter> _recordFormatter;
};

class ChunkRocksUniqueIndex : public ChunkRocksIndexBase
{
public:
    ChunkRocksUniqueIndex(ChunkRocksDBInstance& db, std::unique_ptr<IIndexRecordFormatter> recordFormatter);
    
    Status dupKeyCheck(OperationContext* txn, const BSONObj& key, const RecordId& loc) override;    
    SortedDataBuilderInterface* getBulkBuilder(OperationContext* txn, bool dupsAllowed);

protected:
    Status onInsert(OperationContext* txn, bool dupsAllowed, std::pair<std::string, std::string>& recordKeyValuePair) override;
    Status finalizeUnindex(OperationContext* txn, const std::pair<std::string, std::string>& recordKeyValuePair, bool dupsAllowed) override;
};

class ChunkRocksStandardIndex : public ChunkRocksIndexBase
{
public:
    ChunkRocksStandardIndex(mongo::ChunkRocksDBInstance& db, std::unique_ptr<IIndexRecordFormatter> recordFormatter);
    
    Status dupKeyCheck(OperationContext* txn, const BSONObj& key, const RecordId& loc)
    {
        invariant(false);
    }

    SortedDataBuilderInterface* getBulkBuilder(OperationContext* txn, bool dupsAllowed);

protected:
    Status onInsert(OperationContext* txn, bool dupsAllowed, std::pair<std::string, std::string>& recordKeyValuePair) override;
    Status finalizeUnindex(OperationContext* txn, const std::pair<std::string, std::string>& recordKeyValuePair, bool dupsAllowed) override;
};

} // namespace mongo

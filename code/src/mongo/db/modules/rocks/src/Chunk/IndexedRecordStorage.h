
#pragma once

#include "ChunkMetadata.h"
#include <rocksdb/db.h>
#include <mongo/db/storage/key_string.h>
#include <mongo/bson/ordering.h>

namespace mongo
{

class ChunkRocksRecordStore;

///////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Inteface is responsible for converting MongoDB indexes into RocksDB records
//
class IIndexRecordFormatter
{
public:
    //
    //  Return the id of the column family where record should be written
    //
    virtual uint8_t GetColumnFamilyId() const = 0;

    //
    //  Return ordering of the index
    //
    virtual mongo::Ordering GetOrdering() const = 0;

    //
    //  Check whether index unique (does't allow duplicate records for the same key)
    //
    virtual bool IsUnique() const = 0;

    //
    //  Whether index is used for local key (usually _id, which is part of primary key)
    //
    virtual bool IsLocalKey() const = 0;

    //
    //  Formats the RocksDB record, which represents the MongoDB index record
    //
    virtual rocksdb::Status GenerateRecord(
                                const mongo::BSONObj& key,                              //  Index key
                                const mongo::RecordId& documentRecordId,                //  Shard Key can be here
                                std::pair<std::string, std::string>& recordKeyValuePair
                                ) const = 0;

    //
    //  Take RocksDB record and tries to restore original MongoDB  index
    //
    virtual rocksdb::Status ParseRecord(
                                const std::pair<rocksdb::Slice, rocksdb::Slice>& indexKeyValuePair,
                                mongo::BSONObj* key,
                                mongo::RecordId* documentRecordId
                                ) const = 0;

    //
    //  Formats the RocksDB key, which represent position
    //
    virtual rocksdb::Status GenerateCursorPosition(
                                const mongo::BSONObj& key,                              //  Index key
                                KeyString::Discriminator discriminator,
                                std::string& recordKey
                                ) const = 0;

    //
    //  Optional prefix of the key (TODO: remove and just generate key)
    //
    virtual const std::string& GetPrefix() const = 0;

    //
    //  Destructor
    //
    virtual ~IIndexRecordFormatter() {}
};


///////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Interface describes chunk column family setting. IIndexedRecordStorage use this interface to
//  expose information about column families that it's using.
//
class IChunkColumnFamilyDescription : public rocksdb::IKeyRangeChecker
{
public:
    //
    //  Return the name of the column family
    //
    virtual const char* GetName() const = 0;

    //
    //  Check whether column family for chunk to which this description belongs can contain
    //  record with such key and value. E.g. immidiately after the split of chunk [A, C), 
    //  both parts of the split [A, B) and [B, C) can contain all records related to [A, C).
    //  This function helps to filter out records that don't belong to chunk anymore.
    //
    virtual bool DoesRecordBelongToChunk(const rocksdb::Slice& key, const rocksdb::Slice& value) const
    {
        return true;
    }

    //
    //  Check whether key range belongs to column family for particular chunk.    
    //  Used after the split, the same way as DoesRecordBelongToChunk.
    //  Should be called for SST files and for blocks within this files.
    //
    virtual rocksdb::KeyRangeCheckResult DoesKeyRangeBelongToChunk(const rocksdb::Slice& keyLow, const rocksdb::Slice& keyHigh) const
    {
        return rocksdb::KeyRangeCheckResult::RangeOverlap;
    }

    //
    //  Return to what shard key this record belongs
    //
    virtual rocksdb::Slice GetShardKey(const rocksdb::Slice& key, const rocksdb::Slice& value) const
    {
        return rocksdb::Slice();
    }

    //
    //  Return prefix extractor, which is used to build bloom filters
    //
    virtual std::shared_ptr<const rocksdb::SliceTransform> GetPrefixExtractor() const
    {
        return nullptr;
    }

    //
    //  Destructor
    //
    virtual ~IChunkColumnFamilyDescription() {}
};



///////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Interface describes class, which is responsible for convertion of MongoDB documents into RocksDB records.
//  In particular, class is responsible for:
//      1) Defining of what column families are used
//      2) Generating RecordId that MongoDB uses to identify the record on the Shard
//      3) Generating primary key for a document
//      4) Generating keys and values for index records
//
class IIndexedRecordStorage
{
public:
    static const std::string defaultMetadataKeyPrefix;

public: //  Interface members
    //
    //  Return the list of column families that are used by this storage
    //  Column family descriptor can contain Compaction filter to filter out rows after the split
    //
    virtual const std::vector<const IChunkColumnFamilyDescription*>& GetColumnFamilyDescriptions() const = 0;

    //
    //  Optional initialization stage which allows loading some saved data from database.
    //  like statistics or auto-increments
    //
    virtual rocksdb::Status Init(ChunkRocksRecordStore& recordStore) { return rocksdb::Status::OK(); }

    //
    //  Generate RecordId from document. Documents are stored in RocksDB sorted by RecordId.
    //
    virtual rocksdb::Status GetRecordId(const mongo::BSONObj& document, mongo::RecordId& documentRecordId) const = 0;

    //
    //  Parse Rocks key to get RecordId. TODO: consider using IIndexRecordFormatter for main data family for that.
    //
    virtual rocksdb::Status ParseRecordKey(const rocksdb::Slice& key, mongo::RecordId& documentRecordId) const = 0;

    //
    //  Return the key formatter which represents secondary index
    //
    virtual std::unique_ptr<IIndexRecordFormatter> GetSecondaryIndexFormatter(const IndexDefinition& index) const = 0; 

    //
    //  Return BSON from primay key slice.
    //      includeFieldNames: if false, all field names in BSON will be empty
    //      returnOnlyShardKey: if true, only fields that are part of the shard key are returned
    //  Can be not supported for storages that doesn't support split.
    //
    virtual rocksdb::Status GetBSONFromPrimaryKey(
        const rocksdb::Slice& key,
        bool includeFieldNames,
        bool returnOnlyShardKey,
        mongo::BSONObj& keyBSON
        ) const = 0;

    //
    //  Apply new key range (e.g. after the split)
    //
    virtual void SetKeyRange(const ChunkMetadata::KeyRange& range) = 0;

    //
    //  Id of the column family to store arbitrary chunk-related data, like statistics or metadata
    //
    virtual uint8_t GetMetadataFamilyId() const
    {
        return 0;   //  Data column family by default
    }

    //
    //  Encode the key of the metadata record with logical key metadataKey.
    //  must make sure the datasizekey and numrecordskey is same length.
    //
    virtual std::string GetMetadataRecordKey(const char* metadataKey) const
    {
        return defaultMetadataKeyPrefix + metadataKey;
    }

    virtual std::string GetRightMetadataRecordKey(const char* metadataKey) const
    {
        return defaultMetadataKeyPrefix + metadataKey;
    }

    

    virtual std::string GetLowKey(void) = 0;
    
    virtual std::string GetHighKey(void) = 0;

    //
    //  Destructor
    //
    virtual ~IIndexedRecordStorage() {}

public: //  Static functions
    //
    //  Create storage based on settings in metadata
    //
    static rocksdb::Status Create(const ChunkMetadata& metadata, std::unique_ptr<IIndexedRecordStorage>& storage);
};  //  IIndexedRecordStorage

};  //  mongo

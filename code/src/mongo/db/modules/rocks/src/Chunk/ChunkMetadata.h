
#pragma once

#include <mongo/bson/BSONParser.h>
#include <mongo/bson/bsonobj.h>
#include <mongo/db/index/index_descriptor.h>
#include <mongo/db/record_id.h>
#include <mongo/s/catalog/type_chunk.h>
#include <mongo/s/catalog/type_collection.h>
#include <mongo/stdx/memory.h>
#include <rocksdb/slice.h>
#include <vector>
#include "../mongo_rocks_result_handling.h"

namespace mongo {

#define RES_LOG_ERROR(status, operation) \
    ResultHandling::LogError(status, operation, __FILE__, __LINE__, __FUNCTION__)

    typedef std::string CollectionId;

    //
    //  Contains definition of the index
    //
    class IndexDefinition {
        uint64_t indexId = 0;       //  Used as an index prefix
        mongo::BSONObj definition;  //  MongoDB index description
    public:
        IndexDefinition(uint64_t indexId, mongo::BSONObj definition)
            : indexId(indexId), definition(definition) {}

        uint64_t GetIndexId() const { return indexId; }

        const mongo::BSONObj& GetDefinition() const { return definition; }

        bool IsUnique() const { return IndexDescriptor::hasUniqueModifier(definition) || IsId(); }

        BSONObj GetKeyPattern() const { return IndexDescriptor::getKeyPattern(definition); }

        bool IsId() const { return IndexDescriptor::isIdIndexPattern(GetKeyPattern()); }
    };

    //
    //  Type of the storage
    //
    enum class IndexedRecordStorageId : uint16_t {
        //
        //  Storage places data the same way as original MongoDB on Rocks, implemented by Parse:
        //  RecordId - uint64_t auto-increment within a shard
        //  With this option we just create standard RocksRecordStore and don't touch
        //  ChunkRocksRecordStore.
        //
        MongoRocksStandard = 0,

        //
        //  recordId = shardKey + _id + typeBits + shardKeySize
        //  Two column families:
        //      Data: { dataPrefixId + recordId } => { document }
        //      Index:
        //          Unique: { indexPrefixId + key } -> { valueSize + recordId + typeBits +
        //          shardKeyOffset }
        //          Standard: { indexPrefixId +  key + recordId } -> { typeBits + shardKeyOffset }
        //
        ShardKeyPrefixedDataWithStandardIndexColumnFamily,

        //
        //  Two column families:
        //      Data: { shard key + documentId } => { document }
        //      Index: { shard key + indexId + indexedValue + originalDocumentId } =>
        //          { sizeof(originalDocumentId) "+" sizeof(IndexesValue) }
        //
        ShardKeyPrefixedWithIndexColumnFamily,

        //
        //  Chengdu MaaS team to specify
        //
        MaaS
    };

    //
    //  Class represented metadata of the chunk, including chunk collection metadata and type
    //  of the storage that chunk uses. Metadata is stored in BSON in chunk root folder in BSON and
    //  normally loaded during assignment. It's also presented on Config Server.
    //
    class ChunkMetadata {
    protected:
        int metadataVersion = 1;

        //
        //  Type of the record storage that we use for this chunk
        //  Record storage defines the way we store records and indexes for them in RocksDB,
        //  in particular, what column families we use and how we serialize the keys
        //
        IndexedRecordStorageId indexedRecordStorageId = IndexedRecordStorageId::MongoRocksStandard;

        mongo::ChunkType chunk;
        mongo::CollectionType collection;

        std::vector<IndexDefinition> indexes;

#define CHUNK_METADATA_PARSE_ELEMENT(elem, field) \
    if (!BsonParser::GetValue(elem, field)) RES_RET(rocksdb::Status::Corruption());

#define CHUNK_METADATA_PARSE_FIELD(field) CHUNK_METADATA_PARSE_ELEMENT(fields[fieldId++], field);

        rocksdb::Status InitializeIndexFromCollection() {
            for (const mongo::BSONElement& indexElem : collection.getIndex()) {
                mongo::BSONObj obj;
                CHUNK_METADATA_PARSE_ELEMENT(indexElem, obj);

                uint64_t indexId;
                CHUNK_METADATA_PARSE_ELEMENT(obj.getField("prefix"), indexId);

                //  Insert index metadata (config) into array, index BSON should
                //  already be owned by collection
                indexes.emplace_back(indexId, obj);
            }

            return rocksdb::Status::OK();
        }

    public:
        const ChunkType& GetChunk() const { return chunk; }

        void setChunkVersion(const mongo::ChunkVersion& chunkVersion) {
            chunk.setVersion(chunkVersion);
        }

        void setCollection(const CollectionType& coll) { collection = coll; }

        const CollectionType& GetCollection() const { return collection; }

        IndexedRecordStorageId GetIndexedRecordStorageId() const { return indexedRecordStorageId; }

        const std::vector<IndexDefinition>& GetIndexes() const { return indexes; }

        const IndexDefinition* GetIndex(uint64_t prefix) const {
            auto it = std::find_if(
                indexes.begin(), indexes.end(),
                [=](const IndexDefinition& index) { return index.GetIndexId() == prefix; });
            if (it == indexes.end()) return nullptr;

            return &(*it);
        }

        //
        //  Represent range of the keys
        //
        class KeyRange {
        protected:
            mongo::BSONObj keyLow;
            mongo::BSONObj keyHigh;

        public:
            KeyRange() {}
            KeyRange(const mongo::BSONObj& key_low, const mongo::BSONObj& key_high)
                : keyLow(key_low.getOwned()), keyHigh(key_high.getOwned()) {}

            const mongo::BSONObj& GetKeyLow() const { return keyLow; }

            const mongo::BSONObj& GetKeyHigh() const { return keyHigh; }

            KeyRange& operator=(const KeyRange& keyRange) {
                keyLow = keyRange.keyLow;
                keyHigh = keyRange.keyHigh;
                return *this;
            }

            bool operator==(const KeyRange& keyRange) {
                return BsonParser::AreEqual(keyLow, keyRange.keyLow) &&
                       BsonParser::AreEqual(keyHigh, keyRange.keyHigh);
            }
        };

        KeyRange GetKeyRange() const { return KeyRange(chunk.getMin(), chunk.getMax()); }

        void SetKeyRange(const BSONObj& min, const BSONObj& max) {
            chunk.setMin(min);
            chunk.setMax(max);
        }

    public:
        ChunkMetadata() {}

        ChunkMetadata(IndexedRecordStorageId indexedRecordStorageId, const ChunkType& chunk,
                      const CollectionType& collection)
            : indexedRecordStorageId(indexedRecordStorageId), chunk(chunk), collection(collection) {
            InitializeIndexFromCollection();
        }

        static rocksdb::Status Parse(const rocksdb::Slice& slice,
                                     std::unique_ptr<ChunkMetadata>& metadata) {
            mongo::BSONObj bson(slice.data());
            RES_RET(Parse(bson, metadata));
        }

        static rocksdb::Status Parse(const mongo::BSONObj& bson,
                                     std::unique_ptr<ChunkMetadata>& metadata) {
            auto meta = mongo::stdx::make_unique<ChunkMetadata>();

            std::array<mongo::StringData, 8> fieldNames{"metadataVersion", "indexedRecordStorageId",
                                                        "chunk", "collection"};
            std::array<mongo::BSONElement, fieldNames.size()> fields;
            bson.getFields(fieldNames, &fields);

            int fieldId = 0;
            CHUNK_METADATA_PARSE_FIELD(meta->metadataVersion);
            if (meta->metadataVersion != 1) RES_RET(rocksdb::Status::NotSupported());

            int storageId = 0;
            CHUNK_METADATA_PARSE_FIELD(storageId);
            meta->indexedRecordStorageId = (IndexedRecordStorageId)storageId;

            mongo::BSONObj chunkObj;
            CHUNK_METADATA_PARSE_FIELD(chunkObj);
            auto chunkRes = ChunkType::fromBSON(chunkObj);
            if (!chunkRes.isOK()) {
                RES_LOG_ERROR(chunkRes.getStatus(), "ChunkType::fromBSON");
                return rocksdb::Status::Corruption();
            }

            meta->chunk = std::move(chunkRes.getValue());

            mongo::BSONObj collectionObj;
            CHUNK_METADATA_PARSE_FIELD(collectionObj);
            auto collectionRes = CollectionType::fromBSON(collectionObj);
            if (!collectionRes.isOK()) {
                RES_LOG_ERROR(collectionRes.getStatus(), "CollectionType::fromBSON");
                return rocksdb::Status::Corruption();
            }

            meta->collection = std::move(collectionRes.getValue());

            RES_RIF(meta->InitializeIndexFromCollection());

            metadata = std::move(meta);
            return rocksdb::Status::OK();
        }

        mongo::BSONObj ToBSON() const {
            mongo::BSONObjBuilder builder;
            builder.append("metadataVersion", metadataVersion);
            builder.append("indexedRecordStorageId", (int)indexedRecordStorageId);
            builder.append("chunk", chunk.toBSON());
            builder.append("collection", collection.toBSON());

            return builder.obj();
        }

        std::string ToString() const { return ToBSON().toString(); }
    };

};  //  mongo

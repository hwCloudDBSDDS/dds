
#pragma once

#include <mongo/db/storage/key_string.h>

namespace mongo {

    BSONObj stripFieldNames(const BSONObj& obj);
    constexpr int MaxKeySize = 4096;  //  (1<<12)

    class KeyHeader {
    protected:
        rocksdb::Slice key;
        rocksdb::Slice typeBits;

    public:
        rocksdb::Status GetTypeBits(KeyString::TypeBits& outTypeBits) const {
            if (!outTypeBits.readFromEnd((uint8_t*)typeBits.data(), typeBits.size()).isOK())
                RES_RET(rocksdb::Status::Corruption());

            return rocksdb::Status::OK();
        }

        const rocksdb::Slice& GetKey() const { return key; }
    };

    class RecordIdFormatter : public KeyHeader {
    protected:
        static constexpr int bitsInSize = 6;
        union TailByte {
            struct {
                uint8_t size : bitsInSize;
                bool continuation : 1;
                const bool version : 1;  //  For key versioning: 0 - current
            };

            uint8_t value;

            TailByte() : value(0) {}
        };

        rocksdb::Slice shardKey;

    public:
        RecordIdFormatter() { static_assert(sizeof(TailByte) == 1, "TailByte must be 1 byte"); }

        static void EncodeHeader(StackBufBuilder& buffer, size_t shardKeySize,
                                 const KeyString::TypeBits& typeBits) {
            invariant(shardKeySize < MaxKeySize);

            const bool readFromEnd = true;
            typeBits.Write(buffer, readFromEnd);

            TailByte tailByte;

            //  Need to encode only sharkey size, since primary key always starts with shard key
            if (shardKeySize < (1 << bitsInSize)) {
                tailByte.size = (uint8_t)shardKeySize;
                buffer.appendUChar(tailByte.value);
                return;
            }

            uint8_t sizeRemaining = (shardKeySize >> bitsInSize);
            tailByte.size = (shardKeySize & ((1 << bitsInSize) - 1));
            tailByte.continuation = true;

            buffer.appendUChar(sizeRemaining);
            buffer.appendUChar(tailByte.value);
        }

        rocksdb::Status DecodeHeader(const rocksdb::Slice& buffer) {
            //  key = ShardKey + _id
            //  buffer = ShardKey + _id + typeBits (written with readFromEnd) + tail (1 or 2 bytes
            //  wit TailByte last)
            if (buffer.size() < 3) RES_RET(rocksdb::Status::Corruption());

            TailByte tailByte;
            tailByte.value = buffer[buffer.size() - 1];
            if (tailByte.version != 0) {
                index_err() << "RecordIdFormatter::DecodeHeader version must be 0.";
                RES_RET(rocksdb::Status::NotSupported());
            }
            size_t shardKeySize = tailByte.size;
            if (tailByte.continuation) {
                if (buffer.size() < 2) RES_RET(rocksdb::Status::Corruption());

                shardKeySize |= (uint16_t)(buffer[buffer.size() - 2] << bitsInSize);
            }

            size_t tailSize = tailByte.continuation ? 2 : 1;
            if (buffer.size() < tailSize + 2) RES_RET(rocksdb::Status::Corruption());

            int typeBitsSize =
                KeyString::TypeBits::getRemainingBytesSize(buffer[buffer.size() - tailSize - 1]) +
                1;
            if (tailSize + typeBitsSize + shardKeySize >= buffer.size())
                RES_RET(rocksdb::Status::Corruption());

            size_t keySize = buffer.size() - tailSize - typeBitsSize;
            key = rocksdb::Slice(buffer.data(), keySize);
            if (key.size() < 2) RES_RET(rocksdb::Status::Corruption());

            shardKey = rocksdb::Slice(buffer.data(), shardKeySize);
            typeBits = rocksdb::Slice(buffer.data() + keySize, typeBitsSize);

            if (key[keySize - 1] != KeyString::kEnd) RES_RET(rocksdb::Status::Corruption());

            return rocksdb::Status::OK();
        }

        const rocksdb::Slice& GetShardKey() const { return shardKey; }
    };

    class SecondaryKeyFormatter : protected KeyHeader {
    protected:
        static constexpr int bitsInTailOffset = 5;

        union TailByte {
            struct {
                //  Offset of the primary key from the begining of the record: key or value,
                //  depending of the primaryKeyInValue
                uint8_t primaryKeyOffset : bitsInTailOffset;

                //  If set to true, byte below current (tail) contains remaining high bits of the
                //  primaryKeyOffset
                bool continuation : 1;

                //  if true - primary keys is encoded in value (unique index), otherwise in key
                bool primaryKeyInValue : 1;

                //  For key versioning: 0 - current
                bool version : 1;
            };

            uint8_t value;

            TailByte() : value(0) { version = 1; }
        };

        rocksdb::Slice primaryKey;

    public:
        SecondaryKeyFormatter() { static_assert(sizeof(TailByte) == 1, "TailByte must be 1 byte"); }

        static void EncodeHeader(StackBufBuilder& buffer, const KeyString::TypeBits& typeBits,
                                 bool primaryKeyInValue, uint16_t primaryKeyOffset) {
            invariant(primaryKeyOffset < MaxKeySize);

            const bool readFromEnd = true;
            typeBits.Write(buffer, readFromEnd);

            TailByte tailByte;
            tailByte.primaryKeyInValue = primaryKeyInValue;
            if (primaryKeyInValue) primaryKeyOffset = buffer.len();  //  Offset from the end

            //  Need to encode only sharkey size, since primary key always starts with shard key
            if (primaryKeyOffset < (1 << bitsInTailOffset)) {
                tailByte.primaryKeyOffset = (uint8_t)primaryKeyOffset;
                buffer.appendUChar(tailByte.value);
                return;
            }

            uint8_t sizeRemaining = (primaryKeyOffset >> bitsInTailOffset);
            tailByte.primaryKeyOffset = (primaryKeyOffset & ((1 << bitsInTailOffset) - 1));
            tailByte.continuation = true;

            buffer.appendUChar(sizeRemaining);
            buffer.appendUChar(tailByte.value);
        }

        rocksdb::Status DecodeHeader(const rocksdb::Slice& indexKey,
                                     const rocksdb::Slice& indexValue, size_t prefixSize = 0) {
            //  buffer = [PrimaryKey]+TypeBits (written with readFromEnd) + tail (1 or 2 bytes with
            //  TailByte last)
            if (indexValue.size() < 2) RES_RET(rocksdb::Status::Corruption());

            TailByte tailByte;
            tailByte.value = (uint8_t)indexValue[indexValue.size() - 1];
            if (tailByte.version != 1) {
                index_err() << "SecondaryKeyFormatter::DecodeHeader version must be 1."
                            << ", indexKey:" << indexKey.ToString()
                            << ", indexKey size:" << indexKey.size()
                            << ", indexValue:" << indexValue.ToString()
                            << ", indexValue size:" << indexValue.size();
                RES_RET(rocksdb::Status::NotSupported());
            }

            uint16_t primaryKeyOffset = tailByte.primaryKeyOffset;
            if (tailByte.continuation) {
                if (indexValue.size() < 2) RES_RET(rocksdb::Status::Corruption());
                primaryKeyOffset |=
                    ((uint16_t)(indexValue[indexValue.size() - 2]) << bitsInTailOffset);
            }

            size_t tailSize = tailByte.continuation ? 2 : 1;
            if (indexValue.size() < tailSize + 1) RES_RET(rocksdb::Status::Corruption());

            int typeBitsSize = KeyString::TypeBits::getRemainingBytesSize(
                                   indexValue[indexValue.size() - tailSize - 1]) +
                               1;
            if (tailSize + typeBitsSize > indexValue.size()) RES_RET(rocksdb::Status::Corruption());

            const char* const valueEnd = indexValue.data() + indexValue.size();
            typeBits = rocksdb::Slice(valueEnd - tailSize - typeBitsSize, typeBitsSize);

            if (tailByte.primaryKeyInValue) {  //  Offset from the tail
                if (primaryKeyOffset <= typeBitsSize ||
                    tailSize + primaryKeyOffset > indexValue.size())
                    RES_RET(rocksdb::Status::Corruption());

                key = indexKey;
                primaryKey = rocksdb::Slice(valueEnd - tailSize - primaryKeyOffset,
                                            primaryKeyOffset - typeBitsSize);
            } else {  //  Offset from the begining
                if (primaryKeyOffset >= indexKey.size() || primaryKeyOffset == 0) {
                    std::stringstream errorMessage;
                    errorMessage << " primaryKeyOffset=" << primaryKeyOffset
                                 << " tailSize=" << tailSize << " typeBitsSize=" << typeBitsSize;
                    RES_RET(rocksdb::Status::Corruption(errorMessage.str()));
                }

                key = rocksdb::Slice(indexKey.data(), primaryKeyOffset);
                primaryKey = rocksdb::Slice(indexKey.data() + primaryKeyOffset,
                                            indexKey.size() - primaryKeyOffset);
            }

            key.remove_prefix(prefixSize);

            return rocksdb::Status::OK();
        }

        rocksdb::Status Parse(const std::pair<rocksdb::Slice, rocksdb::Slice>& indexKeyValuePair,
                              size_t prefixSize) {
            RES_RET(DecodeHeader(indexKeyValuePair.first, indexKeyValuePair.second, prefixSize));
        }

        BSONObj GetSecondaryKey(Ordering keyOrdering) {
            KeyString::TypeBits typeBits;
            invariantRocksOK(GetTypeBits(typeBits));
            return KeyString::toBson(key.data(), key.size(), keyOrdering, typeBits);
        }

        RecordId GetRecordId() { return RecordId(primaryKey.data(), primaryKey.size()); }

        rocksdb::Status GetShardKey(rocksdb::Slice& result) const {
            RecordIdFormatter formatter;
            RES_RIF(formatter.DecodeHeader(primaryKey));
            result = formatter.GetShardKey();
            return rocksdb::Status::OK();
        }

        rocksdb::Status GetPrimaryKey(rocksdb::Slice& result) const {
            result = primaryKey;
            return rocksdb::Status::OK();
        }
    };

    class ChunkKeyString : protected KeyString {
    public:
        ChunkKeyString() : KeyString(KeyString::Version::V1) {}

        std::string ToString() const { return std::string(getBuffer(), getSize()); }
    };

    class PrimaryKeyBuilder : public ChunkKeyString {
        int shardKeySize = 0;

        void AppendShardKey(const BSONObj& obj, Ordering ord) {
            int currentLen = _buffer.len();
            _appendElements(obj, ord, Discriminator::kInclusive, true);
            shardKeySize = _buffer.len() - currentLen;
        }

        void FinalizePrimaryKey() {
            _appendKeyEnd();
            RecordIdFormatter::EncodeHeader(_buffer, shardKeySize, _typeBits);
        }

        RecordId ToRecordId() const { return RecordId(getBuffer(), getSize()); }

    public:
        static RecordId Generate(const BSONObj& obj, Ordering ord, const BSONElement* _idElement) {
            PrimaryKeyBuilder builder;
            builder.AppendShardKey(obj, ord);
            if (_idElement)  //  id element is not part of shard key, thus add it separately
            {
                builder._appendBsonValue(*_idElement,
                                         false,   //  _id index is always ascending
                                         nullptr  //  field name
                                         );
            }
            builder.FinalizePrimaryKey();

            return builder.ToRecordId();
        }

        static void GenerateRangeBorderKey(const ChunkMetadata::KeyRange& range, Ordering ordering,
                                           std::string& lowKey, std::string& highKey,
                                           std::string& highKeyNoEnd) {
            PrimaryKeyBuilder builder;
            builder.AppendShardKey(range.GetKeyLow(), ordering);
            lowKey = builder.ToString();

            builder.resetToEmpty();
            builder.AppendShardKey(range.GetKeyHigh(), ordering);
            highKeyNoEnd = builder.ToString();
            builder._appendKeyEnd();
            highKey = builder.ToString();
        }
    };

    //  TODO: rewrite this mess to more appropriate interface
    class SecondaryIndexBuilder : public ChunkKeyString {
    protected:
        int primaryKeyOffset = -1;

    public:
        void Append(const rocksdb::Slice& slice) {
            const bool invert = false;
            _appendBytes(slice.data(), slice.size(), invert);
        }

        void Append(const BSONObj& obj, const Ordering ord,
                    Discriminator discriminator = Discriminator::kInclusive) {
            _appendAllElementsForIndexing(obj, ord, discriminator);
        }

        // void AppendNokEnd(const BSONObj& obj, const Ordering ord, Discriminator discriminator =
        // Discriminator::kInclusive)
        //{
        //    _appendElements(obj, ord, discriminator, true);
        //}

        // void AppendSkey(const BSONObj& skey)
        //{
        //    if(_buffer.len() > 0)
        //    {
        //        _appendKeyEnd();
        //    }
        //    _appendElementsOnly(skey);
        //}

        void Append(const mongo::RecordId& documentRecordId) {
            if (_buffer.len() > 0) _appendKeyEnd();

            primaryKeyOffset = _buffer.len();
            const bool invert = false;
            _appendBytes(documentRecordId.repr().Data(), documentRecordId.repr().Size(), invert);
        }

        const TypeBits& get_TypeBits() const { return getTypeBits(); }
        const char* get_Buffer() const { return getBuffer(); }
        size_t get_Size() const { return getSize(); }

        void AppendHeader(const SecondaryIndexBuilder& keyBuilder) {
            invariant((primaryKeyOffset == -1 || keyBuilder.primaryKeyOffset == -1) &&
                      (primaryKeyOffset >= 0 || keyBuilder.primaryKeyOffset >= 0));

            bool primaryKeyInValue = false;
            if (primaryKeyOffset == -1)
                primaryKeyOffset = keyBuilder.primaryKeyOffset;
            else
                primaryKeyInValue = true;

            SecondaryKeyFormatter::EncodeHeader(_buffer, keyBuilder._typeBits, primaryKeyInValue,
                                                (uint16_t)primaryKeyOffset);
        }
    };

}  // namespace mongo

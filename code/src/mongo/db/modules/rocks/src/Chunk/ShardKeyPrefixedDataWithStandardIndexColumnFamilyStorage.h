#pragma once

#include "IndexedRecordStorage.h"
#include <mongo/stdx/memory.h>
#include <rocksdb/db.h>
#include "ChunkRocksRecordStore.h"
#include "../rocks_record_store.h"
#include "../rocks_engine.h"
#include "chunk_key_string.h"
#include <memory>
#include "mongo/s/shard_key_pattern.h"
#include "rocksdb/slice_transform.h"

namespace mongo
{

class IndexRecordFormatterBase
{
protected:
    bool isUniqueIndex;
    BSONObj keyPattern;
    uint64_t prefix;

    IndexRecordFormatterBase(const IndexDefinition& indexDefinition)
    {
        prefix = indexDefinition.GetIndexId();
        const BSONObj& infoObj = indexDefinition.GetDefinition();
        keyPattern = IndexDescriptor::getKeyPattern(infoObj);
        isUniqueIndex = IndexDescriptor::hasUniqueModifier(infoObj);    //  We never should have _id index here
    }
};


class ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage : public IIndexedRecordStorage
{
protected:
    typedef ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage Storage;

    static rocksdb::Slice RemovePrefix(const rocksdb::Slice& slice)
    {
        rocksdb::Slice result = slice;
        result.remove_prefix(sizeof(int32_t));
        return result;
    }

    static rocksdb::Slice GetSharKeyFromPrimaryKey(const rocksdb::Slice& key)
    {
        RecordIdFormatter formatter;
        invariantRocksOK(formatter.DecodeHeader(RemovePrefix(key)));
        rocksdb::Slice shardKey = formatter.GetShardKey();
        invariant(!shardKey.empty());

        return shardKey;
    }

    class ShardKeyFromPrimaryKeyExtractor : public rocksdb::SliceTransform
    {
    public:
        virtual const char* Name() const
        {
            return "ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage::ShardKeyFromPrimaryKeyExtractor";
        }

        virtual bool InDomain(const rocksdb::Slice& key) const
        {
            return key.size() > sizeof(int32_t)+sizeof(int8_t);
        }

        virtual rocksdb::Slice Transform(const rocksdb::Slice& key) const
        {
            return GetSharKeyFromPrimaryKey(key);
        }

        virtual bool InRange(const rocksdb::Slice& dst) const
        {
            return true;
        }

        virtual bool SameResultWhenAppended(const rocksdb::Slice& prefix) const
        {
            return false;
        }
    };

    class StorageReferenceHolder
    {
    protected:
        const Storage& storage;
    public:
        StorageReferenceHolder(const Storage& storage) : storage(storage) { }
    };

    class CFDescriptionBase : public IChunkColumnFamilyDescription, public StorageReferenceHolder
    {
    public:
        CFDescriptionBase(const Storage& storage) : StorageReferenceHolder(storage) { }

        virtual bool DoesRecordBelongToChunk(const rocksdb::Slice& key, const rocksdb::Slice& value) const override
        {   
            rocksdb::Slice shardKey = GetShardKey(key, value);
            bool belong =  storage.DoesShardKeyBelongToChunk(shardKey);

             if(key.starts_with(defaultMetadataKeyPrefix)){ 
                LOG(0)<<"@@@@####@@@@ key:"<<key.ToString()
                      <<", belong: "<<belong
                      << ", lowKey: " << storage.lowKey <<", l_size:" << storage.lowKey.size()
                      << ", highKey:" << storage.highKey <<", h_size:" << storage.highKey.size()
                      <<", shardKey: "<< shardKey.ToString()<<", s_size:" << shardKey.size()
                      <<",CompareWithLowKey: "<<storage.CompareWithLowKey(shardKey)
                      <<",CompareWithHighKey: "<< storage.CompareWithHighKey(shardKey);
             }

             return belong;
        }
    };

    class DataColumnFamilyDescription : public CFDescriptionBase
    {
    public:
        DataColumnFamilyDescription(const Storage& storage) : CFDescriptionBase(storage)
        {}

        virtual const char* GetName() const override
        {
            return rocksdb::kDefaultColumnFamilyName.c_str();
        }

        virtual rocksdb::KeyRangeCheckResult DoesKeyRangeBelongToChunk(const rocksdb::Slice& rangeKeyLow, const rocksdb::Slice& rangeKeyHigh) const override
        {
            //  Don't extract exact shard key since key always starts with shard key
            return storage.DoesShardKeyRangeBelongToChunk(storage.Adjust(rangeKeyLow), storage.Adjust(rangeKeyHigh));
        }

        virtual rocksdb::Slice GetShardKey(const rocksdb::Slice& key, const rocksdb::Slice& value) const override
        {
            rocksdb::Slice p_key = key;
            
            BSONObj s_key = storage.GetShardKeyBSON(RemovePrefix(p_key));       
            LOG(1)<<"@@##@@ data_cf GetShardKey s_key:"<<s_key;
            return GetSharKeyFromPrimaryKey(key);
        }
        
        virtual std::shared_ptr<const rocksdb::SliceTransform> GetPrefixExtractor() const
        {
            return std::shared_ptr<const rocksdb::SliceTransform>(new ShardKeyFromPrimaryKeyExtractor());
        }
    };

    class IndexColumnFamilyDescription : public CFDescriptionBase
    {
    public:
        IndexColumnFamilyDescription(const Storage& storage) : CFDescriptionBase(storage)
        {}

        virtual const char* GetName() const override
        {
            return "index";
        }

        virtual rocksdb::Slice GetShardKey(const rocksdb::Slice& key, const rocksdb::Slice& value) const
        {

            rocksdb::Slice shardKey;
            std::string meta_key("numrecords");
            if(key.starts_with(defaultMetadataKeyPrefix)){    //  Metadata section
                int ignore_len = defaultMetadataKeyPrefix.size() + meta_key.size();
                shardKey = rocksdb::Slice(key.data() + ignore_len, key.size() - ignore_len);
                LOG(1)<< "@@@@####@@@@defaultMetadataKeyPrefix.size():"<< defaultMetadataKeyPrefix.size()
                      << ", meta_key.size():"<<meta_key.size()<<",shardKey:" <<shardKey.ToString();

                return shardKey;
            }


            SecondaryKeyFormatter reader;
            invariantRocksOK(reader.DecodeHeader(key, value));

            invariantRocksOK(reader.GetShardKey(shardKey));

            rocksdb::Slice p_key;
            reader.GetPrimaryKey(p_key);

            BSONObj s_key = storage.GetShardKeyBSON(p_key);
            LOG(1)<<"@@##@@ idx_cf GetShardKey s_key:"<<s_key;
            invariant(!shardKey.empty());

            return shardKey;
        }
    };

    class IndexRecordFormatter : public IIndexRecordFormatter, public StorageReferenceHolder
    {
        std::string prefix;
        bool isUnique;
        BSONObj keyPattern;
        Ordering keyOrdering;
        bool isLocalKey;
    public:
        IndexRecordFormatter(const Storage& storage, const IndexDefinition& index) : StorageReferenceHolder(storage)
        {
            prefix = RocksEngine::encodePrefix(index.GetIndexId());
            isUnique = index.IsUnique();
            keyPattern = index.GetKeyPattern();
            keyOrdering = Ordering::make(keyPattern);
            isLocalKey = index.IsId();
        }

        virtual uint8_t GetColumnFamilyId() const override
        {
            return 1;   // All indexes go to the second column family
        }

        virtual mongo::Ordering GetOrdering() const override
        {
            return keyOrdering;
        }

        virtual bool IsUnique() const override
        {
            return isUnique;
        }

        virtual bool IsLocalKey() const override
        {
            return isLocalKey;
        }        

        virtual rocksdb::Status GenerateRecord(
                                    const mongo::BSONObj& key,                              //  Index key
                                    const mongo::RecordId& documentRecordId,                //  Shard Key can be here
                                    std::pair<std::string, std::string>& recordKeyValuePair
                                    ) const override
        {
            SecondaryIndexBuilder keyBuilder;
            SecondaryIndexBuilder valueBuilder;

            keyBuilder.Append(prefix);
            keyBuilder.Append(key, keyOrdering);
            LOG(1)<<"IndexRecordFormatter GenerateRecord unique:"<<isUnique
                  <<",key:"<<key <<",RecordId:"<<documentRecordId.ToString();

            if(isUnique)
                valueBuilder.Append(documentRecordId);
            else            
                keyBuilder.Append(documentRecordId);

            valueBuilder.AppendHeader(keyBuilder);

            recordKeyValuePair.first = keyBuilder.ToString();
            recordKeyValuePair.second = valueBuilder.ToString();


            //
            //  TODO: Just invariant check that we can get shard key, otherwise crash.
            //  Can be removed, when code is stable
            //
            SecondaryKeyFormatter reader;
            invariantRocksOK(reader.DecodeHeader(recordKeyValuePair.first, recordKeyValuePair.second));

            rocksdb::Slice shardKey;
            invariantRocksOK(reader.GetShardKey(shardKey));

            rocksdb::Slice primaryKey;
            invariantRocksOK(reader.GetPrimaryKey(primaryKey));
            LOG(1)<<"IndexRecordFormatter GenerateRecord shardKey:"<<storage.GetShardKeyBSON(primaryKey)
                <<", _lowKey" << storage._lowKey <<",_highKey" << storage._highKey
                <<", idxKeyLen:" << recordKeyValuePair.first.size()
                <<", valLen:" << recordKeyValuePair.second.size();
            invariant(!shardKey.empty());


            return rocksdb::Status::OK();
        }
        
        virtual rocksdb::Status GenerateCursorPosition(
                                const mongo::BSONObj& key,  //  Index key
                                KeyString::Discriminator discriminator,
                                std::string& recordKey
                                ) const override
        {
            SecondaryIndexBuilder keyBuilder;

            keyBuilder.Append(prefix);
            keyBuilder.Append(key, keyOrdering, discriminator);
            recordKey = keyBuilder.ToString();

            return rocksdb::Status::OK();
        }

        virtual rocksdb::Status ParseRecord(
                                    const std::pair<rocksdb::Slice, rocksdb::Slice>& indexKeyValuePair,
                                    mongo::BSONObj* key,    //  Optional key to return
                                    mongo::RecordId* documentRecordId   //  Optional RecordId to return
                                    ) const override
        {
            SecondaryKeyFormatter keyReader;
            RES_RIF(keyReader.Parse(indexKeyValuePair, prefix.size()));

            if(key)
                *key = keyReader.GetSecondaryKey(keyOrdering);

            if(documentRecordId)
                *documentRecordId = keyReader.GetRecordId();

            return rocksdb::Status::OK();
        }

        virtual const std::string& GetPrefix() const
        {
            return prefix;
        }
    
    };

    class ShardKeyInfo
    {
    protected:
        static bool DoesShardKeyContainIdField(const ShardKeyPattern& shardKeyPattern)
        {
            for(const mongo::BSONElement& element : shardKeyPattern.toBSON())
            {
                if(strcmp(element.fieldName(), "_id") == 0)
                    return true;
            }
            return false;
        }
        
    public:
        const ShardKeyPattern pattern;        
        const Ordering ordering;
        const bool containsIdField;

        ShardKeyInfo(const KeyPattern& keyPattern) :
            pattern(keyPattern),
            ordering(Ordering::make(pattern.toBSON())),
            containsIdField(DoesShardKeyContainIdField(pattern)) { }
        
        RecordId GenerateRecordId(const mongo::BSONObj& document) const
        {
            BSONObj shardKey = pattern.extractShardKeyFromDoc(document);

            if (shardKey.objsize() > MaxKeySize) {
                index_err() << "shardKey too long : " << shardKey << "; size: " << shardKey.objsize();
                return RecordId();
            }

            if(containsIdField)
                return PrimaryKeyBuilder::Generate(shardKey, ordering, nullptr);
            
            BSONElement idField(document.getField("_id"));
            return PrimaryKeyBuilder::Generate(shardKey,ordering, &idField);
        }

        BSONObj GetShardKeyBSON(const rocksdb::Slice& key, bool includeFieldNames) const
        {
            RecordIdFormatter formatter;
            invariantRocksOK(formatter.DecodeHeader(key));

            KeyString::TypeBits outTypeBits;
            invariantRocksOK(formatter.GetTypeBits(outTypeBits));

            rocksdb::Slice shardKey = formatter.GetShardKey();
            return KeyString::toBson(
                shardKey.data(), shardKey.size(), ordering, outTypeBits,
                includeFieldNames ? &pattern.getKeyPattern() : nullptr);
        }
    };

    rocksdb::Slice Adjust(const rocksdb::Slice& slice) const
    {
        return RemovePrefix(slice);
    }

    int CompareWithLowKey(const rocksdb::Slice& key) const
    {
        if(lowKey.empty())
            return 1;
        return key.compare(lowKey);
    }

    int CompareWithHighKey(const rocksdb::Slice& key) const
    {
        if(highKey.empty())
            return -1;

        return key.compare(highKeyNoEnd);
    }

    bool DoesShardKeyBelongToChunk(const rocksdb::Slice& key) const
    {
        bool belong = CompareWithLowKey(key) >= 0 && CompareWithHighKey(key) < 0;
        LOG(1) << "@@##@@ idxStorage DoesShardKeyBelongToChunk "
               << ", belong: " << belong          
               << ", key: "   << key.ToString() << ", key.size(): " << key.size()
               << ", lowKey: " << lowKey <<", l_size:" << lowKey.size()
               << ", highKey:" <<highKey <<", h_size:" << highKey.size()
               << ", _lowKey" << _lowKey <<",_highKey" <<_highKey
               << ", CompareWithLowKey(key):" << CompareWithLowKey(key)
               << ", CompareWithHighKey(key):"<< CompareWithHighKey(key);

        return belong;
    }

    virtual rocksdb::KeyRangeCheckResult DoesShardKeyRangeBelongToChunk(const rocksdb::Slice& rangeKeyLow, const rocksdb::Slice& rangeKeyHigh) const
    {
        LOG(1) << "inputRangeKeyLow: " << GetShardKeyBSON(rangeKeyLow) << "; inputRangeKeyHigh: " << GetShardKeyBSON(rangeKeyHigh)
              << "; CompareWithLowKey(rangeKeyHigh): " << CompareWithLowKey(rangeKeyHigh)
              << "; CompareWithHighKey(rangeKeyLow): " << CompareWithHighKey(rangeKeyLow)
              << "; CompareWithLowKey(rangeKeyLow): " << CompareWithLowKey(rangeKeyLow)
              << "; CompareWithHighKey(rangeKeyHigh): " << CompareWithHighKey(rangeKeyHigh)       
              << "; lowKey: " << _lowKey << "; highKey: " << _highKey;

        /*
         sst:  -------                         ---------

         chunk:         ------       -------  
         */
        if (CompareWithLowKey(rangeKeyHigh) < 0 || CompareWithHighKey(rangeKeyLow) >= 0)
            return rocksdb::KeyRangeCheckResult::RangeSeparated;

        /*
         sst:              -------   
         chunk:         -------------  
         */

        if (CompareWithLowKey(rangeKeyLow) >= 0 && CompareWithHighKey(rangeKeyHigh) <= 0)
            return rocksdb::KeyRangeCheckResult::RangeContained;
        
        return rocksdb::KeyRangeCheckResult::RangeOverlap;
    }    

public:

    ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage(const ChunkMetadata& metadata) :
        dataColumnFamily(*this), indexColumnFamily(*this)
    {
        columnFamilies.push_back(&dataColumnFamily);
        columnFamilies.push_back(&indexColumnFamily);

        shardKeyInfo = stdx::make_unique<ShardKeyInfo>(metadata.GetCollection().getKeyPattern());
        index_log() << "[assign] ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage keyPattern: "
            << metadata.GetCollection().getKeyPattern();

        SetKeyRange(metadata.GetKeyRange());
    }

    virtual rocksdb::Status Init(ChunkRocksRecordStore& rs)
    {
        return rocksdb::Status::OK();
    }

    virtual const std::vector<const IChunkColumnFamilyDescription*>& GetColumnFamilyDescriptions() const
    {
        return columnFamilies;
    }

    virtual rocksdb::Status GetRecordId(const mongo::BSONObj& document, mongo::RecordId& documentRecordId) const
    {        
        documentRecordId = shardKeyInfo->GenerateRecordId(document);
        return rocksdb::Status::OK();
    }

    virtual rocksdb::Status ParseRecordKey(const rocksdb::Slice& key, mongo::RecordId& documentRecordId) const
    {
        documentRecordId = RecordId(key.data(), key.size());
        return rocksdb::Status::OK();
    }

    virtual std::unique_ptr<IIndexRecordFormatter> GetSecondaryIndexFormatter(const IndexDefinition& index) const
    {
        return stdx::make_unique<IndexRecordFormatter>(*this, index);
    }

    virtual void SetKeyRange(const ChunkMetadata::KeyRange& range)
    {
        _lowKey = range.GetKeyLow();
        _highKey = range.GetKeyHigh();

        PrimaryKeyBuilder::GenerateRangeBorderKey(range, shardKeyInfo->ordering, lowKey, highKey, highKeyNoEnd);
        index_LOG(1)  << "SetKeyRange()-> low: " << range.GetKeyLow() << "; high: " << range.GetKeyHigh();
        index_LOG(1)  << "SetKeyRange()-> lowKey: " << lowKey << "; highKey: " << highKey;
    }
    std::string GetLowKey(void) override
    {
        //index_log() << "lowKey: " << lowKey << ";";
        return lowKey;
    }

    std::string GetHighKey(void) override
    {
        //index_log() << "highKeyNoEnd: " << highKeyNoEnd << ";";
        return highKeyNoEnd;
    }
    std::string GetMetadataRecordKey(const char* metadataKey) const override
    {
        std::string const_datasize_key("datasize");
        std::string input_key(metadataKey);

        std::string out_meta_key;
        if( input_key != const_datasize_key){
            out_meta_key = defaultMetadataKeyPrefix + metadataKey + lowKey;
        }else{
            out_meta_key = defaultMetadataKeyPrefix + metadataKey + "**" + lowKey;
        }
        return out_meta_key;
    }

    std::string GetRightMetadataRecordKey(const char* metadataKey) const override
    {
        std::string const_datasize_key("datasize");
        std::string input_key(metadataKey);

        std::string out_meta_key;
        if( input_key != const_datasize_key){
            out_meta_key = defaultMetadataKeyPrefix + metadataKey + highKeyNoEnd;
        }else{
            out_meta_key = defaultMetadataKeyPrefix + metadataKey + "**" + highKeyNoEnd;
        }
        return out_meta_key;
    }


    BSONObj GetShardKeyBSON(mongo::RecordId& documentRecordId) const
    {
        rocksdb::Slice primaryKey(documentRecordId.repr().Data(), documentRecordId.repr().Size());
        return GetShardKeyBSON(primaryKey);
    }
    BSONObj GetShardKeyBSON(rocksdb::Slice primaryKey) const
    {
        mongo::BSONObj keyBSON;
        invariantRocksOK(GetBSONFromPrimaryKey(primaryKey, true, true, keyBSON));
        return keyBSON;
    }

    virtual rocksdb::Status GetBSONFromPrimaryKey(
        const rocksdb::Slice& key,
        bool includeFieldNames,
        bool returnOnlyShardKey,
        mongo::BSONObj& keyBSON
        ) const override
    {
        invariant(returnOnlyShardKey);  //  Full key is not implemented yet. TODO: implement
        keyBSON = shardKeyInfo->GetShardKeyBSON(key, includeFieldNames);

        return rocksdb::Status::OK();
    }

    virtual uint8_t GetMetadataFamilyId() const
    {
        return 1;   //  Put into index column family
    }    

protected:
    DataColumnFamilyDescription dataColumnFamily;
    IndexColumnFamilyDescription indexColumnFamily;
    std::vector<const IChunkColumnFamilyDescription*> columnFamilies;

    std::unique_ptr<ShardKeyInfo> shardKeyInfo;

    //  Shard keys, that do not contain prefix
    std::string lowKey;
    std::string highKey;
    std::string highKeyNoEnd;
    //test
    BSONObj _lowKey;
    BSONObj _highKey;

    friend class ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorageTest;
};

}  //  mongo

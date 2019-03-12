
#pragma once

#include <rocksdb/compaction_filter.h>

namespace mongo
{

///////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Class describes RocksDB instance related to chunk and information associated with it.
//
class ChunkCompactionFilter : public rocksdb::CompactionFilter
{
    const IChunkColumnFamilyDescription& columnFamily;
    std::unique_ptr<rocksdb::CompactionFilter> additionalFilter;

public:

    ChunkCompactionFilter(
        const IChunkColumnFamilyDescription& chunkColumnFamily,
        std::unique_ptr<rocksdb::CompactionFilter> additionalFilterToCheck //  If specified, this filter will be checked after current
        ) : columnFamily(chunkColumnFamily), additionalFilter(std::move(additionalFilterToCheck))
    {
    }
    
    virtual bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& existing_value,
                    std::string* new_value, bool* value_changed) const
    {
        return !columnFamily.DoesRecordBelongToChunk(key, existing_value);
    }
    
    virtual Decision FilterV2(int level, const rocksdb::Slice& key, ValueType value_type,
                            const rocksdb::Slice& existing_value, std::string* new_value,
                            std::string* skip_until) const
    {

        //
        //  Check ourselves
        //
        Decision decision = rocksdb::CompactionFilter::FilterV2(
                level, key, value_type, existing_value, new_value, skip_until);
        
        if(decision == Decision::kRemove)
            return decision;

        //
        //  Check additional filter, if needed
        //
        if(additionalFilter)
        {
            decision = additionalFilter->FilterV2(
                level, key, value_type, existing_value, new_value, skip_until);
        }

        return decision;
    }

    virtual const char* Name() const
    {
        return "ChunkCompactionFilter";
    }
};



///////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Class provides filter for RocksDB instances.
//
class ChunkCompactionFilterFactory : public rocksdb::CompactionFilterFactory
{
    const IIndexedRecordStorage& storage;
    std::shared_ptr<rocksdb::CompactionFilterFactory> additionalFactory;
    

public:
    ChunkCompactionFilterFactory(
        const IIndexedRecordStorage& indexedStorage,
        std::shared_ptr<rocksdb::CompactionFilterFactory> additionalFactoryToCheck
        ) : storage(indexedStorage), additionalFactory(additionalFactoryToCheck)
    {}

    virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
        const rocksdb::CompactionFilter::Context& context) override
    {
        const std::vector<const IChunkColumnFamilyDescription*>& columnFamilies =
            storage.GetColumnFamilyDescriptions();

        invariant(columnFamilies.size() > context.column_family_id);

        return mongo::stdx::make_unique<ChunkCompactionFilter>(
            *columnFamilies[context.column_family_id],
            std::unique_ptr<rocksdb::CompactionFilter>(
                additionalFactory ? additionalFactory->CreateCompactionFilter(context) : nullptr
            )            
        );     
    }

    virtual const char* Name() const override
    {
        return "ChunkCompactionFilterFactory";
    }
};

};  //  mongo

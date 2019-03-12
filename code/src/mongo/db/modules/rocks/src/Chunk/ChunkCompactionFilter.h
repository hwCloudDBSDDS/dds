
#pragma once

#include <rocksdb/compaction_filter.h>
#include "IndexedRecordStorage.h"

namespace mongo {

    ///////////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Class describes RocksDB instance related to chunk and information associated with it.
    //
    class ChunkCompactionFilter : public rocksdb::CompactionFilter {
        const IChunkColumnFamilyDescription& columnFamily;
        std::unique_ptr<rocksdb::CompactionFilter> _filter;

    public:
        ChunkCompactionFilter(
            const IChunkColumnFamilyDescription& chunkColumnFamily,
            std::unique_ptr<rocksdb::CompactionFilter> filter)
            : columnFamily(chunkColumnFamily),
              _filter(std::move(filter)) {}

        /*
 
         */
        virtual bool Filter(int level, const rocksdb::Slice& key,
                            const rocksdb::Slice& existing_value, std::string* new_value,
                            bool* value_changed) const {
            /*
             step1: call dropped prefix filter.
             step2: call DoesRecordBelongToChunk;
            */
            if (_filter){
                if(_filter->Filter(level, key,existing_value, new_value, value_changed)){
                    return true;
                }
            }
            return false;
            //maybe can not call DoesRecordBelongToChunk; the key-value must belong this chunk here.
            //return !columnFamily.DoesRecordBelongToChunk(key, existing_value);
        }

        // virtual bool IgnoreSnapshots() const { return true; }

        virtual const char* Name() const { return "ChunkCompactionFilter"; }
    };


    ///////////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Class provides filter for RocksDB instances. add prefix dropped filter for index column.
    //
    class ChunkCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
        const IIndexedRecordStorage& storage;
        std::shared_ptr<rocksdb::CompactionFilterFactory> _filterFactory;
        //1. _filterFactory is PrefixDeletingCompactionFilterFactory from rocks_engin.cpp for system db.
        //2. _filterFactory is IdxPrefixDeletingCompactionFilterFactory from ChunkRocksDBInstance.cpp for user db.
        
    public:
        ChunkCompactionFilterFactory(
            const IIndexedRecordStorage& indexedStorage,
            std::shared_ptr<rocksdb::CompactionFilterFactory> filterFactory)
            : storage(indexedStorage), _filterFactory(filterFactory) {}

        virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
            const rocksdb::CompactionFilter::Context& context) override {
            const std::vector<const IChunkColumnFamilyDescription*>& columnFamilies =
                                                                     storage.GetColumnFamilyDescriptions();
            invariant(columnFamilies.size() > context.column_family_id);

            return mongo::stdx::make_unique<ChunkCompactionFilter>(
                *columnFamilies[context.column_family_id],
                std::unique_ptr<rocksdb::CompactionFilter>(
                    _filterFactory ? _filterFactory->CreateCompactionFilter(context) : nullptr));
        }

        virtual const char* Name() const override { return "ChunkCompactionFilterFactory"; }
    };

};  //  mongo

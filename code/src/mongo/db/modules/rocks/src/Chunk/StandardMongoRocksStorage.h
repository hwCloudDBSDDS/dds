
#pragma once

#include <mongo/stdx/memory.h>
#include <rocksdb/db.h>
#include "../rocks_record_store.h"
#include "ChunkRocksRecordStore.h"
#include "IndexedRecordStorage.h"

namespace mongo {

    //
    //  Storage that store data the same way as original RocksRecordStore
    //
    class StandardMongoRocksStorage : public IIndexedRecordStorage {
        class ChunkColumnFamilyDescription : public IChunkColumnFamilyDescription {
        public:
            virtual const char* GetName() const override {
                return rocksdb::kDefaultColumnFamilyName.c_str();
            }

            //
            //  TODO: support splitting
            //
        };

        ChunkRocksRecordStore* recordStore = nullptr;
        ChunkColumnFamilyDescription mainColumnFamily;
        std::vector<const IChunkColumnFamilyDescription*> columnFamilies;

    public:
        StandardMongoRocksStorage() { columnFamilies.push_back(&mainColumnFamily); }

        virtual rocksdb::Status Init(ChunkRocksRecordStore& rs) {
            recordStore = &rs;

            //  Need to load nextid since it was ignored in ChunkRocksRecordStore
            recordStore->mongo::RocksRecordStore::loadNextIdNum();
            return rocksdb::Status::OK();
        }

        virtual const std::vector<const IChunkColumnFamilyDescription*>&
        GetColumnFamilyDescriptions() const {
            return columnFamilies;
        }

        virtual rocksdb::Status GetRecordId(const mongo::BSONObj& document,
                                            mongo::RecordId& documentRecordId) const {
            documentRecordId = recordStore->_nextId();  //  Just auto-increment
            return rocksdb::Status::OK();
        }

        virtual rocksdb::Status ParseRecordKey(const rocksdb::Slice& key,
                                               mongo::RecordId& documentRecordId) const {
            documentRecordId = recordStore->mongo::RocksRecordStore::_makeRecordId(key);
            return rocksdb::Status::OK();
        }

        virtual std::unique_ptr<IIndexRecordFormatter> GetSecondaryIndexFormatter(
            const IndexDefinition& index) const {
            return nullptr;
        }

        virtual rocksdb::Status GetBSONFromPrimaryKey(const rocksdb::Slice& key,
                                                      bool includeFieldNames,
                                                      bool returnOnlyShardKey,
                                                      mongo::BSONObj& keyBSON) const override {
            invariant(false);
            RES_RET(rocksdb::Status::NotSupported())
        }

        virtual void SetKeyRange(const ChunkMetadata::KeyRange& range) {
            invariant(false);
            //  TODO: Implement with split
        }

        virtual std::string GetLowKey(void) { return std::string(); }

        virtual std::string GetHighKey(void) { return std::string(); }
    };

};  //  mongo

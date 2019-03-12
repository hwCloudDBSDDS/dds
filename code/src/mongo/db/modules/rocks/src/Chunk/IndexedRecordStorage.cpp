
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "IndexedRecordStorage.h"
#include <mongo/stdx/memory.h>
#include <mongo/util/log.h>
#include <rocksdb/db.h>
#include "../rocks_record_store.h"
#include "ChunkRocksRecordStore.h"
#include "ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage.h"
#include "StandardMongoRocksStorage.h"

namespace mongo {

    const std::string IIndexedRecordStorage::defaultMetadataKeyPrefix("\0\0\0\0", 4);

    rocksdb::Status IIndexedRecordStorage::Create(const ChunkMetadata& metadata,
                                                  std::unique_ptr<IIndexedRecordStorage>& storage) {
        mongo::log() << "IIndexedRecordStorage::Create ";
        switch (metadata.GetIndexedRecordStorageId()) {
            case IndexedRecordStorageId::MongoRocksStandard: {
                storage = stdx::make_unique<StandardMongoRocksStorage>();
                return rocksdb::Status::OK();
            }

            case IndexedRecordStorageId::ShardKeyPrefixedDataWithStandardIndexColumnFamily: {
                mongo::log() << "IIndexedRecordStorage::Create "
                                "ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage";
                storage =
                    stdx::make_unique<ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage>(
                        metadata);
                return rocksdb::Status::OK();
            }

            default:
                RDB_RET(rocksdb::Status::NotSupported());
        }
    }

}  //  mongo

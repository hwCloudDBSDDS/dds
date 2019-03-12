#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/s/catalog/catalog_extend/sharding_catalog_client_extend.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/client/shard.h"
#include "mongo/util/log.h"
#include <string>

namespace mongo {

ShardingCatalogClientExtend::ShardingCatalogClientExtend(
    std::unique_ptr<DistLockManager> distLockManager)
    : ShardingCatalogClientImpl(std::move(distLockManager)) {}

ShardingCatalogClientExtend::~ShardingCatalogClientExtend() = default;

Status ShardingCatalogClientExtend::insertConfigDocument(OperationContext* txn,
                                                         const std::string& ns,
                                                         const BSONObj& doc,
                                                         const WriteConcernOptions& writeConcern) {
    if (ChunkType::ConfigNS != ns) {
        return ShardingCatalogClientImpl::insertConfigDocument(txn, ns, doc, writeConcern);
    }

    BSONObj newDoc;
    Shard::fixupNewDoc(doc, newDoc, false);
    return ShardingCatalogClientImpl::insertConfigDocument(txn, ns, newDoc, writeConcern);
}

StatusWith<bool> ShardingCatalogClientExtend::updateConfigDocument(
    OperationContext* txn,
    const std::string& ns,
    const BSONObj& query,
    const BSONObj& update,
    bool upsert,
    const WriteConcernOptions& writeConcern) {
    if (ChunkType::ConfigNS != ns) {
        return ShardingCatalogClientImpl::updateConfigDocument(
            txn, ns, query, update, upsert, writeConcern);
    }

    BSONObj newUpdate;
    Shard::fixupNewDoc(update, newUpdate, false);

    BSONObj newQuery;
    Shard::fixupNewDoc(query, newQuery, false);
    return ShardingCatalogClientImpl::updateConfigDocument(
        txn, ns, newQuery, newUpdate, upsert, writeConcern);
}

Status ShardingCatalogClientExtend::updateConfigDocuments(OperationContext* txn,
                                                          const std::string& ns,
                                                          const BSONObj& query,
                                                          const BSONObj& update,
                                                          bool upsert,
                                                          const WriteConcernOptions& writeConcern) {
    if (ChunkType::ConfigNS != ns) {
        return ShardingCatalogClientImpl::updateConfigDocuments(
            txn, ns, query, update, upsert, writeConcern);
    }

    BSONObj newUpdate;
    Shard::fixupNewDoc(update, newUpdate, false);

    BSONObj newQuery;
    Shard::fixupNewDoc(query, newQuery, false);
    return ShardingCatalogClientImpl::updateConfigDocuments(
        txn, ns, newQuery, newUpdate, upsert, writeConcern);
}

Status ShardingCatalogClientExtend::removeConfigDocuments(OperationContext* txn,
                                                          const std::string& ns,
                                                          const BSONObj& query,
                                                          const WriteConcernOptions& writeConcern) {
    if (ChunkType::ConfigNS != ns) {
        return ShardingCatalogClientImpl::removeConfigDocuments(txn, ns, query, writeConcern);
    }

    BSONObj newQuery;
    Shard::fixupNewDoc(query, newQuery, false);
    return ShardingCatalogClientImpl::removeConfigDocuments(txn, ns, newQuery, writeConcern);
}
}
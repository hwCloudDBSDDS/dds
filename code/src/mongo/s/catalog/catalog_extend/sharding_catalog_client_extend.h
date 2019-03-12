#include "mongo/s/catalog/sharding_catalog_client_impl.h"

namespace mongo {

class ShardingCatalogClientExtend final : public ShardingCatalogClientImpl {
public:
    explicit ShardingCatalogClientExtend(std::unique_ptr<DistLockManager> distLockManager);
    virtual ~ShardingCatalogClientExtend();

    Status insertConfigDocument(OperationContext* txn,
                                const std::string& ns,
                                const BSONObj& doc,
                                const WriteConcernOptions& writeConcern) override;

    StatusWith<bool> updateConfigDocument(OperationContext* txn,
                                          const std::string& ns,
                                          const BSONObj& query,
                                          const BSONObj& update,
                                          bool upsert,
                                          const WriteConcernOptions& writeConcern) override;

    Status updateConfigDocuments(OperationContext* txn,
                                 const std::string& ns,
                                 const BSONObj& query,
                                 const BSONObj& update,
                                 bool upsert,
                                 const WriteConcernOptions& writeConcern) override;

    Status removeConfigDocuments(OperationContext* txn,
                                 const std::string& ns,
                                 const BSONObj& query,
                                 const WriteConcernOptions& writeConcern) override;
};
}
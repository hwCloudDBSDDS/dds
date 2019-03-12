#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include <string>

#include "mongo/db/commands.h"
#include "mongo/s/catalog/sharding_catalog_manager.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"


namespace mongo {

const ReadPreferenceSetting kPrimaryOnlyReadPreference{ReadPreference::PrimaryOnly};

using std::string;


namespace {

class CmdGetAutoShard : public Command {

public:
    CmdGetAutoShard() : Command("getAutoShard", false, "getAutoShard") {}

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }
    virtual bool slaveOk() const {
        return false;
    }

    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
        ActionSet actions;
        actions.addAction(ActionType::getAutoShard);
        out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
    }

    virtual bool run(OperationContext* txn,
                     const string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     string& errmsg,
                     BSONObjBuilder& result) {
        auto status =
            Grid::get(txn)->catalogManager()->getShardServerManager()->getPrimaryShard(txn);
        if (!status.isOK()) {
            return appendCommandStatus(result, status.getStatus());
        }
        ShardType shardType = status.getValue();
        std::string shard = shardType.getName();
        result.append("auto",shard);
        return true;
    }
} CmdGetAutoShard;
}  // namespace
}  // namespace mongo

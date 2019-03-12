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

class CmdRemoveShardMap : public Command {

public:
    CmdRemoveShardMap() : Command("removeShardMap", false, "removeShardMap") {}

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
        actions.addAction(ActionType::removeShardMap);
        out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
    }

    virtual bool run(OperationContext* txn,
                     const string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     string& errmsg,
                     BSONObjBuilder& result) {
        const string shardName = cmdObj.getStringField("shardName");
        auto status =
            Grid::get(txn)->catalogManager()->getShardServerManager()->removeShardTypeByShardName(shardName);
        if (!status.isOK()) {
            return appendCommandStatus(result, status);
        }
        return true;
    }
} CmdRemoveShardMap;
}  // namespace
}  // namespace mongo

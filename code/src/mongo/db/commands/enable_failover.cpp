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

class CmdEnableFailover : public Command {

public:
    CmdEnableFailover() : Command("enableFailover", false, "enableFailover") {}

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
        actions.addAction(ActionType::enableFailover);
        out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
    }

    virtual bool run(OperationContext* txn,
                     const string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     string& errmsg,
                     BSONObjBuilder& result) {
        auto status =
            Grid::get(txn)->catalogManager()->getShardServerManager()->enableFailOver(txn, true);
        if (!status.isOK()) {
            return appendCommandStatus(result, status);
        }
        return true;
    }
} CmdEnableFailover;
}  // namespace
}  // namespace mongo

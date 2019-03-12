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

class CmdEndSnapshot : public Command {

public:
    CmdEndSnapshot() : Command("endSnapshot", false, "endSnapshot") {}

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }
    virtual bool slaveOk() const {
        return true;
    }

    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
        ActionSet actions;
        actions.addAction(ActionType::endSnapshot);
        out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
    }

    virtual bool run(OperationContext* txn,
                     const string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     string& errmsg,
                     BSONObjBuilder& result) {
        auto g_ctx = getGlobalServiceContext();
        if (g_ctx == nullptr) {
            index_err() << " getGlobalServiceContext failed.";
            return appendCommandStatus(
                result,
                {ErrorCodes::InternalError, str::stream() << "getGlobalServiceContext failed."});
        }

        StorageEngine* storageEngine = g_ctx->getGlobalStorageEngine();
        if (storageEngine == nullptr) {
            index_err() << "getGlobalStorageEngine failed.";
            return appendCommandStatus(
                result,
                {ErrorCodes::InternalError, str::stream() << "getGlobalStorageEngine failed."});
        }

        auto status = storageEngine->endSnapshot();
        if (!status.isOK()) {
            return appendCommandStatus(result, status);
        }
        return true;
    }
} CmdEndSnapshot;
}  // namespace
}  // namespace mongo

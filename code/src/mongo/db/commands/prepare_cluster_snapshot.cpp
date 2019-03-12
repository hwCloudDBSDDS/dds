#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include <string>

#include "mongo/db/commands.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/s/catalog/sharding_catalog_manager.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"
#include "mongo/s/client/shard_registry.h"

namespace mongo {

using std::string;
using std::vector;

namespace {

class CmdPrepareClusterSnapshot : public Command {

public:
    CmdPrepareClusterSnapshot() : Command("prepareClusterSnapshot", false, "prepareClusterSnapshot") {}

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }
    virtual bool slaveOk() const {
        return true;
    }

    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
        ActionSet actions;
        actions.addAction(ActionType::prepareClusterSnapshot);
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
            index_err() << "getGlobalServiceContext failed.";
            return appendCommandStatus(
                result,
                {ErrorCodes::InternalError, str::stream() << "getGlobalServiceContext failed."});
        }

        BSONElement elementRepairDBWAL = cmdObj["repairDBWAL"];
        BSONObj cmdBson = elementRepairDBWAL.eoo() ? BSON("prepareSnapshot" << 1) : BSON("prepareSnapshot" << 1 << "repairDBWAL" << 1);

        //prepare snapshot on config
        auto configShard = Grid::get(txn)->shardRegistry()->getConfigShard();
        auto response = configShard->runCommand(
            txn,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
           "admin",
            cmdBson,
            Shard::RetryPolicy::kIdempotent);

        if (!response.isOK())
        {
            return 0;
        }
        
        BSONObjBuilder temp(result.subobjStart("walPaths"));
        BSONElement elementWalPaths = response.getValue().response["walPaths"];
        if ( !elementWalPaths.eoo() )
            temp.appendElements(elementWalPaths.Obj());

        bool needRollBack = false;

        vector<shared_ptr<Shard>> shardRollBackArray;

        shardRollBackArray.push_back(configShard);

        //prepare snapshot on shards
        std::vector<ShardId> shardIds;
        grid.shardRegistry()->getAllShardIds(&shardIds);
        for (const ShardId& shardId : shardIds) {
            const auto shardStatus = grid.shardRegistry()->getShard(txn, shardId);
            if (!shardStatus.isOK()) {
                continue;
            }
            const auto shard = shardStatus.getValue();
          
            response = shard->runCommand(
                txn,
                ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                "admin",
                cmdBson,
                Shard::RetryPolicy::kIdempotent);

            if (!response.isOK()) {
                needRollBack = true;
                break;
            }
            elementWalPaths = response.getValue().response["walPaths"];
            if ( !elementWalPaths.eoo() )
                temp.appendElements(elementWalPaths.Obj());
            shardRollBackArray.push_back(shard);

        }

        if (needRollBack) {
            for (auto shard : shardRollBackArray) {
                shard->runCommand(
                    txn,
                    ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                    "admin",
                    BSON("endSnapshot" << 1),
                    Shard::RetryPolicy::kIdempotent);
            }

            return 0;
        }

        return 1;
    }
} CmdPrepareClusterSnapshot;
}  // namespace
}  // namespace mongo

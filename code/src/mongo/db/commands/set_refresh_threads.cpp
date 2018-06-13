
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/s/sharding_state.h"

namespace mongo {

using std::string;
using std::stringstream;

class CmdSetRefreshThreads : public Command {
public:
    CmdSetRefreshThreads() : Command("setRefreshThreads", true) {}

    virtual bool slaveOk() const {
        return false;
    }
    virtual bool slaveOverrideOk() const {
        return true;
    }
    virtual bool adminOnly() const {
        return true;
    }
    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }
    virtual void help(stringstream& help) const {
        help << "set kMaxConfigServerRefreshThreads for this shard server";
    }

    bool run(OperationContext* txn,
             const string& dbname,
             BSONObj& cmdObj,
             int,
             string& errmsg,
             BSONObjBuilder& result) {
        if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
            errmsg = "setRefreshThreads is not allowed on configserver";
            return false;
        }

        const auto sizeElem = cmdObj.firstElement();
        int newSize = sizeElem.Int();

        Status setStatus = ShardingState::get(txn)->resizeConfigServerTickets(newSize);
        if (!setStatus.isOK()) {
            return appendCommandStatus(result, setStatus);
        }

        return true;
    }
} cmdSetRefreshThreads;
}


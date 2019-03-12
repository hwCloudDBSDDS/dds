
#include "mongo/platform/basic.h"

#include "mongo/db/audit.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/s/balancer_configuration.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/client/shard_connection.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/config.h"
#include "mongo/s/config_server_client.h"
#include "mongo/s/grid.h"
#include "mongo/s/sharding_raii.h"

namespace mongo {

using std::shared_ptr;
using std::string;

namespace {

class CmdAssignChunk : public Command {
public:
    CmdAssignChunk() : Command("assignChunk", false, "assignchunk") {}

    virtual bool slaveOk() const {
        return true;
    }

    virtual bool adminOnly() const {
        return true;
    }

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    virtual void help(std::stringstream& help) const {
        help << "assign a chunk specified by find\n";
    }

    virtual Status checkAuthForCommand(Client* client,
                                       const std::string& dbname,
                                       const BSONObj& cmdObj) {
        if (!AuthorizationSession::get(client)->isAuthorizedForActionsOnResource(
                ResourcePattern::forExactNamespace(NamespaceString(parseNs(dbname, cmdObj))),
                ActionType::assignChunk)) {
            return Status(ErrorCodes::Unauthorized, "Unauthorized");
        }

        return Status::OK();
    }

    virtual std::string parseNs(const std::string& dbname, const BSONObj& cmdObj) const {
        return parseNsFullyQualified(dbname, cmdObj);
    }

    virtual bool run(OperationContext* txn,
                     const std::string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
        const NamespaceString nss(parseNs(dbname, cmdObj));

        std::shared_ptr<DBConfig> config;
        {
            auto status = grid.catalogCache()->getDatabase(txn, nss.db().toString());
            if (!status.isOK()) {
                return appendCommandStatus(result, status.getStatus());
            }

            config = status.getValue();
        }
        if (!config->isSharded(nss.ns())) {
            config->reload(txn);

            if (!config->isSharded(nss.ns())) {
                return appendCommandStatus(result,
                                           Status(ErrorCodes::NamespaceNotSharded,
                                                  "ns [" + nss.ns() + " is not sharded."));
            }
        }

        const string toString = cmdObj["to"].valuestrsafe();
        if (!toString.size()) {
            errmsg = "you have to specify where you want to assign the chunk";
            return false;
        }

        const auto toStatus = grid.shardRegistry()->getShard(txn, toString);
        if (!toStatus.isOK()) {
            string msg(str::stream() << "Could not assign chunk in '" << nss.ns() << "' to shard '"
                                     << toString
                                     << "' because that shard does not exist");
            return appendCommandStatus(result, Status(ErrorCodes::ShardNotFound, msg));
        }
        const auto to = toStatus.getValue();

        auto chunkManagerStatus = ScopedChunkManager::getExisting(txn, nss);
        if (!chunkManagerStatus.isOK()) {
            return appendCommandStatus(result, chunkManagerStatus.getStatus());
        }
        ChunkManager* const info = chunkManagerStatus.getValue().cm();
        shared_ptr<Chunk> chunk;

        BSONObj find = cmdObj.getObjectField("find");
        if (!find.isEmpty()) {
            StatusWith<BSONObj> status =
                info->getShardKeyPattern().extractShardKeyFromQuery(txn, find);
            if (!status.isOK())
                return appendCommandStatus(result, status.getStatus());

            BSONObj shardKey = status.getValue();
            if (shardKey.isEmpty()) {
                errmsg = str::stream() << "no shard key found in chunk query " << find;
                return false;
            }

            chunk = info->findIntersectingChunkWithSimpleCollation(txn, shardKey);
        } else {
            const string idString = cmdObj["chunkid"].valuestrsafe();
            if (!idString.size()) {
                errmsg = "you have to specify shardkey or chunkid";
                return false;
            }

            chunk = info->findChunkByChunkId(idString);
            if (!chunk) {
                errmsg = "chunkid not found";
                return false;
            }
        }

        ChunkType chunkType;
        chunkType.setNS(nss.ns());
        chunk->constructChunkType(&chunkType);

        auto assignStatus = configsvr_client::assignChunk(txn, chunkType, to->getId());
        if (!assignStatus.isOK()) {
            return appendCommandStatus(result, assignStatus);
        }

        info->reload(txn);

        return true;
    }
} cmdAssignChunk;

}  // namespace
}  // namespace mongo

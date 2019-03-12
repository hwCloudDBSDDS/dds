/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand
#include "mongo/platform/basic.h"

#include <string>
#include <vector>

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/find_and_modify.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/s/balancer_configuration.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/chunk_manager.h"
#include "mongo/s/client/shard_connection.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/commands/checkView.h"
#include "mongo/s/commands/cluster_explain.h"
#include "mongo/s/commands/sharded_command_processing.h"
#include "mongo/s/commands/strategy.h"
#include "mongo/s/config.h"
#include "mongo/s/grid.h"
#include "mongo/s/mongos_options.h"
#include "mongo/s/sharding_raii.h"
#include "mongo/s/stale_exception.h"
#include "mongo/util/log.h"
#include "mongo/util/timer.h"
namespace mongo {
namespace {

using std::shared_ptr;
using std::string;
using std::vector;

static const int kMaxRetryRounds(5);

class FindAndModifyCmd : public Command {
public:
    FindAndModifyCmd() : Command("findAndModify", false, "findandmodify") {}

    virtual bool slaveOk() const {
        return true;
    }

    virtual bool adminOnly() const {
        return false;
    }


    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
        find_and_modify::addPrivilegesRequiredForFindAndModify(this, dbname, cmdObj, out);
    }

    virtual Status explain(OperationContext* txn,
                           const std::string& dbName,
                           const BSONObj& cmdObj,
                           ExplainCommon::Verbosity verbosity,
                           const rpc::ServerSelectionMetadata& serverSelectionMetadata,
                           BSONObjBuilder* out) const {
        const NamespaceString nss = parseNsCollectionRequired(dbName, cmdObj);
        auto scopedDB = uassertStatusOK(ScopedShardDatabase::getExisting(txn, dbName));
        DBConfig* conf = scopedDB.db();

        shared_ptr<ChunkManager> chunkMgr;
        shared_ptr<Shard> shard;
        shared_ptr<Chunk> ck = nullptr;

        bool isExist = false;
        auto res = conf->isCollectionExist(txn, nss.ns());
        if (res.isOK()) {
            isExist = res.getValue();
        } else {
            return res.getStatus();
        }

        if (!conf->isShardingEnabled() || !isExist) {
            auto shardStatus = Grid::get(txn)->shardRegistry()->getShard(txn, conf->getPrimaryId());
            if (!shardStatus.isOK()) {
                return shardStatus.getStatus();
            }
            shard = shardStatus.getValue();
        } else {
            chunkMgr = _getChunkManager(txn, conf, nss);

            const BSONObj query = cmdObj.getObjectField("query");

            BSONObj collation;
            BSONElement collationElement;
            auto collationElementStatus =
                bsonExtractTypedField(cmdObj, "collation", BSONType::Object, &collationElement);
            if (collationElementStatus.isOK()) {
                collation = collationElement.Obj();
            } else if (collationElementStatus != ErrorCodes::NoSuchKey) {
                return collationElementStatus;
            }

            StatusWith<BSONObj> status = _getShardKey(txn, chunkMgr, query);
            if (!status.isOK()) {
                if ((status.getStatus().code() == ErrorCodes::ShardKeyNotFound) &&
                    CollectionType::TableType::kNonShard == conf->getCollTabType(nss.ns())) {
                    auto shardStatus =
                        Grid::get(txn)->shardRegistry()->getShard(txn, conf->getPrimaryId());
                    if (!shardStatus.isOK()) {
                        return shardStatus.getStatus();
                    }
                    shard = shardStatus.getValue();
                } else {
                    return status.getStatus();
                }
            } else {
                BSONObj shardKey = status.getValue();
                auto chunk = chunkMgr->findIntersectingChunk(txn, shardKey, collation);

                if (!chunk.isOK()) {
                    uasserted(ErrorCodes::ShardKeyNotFound,
                              "findAndModify must target a single shard, but was not able to due "
                              "to non-simple collation");
                }
                ck = chunk.getValue();
                auto shardStatus = Grid::get(txn)->shardRegistry()->getShard(txn, ck->getShardId());
                if (!shardStatus.isOK()) {
                    return shardStatus.getStatus();
                }
                shard = shardStatus.getValue();
            }
        }

        BSONObjBuilder explainCmd;
        int options = 0;
        ClusterExplain::wrapAsExplain(
            cmdObj, verbosity, serverSelectionMetadata, &explainCmd, &options);

        // Time how long it takes to run the explain command on the shard.
        Timer timer;
        if (ck) {
            index_LOG(0) << "ck not null";
            explainCmd.append("chunkId", ck->getChunkId().toString());
        }
        BSONObj cmd = explainCmd.obj();
        BSONObjBuilder result;
        auto runStatus = _runCommand(txn, conf, chunkMgr, shard->getId(), nss, cmd, result);
        if (!runStatus.isOK()) {
            return runStatus.getStatus();
        }
        long long millisElapsed = timer.millis();

        if (!runStatus.getValue()) {
            BSONObj res = result.obj();
            return Status(ErrorCodes::OperationFailed,
                          str::stream() << "Explain for findAndModify failed: " << res);
        }

        Strategy::CommandResult cmdResult;
        cmdResult.shardTargetId = shard->getId();
        cmdResult.target = shard->getConnString();
        cmdResult.result = result.obj();

        vector<Strategy::CommandResult> shardResults;
        shardResults.push_back(cmdResult);

        return ClusterExplain::buildExplainResult(
            txn, shardResults, ClusterExplain::kSingleShard, millisElapsed, out);
    }
    static bool CreateCollIfNotExist(OperationContext* txn,
                                     const std::string& dbName,
                                     const NamespaceString& ns) {
        auto config = uassertStatusOK(grid.catalogCache()->getDatabase(txn, dbName));

        bool isExist = false;
        auto res = config->isCollectionExist(txn, dbName + "." + ns.coll());
        if (res.isOK()) {
            isExist = res.getValue();
        } else {
            return false;
        }

        if (!isExist) {
            Command* createCmd = Command::findCommand("create");
            int queryOptions = 0;
            string errmsg;
            BSONObjBuilder cmdBob;
            cmdBob.append("create", ns.coll());
            auto createCmdObj = cmdBob.done();
            bool ok = false;
            try {
                ok = createCmd->run(txn, dbName, createCmdObj, queryOptions, errmsg, cmdBob);
            } catch (const DBException& e) {
                index_LOG(1) << "run create cmd error";
                return false;
            }
            return ok;
        }
        return true;
    }
    // end
    virtual bool run(OperationContext* txn,
                     const std::string& dbName,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
        const NamespaceString nss = parseNsCollectionRequired(dbName, cmdObj);
        // findAndModify should only be creating database if upsert is true, but this would require
        // that the parsing be pulled into this function.
        auto scopedDb = uassertStatusOK(ScopedShardDatabase::getOrCreate(txn, dbName));
        DBConfig* conf = scopedDb.db();

        auto isviewStatus = isView(txn, dbName, nss);

        if (!isviewStatus.isOK()) {
            return appendCommandStatus(result, isviewStatus.getStatus());
        }
        bool isExist = uassertStatusOK(isviewStatus);

        if (isExist) {
            return appendCommandStatus(result,
                                       Status(ErrorCodes::CommandNotSupportedOnView,
                                              "findAndModify not supported on a view"));
        }

        if (!CreateCollIfNotExist(txn, dbName, nss)) {
            return false;
        }

        if (nss.isOnInternalDb() ||
            CollectionType::TableType::kNonShard == conf->getCollTabType(nss.ns())) {
            auto runStatus =
                _runCommand(txn, conf, nullptr, conf->getPrimaryId(), nss, cmdObj, result);
            if (!runStatus.isOK()) {
                return appendCommandStatus(result, runStatus.getStatus());
            }
            return runStatus.getValue();
        }

        if (!conf->isShardedAfterReload(txn, nss.ns())) {
            return appendCommandStatus(
                result,
                Status(ErrorCodes::NamespaceNotSharded,
                       str::stream() << "ns " << nss.ns() << " is not sharded yet"));
        }

        shared_ptr<ChunkManager> chunkMgr = _getChunkManager(txn, conf, nss);

        const BSONObj query = cmdObj.getObjectField("query");

        BSONObj collation;
        BSONElement collationElement;
        auto collationElementStatus =
            bsonExtractTypedField(cmdObj, "collation", BSONType::Object, &collationElement);
        if (collationElementStatus.isOK()) {
            collation = collationElement.Obj();
        } else if (collationElementStatus != ErrorCodes::NoSuchKey) {
            return appendCommandStatus(result, collationElementStatus);
        }

        StatusWith<BSONObj> status = _getShardKey(txn, chunkMgr, query);
        if (!status.isOK()) {
            // Bad query
            return appendCommandStatus(result, status.getStatus());
        }

        BSONObj shardKey = status.getValue();
        auto chunk = chunkMgr->findIntersectingChunk(txn, shardKey, collation);

        if (!chunk.isOK()) {
            uasserted(ErrorCodes::ShardKeyNotFound,
                      "findAndModify must target a single shard, but was not able to due to "
                      "non-simple collation");
        }

        // we need to add chunkver and chunkid info, so re-build the command
        BSONObjBuilder cmdBuilder;
        cmdBuilder.appendElements(cmdObj);

        ChunkVersion version(chunk.getValue()->getLastmod());
        version.appendForCommands(&cmdBuilder);
        ChunkId chunkId(chunk.getValue()->getChunkId());
        chunkId.appendForCommands(&cmdBuilder);

        auto runStatus = _runCommand(
            txn, conf, chunkMgr, chunk.getValue()->getShardId(), nss, cmdBuilder.obj(), result);
        if (runStatus.isOK()) {
            return runStatus.getValue();
        }

        if (runStatus == ErrorCodes::ShardNotFound) {
            chunkMgr->reload(txn);
        }

        return appendCommandStatus(result, runStatus.getStatus());
    }

private:
    shared_ptr<ChunkManager> _getChunkManager(OperationContext* txn,
                                              DBConfig* conf,
                                              const NamespaceString& nss) const {
        shared_ptr<ChunkManager> chunkMgr = conf->getChunkManager(txn, nss.ns(), true);
        massert(13002, "shard internal error chunk manager should never be null", chunkMgr);

        return chunkMgr;
    }

    StatusWith<BSONObj> _getShardKey(OperationContext* txn,
                                     shared_ptr<ChunkManager> chunkMgr,
                                     const BSONObj& query) const {
        // Verify that the query has an equality predicate using the shard key
        StatusWith<BSONObj> status =
            chunkMgr->getShardKeyPattern().extractShardKeyFromQuery(txn, query);

        if (!status.isOK()) {
            return status;
        }

        BSONObj shardKey = status.getValue();

        if (shardKey.isEmpty()) {
            return Status(ErrorCodes::ShardKeyNotFound,
                          "query for sharded findAndModify must have shardkey");
        }

        return shardKey;
    }

    StatusWith<bool> _runCommand(OperationContext* txn,
                                 DBConfig* conf,
                                 shared_ptr<ChunkManager> chunkManager,
                                 const ShardId& shardId,
                                 const NamespaceString& nss,
                                 const BSONObj& cmdObj,
                                 BSONObjBuilder& result) const {
        BSONObj res;

        auto shardStatus = Grid::get(txn)->shardRegistry()->getShard(txn, shardId);
        if (!shardStatus.isOK()) {
            return shardStatus.getStatus();
        }
        std::vector<ChunkType> Chunks;
        uassertStatusOK(
            grid.catalogClient(txn)->getChunks(txn,
                                               BSON(ChunkType::ns(nss.ns())),
                                               BSONObj(),
                                               boost::none,
                                               &Chunks,
                                               nullptr,
                                               repl::ReadConcernLevel::kMajorityReadConcern));

        BSONObjBuilder newCmd;
        if (Chunks.size() == 0) {
            newCmd.appendElements(cmdObj);
            LOG(1) << "ns not found on mongos";
        } else {
            newCmd.appendElements(cmdObj);
            newCmd.append("chunkId", Chunks[0].getName());
        }
        ShardConnection conn(shardStatus.getValue()->getConnString(), nss.ns(), chunkManager);
        bool ok = conn->runCommand(conf->name(), newCmd.obj(), res);
        conn.done();

        if (!ok) {
            int errCode = res.getIntField("code");
            if (errCode == ErrorCodes::RecvStaleConfig)
                throw RecvStaleConfigException("FindAndModify", res);

            if (errCode == ErrorCodes::FailedToSatisfyReadPreference ||
                ErrorCodes::isNetworkError(static_cast<ErrorCodes::Error>(errCode))) {
                return {ErrorCodes::ShardNotFound,
                        str::stream() << "shard cannt be accessed " << errCode};
            }
        }

        // First append the properly constructed writeConcernError. It will then be skipped
        // in appendElementsUnique.
        if (auto wcErrorElem = res["writeConcernError"]) {
            appendWriteConcernErrorToCmdResponse(shardId, wcErrorElem, result);
        }

        result.appendElementsUnique(res);
        return ok;
    }

} findAndModifyCmd;

}  // namespace
}  // namespace mongo

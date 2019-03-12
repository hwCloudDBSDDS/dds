/**
 *    Copyright (C) 2015 MongoDB Inc.
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


#include "mongo/base/status.h"
#include "mongo/db/commands.h"
#include "mongo/db/operation_context.h"
#include "mongo/s/catalog/dist_lock_manager.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/commands/cluster_commands_common.h"
#include "mongo/s/config.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"

namespace mongo {

using std::shared_ptr;

namespace {

class DropDatabaseCmd : public Command {
public:
    DropDatabaseCmd() : Command("dropDatabase") {}

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
        ActionSet actions;
        actions.addAction(ActionType::dropDatabase);
        out->push_back(Privilege(ResourcePattern::forDatabaseName(dbname), actions));
    }

    virtual bool run(OperationContext* txn,
                     const std::string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
        // Disallow dropping the config database from mongos
        if (dbname == "config") {
            return appendCommandStatus(
                result, Status(ErrorCodes::IllegalOperation, "Cannot drop the config database"));
        }

        BSONElement e = cmdObj.firstElement();

        if (!e.isNumber() || e.number() != 1) {
            errmsg = "invalid params";
            return 0;
        }
       
        auto const catalogClient = Grid::get(txn)->catalogClient(txn);

        auto scopedDistLock = Grid::get(txn)->catalogClient(txn)->getDistLockManager()->lock(
            txn, dbname, "dropDatabase", DistLockManager::kDefaultLockTimeout);
        if (!scopedDistLock.isOK()) {
            return appendCommandStatus(result, scopedDistLock.getStatus());
        }
        // Refresh the database metadata
        grid.catalogCache()->invalidate(dbname);

        auto status = grid.catalogCache()->getDatabase(txn, dbname);
        if (!status.isOK()) {
            if (status == ErrorCodes::NamespaceNotFound) {
                result.append("info", "database does not exist");
                return true;
            }

            return appendCommandStatus(result, status.getStatus());
        }

        log() << "DROP DATABASE: " << dbname;

        catalogClient->logChange(txn,
                                 "dropDatabase.start",
                                 dbname,
                                 BSONObj(),
                                 ShardingCatalogClient::kMajorityWriteConcern);

        for (const auto& nss : getAllShardedCollectionsForDb(txn, dbname)) {
            auto status = catalogClient->dropCollection(txn, nss);
            if (!status.isOK()) {
                index_err() << "[dropDatabase] drop collection " << nss << " failed, status: " << status;
                return appendCommandStatus(result, status);
            }
        }
        
        //after drop sharded collecton, check whether there are chunks under the db
        BSONObjBuilder collQuery;
        std::string ns = "^"; 
        ns += dbname;
        ns += "\\.";

        collQuery.appendRegex(ChunkType::ns(), ns); 
        std::vector<ChunkType> chunks;
        uassertStatusOK(
            grid.catalogClient(txn)->getChunks(txn,
                                               collQuery.obj(),
                                               BSONObj(),
                                               boost::none,
                                               &chunks,
                                               nullptr,
                                               repl::ReadConcernLevel::kMajorityReadConcern));
        if (chunks.size() == 0) {
            index_LOG(1) << "[DropDatabase] no chunks found under database " << dbname; 
        } else {
            index_log() << "[DropDatabase] " << chunks.size() << " chunks are not dropped under " << dbname;
            //continue dropCollection
            std::set<std::string> collections;
            for(auto && chunk : chunks) {
				auto ret = collections.insert(chunk.getNS());
				if (!ret.second) {
					index_LOG(1) << "[DropDatabase] push " << chunk.getNS() << " failed";
                }
            }

            for(auto collection : collections) {
                NamespaceString nss(collection);
                auto status = catalogClient->dropCollection(txn, nss);
                if (!status.isOK()) {
                    index_err() << "[dropDatabase] drop collection " << nss << " failed, status: " << status;
                    return appendCommandStatus(result, status);
                }
            }
        }

        shared_ptr<DBConfig> conf = status.getValue();

        // Drop the database from the primary shard first
        _dropDatabaseFromShard(txn, conf->getPrimaryId(), dbname);

        // Drop the database from each of the remaining shards
        {
            std::vector<ShardId> allShardIds;
            Grid::get(txn)->shardRegistry()->getAllShardIds(&allShardIds);

            for (const ShardId& shardId : allShardIds) {
                _dropDatabaseFromShard(txn, shardId, dbname);
            }
        }

        // Remove the database entry from the metadata
        Status rmStatus =
            catalogClient->removeConfigDocuments(txn,
                                                 DatabaseType::ConfigNS,
                                                 BSON(DatabaseType::name(dbname)),
                                                 ShardingCatalogClient::kMajorityWriteConcern);
        if (!rmStatus.isOK()) {
            uassertStatusOK({rmStatus.code(),
                             str::stream() << "Could not remove database '" << dbname
                                           << "' from metadata due to "
                                           << rmStatus.reason()});
        }

        //Invalidate the database so the next access will do a full reload
        grid.catalogCache()->invalidate(dbname);

        catalogClient->logChange(
            txn, "dropDatabase", dbname, BSONObj(), ShardingCatalogClient::kMajorityWriteConcern);

        result.append("dropped", dbname);
        return true;
    }
private:
    static void _dropDatabaseFromShard(OperationContext* opCtx,
                                      const ShardId& shardId,
                                      const std::string& dbName) {
        const auto dropDatabaseCommandBSON = [opCtx, &dbName] {
            BSONObjBuilder builder;
            builder.append("dropDatabase", 1);
        
            if (!opCtx->getWriteConcern().usedDefault) {
                builder.append(WriteConcernOptions::kWriteConcernField,
                   opCtx->getWriteConcern().toBSON());
            }
            
            return builder.obj();
        }();
        
        const auto shard =
            uassertStatusOK(Grid::get(opCtx)->shardRegistry()->getShard(opCtx, shardId));
        auto cmdDropDatabaseResult = shard->runCommandWithFixedRetryAttempts(
            opCtx,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            dbName,
            dropDatabaseCommandBSON,
            Shard::RetryPolicy::kIdempotent);
        
        if (!cmdDropDatabaseResult.isOK()) {
            index_err() << "[dropDatabase] dropDatabase error on shard: " << shardId.toString() << " ,dbname: "
                << dbName << " ,status: " << cmdDropDatabaseResult.getStatus();
            return;
        }  
        
        auto dropStatus = std::move(cmdDropDatabaseResult.getValue().commandStatus);
        if (!dropStatus.isOK()) {
            if (dropStatus.code() != ErrorCodes::NamespaceNotFound) {
                index_err() << "[dropDatabase] dropDatabase return error on shard: " << shardId.toString() << " ,dbname: "
                    << dbName << " ,status: " << dropStatus;
            }
        }
        auto wcStatus = std::move(cmdDropDatabaseResult.getValue().writeConcernStatus);
        if (!wcStatus.isOK()) {
            index_err() << "[dropDatabase] dropDatabase return writeConcern error on shard: " << shardId.toString() << " ,dbname: "
                << dbName << " ,status: " << wcStatus;
        }
    }

} clusterDropDatabaseCmd;

}  // namespace
}  // namespace mongo

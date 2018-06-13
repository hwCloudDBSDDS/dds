/**
 *    Copyright (C) 2016 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/db/audit.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/replica_set_config.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/replication_coordinator_impl.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/s/shard_id.h"
#include "mongo/s/catalog/sharding_catalog_manager.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/grid.h"
#include "mongo/s/request_types/register_shardsvr_request_type.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo
{

    class RegShardSvrRequest;

    namespace repl
    {
        using std::string;

        /**
         * Internal sharding command run on config servers to split a chunk.
         *
         * Format:
         * {
         *   _configsvrRegShardSvr: <string namespace>,
         *   hostAndPort: <ConnectionString ipPort>,
         * }
         */
        class ConfigSvrRegShardSvrCommand : public Command
        {
        public:
            ConfigSvrRegShardSvrCommand() : Command("_configsvrRegShardSvr") {}

            void help(std::stringstream& help) const override
            {
                help << "Internal command, which is sent by a shard server to the sharding config server. Do "
                     "not call directly. Receives, validates, and processes a RegShardSvrRequest.";
            }

            bool slaveOk() const override
            {
                return false;
            }

            bool adminOnly() const override
            {
                return true;
            }

            bool supportsWriteConcern(const BSONObj& cmd) const override
            {
                return true;
            }

            Status checkAuthForCommand(Client* client,
                                       const std::string& dbname,
                                       const BSONObj& cmdObj) override
            {
                if (!AuthorizationSession::get(client)->isAuthorizedForActionsOnResource(
                        ResourcePattern::forClusterResource(), ActionType::internal))
                {
                    return Status(ErrorCodes::Unauthorized, "Unauthorized");
                }

                return Status::OK();
            }

            bool run(OperationContext* txn,
                     const std::string& dbName,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) override
            {
                log() << "Receive register cmd (" << cmdObj.toString() << ")";

                if (serverGlobalParams.clusterRole != ClusterRole::ConfigServer)
                {
                    uasserted(ErrorCodes::IllegalOperation,
                              "_configsvrRegShardSvr can only be run on config servers");
                }

                Status primaryStatus = getGlobalReplicationCoordinator()->checkIfIAmPrimary();
                if (!primaryStatus.isOK())
                {
                    log() << "Register cmd (" << cmdObj.toString() << ")"
                          << " failed" << causedBy(primaryStatus);
                    return appendCommandStatus(result, primaryStatus);
                }

                auto swParsedRequest = RegShardSvrRequest::parseFromConfigCommand(cmdObj);
                if (!swParsedRequest.isOK())
                {
                    log() << "Register cmd (" << cmdObj.toString() << ")"
                          << " failed" << causedBy(swParsedRequest.getStatus());
                    return appendCommandStatus(result, swParsedRequest.getStatus());
                }
                auto parsedRequest = std::move(swParsedRequest.getValue());

                // call new function addShardServer() in sharding_catalog_shard_server_manager.cpp
                // Ask config server to addShard against the new shard server
                
                log() << "Add shardServer in register request";
                StatusWith<BSONObj> addShardServerResult = 
                    Grid::get(txn)->catalogManager()->getShardServerManager()->addShardDocument(
                        txn,
                        parsedRequest.getConnString(),
                        parsedRequest.getExtendIPs(),
                        parsedRequest.getProcessIdentity());

                if (!addShardServerResult.isOK())
                {
                    log() << "Register cmd (" << cmdObj.toString() << ")"
                          << " failed" << causedBy(addShardServerResult.getStatus());
                    return appendCommandStatus(result, addShardServerResult.getStatus());
                }

                //result.append("shardServerID", shardServerID); // reserved
                result.append("shardName", addShardServerResult.getValue().getStringField("shardName"));
                log() << "Finish register cmd (" << cmdObj.toString() << ")";
                
                return appendCommandStatus(result, Status::OK());
            }

            
        } configsvrRegShardSvrCmd;
    }  // namespace repl
}  // namespace mongo

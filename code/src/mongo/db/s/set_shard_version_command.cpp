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

#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/s/migration_source_manager.h"
#include "mongo/db/s/sharded_connection_info.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/wire_version.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"
#include "mongo/util/stringutils.h"

namespace mongo {

using std::string;
using str::stream;

namespace {

class SetShardVersion : public Command {
public:
    SetShardVersion() : Command("setShardVersion") {}

    void help(std::stringstream& help) const override {
        help << "internal";
    }

    bool adminOnly() const override {
        return true;
    }

    bool slaveOk() const override {
        return true;
    }

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    void addRequiredPrivileges(const std::string& dbname,
                               const BSONObj& cmdObj,
                               std::vector<Privilege>* out) override {
        ActionSet actions;
        actions.addAction(ActionType::internal);
        out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
    }

    bool run(OperationContext* txn,
             const std::string&,
             BSONObj& cmdObj,
             int options,
             string& errmsg,
             BSONObjBuilder& result) {

        if (serverGlobalParams.clusterRole != ClusterRole::ShardServer) {
            uassertStatusOK({ErrorCodes::NoShardingEnabled,
                             "Cannot accept sharding commands if not started with --shardsvr"});
        }

        // we donnt need shardversion anymore
        return true;
    }

private:
    /**
     * Checks if this server has already been initialized. If yes, then checks that the configdb
     * settings matches the initialized settings. Otherwise, initializes the server with the given
     * settings.
     */
    bool _checkConfigOrInit(OperationContext* txn,
                            const string& configdb,
                            const string& shardName,
                            bool authoritative,
                            string& errmsg,
                            BSONObjBuilder& result) {
        if (configdb.size() == 0) {
            errmsg = "no configdb";
            return false;
        }

        auto givenConnStrStatus = ConnectionString::parse(configdb);
        if (!givenConnStrStatus.isOK()) {
            errmsg = str::stream() << "error parsing given config string: " << configdb
                                   << causedBy(givenConnStrStatus.getStatus());
            return false;
        }

        const auto& givenConnStr = givenConnStrStatus.getValue();
        ConnectionString storedConnStr;

        if (shardName == "config") {
            stdx::lock_guard<stdx::mutex> lk(_mutex);
            if (!_configStr.isValid()) {
                _configStr = givenConnStr;
                return true;
            } else {
                storedConnStr = _configStr;
            }
        } else if (ShardingState::get(txn)->enabled()) {
            invariant(!_configStr.isValid());
            storedConnStr = ShardingState::get(txn)->getConfigServer(txn);
        }

        if (storedConnStr.isValid()) {
            if (givenConnStr.type() == ConnectionString::SET &&
                storedConnStr.type() == ConnectionString::SET) {
                if (givenConnStr.getSetName() != storedConnStr.getSetName()) {
                    errmsg = str::stream()
                        << "given config server set name: " << givenConnStr.getSetName()
                        << " differs from known set name: " << storedConnStr.getSetName();

                    return false;
                }

                return true;
            }

            const auto& storedRawConfigString = storedConnStr.toString();
            if (storedRawConfigString == configdb) {
                return true;
            }

            result.append("configdb",
                          BSON("stored" << storedRawConfigString << "given" << configdb));

            errmsg = str::stream() << "mongos specified a different config database string : "
                                   << "stored : " << storedRawConfigString
                                   << " vs given : " << configdb;
            return false;
        }

        invariant(shardName != "config");

        if (!authoritative) {
            result.appendBool("need_authoritative", true);
            errmsg = "first setShardVersion";
            return false;
        }

        ShardingState::get(txn)->initializeFromConfigConnString(txn, configdb, shardName);
        return true;
    }

    // Only for servers that are running as a config server.
    stdx::mutex _mutex;
    ConnectionString _configStr;

} setShardVersionCmd;

}  // namespace
}  // namespace mongo

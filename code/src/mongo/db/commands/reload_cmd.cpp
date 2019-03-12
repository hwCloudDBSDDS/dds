/*    Copyright 2014 10gen Inc.
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


#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl


#include <sstream>
#include <string>
#include <vector>

#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_options_helpers.h"
#include "mongo/util/log.h"
#include "mongo/util/stringutils.h"

using namespace std;

namespace mongo {
class ReloadCommand : public Command {
public:
    ReloadCommand() : Command("reload") {}
    virtual bool isWriteCommandForConfigServer() const {
        return false;
    }
    virtual bool slaveOk() const {
        return true;
    }
    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }
    virtual bool adminOnly() const {
        return true;
    }
    virtual void help(stringstream& h) const {
        h << "Reload resource.\n"
             "Example: {reload: 'adminWhiteListPath', param: '/var/admin_whitelist'}\n"
             "         {reload: 'auditOpFilter', param: "
             "'auth,admin,slow,insert,update,delete,command,query,all,off'}\n"
             "         {reload: 'auditAuthSuccess', param: 'true|false'}\n"
             "         {reload: 'externalConfig param: 'true|false'}'\n";
    }

    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
        ActionSet actions;
        actions.addAction(ActionType::reload);
        out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
    }

    virtual bool run(OperationContext* txn,
                     const string& dbname,
                     BSONObj& cmdObj,
                     int,
                     string& errmsg,
                     BSONObjBuilder& result) {
        BSONElement k = cmdObj["reload"];
        if (k.type() != String) {
            errmsg = "reload: key must be 'String' type";
            return false;
        }

        BSONElement v = cmdObj["param"];
        if (v.eoo()) {
            errmsg = "reload: must have 'param' field";
            return false;
        }

        std::string key = k.String();

        HostAndPort remote = txn->getClient()->getRemote();

        if (!serverGlobalParams.adminWhiteList.include(remote.host())) {
            errmsg = "reload: authentication fail";
            return false;
        }

        if (key == "adminWhiteListPath") {
            if (v.type() != String) {
                errmsg = "reload: " + key + " 's param must be 'String' type";
                return false;
            }

            std::string value = v.String();
            std::string oldPath = serverGlobalParams.adminWhiteList.path();
            if (!serverGlobalParams.adminWhiteList.parseFromFile(value, errmsg)) {
                return false;
            }
            result.append("adminWhiteListPath_old", oldPath);
            result.append("adminWhiteListPath_new", serverGlobalParams.adminWhiteList.path());
            index_log() << "security.whitelist.adminWhiteListPath: " << value << std::endl;
            index_log() << "adminWhiteList: " << serverGlobalParams.adminWhiteList.toString()
                        << std::endl;
        } else if (key == "auditOpFilter") {
            if (v.type() != String) {
                errmsg = "reload: " + key + " 's param must be 'String' type";
                return false;
            }

            std::string value = v.String();
            if (!parseAuditOpFilter(value, serverGlobalParams.auditOpFilter)) {
                errmsg = "reload: invalid value " + value;
                return false;
            }
            index_log() << "auditLog.opFilter from: " << serverGlobalParams.auditOpFilterStr
                        << " change to: " << value << std::endl;
            serverGlobalParams.auditOpFilterStr = value;
        } else if (key == "auditAuthSuccess") {
            if (v.type() != Bool) {
                errmsg = "reload: " + key + " 's param must be 'Bool' type";
                return false;
            }

            index_log() << "auditLog.authSuccess from: " << serverGlobalParams.auditAuthSuccess
                        << " change to: " << v.Bool() << std::endl;
            serverGlobalParams.auditAuthSuccess = v.Bool();
        } else {
            index_log() << "CMD reload: reload: err: ";
            errmsg = "reload: invalid key " + key;
            return false;
        }

        return true;
    }

} reloadCmd;
}

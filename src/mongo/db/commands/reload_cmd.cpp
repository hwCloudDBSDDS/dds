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
 
 
#include <string>
#include <sstream>
#include <vector>
 
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_options_helpers.h"
#include "mongo/util/log.h"
#include "mongo/util/stringutils.h"
 
using namespace std;
 
namespace mongo {
 
    class ReloadCommand : public ErrmsgCommandDeprecated {
    public:
        ReloadCommand() : ErrmsgCommandDeprecated("reload") {  }
        virtual bool isWriteCommandForConfigServer() const { return false; }
        virtual bool slaveOk() const { return true; }
        virtual bool adminOnly() const { return true; }
        virtual bool supportsWriteConcern(const BSONObj& cmd) const override {return false;}
        virtual std::string help() const {
            return "Reload resource.\n"
                "Example: {reload: 'adminWhiteListPath', param: '/var/admin_whitelist'}\n"
                "         {reload: 'auditOpFilter', param: 'auth,admin,slow,insert,update,delete,command,query,all,off'}\n"
                "         {reload: 'auditAuthSuccess', param: 'true|false'}\n"
                "         {reload: 'externalConfig param: 'true|false'}'\n"
                "         {reload: 'nsFilter', param: {'query': 'testdb', insert': 'testdb1,testdb2.testColl,testdb3'}}\n";
        }

 
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) const {
            ActionSet actions;
            actions.addAction(ActionType::reload);
            out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
        }

        AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
            return AllowedOnSecondary::kAlways;
        }
        
        bool errmsgRun(OperationContext* txn,
                       const std::string& dbname,
                       const BSONObj& cmdObj,
                       std::string& errmsg,
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
            log() << "CMD reload: reload: " << key << " param: " << v
                  << " from " << remote.host() << ":" << remote.port() << std::endl;
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
                log() << "security.whitelist.adminWhiteListPath: " << value << std::endl;
                log() << "adminWhiteList: " << serverGlobalParams.adminWhiteList.toString() << std::endl;
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
                log() << "auditLog.opFilter from: " << serverGlobalParams.auditOpFilterStr << " change to: " << value << std::endl;
                serverGlobalParams.auditOpFilterStr = value;
            } else if (key == "auditAuthSuccess") {
                if (v.type() != Bool) {
                    errmsg = "reload: " + key + " 's param must be 'Bool' type";
                    return false;
                }
                log() << "auditLog.authSuccess from: " << serverGlobalParams.auditAuthSuccess << " change to: " << v.Bool() << std::endl;
                serverGlobalParams.auditAuthSuccess = v.Bool();
            } else if (key == "externalConfig") {
                std::string path = serverGlobalParams.externalConfig.path();
                if(path.empty()) {
                    log() << "path is none. reloade externalConfig is cancle." << std::endl;
                    return true;
                }
                if (!serverGlobalParams.externalConfig.parseFromFile(path, errmsg)) {
                    return false;
                }

                log() << "reloade is done. externalConfig: " << serverGlobalParams.externalConfig.toString() << std::endl;
            }  else if (key == "nsFilter") {
                if (v.type() != Object) {
                    errmsg = "reload: " + key + " 's param must be 'Object' type";
                    return false;
                }
                BSONObj value = v.Obj();
                std::map<std::string, std::string> auditNsFilterMap;
                for (auto op : logger::auditOpList) {
                    auditNsFilterMap.insert({op, value.getStringField(op)});
                }
                std::string oldValue = "";            
                if (!serverGlobalParams.auditNsFilterMap.empty()) {
                    for(auto& nsFilterIt : serverGlobalParams.auditNsFilterMap) {
                        if (!nsFilterIt.second.empty()) {
                            oldValue.append(nsFilterIt.first);
                            oldValue.append(": ");
                            oldValue.append(nsFilterIt.second);
                            oldValue.append(",");
                            oldValue.pop_back();
                        }
                    }
                }
                serverGlobalParams.auditOpNsFilterMap.clear();
                if (!parseAuditNsFilter(auditNsFilterMap)) {
                    serverGlobalParams.auditOpNsFilterMap.clear();
                    parseAuditNsFilter(serverGlobalParams.auditNsFilterMap);
                    errmsg = "reload: invalid value " + value.toString();
                    return false;
                }
                log() << "auditLog.nsFilter from: " << oldValue << " change to: " << value.toString() << std::endl;
                serverGlobalParams.auditNsFilterMap = auditNsFilterMap;
            } else {
                errmsg = "reload: invalid key " + key;
                return false;
            }
            return true;
        }
    } reloadCmd;
}


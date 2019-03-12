/**
 *    Copyright (C) 2013 10gen Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/curop.h"
#include "mongo/logger/logger.h"
#include "mongo/util/background.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"


#if MONGO_ENTERPRISE_VERSION
#define MONGO_AUDIT_STUB ;
#else
#define MONGO_AUDIT_STUB \
    {}
#endif

namespace mongo {
//
// AuditLogFlusher
//

/**
 * Thread for flush audit log
 */
class AuditLogFlusher : public BackgroundJob {
public:
    std::string name() const {
        return "AuditLogFlusher";
    }

    void run() {
        const int Secs = 1;
        while (!inShutdown()) {
            logger::globalAuditLogDomain()->flush();
            sleepsecs(Secs);
        }
    }
};

namespace audit {

using namespace mongo::logger;

const std::string ROLE_NAME_FIELD_NAME = "role";
const std::string ROLE_DB_FIELD_NAME = "db";

void getAuthenticatedUsersAndRoles(Client* client,
                                   std::vector<UserName>* userNames,
                                   std::vector<RoleName>* roleNames) {
    if (client != NULL) {
        AuthorizationSession* authzSession = AuthorizationSession::get(client);
        if (authzSession != NULL) {
            for (UserNameIterator nameIter = authzSession->getAuthenticatedUserNames();
                 nameIter.more();
                 nameIter.next()) {
                userNames->push_back(nameIter.get());
            }
            for (RoleNameIterator nameIter = authzSession->getAuthenticatedRoleNames();
                 nameIter.more();
                 nameIter.next()) {
                roleNames->push_back(nameIter.get());
            }
        }
    }
}

// TODO: audit lib should not dependent on serveronly lib,
// so duplicate code here(copied from db/commands/user_management_commands.cpp),
// should put it elsewhere
BSONArray rolesVectorToBSONArray(const std::vector<RoleName>& roles) {
    BSONArrayBuilder rolesArrayBuilder;
    for (std::vector<RoleName>::const_iterator it = roles.begin(); it != roles.end(); ++it) {
        const RoleName& role = *it;
        rolesArrayBuilder.append(
            BSON(ROLE_NAME_FIELD_NAME << role.getRole() << ROLE_DB_FIELD_NAME << role.getDB()));
    }
    return rolesArrayBuilder.arr();
}

Status privilegeVectorToBSONArray(const PrivilegeVector& privileges, BSONArray* result) {
    BSONArrayBuilder arrBuilder;
    for (PrivilegeVector::const_iterator it = privileges.begin(); it != privileges.end(); ++it) {
        const Privilege& privilege = *it;

        ParsedPrivilege parsedPrivilege;
        std::string errmsg;
        if (!ParsedPrivilege::privilegeToParsedPrivilege(privilege, &parsedPrivilege, &errmsg)) {
            return Status(ErrorCodes::FailedToParse, errmsg);
        }
        if (!parsedPrivilege.isValid(&errmsg)) {
            return Status(ErrorCodes::FailedToParse, errmsg);
        }
        arrBuilder.append(parsedPrivilege.toBSON());
    }
    *result = arrBuilder.arr();
    return Status::OK();
}


void logAuditEventCommon(StringData atype,
                         Client* client,
                         BSONObj& param,
                         ErrorCodes::Error result) {
    HostAndPort local;
    HostAndPort remote;
    if (client != NULL && client->hasRemote()) {
        local = client->getLocal();
        remote = client->getRemote();
    }

    std::vector<UserName> userNames;
    std::vector<RoleName> roleNames;
    getAuthenticatedUsersAndRoles(client, &userNames, &roleNames);

    AuditEventEphemeral event(atype,
                              Date_t::now(),
                              local.host(),
                              local.port(),
                              remote.host(),
                              remote.port(),
                              &userNames,
                              &roleNames,
                              0,
                              &param,
                              result);
    Status status = globalAuditLogDomain()->append(event);
    if (!status.isOK()) {
        warning() << "append audit log failure, status: " << status << std::endl;
    }
}

void logAuthentication(Client* client,
                       StringData mechanism,
                       const UserName& user,
                       ErrorCodes::Error result) {
    if ((serverGlobalParams.auditOpFilter & opAuth) == 0) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("user", user.getUser());
    builder.append("db", user.getDB());
    builder.append("mechanism", mechanism);
    BSONObj param = builder.obj();

    logAuditEventCommon("authenticate", client, param, result);
}

bool needLogAuthzCheck(Client* client, ErrorCodes::Error result) {
    if (result == ErrorCodes::OK) {
        if (!serverGlobalParams.auditAuthSuccess) {
            return false;
        }
    }

    return true;
}

bool needLogAdminOp() {
    return (serverGlobalParams.auditOpFilter & opAdmin) != 0;
}

bool needLogSlowOp() {
    return (serverGlobalParams.auditOpFilter & opSlow) != 0;
}

void logCommandAuthzCheck(Client* client,
                          const std::string& dbname,
                          const BSONObj& cmdObj,
                          Command* command,
                          ErrorCodes::Error result) {
    if (!needLogAuthzCheck(client, result)) {
        return;
    }

    if ((serverGlobalParams.auditOpFilter & opCommand) == 0) {
        return;
    }

    BSONObjBuilder builder;
    if (command != NULL) {
        builder.append("command", command->getName());
        builder.append("ns", command->parseNs(dbname, cmdObj));
        // builder.append("args", command->getRedactedCopyForLogging(cmdObj));
    } else {
        builder.append("command", "null");
    }
    BSONObj param = builder.obj();

    logAuditEventCommon("authCheck", client, param, result);
}

void logDeleteAuthzCheck(Client* client,
                         const NamespaceString& ns,
                         const BSONObj& pattern,
                         ErrorCodes::Error result) {
    if (!needLogAuthzCheck(client, result)) {
        return;
    }
    if ((serverGlobalParams.auditOpFilter & opDelete) == 0) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("command", "delete");
    builder.append("ns", ns.ns());
    builder.append("args", pattern);
    BSONObj param = builder.obj();

    logAuditEventCommon("authCheck", client, param, result);
}

void logGetMoreAuthzCheck(Client* client,
                          const NamespaceString& ns,
                          long long cursorId,
                          ErrorCodes::Error result) {
    if (!needLogAuthzCheck(client, result)) {
        return;
    }

    if ((serverGlobalParams.auditOpFilter & opQuery) == 0) {
        return;
    }
    BSONObjBuilder builder;
    builder.append("command", "getMore");
    builder.append("ns", ns.ns());
    builder.append("args", BSON("getMore" << ns.db() << "cursorId" << cursorId));
    BSONObj param = builder.obj();

    logAuditEventCommon("authCheck", client, param, result);
}

void logInsertAuthzCheck(Client* client,
                         const NamespaceString& ns,
                         const BSONObj& insertedObj,
                         ErrorCodes::Error result) {
    if (!needLogAuthzCheck(client, result)) {
        return;
    }

    if ((serverGlobalParams.auditOpFilter & opInsert) == 0) {
        return;
    }
    BSONObjBuilder builder;
    builder.append("command", "insert");
    builder.append("ns", ns.ns());
    builder.append("args", insertedObj);
    BSONObj param = builder.obj();

    logAuditEventCommon("authCheck", client, param, result);
}

void logKillCursorsAuthzCheck(Client* client,
                              const NamespaceString& ns,
                              long long cursorId,
                              ErrorCodes::Error result) {
    if (!needLogAuthzCheck(client, result)) {
        return;
    }

    if ((serverGlobalParams.auditOpFilter & opQuery) == 0) {
        return;
    }
    BSONObjBuilder builder;
    builder.append("command", "killCursors");
    builder.append("ns", ns.ns());
    builder.append("args", BSON("killCursors" << ns.db() << "cursorId" << cursorId));
    BSONObj param = builder.obj();

    logAuditEventCommon("authCheck", client, param, result);
}

void logQueryAuthzCheck(Client* client,
                        const NamespaceString& ns,
                        const BSONObj& query,
                        ErrorCodes::Error result) {
    if (!needLogAuthzCheck(client, result)) {
        return;
    }

    if ((serverGlobalParams.auditOpFilter & opQuery) == 0) {
        return;
    }
    BSONObjBuilder builder;
    builder.append("command", "query");
    builder.append("ns", ns.ns());
    builder.append("args", query);
    BSONObj param = builder.obj();

    logAuditEventCommon("authCheck", client, param, result);
}

void logUpdateAuthzCheck(Client* client,
                         const NamespaceString& ns,
                         const BSONObj& query,
                         const BSONObj& updateObj,
                         bool isUpsert,
                         bool isMulti,
                         ErrorCodes::Error result) {
    if (!needLogAuthzCheck(client, result)) {
        return;
    }

    if ((serverGlobalParams.auditOpFilter & opUpdate) == 0) {
        return;
    }
    BSONObjBuilder builder;
    builder.append("command", "update");
    builder.append("ns", ns.ns());
    builder.append(
        "args",
        BSON("update" << ns.db() << "updates"
                      << BSON_ARRAY("q" << query << "u" << updateObj << "multi" << isMulti
                                        << "upsert"
                                        << isUpsert)));
    BSONObj param = builder.obj();

    logAuditEventCommon("authCheck", client, param, result);
}

void logCreateUser(Client* client,
                   const UserName& username,
                   bool password,
                   const BSONObj* customData,
                   const std::vector<RoleName>& roles) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("user", username.getUser());
    builder.append("db", username.getDB());
    if (customData != NULL) {
        builder.append("customData", *customData);
    }
    builder.append("roles", rolesVectorToBSONArray(roles));
    BSONObj param = builder.obj();

    logAuditEventCommon("createUser", client, param, ErrorCodes::OK);
}

void logDropUser(Client* client, const UserName& username) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("user", username.getUser());
    builder.append("db", username.getDB());
    BSONObj param = builder.obj();

    logAuditEventCommon("dropUser", client, param, ErrorCodes::OK);
}

void logDropAllUsersFromDatabase(Client* client, StringData dbname) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("db", dbname);
    BSONObj param = builder.obj();

    logAuditEventCommon("dropAllUsersFromDatabase", client, param, ErrorCodes::OK);
}

void logUpdateUser(Client* client,
                   const UserName& username,
                   bool password,
                   const BSONObj* customData,
                   const std::vector<RoleName>* roles) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("user", username.getUser());
    builder.append("db", username.getDB());
    builder.append("passwordChanged", password);
    if (customData != NULL) {
        builder.append("customData", *customData);
    }
    if (roles != NULL && !roles->empty()) {
        builder.append("roles", rolesVectorToBSONArray(*roles));
    }
    BSONObj param = builder.obj();

    logAuditEventCommon("updateUser", client, param, ErrorCodes::OK);
}

void logGrantRolesToUser(Client* client,
                         const UserName& username,
                         const std::vector<RoleName>& roles) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("user", username.getUser());
    builder.append("db", username.getDB());
    builder.append("roles", rolesVectorToBSONArray(roles));
    BSONObj param = builder.obj();

    logAuditEventCommon("grantRolesToUser", client, param, ErrorCodes::OK);
}

void logRevokeRolesFromUser(Client* client,
                            const UserName& username,
                            const std::vector<RoleName>& roles) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("user", username.getUser());
    builder.append("db", username.getDB());
    builder.append("roles", rolesVectorToBSONArray(roles));
    BSONObj param = builder.obj();

    logAuditEventCommon("revokeRolesFromUser", client, param, ErrorCodes::OK);
}

void logCreateRole(Client* client,
                   const RoleName& role,
                   const std::vector<RoleName>& roles,
                   const PrivilegeVector& privileges) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("role", role.getRole());
    builder.append("db", role.getDB());
    if (!roles.empty()) {
        builder.append("roles", rolesVectorToBSONArray(roles));
    }
    if (!privileges.empty()) {
        BSONArray arr;
        if (privilegeVectorToBSONArray(privileges, &arr) == Status::OK()) {
            builder.append("privileges", arr);
        }
    }
    BSONObj param = builder.obj();

    logAuditEventCommon("createRole", client, param, ErrorCodes::OK);
}

void logUpdateRole(Client* client,
                   const RoleName& role,
                   const std::vector<RoleName>* roles,
                   const PrivilegeVector* privileges) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("role", role.getRole());
    builder.append("db", role.getDB());
    if (roles != NULL && !roles->empty()) {
        builder.append("roles", rolesVectorToBSONArray(*roles));
    }
    if (privileges != NULL && !privileges->empty()) {
        BSONArray arr;
        if (privilegeVectorToBSONArray(*privileges, &arr) == Status::OK()) {
            builder.append("privileges", arr);
        }
    }
    BSONObj param = builder.obj();

    logAuditEventCommon("updateRole", client, param, ErrorCodes::OK);
}

void logDropRole(Client* client, const RoleName& role) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("role", role.getRole());
    builder.append("db", role.getDB());
    BSONObj param = builder.obj();

    logAuditEventCommon("dropRole", client, param, ErrorCodes::OK);
}

void logDropAllRolesFromDatabase(Client* client, StringData dbname) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("db", dbname);
    BSONObj param = builder.obj();

    logAuditEventCommon("dropAllRolesFromDatabase", client, param, ErrorCodes::OK);
}

void logGrantRolesToRole(Client* client, const RoleName& role, const std::vector<RoleName>& roles) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("role", role.getRole());
    builder.append("db", role.getDB());
    builder.append("roles", rolesVectorToBSONArray(roles));
    BSONObj param = builder.obj();

    logAuditEventCommon("grantRolesToRole", client, param, ErrorCodes::OK);
}

void logRevokeRolesFromRole(Client* client,
                            const RoleName& role,
                            const std::vector<RoleName>& roles) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("role", role.getRole());
    builder.append("db", role.getDB());
    builder.append("roles", rolesVectorToBSONArray(roles));
    BSONObj param = builder.obj();

    logAuditEventCommon("revokeRolesFromRole", client, param, ErrorCodes::OK);
}

void logGrantPrivilegesToRole(Client* client,
                              const RoleName& role,
                              const PrivilegeVector& privileges) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("role", role.getRole());
    builder.append("db", role.getDB());
    BSONArray arr;
    if (privilegeVectorToBSONArray(privileges, &arr) == Status::OK()) {
        builder.append("privileges", arr);
    }
    BSONObj param = builder.obj();

    logAuditEventCommon("grantPrivilegesToRole", client, param, ErrorCodes::OK);
}

void logRevokePrivilegesFromRole(Client* client,
                                 const RoleName& role,
                                 const PrivilegeVector& privileges) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("role", role.getRole());
    builder.append("db", role.getDB());
    BSONArray arr;
    if (privilegeVectorToBSONArray(privileges, &arr) == Status::OK()) {
        builder.append("privileges", arr);
    }
    BSONObj param = builder.obj();

    logAuditEventCommon("revokePrivilegesFromRole", client, param, ErrorCodes::OK);
}

void logReplSetReconfig(Client* client, const BSONObj* oldConfig, const BSONObj* newConfig) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    if (oldConfig != NULL) {
        builder.append("old", *oldConfig);
    }
    if (newConfig != NULL) {
        builder.append("new", *newConfig);
    }
    BSONObj param = builder.obj();

    logAuditEventCommon("replSetReconfig", client, param, ErrorCodes::OK);
}

void logApplicationMessage(Client* client, StringData msg) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("msg", msg);
    BSONObj param = builder.obj();

    logAuditEventCommon("applicationMessage", client, param, ErrorCodes::OK);
}

void logShutdown(Client* client) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    BSONObj param = builder.obj();

    logAuditEventCommon("shutdown", client, param, ErrorCodes::OK);
}

void logCreateIndex(Client* client,
                    const BSONObj* indexSpec,
                    StringData indexname,
                    StringData nsname) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("ns", nsname);
    builder.append("indexName", indexname);
    if (indexSpec != NULL) {
        builder.append("indexSpec", *indexSpec);
    }
    BSONObj param = builder.obj();

    logAuditEventCommon("createIndex", client, param, ErrorCodes::OK);
}

void logCreateCollection(Client* client, StringData nsname) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("ns", nsname);
    BSONObj param = builder.obj();
    logAuditEventCommon("createCollection", client, param, ErrorCodes::OK);
}

void logCreateDatabase(Client* client, StringData dbname) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("ns", dbname);
    BSONObj param = builder.obj();

    logAuditEventCommon("createDatabase", client, param, ErrorCodes::OK);
}

void logDropIndex(Client* client, StringData indexname, StringData nsname) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("ns", nsname);
    builder.append("indexName", indexname);
    BSONObj param = builder.obj();

    logAuditEventCommon("dropIndex", client, param, ErrorCodes::OK);
}

void logDropCollection(Client* client, StringData nsname) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("ns", nsname);
    BSONObj param = builder.obj();

    logAuditEventCommon("dropCollection", client, param, ErrorCodes::OK);
}

void logDropDatabase(Client* client, StringData dbname) {
    BSONObjBuilder builder;
    builder.append("ns", dbname);
    BSONObj param = builder.obj();

    logAuditEventCommon("dropDatabase", client, param, ErrorCodes::OK);
}

void logRenameCollection(Client* client, StringData source, StringData target) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("old", source);
    builder.append("new", target);
    BSONObj param = builder.obj();

    logAuditEventCommon("renameCollection", client, param, ErrorCodes::OK);
}

void logEnableSharding(Client* client, StringData dbname) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("ns", dbname);
    BSONObj param = builder.obj();

    logAuditEventCommon("enableSharding", client, param, ErrorCodes::OK);
}

void logAddShard(Client* client, StringData name, const std::string& servers, long long maxSize) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("shard", name);
    builder.append("connectionString", servers);
    builder.append("maxSize", maxSize);
    BSONObj param = builder.obj();

    logAuditEventCommon("addShard", client, param, ErrorCodes::OK);
}

void logRemoveShard(Client* client, StringData shardname) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("shard", shardname);
    BSONObj param = builder.obj();

    logAuditEventCommon("removeShard", client, param, ErrorCodes::OK);
}

void logShardCollection(Client* client, StringData ns, const BSONObj& keyPattern, bool unique) {
    if (!needLogAdminOp()) {
        return;
    }

    BSONObjBuilder builder;
    builder.append("ns", ns);
    builder.append("key", keyPattern);
    builder.append("options", BSON("unique" << unique));
    BSONObj param = builder.obj();

    logAuditEventCommon("shardCollection", client, param, ErrorCodes::OK);
}
void writeImpersonatedUsersToMetadata(OperationContext* txn, BSONObjBuilder* metadata) {}

void parseAndRemoveImpersonatedUsersField(BSONObj cmdObj,
                                          std::vector<UserName>* parsedUserNames,
                                          bool* fieldIsPresent) {}

void parseAndRemoveImpersonatedRolesField(BSONObj cmdObj,
                                          std::vector<RoleName>* parsedRoleNames,
                                          bool* fieldIsPresent) {}

void logSlowOp(Client* client, CurOp& curop, const SingleThreadedLockStats& lockStats) {
    if (!needLogSlowOp()) {
        return;
    }

    HostAndPort local;
    HostAndPort remote;
    if (client != NULL && client->hasRemote()) {
        local = client->getLocal();
        remote = client->getRemote();
    }

    BSONObjBuilder builder;
    curop.debug().append(curop, lockStats, builder);
    BSONObj param = builder.obj();

    std::vector<UserName> userNames;
    std::vector<RoleName> roleNames;
    getAuthenticatedUsersAndRoles(client, &userNames, &roleNames);

    AuditEventEphemeral event("slowOp",
                              Date_t::now(),
                              local.host(),
                              local.port(),
                              remote.host(),
                              remote.port(),
                              &userNames,
                              &roleNames,
                              curop.elapsedMicros(),
                              &param,
                              ErrorCodes::OK);
    globalAuditLogDomain()->append(event);
}
}  // namespace audit
namespace {
// Only one instance of the AuditLogFlusher exists
AuditLogFlusher auditLogFlusher;
}

void startAuditLogFlusher() {
    auditLogFlusher.go();
}
}  // namespace mongo

/**
 *    Copyright (C) 2009-2016 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"

#include <string>
#include <vector>

#include "mongo/bson/mutable/document.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/client.h"
#include "mongo/db/curop.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/modules/rocks/src/gc_common.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/server_parameters.h"
#include "mongo/rpc/metadata.h"
#include "mongo/rpc/write_concern_error_detail.h"
#include "mongo/s/stale_exception.h"
#include "mongo/util/log.h"
#include "mongo/util/util_extend/GlobalConfig.h"

namespace mongo {

using std::string;
using std::stringstream;

using logger::LogComponent;

Command::CommandMap* Command::_commandsByBestName;
Command::CommandMap* Command::_commands;

Counter64 Command::unknownCommands;
static ServerStatusMetricField<Counter64> displayUnknownCommands("commands.<UNKNOWN>",
                                                                 &Command::unknownCommands);
const char kChunkIdField[] = "chunkId";

namespace {

ExportedServerParameter<bool, ServerParameterType::kStartupOnly> testCommandsParameter(
    ServerParameterSet::getGlobal(), "enableTestCommands", &Command::testCommandsEnabled);

}  // namespace

Command::~Command() = default;

string Command::parseNsFullyQualified(const string& dbname, const BSONObj& cmdObj) {
    BSONElement first = cmdObj.firstElement();
    uassert(ErrorCodes::BadValue,
            str::stream() << "collection name has invalid type " << typeName(first.type()),
            first.canonicalType() == canonicalizeBSONType(mongo::String));
    const NamespaceString nss(first.valueStringData());
    uassert(ErrorCodes::InvalidNamespace,
            str::stream() << "Invalid namespace specified '" << nss.ns() << "'",
            nss.isValid());
    return nss.ns();
}

NamespaceString Command::parseNsCollectionRequired(const string& dbname, const BSONObj& cmdObj) {
    // Accepts both BSON String and Symbol for collection name per SERVER-16260
    // TODO(kangas) remove Symbol support in MongoDB 3.0 after Ruby driver audit
    BSONElement first = cmdObj.firstElement();
    uassert(ErrorCodes::BadValue,
            str::stream() << "collection name has invalid type " << typeName(first.type()),
            first.canonicalType() == canonicalizeBSONType(mongo::String));
    const NamespaceString nss(dbname, first.valueStringData());
    uassert(ErrorCodes::InvalidNamespace,
            str::stream() << "Invalid namespace specified '" << nss.ns() << "'",
            nss.isValid());
    return nss;
}

string Command::parseNs(const std::string& dbname, const BSONObj& cmdObj) const {
    std::string collforchunk;

    BSONElement collElement = cmdObj.firstElement();
    if (collElement.type() != mongo::String)
        return dbname;

    BSONElement chunkidElement;
    Status chunkidElementStatus =
        bsonExtractTypedField(cmdObj, kChunkIdField, BSONType::String, &chunkidElement);
    if (chunkidElementStatus.isOK()) {
        collforchunk = collElement.valueStringData() + "$" + chunkidElement.valueStringData();
    } else {
        collforchunk = collElement.valuestr();
    }

    return dbname + '.' + collforchunk;
}

ResourcePattern Command::parseResourcePattern(const std::string& dbname,
                                              const BSONObj& cmdObj) const {
    std::string ns = parseNs(dbname, cmdObj);
    if (ns.find('.') == std::string::npos) {
        return ResourcePattern::forDatabaseName(ns);
    }
    return ResourcePattern::forExactNamespace(NamespaceString(ns));
}

Command::Command(StringData name, bool webUI, StringData oldName)
    : _name(name.toString()),
      _webUI(webUI),
      _commandsExecutedMetric("commands." + _name + ".total", &_commandsExecuted),
      _commandsFailedMetric("commands." + _name + ".failed", &_commandsFailed) {
    // register ourself.
    if (_commands == 0)
        _commands = new CommandMap();
    if (_commandsByBestName == 0)
        _commandsByBestName = new CommandMap();
    Command*& c = (*_commands)[name];
    if (c)
        log() << "warning: 2 commands with name: " << _name;
    c = this;
    (*_commandsByBestName)[name] = this;

    if (!oldName.empty())
        (*_commands)[oldName.toString()] = this;
}

void Command::help(stringstream& help) const {
    help << "no help defined";
}

Status Command::explain(OperationContext* txn,
                        const string& dbname,
                        const BSONObj& cmdObj,
                        ExplainCommon::Verbosity verbosity,
                        const rpc::ServerSelectionMetadata& serverSelectionMetadata,
                        BSONObjBuilder* out) const {
    return {ErrorCodes::IllegalOperation, str::stream() << "Cannot explain cmd: " << getName()};
}

Command* Command::findCommand(StringData name) {
    CommandMap::const_iterator i = _commands->find(name);
    if (i == _commands->end())
        return 0;
    return i->second;
}

bool Command::appendCommandStatus(BSONObjBuilder& result, const Status& status) {
    appendCommandStatus(result, status.isOK(), status.reason());
    BSONObj tmp = result.asTempObj();
    if (!status.isOK() && !tmp.hasField("code")) {
        result.append("code", status.code());
        result.append("codeName", ErrorCodes::errorString(status.code()));
        index_log() << "[Command] errCode: " << static_cast<int>(status.code())
                    << " ; errStr: " << ErrorCodes::errorString(status.code())
                    << "; errReason : " << status.reason();
    }
    return status.isOK();
}

void Command::appendCommandStatus(BSONObjBuilder& result, bool ok, const std::string& errmsg) {
    BSONObj tmp = result.asTempObj();
    bool have_ok = tmp.hasField("ok");
    bool need_errmsg = !ok && !tmp.hasField("errmsg");

    if (!have_ok)
        result.append("ok", ok ? 1.0 : 0.0);

    if (need_errmsg) {
        result.append("errmsg", errmsg);
    }
}

void Command::appendCommandWCStatus(BSONObjBuilder& result,
                                    const Status& awaitReplicationStatus,
                                    const WriteConcernResult& wcResult) {
    if (!awaitReplicationStatus.isOK() && !result.hasField("writeConcernError")) {
        WriteConcernErrorDetail wcError;
        wcError.setErrCode(awaitReplicationStatus.code());
        wcError.setErrMessage(awaitReplicationStatus.reason());
        if (wcResult.wTimedOut) {
            wcError.setErrInfo(BSON("wtimeout" << true));
        }
        result.append("writeConcernError", wcError.toBSON());
    }
}

Status Command::checkAuthForOperation(OperationContext* txn,
                                      const std::string& dbname,
                                      const BSONObj& cmdObj) {
    return checkAuthForCommand(txn->getClient(), dbname, cmdObj);
}

Status Command::checkAuthForCommand(Client* client,
                                    const std::string& dbname,
                                    const BSONObj& cmdObj) {
    std::vector<Privilege> privileges;
    this->addRequiredPrivileges(dbname, cmdObj, &privileges);
    if (AuthorizationSession::get(client)->isAuthorizedForPrivileges(privileges))
        return Status::OK();
    return Status(ErrorCodes::Unauthorized, "unauthorized");
}

void Command::redactForLogging(mutablebson::Document* cmdObj) {}

BSONObj Command::getRedactedCopyForLogging(const BSONObj& cmdObj) {
    namespace mmb = mutablebson;
    mmb::Document cmdToLog(cmdObj, mmb::Document::kInPlaceDisabled);
    redactForLogging(&cmdToLog);
    BSONObjBuilder bob;
    cmdToLog.writeTo(&bob);
    return bob.obj();
}

/*****modify mongodb code start*****/

namespace {
const std::string CUSTOM_USER = "rwuser@admin";
}  // namespace

std::set<std::string> globleAllowedCommands{"isMaster",
                                            "listCollections",
                                            "getLastError",
                                            "resetError",
                                            "logout",
                                            "updateUser",
                                            "authenticate",
                                            "saslStart",
                                            "saslContinue",
                                            "buildInfo",
                                            "serverStatus",
                                            "listDatabases",
                                            "ping"};

/*check if allow commands against admin/config database*/
static bool _checkIfAllowedCommands(const std::string& cmdname) {
    if (globleAllowedCommands.find(cmdname) != globleAllowedCommands.end()) {
        return true;
    }
    return false;
}

std::set<std::string> globleDisableCommands{"authSchemaUpgrade",
                                            "replSetElect",
                                            "replSetUpdatePosition",
                                            "appendOplogNote",
                                            "replSetFreeze",
                                            "replSetInitiate",
                                            "replSetMaintenance",
                                            "replSetReconfig",
                                            "replSetStepDown",
                                            "replSetSyncFrom",
                                            "replSetRequestVotes",
                                            "resync",
                                            "applyOps",
                                            "replSetGetConfig",
                                            "flushRouterConfig",
                                            "flushrouterconfig",
                                            "addShard",
                                            "addshard",
                                            "cleanupOrphaned",
                                            "checkShardingIndex",
                                            "listShards",
                                            "listshards",
                                            "removeShard",
                                            "removeshard",
                                            "getShardMap",
                                            "setShardVersion",
                                            "getShardVersion",
                                            "getshardversion",
                                            "addShardToZone",
                                            "addshardtozone",
                                            "removeShardFromZone",
                                            "removeshardfromzone",
                                            "shardingState",
                                            "unsetSharding",
                                            "copydb",
                                            "clone",
                                            "clean",
                                            "connPoolSync",
                                            "connpoolsync",
                                            "compact",
                                            "setParameter",
                                            "repairDatabase",
                                            "repairCursor",
                                            "shutdown",
                                            "logRotate",
                                            "connPoolStats",
                                            "dbHash",
                                            "dbhash",
                                            "diagLogging",
                                            "driverOIDTest",
                                            "getCmdLineOpts",
                                            "getLog",
                                            "hostInfo",
                                            "_isSelf",
                                            "netstat",
                                            "profile",
                                            "shardConnPoolStats",
                                            "validate",
                                            "handshake",
                                            "_recvChunkAbort",
                                            "_recvChunkCommit",
                                            "_recvChunkStart",
                                            "_recvChunkStatus",
                                            "replSetFresh",
                                            "mapreduce.shardedfinish",
                                            "_transferMods",
                                            "replSetHeartbeat",
                                            "replSetGetRBID",
                                            "_migrateClone",
                                            "writeBacksQueued",
                                            "_getUserCacheGeneration",
                                            "splitVector",
                                            "split",
                                            "moveChunk",
                                            "movechunk",
                                            "getPrevError",
                                            "mergeChunks",
                                            "movePrimary",
                                            "moveprimary",
                                            "authSchemaUpgrade",
                                            "updateZoneKeyRange",
                                            "updatezonekeyRange",
                                            "balancerStart",
                                            "balancerStop",
                                            "balancerStatus"};

bool Command::_checkIfDisableCommands(const std::string& cmdname) {
    // below cmds is hw contains, the
    // applyOps, flushRouterConfig, flushrouterconfig, addShard, addshard, cleanupOrphaned
    // checkShardingIndex, listShards, listshards, removeShard, removeshard, getShardMap
    // setShardVersion, shardingState, unsetSharding, dbHash, dbhash
    //
    // below cmds is hw do not contains
    // setReadOnly

    if (globleDisableCommands.find(cmdname) != globleDisableCommands.end()) {
        return true;
    }
    return false;
}

static bool _isDisabledCommandsAndPameterForCustomer(Command* c,
                                                     const std::string& cmdname,
                                                     const std::string& dbname,
                                                     const BSONObj& cmdObj) {
    if (cmdname == "dropDatabase" || cmdname == "createIndexes" || cmdname == "dropIndexes"
        || cmdname == "convertToCapped") {
        if (dbname == "admin") {
            return true;
        }
        return false;
    }

    if (cmdname == "findAndModify" || cmdname == "findandmodify" || cmdname == "group" ||
        cmdname == "mapreduce" || cmdname == "mapReduce" || cmdname == "aggregate") {
        const std::string fullNs = c->parseNs(dbname, cmdObj);
        if (fullNs == "admin.system.users" || fullNs == "admin.system.roles") {
            return true;
        }
        return false;
    }

    return false;
}

static Status _checkAuthForUser(Command* c,
                                Client* client,
                                const std::string& dbname,
                                const BSONObj& cmdObj) {

    // because user may connect with inner network because some case in our clould instance,
    // add temp fix that : when user is auth, do not check the connection way.
    // TODO: when our cloud instance is fix, need fix this back.
    if (AuthorizationSession::get(client)->isAuthWithCustomer() ||
        (client->isCustomerConnection() &&
         AuthorizationSession::get(client)
             ->isAuthWithCustomerOrNoAuthUser())) {  // check if consumer

        std::string cmdname = c->getName();

        if (Command::_checkIfDisableCommands(cmdname)) {
            return Status(ErrorCodes::CommandNotFound, "no this command " + cmdname);
        }

        if (_checkIfAllowedCommands(cmdname)) {  // check if allowed commands
            LOG(4) << "Allow Mongodb consumer run command " << cmdname << "against the " << dbname
                   << " database.";
        } else if (_isDisabledCommandsAndPameterForCustomer(c, cmdname, dbname, cmdObj)) {
            return Status(ErrorCodes::Unauthorized, "unauthorized");
        }
    }

    return Status::OK();
}

std::set<std::string> globalReadComand{"find",
                                       "getMore",
                                       "authenticate",
                                       "collStats",
                                       "collMod",
                                       "connectionStatus",
                                       "count",
                                       "dbStats",
                                       "dbstats",
                                       "explain",
                                       "filemd5",
                                       "geoNear",
                                       "getLastError",
                                       "getParameter",
                                       "isMaster",
                                       "killOp",
                                       "listCollections",
                                       "listCommands",
                                       "listDatabases",
                                       "listIndexes",
                                       "logout",
                                       "saslStart",
                                       "saslContinue",
                                       "usersInfo",
                                       "rolesInfo",
                                       "serverStatus",
                                       "buildInfo",
                                       "buildinfo",
                                       "distinct",
                                       "isMaster",
                                       "isdbgrid",
                                       "planCacheListFilters",
                                       "planCacheListPlans",
                                       "planCacheListQueryShapes",
                                       "availableQueryOptions",
                                       "availablequeryoptions",
                                       "datasize",
                                       "dataSize",
                                       "features",
                                       "ping",
                                       "whatsmyuri"
};

Status Command::_checkWriteAllow(OperationContext* txn,
                                 const std::string& dbname,
                                 const BSONObj& cmdObj) {
    auto client = txn->getClient();
    invariant(AuthorizationSession::get(client)->getAuthorizationManager().isAuthEnabled());

    if (!serverGlobalParams.readOnly) {
        return Status::OK();
    }

    if (AuthorizationSession::get(client)->isAuthWithCustomer() ||
        (client->isCustomerConnection() &&
         AuthorizationSession::get(client)
             ->isAuthWithCustomerOrNoAuthUser())) {  // check if consumer
        if (globalReadComand.find(_name) == globalReadComand.end()) {
            index_err() << "No storage space commandName: " << _name;
            return Status(ErrorCodes::Unauthorized,
                          str::stream() << getName() << " No storage space to execute: " << _name);
        }
    }

    return Status::OK();
}

static Status _checkAuthorizationImpl(Command* c,
                                      OperationContext* txn,
                                      const std::string& dbname,
                                      const BSONObj& cmdObj) {
    namespace mmb = mutablebson;
    auto client = txn->getClient();
    if (c->adminOnly() && dbname != "admin") {
        return Status(ErrorCodes::Unauthorized,
                      str::stream() << c->getName()
                                    << " may only be run against the admin database.");
    }
    if (AuthorizationSession::get(client)->getAuthorizationManager().isAuthEnabled()) {
        Status status = c->checkAuthForOperation(txn, dbname, cmdObj);
        if (status == ErrorCodes::Unauthorized) {
            mmb::Document cmdToLog(cmdObj, mmb::Document::kInPlaceDisabled);
            c->redactForLogging(&cmdToLog);
            index_err() << "checkAuthForOperation not authorized on: " << dbname 
                << " cmd: " << c->getName();
            return Status(ErrorCodes::Unauthorized,
                          str::stream() << "not authorized on " << dbname << " to execute command "
                                        << cmdToLog.toString());
        }
        if (!status.isOK()) {
            index_err() << "checkAuthForOperation authorized faild cmd: " << c->getName() 
                << "; dbname: " << dbname << "; status: " << status;
            return status;
        }

        status = _checkAuthForUser(c, client, dbname, cmdObj);
        if (status.isOK()) {
            status = c->_checkWriteAllow(txn, dbname, cmdObj);
        } else {
            index_err() << "_checkAuthForUser faild: " << dbname << " cmd: " << c->getName() 
                << "; status: " << status;
        }

        if (!status.isOK()) {
            index_err() << "_checkWriteAllow or _checkAuthForUser faild: " << dbname << 
                " cmd: " << c->getName() << "; status: " << status;
            mmb::Document cmdToLog(cmdObj, mmb::Document::kInPlaceDisabled);
            c->redactForLogging(&cmdToLog);

            if (status == ErrorCodes::CommandNotFound) {
                return Status(ErrorCodes::Unauthorized,
                              str::stream() << "no this command" << cmdToLog.toString());
            } else {
                return Status(ErrorCodes::Unauthorized,
                              str::stream() << "not authorized on " << dbname
                                            << " to execute command "
                                            << cmdToLog.toString());
            }
        }
    } else if (c->adminOnly() && c->localHostOnlyIfNoAuth(cmdObj) &&
               !client->getIsLocalHostConnection()) {
        return Status(ErrorCodes::Unauthorized,
                      str::stream() << c->getName()
                                    << " must run from localhost when running db without auth");
    }
    return Status::OK();
}

/*****modify mongodb code end*****/

Status Command::checkAuthorization(Command* c,
                                   OperationContext* txn,
                                   const std::string& dbname,
                                   const BSONObj& cmdObj) {
    namespace mmb = mutablebson;
    Status status = _checkAuthorizationImpl(c, txn, dbname, cmdObj);
    if (!status.isOK()) {
        log(LogComponent::kAccessControl) << status;
    }
    audit::logCommandAuthzCheck(txn->getClient(), dbname, cmdObj, c, status.code());
    return status;
}

bool Command::isHelpRequest(const BSONElement& helpElem) {
    return !helpElem.eoo() && helpElem.trueValue();
}

const char Command::kHelpFieldName[] = "help";

void Command::generateHelpResponse(OperationContext* txn,
                                   const rpc::RequestInterface& request,
                                   rpc::ReplyBuilderInterface* replyBuilder,
                                   const Command& command) {
    std::stringstream ss;
    BSONObjBuilder helpBuilder;
    ss << "help for: " << command.getName() << " ";
    command.help(ss);
    helpBuilder.append("help", ss.str());

    replyBuilder->setCommandReply(helpBuilder.done());
    replyBuilder->setMetadata(rpc::makeEmptyMetadata());
}

namespace {

void _generateErrorResponse(OperationContext* txn,
                            rpc::ReplyBuilderInterface* replyBuilder,
                            const DBException& exception,
                            const BSONObj& metadata) {
    Command::registerError(txn, exception);

    // We could have thrown an exception after setting fields in the builder,
    // so we need to reset it to a clean state just to be sure.
    replyBuilder->reset();

    // We need to include some extra information for SendStaleConfig.
    if (exception.getCode() == ErrorCodes::SendStaleConfig) {
        const SendStaleConfigException& scex =
            static_cast<const SendStaleConfigException&>(exception);
        replyBuilder->setCommandReply(scex.toStatus(),
                                      BSON("ns" << scex.getns() << "vReceived"
                                                << BSONArray(scex.getVersionReceived().toBSON())
                                                << "vWanted"
                                                << BSONArray(scex.getVersionWanted().toBSON())));
    } else {
        replyBuilder->setCommandReply(exception.toStatus());
    }

    replyBuilder->setMetadata(metadata);
}

}  // namespace

void Command::generateErrorResponse(OperationContext* txn,
                                    rpc::ReplyBuilderInterface* replyBuilder,
                                    const DBException& exception,
                                    const rpc::RequestInterface& request,
                                    Command* command,
                                    const BSONObj& metadata) {
    LOG(1) << "assertion while executing command '" << request.getCommandName() << "' "
           << "on database '" << request.getDatabase() << "' "
           << "with arguments '" << command->getRedactedCopyForLogging(request.getCommandArgs())
           << "' "
           << "and metadata '" << request.getMetadata() << "': " << exception.toString();

    _generateErrorResponse(txn, replyBuilder, exception, metadata);
}

void Command::generateErrorResponse(OperationContext* txn,
                                    rpc::ReplyBuilderInterface* replyBuilder,
                                    const DBException& exception,
                                    const rpc::RequestInterface& request) {
    LOG(1) << "assertion while executing command '" << request.getCommandName() << "' "
           << "on database '" << request.getDatabase() << "': " << exception.toString();

    _generateErrorResponse(txn, replyBuilder, exception, rpc::makeEmptyMetadata());
}

void Command::generateErrorResponse(OperationContext* txn,
                                    rpc::ReplyBuilderInterface* replyBuilder,
                                    const DBException& exception) {
    LOG(1) << "assertion while executing command: " << exception.toString();
    _generateErrorResponse(txn, replyBuilder, exception, rpc::makeEmptyMetadata());
}

bool Command::hideDataBase(const std::string& dbname) const {
    if (!GLOBAL_CONFIG_GET(ShowInternalInfo)) {
        if (dbname == GcDbName) {
            return true;
        } else {
            return false;
        }
    }
    return false;
}

namespace {
const stdx::unordered_set<std::string> userManagementCommands{"createUser",
                                                              "updateUser",
                                                              "dropUser",
                                                              "dropAllUsersFromDatabase",
                                                              "grantRolesToUser",
                                                              "revokeRolesFromUser",
                                                              "createRole",
                                                              "updateRole",
                                                              "dropRole",
                                                              "dropAllRolesFromDatabase",
                                                              "grantPrivilegesToRole",
                                                              "revokePrivilegesFromRole",
                                                              "grantRolesToRole",
                                                              "revokeRolesFromRole",
                                                              "_mergeAuthzCollections",
                                                              "authSchemaUpgrade"};
}  // namespace

bool Command::isUserManagementCommand(const std::string& name) {
    return userManagementCommands.count(name);
}

}  // namespace mongo

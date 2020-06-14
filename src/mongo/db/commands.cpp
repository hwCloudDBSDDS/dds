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
#include "mongo/bson/timestamp.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/client.h"
#include "mongo/db/command_generic_argument.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/curop.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/server_parameters.h"
#include "mongo/rpc/write_concern_error_detail.h"
#include "mongo/s/stale_exception.h"
#include "mongo/util/invariant.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

using logger::LogComponent;

namespace {

const char kWriteConcernField[] = "writeConcern";
const WriteConcernOptions kMajorityWriteConcern(
    WriteConcernOptions::kMajority,
    // Note: Even though we're setting UNSET here, kMajority implies JOURNAL if journaling is
    // supported by the mongod.
    WriteConcernOptions::SyncMode::UNSET,
    Seconds(60));
}  // namespace

/*****modify mongodb code start*****/

namespace {
const std::string CUSTOM_USER = "rwuser@admin";
}  // namespace

/*check if allow commands against admin/config database*/
static bool _checkIfAllowedCommands(const std::string& cmdname) {

    static std::string allowedCommands =
        "\
,isMaster\
,listCollections\
,getLastError\
,resetError\
,logout\
,updateUser\
,authenticate\
,saslStart\
,saslContinue\
,buildInfo\
,serverStatus\
,listDatabases\
,ping\
,";

    if (allowedCommands.find("," + cmdname + ",") != std::string::npos) {
        return true;
    }
    return false;
}

std::set<std::string> Command::globleDisableCommands{"authSchemaUpgrade",
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
                                                     "shardingState",
                                                     "unsetSharding",
                                                     "copydb",
                                                     "clone",
                                                     "clean",
                                                     "connPoolSync",
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
                                                     "replSetResizeOplog"};

bool Command::checkIfDisableCommands(const std::string& cmdname) {
    // below cmds is hw contains, the
    // applyOps, flushRouterConfig, flushrouterconfig, addShard, addshard, cleanupOrphaned
    // checkShardingIndex, listShards, listshards, removeShard, removeshard, getShardMap
    // setShardVersion, shardingState, unsetSharding, dbHash, dbhash
    //
    // below cmds is hw do not contains
    // setReadOnly
    if (Command::globleDisableCommands.find(cmdname) != Command::globleDisableCommands.end() &&
        find(serverGlobalParams.allowCommands.begin(),
             serverGlobalParams.allowCommands.end(),
             cmdname) == serverGlobalParams.allowCommands.end()) {
        return true;
    }
    return false;
}

std::set<std::string> adminDisableCommand{"dropDatabase",
                                          "createIndexes",
                                          "dropIndexes",
                                          "deleteIndexes",
                                          "convertToCapped",
                                          "insert",
                                          "update",
                                          "delete",
                                          "create",
                                          "drop",
                                          "findAndModify",
                                          "findandmodify",
                                          "reIndex"};

static bool _isDisabledCommandsAndPameterForCustomer(const Command* c,
                                                     const std::string& cmdname,
                                                     const std::string& dbname,
                                                     const BSONObj& cmdObj) {

    std::string fullNs = "";
    if (cmdname == "findAndModify" || cmdname == "findandmodify" || cmdname == "group" ||
        cmdname == "mapreduce" || cmdname == "mapReduce" || cmdname == "aggregate") {
        fullNs = c->parseNs(dbname, cmdObj);
        if (fullNs == "admin.system.users" || fullNs == "admin.system.roles") {
            return true;
        }
    }

    if (dbname == "admin" || dbname == "config") {
        if (fullNs == "") {
            fullNs = c->parseNs(dbname, cmdObj);
        }
        // for replica and single mode
        if (fullNs != "admin.system.users" && fullNs != "admin.system.roles" &&
            fullNs != "admin.system.version") {
            if (adminDisableCommand.find(cmdname) != adminDisableCommand.end()) {
                return true;
            }
        }
    }

    return false;
}

static Status _checkAuthForUser(const Command* c,
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
        LOG(4) << "Mongodb consumer run command " << dbname << ".$cmd" << ' ' << cmdname;

        if (Command::checkIfDisableCommands(cmdname)) {
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
                                       "distinct",
                                       "isMaster",
                                       "isdbgrid",
                                       "planCacheListFilters",
                                       "planCacheListPlans",
                                       "planCacheListQueryShapes",
                                       "dropDatabase",
                                       "drop"};
/*****modify mongodb code end*****/

// Returns true if found to be authorized, false if undecided. Throws if unauthorized.
bool checkAuthorizationImplPreParse(OperationContext* opCtx,
                                    const Command* command,
                                    const OpMsgRequest& request) {
    auto client = opCtx->getClient();
    if (client->isInDirectClient())
        return true;
    uassert(ErrorCodes::Unauthorized,
            str::stream() << command->getName() << " may only be run against the admin database.",
            !command->adminOnly() || request.getDatabase() == NamespaceString::kAdminDb);

    auto authzSession = AuthorizationSession::get(client);
    if (!authzSession->getAuthorizationManager().isAuthEnabled()) {
        // Running without auth, so everything should be allowed except remotely invoked
        // commands that have the 'localHostOnlyIfNoAuth' restriction.
        uassert(ErrorCodes::Unauthorized,
                str::stream() << command->getName()
                              << " must run from localhost when running db without auth",
                !command->adminOnly() || !command->localHostOnlyIfNoAuth() ||
                    client->getIsLocalHostConnection());
        return true;  // Blanket authorization: don't need to check anything else.
    }

    if (authzSession->isUsingLocalhostBypass())
        return false;  // Still can't decide on auth because of the localhost bypass.
    uassert(ErrorCodes::Unauthorized,
            str::stream() << "command " << command->getName() << " requires authentication",
            !command->requiresAuth() || authzSession->isAuthenticated());
    return false;
}

//////////////////////////////////////////////////////////////
// CommandHelpers

BSONObj CommandHelpers::runCommandDirectly(OperationContext* opCtx, const OpMsgRequest& request) {
    auto command = globalCommandRegistry()->findCommand(request.getCommandName());
    invariant(command);
    BufBuilder bb;
    CommandReplyBuilder crb(BSONObjBuilder{bb});
    try {
        auto invocation = command->parse(opCtx, request);
        invocation->run(opCtx, &crb);
        auto body = crb.getBodyBuilder();
        CommandHelpers::extractOrAppendOk(body);
    } catch (const StaleConfigException&) {
        // These exceptions are intended to be handled at a higher level.
        throw;
    } catch (const DBException& ex) {
        auto body = crb.getBodyBuilder();
        body.resetToEmpty();
        appendCommandStatusNoThrow(body, ex.toStatus());
    }
    return BSONObj(bb.release());
}

void CommandHelpers::auditLogAuthEvent(OperationContext* opCtx,
                                       const CommandInvocation* invocation,
                                       const OpMsgRequest& request,
                                       ErrorCodes::Error err) {
    class Hook final : public audit::CommandInterface {
    public:
        explicit Hook(const CommandInvocation* invocation, const NamespaceString* nss)
            : _invocation(invocation), _nss(nss) {}

        void redactForLogging(mutablebson::Document* cmdObj) const override {
            if (_invocation) {
                _invocation->definition()->redactForLogging(cmdObj);
            }
        }

        NamespaceString ns() const override {
            return *_nss;
        }

        bool redactArgs() const override {
            return !_invocation;
        }

        const Command* getCommand() const override {
            if (_invocation) {
                return _invocation->definition();
            } else {
                return nullptr;
            }
        }

    private:
        const CommandInvocation* _invocation;
        const NamespaceString* _nss;
    };

    NamespaceString nss = invocation ? invocation->ns() : NamespaceString(request.getDatabase());
    audit::logCommandAuthzCheck(opCtx->getClient(), request, Hook(invocation, &nss), err);
}

void CommandHelpers::uassertNoDocumentSequences(StringData commandName,
                                                const OpMsgRequest& request) {
    uassert(40472,
            str::stream() << "The " << commandName
                          << " command does not support document sequences.",
            request.sequences.empty());
}

std::string CommandHelpers::parseNsFullyQualified(const BSONObj& cmdObj) {
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

NamespaceString CommandHelpers::parseNsCollectionRequired(StringData dbname,
                                                          const BSONObj& cmdObj) {
    // Accepts both BSON String and Symbol for collection name per SERVER-16260
    // TODO(kangas) remove Symbol support in MongoDB 3.0 after Ruby driver audit
    BSONElement first = cmdObj.firstElement();
    uassert(ErrorCodes::InvalidNamespace,
            str::stream() << "collection name has invalid type " << typeName(first.type()),
            first.canonicalType() == canonicalizeBSONType(mongo::String));
    const NamespaceString nss(dbname, first.valueStringData());
    uassert(ErrorCodes::InvalidNamespace,
            str::stream() << "Invalid namespace specified '" << nss.ns() << "'",
            nss.isValid());
    return nss;
}

NamespaceStringOrUUID CommandHelpers::parseNsOrUUID(StringData dbname, const BSONObj& cmdObj) {
    BSONElement first = cmdObj.firstElement();
    if (first.type() == BinData && first.binDataType() == BinDataType::newUUID) {
        return {dbname.toString(), uassertStatusOK(UUID::parse(first))};
    } else {
        // Ensure collection identifier is not a Command
        const NamespaceString nss(parseNsCollectionRequired(dbname, cmdObj));
        uassert(ErrorCodes::InvalidNamespace,
                str::stream() << "Invalid collection name specified '" << nss.ns() << "'",
                nss.isNormal());
        return nss;
    }
}

std::string CommandHelpers::parseNsFromCommand(StringData dbname, const BSONObj& cmdObj) {
    BSONElement first = cmdObj.firstElement();
    if (first.type() != mongo::String)
        return dbname.toString();
    return str::stream() << dbname << '.' << cmdObj.firstElement().valueStringData();
}

ResourcePattern CommandHelpers::resourcePatternForNamespace(const std::string& ns) {
    if (!NamespaceString::validCollectionComponent(ns)) {
        return ResourcePattern::forDatabaseName(ns);
    }
    return ResourcePattern::forExactNamespace(NamespaceString(ns));
}

Command* CommandHelpers::findCommand(StringData name) {
    return globalCommandRegistry()->findCommand(name);
}

bool CommandHelpers::appendCommandStatusNoThrow(BSONObjBuilder& result, const Status& status) {
    appendSimpleCommandStatus(result, status.isOK(), status.reason());
    BSONObj tmp = result.asTempObj();
    if (!status.isOK() && !tmp.hasField("code")) {
        result.append("code", status.code());
        result.append("codeName", ErrorCodes::errorString(status.code()));
    }
    if (auto extraInfo = status.extraInfo()) {
        extraInfo->serialize(&result);
    }
    return status.isOK();
}

void CommandHelpers::appendSimpleCommandStatus(BSONObjBuilder& result,
                                               bool ok,
                                               const std::string& errmsg) {
    BSONObj tmp = result.asTempObj();
    bool have_ok = tmp.hasField("ok");
    bool need_errmsg = !ok && !tmp.hasField("errmsg");

    if (!have_ok)
        result.append("ok", ok ? 1.0 : 0.0);

    if (need_errmsg) {
        result.append("errmsg", errmsg);
    }
}

bool CommandHelpers::extractOrAppendOk(BSONObjBuilder& reply) {
    if (auto okField = reply.asTempObj()["ok"]) {
        // If ok is present, use its truthiness.
        return okField.trueValue();
    }
    // Missing "ok" field is an implied success.
    CommandHelpers::appendSimpleCommandStatus(reply, true);
    return true;
}

void CommandHelpers::appendCommandWCStatus(BSONObjBuilder& result,
                                           const Status& awaitReplicationStatus,
                                           const WriteConcernResult& wcResult) {
    if (!awaitReplicationStatus.isOK() && !result.hasField("writeConcernError")) {
        WriteConcernErrorDetail wcError;
        wcError.setStatus(awaitReplicationStatus);
        if (wcResult.wTimedOut) {
            wcError.setErrInfo(BSON("wtimeout" << true));
        }
        result.append("writeConcernError", wcError.toBSON());
    }
}

BSONObj CommandHelpers::appendPassthroughFields(const BSONObj& cmdObjWithPassthroughFields,
                                                const BSONObj& request) {
    BSONObjBuilder b;
    b.appendElements(request);
    for (const auto& elem : filterCommandRequestForPassthrough(cmdObjWithPassthroughFields)) {
        const auto name = elem.fieldNameStringData();
        if (isGenericArgument(name) && !request.hasField(name)) {
            b.append(elem);
        }
    }
    return b.obj();
}

BSONObj CommandHelpers::appendMajorityWriteConcern(const BSONObj& cmdObj) {
    WriteConcernOptions newWC = kMajorityWriteConcern;

    if (cmdObj.hasField(kWriteConcernField)) {
        auto wc = cmdObj.getField(kWriteConcernField);
        // The command has a writeConcern field and it's majority, so we can
        // return it as-is.
        if (wc["w"].ok() && wc["w"].str() == "majority") {
            return cmdObj;
        }

        if (wc["wtimeout"].ok()) {
            // They set a timeout, but aren't using majority WC. We want to use their
            // timeout along with majority WC.
            newWC = WriteConcernOptions(WriteConcernOptions::kMajority,
                                        WriteConcernOptions::SyncMode::UNSET,
                                        wc["wtimeout"].Number());
        }
    }

    // Append all original fields except the writeConcern field to the new command.
    BSONObjBuilder cmdObjWithWriteConcern;
    for (const auto& elem : cmdObj) {
        const auto name = elem.fieldNameStringData();
        if (name != "writeConcern" && !cmdObjWithWriteConcern.hasField(name)) {
            cmdObjWithWriteConcern.append(elem);
        }
    }

    // Finally, add the new write concern.
    cmdObjWithWriteConcern.append(kWriteConcernField, newWC.toBSON());
    return cmdObjWithWriteConcern.obj();
}

BSONObj CommandHelpers::filterCommandRequestForPassthrough(const BSONObj& cmdObj) {
    BSONObjIterator cmdIter(cmdObj);
    BSONObjBuilder bob;
    filterCommandRequestForPassthrough(&cmdIter, &bob);
    return bob.obj();
}

void CommandHelpers::filterCommandRequestForPassthrough(BSONObjIterator* cmdIter,
                                                        BSONObjBuilder* requestBuilder) {
    while (cmdIter->more()) {
        auto elem = cmdIter->next();
        const auto name = elem.fieldNameStringData();
        if (name == "$readPreference") {
            BSONObjBuilder(requestBuilder->subobjStart("$queryOptions")).append(elem);
            continue;
        }
        if (isRequestStripArgument(name))
            continue;
        requestBuilder->append(elem);
    }
}

void CommandHelpers::filterCommandReplyForPassthrough(const BSONObj& cmdObj,
                                                      BSONObjBuilder* output) {
    for (auto elem : cmdObj) {
        const auto name = elem.fieldNameStringData();
        if (isReplyStripArgument(name))
            continue;
        output->append(elem);
    }
}

BSONObj CommandHelpers::filterCommandReplyForPassthrough(const BSONObj& cmdObj) {
    BSONObjBuilder bob;
    filterCommandReplyForPassthrough(cmdObj, &bob);
    return bob.obj();
}

bool CommandHelpers::isHelpRequest(const BSONElement& helpElem) {
    return !helpElem.eoo() && helpElem.trueValue();
}

bool CommandHelpers::uassertShouldAttemptParse(OperationContext* opCtx,
                                               const Command* command,
                                               const OpMsgRequest& request) {
    try {
        bool result = checkAuthorizationImplPreParse(opCtx, command, request);
        // add hw exter check
        if (false == result) {
            auto dbname = request.getDatabase().toString();
            uassert(ErrorCodes::Unauthorized,
                    str::stream() << command->getName() << " No storage space to execute: "
                                  << command->getName(),
                    command->_checkWriteAllow(opCtx, dbname));
        }
        return result;
    } catch (const ExceptionFor<ErrorCodes::Unauthorized>& e) {
        CommandHelpers::auditLogAuthEvent(opCtx, nullptr, request, e.code());
        throw;
    }
}

constexpr StringData CommandHelpers::kHelpFieldName;

//////////////////////////////////////////////////////////////
// CommandReplyBuilder

CommandReplyBuilder::CommandReplyBuilder(BSONObjBuilder bodyObj)
    : _bodyBuf(&bodyObj.bb()), _bodyOffset(bodyObj.offset()) {
    // CommandReplyBuilder requires that bodyObj build into an externally-owned buffer.
    invariant(!bodyObj.owned());
    bodyObj.doneFast();
}

BSONObjBuilder CommandReplyBuilder::getBodyBuilder() const {
    return BSONObjBuilder(BSONObjBuilder::ResumeBuildingTag{}, *_bodyBuf, _bodyOffset);
}

void CommandReplyBuilder::reset() {
    getBodyBuilder().resetToEmpty();
}

//////////////////////////////////////////////////////////////
// CommandInvocation

CommandInvocation::~CommandInvocation() = default;

void CommandInvocation::checkAuthorization(OperationContext* opCtx,
                                           const OpMsgRequest& request) const {
    // Always send an authorization event to audit log, even if OK.
    // Not using a scope guard because auditLogAuthEvent could conceivably throw.
    try {
        const Command* c = definition();
        if (checkAuthorizationImplPreParse(opCtx, c, request)) {
            // Blanket authorization: don't need to check anything else.
        } else {
            try {
                doCheckAuthorization(opCtx);
            } catch (const ExceptionFor<ErrorCodes::Unauthorized>&) {
                namespace mmb = mutablebson;
                mmb::Document cmdToLog(request.body, mmb::Document::kInPlaceDisabled);
                c->redactForLogging(&cmdToLog);
                auto dbname = request.getDatabase();
                uasserted(ErrorCodes::Unauthorized,
                          str::stream() << "not authorized on " << dbname << " to execute command "
                                        << redact(cmdToLog.getObject()));
            }
            // add hw exter check
            auto client = opCtx->getClient();
            auto dbname = request.getDatabase().toString();
            auto cmdObj = request.body;
            const Command* command = definition();
            uassert(ErrorCodes::Unauthorized,
                    str::stream() << command->getName() << " No storage space to execute: "
                                  << command->getName(),
                    command->_checkWriteAllow(opCtx, dbname));

            Status status = _checkAuthForUser(command, client, dbname, cmdObj);
            if (!status.isOK()) {
                namespace mmb = mutablebson;
                mmb::Document cmdToLog(cmdObj, mutablebson::Document::kInPlaceDisabled);
                command->redactForLogging(&cmdToLog);

                if (status == ErrorCodes::CommandNotFound) {
                    uasserted(ErrorCodes::Unauthorized,
                              str::stream() << "no this command" << cmdToLog.toString());
                } else {
                    uasserted(ErrorCodes::Unauthorized,
                              str::stream() << "not  authorized on " << dbname
                                            << " to execute command "
                                            << cmdToLog.toString());
                }
            }
        }

    } catch (const DBException& e) {
        log(LogComponent::kAccessControl) << e.toStatus();
        CommandHelpers::auditLogAuthEvent(opCtx, this, request, e.code());
        throw;
    }

    CommandHelpers::auditLogAuthEvent(opCtx, this, request, ErrorCodes::OK);
}

//////////////////////////////////////////////////////////////
// Command

class BasicCommand::Invocation final : public CommandInvocation {
public:
    Invocation(OperationContext*, const OpMsgRequest& request, BasicCommand* command)
        : CommandInvocation(command),
          _command(command),
          _request(&request),
          _dbName(_request->getDatabase().toString()) {}

private:
    void run(OperationContext* opCtx, CommandReplyBuilder* result) override {
        try {
            BSONObjBuilder bob = result->getBodyBuilder();
            bool ok = _command->run(opCtx, _dbName, _request->body, bob);
            CommandHelpers::appendSimpleCommandStatus(bob, ok);
        } catch (const ExceptionFor<ErrorCodes::Unauthorized>& e) {
            CommandHelpers::auditLogAuthEvent(opCtx, this, *_request, e.code());
            throw;
        }
    }

    void explain(OperationContext* opCtx,
                 ExplainOptions::Verbosity verbosity,
                 BSONObjBuilder* result) override {
        uassertStatusOK(_command->explain(opCtx, *_request, verbosity, result));
    }

    NamespaceString ns() const override {
        return NamespaceString(_command->parseNs(_dbName, cmdObj()));
    }

    bool supportsWriteConcern() const override {
        return _command->supportsWriteConcern(cmdObj());
    }

    bool supportsReadConcern(repl::ReadConcernLevel level) const override {
        return _command->supportsReadConcern(_dbName, cmdObj(), level);
    }

    bool allowsAfterClusterTime() const override {
        return _command->allowsAfterClusterTime(cmdObj());
    }

    void doCheckAuthorization(OperationContext* opCtx) const override {
        uassertStatusOK(_command->checkAuthForOperation(
            opCtx, _request->getDatabase().toString(), _request->body));
    }

    const BSONObj& cmdObj() const {
        return _request->body;
    }

    BasicCommand* const _command;
    const OpMsgRequest* const _request;
    const std::string _dbName;
};

Command::~Command() = default;

std::unique_ptr<CommandInvocation> BasicCommand::parse(OperationContext* opCtx,
                                                       const OpMsgRequest& request) {
    CommandHelpers::uassertNoDocumentSequences(getName(), request);
    return stdx::make_unique<Invocation>(opCtx, request, this);
}

Command::Command(StringData name, StringData oldName)
    : _name(name.toString()),
      _commandsExecutedMetric("commands." + _name + ".total", &_commandsExecuted),
      _commandsFailedMetric("commands." + _name + ".failed", &_commandsFailed) {
    globalCommandRegistry()->registerCommand(this, name, oldName);
}

Status BasicCommand::explain(OperationContext* opCtx,
                             const OpMsgRequest& request,
                             ExplainOptions::Verbosity verbosity,
                             BSONObjBuilder* out) const {
    return {ErrorCodes::IllegalOperation, str::stream() << "Cannot explain cmd: " << getName()};
}

Status BasicCommand::checkAuthForOperation(OperationContext* opCtx,
                                           const std::string& dbname,
                                           const BSONObj& cmdObj) const {
    return checkAuthForCommand(opCtx->getClient(), dbname, cmdObj);
}

Status BasicCommand::checkAuthForCommand(Client* client,
                                         const std::string& dbname,
                                         const BSONObj& cmdObj) const {
    std::vector<Privilege> privileges;
    this->addRequiredPrivileges(dbname, cmdObj, &privileges);
    if (AuthorizationSession::get(client)->isAuthorizedForPrivileges(privileges))
        return Status::OK();
    return Status(ErrorCodes::Unauthorized, "unauthorized");
}

void Command::generateHelpResponse(OperationContext* opCtx,
                                   rpc::ReplyBuilderInterface* replyBuilder,
                                   const Command& command) {
    BSONObjBuilder helpBuilder;
    helpBuilder.append("help",
                       str::stream() << "help for: " << command.getName() << " " << command.help());
    replyBuilder->setCommandReply(helpBuilder.obj());
    replyBuilder->setMetadata(rpc::makeEmptyMetadata());
}

bool Command::_checkWriteAllow(OperationContext* txn, const std::string& dbname) const {
    auto client = txn->getClient();
    invariant(AuthorizationSession::get(client)->getAuthorizationManager().isAuthEnabled());

    if (!serverGlobalParams.readOnly) {
        return true;
    }

    if (AuthorizationSession::get(client)->isAuthWithCustomer() ||
        (client->isCustomerConnection() &&
         AuthorizationSession::get(client)
             ->isAuthWithCustomerOrNoAuthUser())) {  // check if consumer
        if (globalReadComand.find(_name) == globalReadComand.end()) {
            log() << "No storage space commandName: " << _name;
            return false;
        }
    }
    return true;
}

bool ErrmsgCommandDeprecated::run(OperationContext* opCtx,
                                  const std::string& db,
                                  const BSONObj& cmdObj,
                                  BSONObjBuilder& result) {
    std::string errmsg;
    auto ok = errmsgRun(opCtx, db, cmdObj, errmsg, result);
    if (!errmsg.empty()) {
        CommandHelpers::appendSimpleCommandStatus(result, ok, errmsg);
    }
    return ok;
}

//////////////////////////////////////////////////////////////
// CommandRegistry

void CommandRegistry::registerCommand(Command* command, StringData name, StringData oldName) {
    for (StringData key : {name, oldName}) {
        if (key.empty()) {
            continue;
        }
        auto hashedKey = CommandMap::HashedKey(key);
        auto iter = _commands.find(hashedKey);
        invariant(iter == _commands.end(), str::stream() << "command name collision: " << key);
        _commands[hashedKey] = command;
    }
}

Command* CommandRegistry::findCommand(StringData name) const {
    auto it = _commands.find(name);
    if (it == _commands.end())
        return nullptr;
    return it->second;
}

CommandRegistry* globalCommandRegistry() {
    static auto reg = new CommandRegistry();
    return reg;
}

}  // namespace mongo

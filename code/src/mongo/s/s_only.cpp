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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include <tuple>

#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_global.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/rpc/metadata.h"
#include "mongo/rpc/metadata/tracking_metadata.h"
#include "mongo/rpc/reply_builder_interface.h"
#include "mongo/rpc/request_interface.h"
#include "mongo/s/client/shard_connection.h"
#include "mongo/s/client/version_manager.h"
#include "mongo/s/cluster_last_error_info.h"
#include "mongo/s/commands/strategy.h"
#include "mongo/s/stale_exception.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/concurrency/thread_name.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/util_extend/default_parameters.h"
#include <sys/time.h>
#include <unordered_map>

namespace mongo {

using std::string;
using std::stringstream;

bool isMongos() {
    return true;
}

/**
 * When this callback is run, we record a shard that we've used for useful work in an operation to
 * be read later by getLastError()
 */
void usingAShardConnection(const std::string& addr) {
    ClusterLastErrorInfo::get(cc()).addShardHost(addr);
}

// Get status code from result
int getStatusCodeOfResult(BSONObjBuilder& result, const bool ok) {
    BSONObj tmp = result.asTempObj();
    BSONElement code_el = tmp.getField("code");
    if (!code_el.eoo() && code_el.isNumber()) {
        return code_el.numberInt();
    }

    BSONElement writeError_el = tmp.getField("writeErrors");
    if (writeError_el.eoo() || writeError_el.type() != mongo::Array) {
        return ErrorCodes::OK;
    }

    std::vector<BSONElement> arrElems = writeError_el.Array();
    for (auto& element : arrElems) {
        if (!element.isABSONObj()) {
            return ErrorCodes::OK;
        }

        BSONObj code_obj = element.Obj().getOwned();
        if (code_obj.isEmpty()) {
            continue;
        }

        BSONElement eleCode = code_obj.getField("code");
        if (!eleCode.eoo() && eleCode.isNumber()) {
            return eleCode.numberInt();
        }
    }

    return ErrorCodes::OK;
}

// err code need try
std::map<ErrorCodes::Error, ErrorCodes::Error> g_need_retry_errcode;
// init g_need_retry_errcode
void Command::initNeedRetryCode() {
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::HostUnreachable, ErrorCodes::HostUnreachable));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::HostNotFound, ErrorCodes::HostNotFound));
    // g_need_retry_errcode.insert(std::pair<ErrorCodes::Error,ErrorCodes::Error>(ErrorCodes::FailedToParse,ErrorCodes::FailedToParse));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::LockTimeout, ErrorCodes::LockTimeout));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::LockBusy, ErrorCodes::LockBusy));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::ExceededTimeLimit, ErrorCodes::ExceededTimeLimit));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::ShardNotFound, ErrorCodes::ShardNotFound));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::ReplicaSetNotFound, ErrorCodes::ReplicaSetNotFound));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::NodeNotFound, ErrorCodes::NodeNotFound));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::NetworkTimeout, ErrorCodes::NetworkTimeout));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::ShutdownInProgress, ErrorCodes::ShutdownInProgress));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::NotYetInitialized, ErrorCodes::NotYetInitialized));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::CannotSatisfyWriteConcern, ErrorCodes::CannotSatisfyWriteConcern));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::LockFailed, ErrorCodes::LockFailed));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::OBSOLETE_ReadAfterOptimeTimeout, ErrorCodes::OBSOLETE_ReadAfterOptimeTimeout));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::PrimarySteppedDown, ErrorCodes::PrimarySteppedDown));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::ShardServerNotFound, ErrorCodes::ShardServerNotFound));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::SocketException, ErrorCodes::SocketException));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::RecvStaleConfig, ErrorCodes::RecvStaleConfig));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::NotMaster, ErrorCodes::NotMaster));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::NoPrimary, ErrorCodes::NoPrimary));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::InterruptedAtShutdown, ErrorCodes::InterruptedAtShutdown));
    // g_need_retry_errcode.insert(std::pair<ErrorCodes::Error,ErrorCodes::Error>(ErrorCodes::Interrupted,ErrorCodes::Interrupted));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::InterruptedDueToReplStateChange, ErrorCodes::InterruptedDueToReplStateChange));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::NotMasterOrSecondary, ErrorCodes::NotMasterOrSecondary));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::SendStaleConfig, ErrorCodes::SendStaleConfig));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::NoProgressMade, ErrorCodes::NoProgressMade));

    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::StaleShardVersion, ErrorCodes::StaleShardVersion));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::ChunkNotAssigned, ErrorCodes::ChunkNotAssigned));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::ChunkBusy, ErrorCodes::ChunkBusy));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::StaleEpoch, ErrorCodes::StaleEpoch));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::WriteConcernFailed, ErrorCodes::WriteConcernFailed));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::FailedToSatisfyReadPreference, ErrorCodes::FailedToSatisfyReadPreference));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::SocketError, ErrorCodes::SocketError));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::NoShardingEnabled, ErrorCodes::NoShardingEnabled));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::OperationFailed, ErrorCodes::OperationFailed));
    g_need_retry_errcode.insert(std::pair<ErrorCodes::Error, ErrorCodes::Error>(
        ErrorCodes::ChunkChanged, ErrorCodes::ChunkChanged));
    // g_need_retry_errcode.insert(std::pair<ErrorCodes::Error,ErrorCodes::Error>(ErrorCodes::NoReplicaSetMonitor,ErrorCodes::NoReplicaSetMonitor));
    // g_need_retry_errcode.insert(std::pair<ErrorCodes::Error,ErrorCodes::Error>(ErrorCodes::NoGoodNodeAvailableForSet,ErrorCodes::NoGoodNodeAvailableForSet));
    // g_need_retry_errcode.insert(std::pair<ErrorCodes::Error,ErrorCodes::Error>(ErrorCodes::NoGoodNodeForQuery,ErrorCodes::NoGoodNodeForQuery));
    // g_need_retry_errcode.insert(std::pair<ErrorCodes::Error,ErrorCodes::Error>(ErrorCodes::NoGoodNodeForFindOne,ErrorCodes::NoGoodNodeForFindOne));
    // g_need_retry_errcode.insert(std::pair<ErrorCodes::Error,ErrorCodes::Error>(ErrorCodes::NoGoodNodeForSay,ErrorCodes::NoGoodNodeForSay));
    // g_need_retry_errcode.insert(std::pair<ErrorCodes::Error,ErrorCodes::Error>(ErrorCodes::NoGoodNodeForSelect,ErrorCodes::NoGoodNodeForSelect));
    // g_need_retry_errcode.insert(std::pair<ErrorCodes::Error,ErrorCodes::Error>(ErrorCodes::ReplicaSetMonitorRemoved,ErrorCodes::ReplicaSetMonitorRemoved));
    return;
}

bool isNeedRetryErrCode(const ErrorCodes::Error errCode) {
    auto it = g_need_retry_errcode.find(errCode);
    if (it != g_need_retry_errcode.end()) {
        return true;
    }
    return false;
}

bool isNeedRetryCommand(const Command* command) {
    const std::string name = command->getName();
    if (name == "insert" || name == "update" || name == "delete") {
        return false;
    }
    return true;
}

// called into by the web server. For now we just translate the parameters
// to their old style equivalents.
void Command::execCommand(OperationContext* txn,
                          Command* command,
                          const rpc::RequestInterface& request,
                          rpc::ReplyBuilderInterface* replyBuilder) {
    int queryFlags = 0;
    BSONObj cmdObj;

    std::tie(cmdObj, queryFlags) = uassertStatusOK(
        rpc::downconvertRequestMetadata(request.getCommandArgs(), request.getMetadata()));

    std::string db = request.getDatabase().rawData();
    BSONObjBuilder result;

    execCommandClient(txn, command, queryFlags, request.getDatabase().rawData(), cmdObj, result);

    replyBuilder->setCommandReply(result.done()).setMetadata(rpc::makeEmptyMetadata());
}

void Command::execCommandClient(OperationContext* txn,
                                Command* c,
                                int queryOptions,
                                const char* ns,
                                BSONObj& cmdObj,
                                BSONObjBuilder& result) {
    std::string dbname = nsToDatabase(ns);

    if (cmdObj.getBoolField("help")) {
        stringstream help;
        help << "help for: " << c->getName() << " ";
        c->help(help);
        result.append("help", help.str());
        appendCommandStatus(result, true, "");
        return;
    }

    Status status = checkAuthorization(c, txn, dbname, cmdObj);
    if (!status.isOK()) {
        appendCommandStatus(result, status);
        return;
    }

    c->_commandsExecuted.increment();

    if (c->shouldAffectCommandCounter()) {
        globalOpCounters.gotCommand();
    }

    StatusWith<WriteConcernOptions> wcResult =
        WriteConcernOptions::extractWCFromCommand(cmdObj, dbname);
    if (!wcResult.isOK()) {
        appendCommandStatus(result, wcResult.getStatus());
        return;
    }

    bool supportsWriteConcern = c->supportsWriteConcern(cmdObj);
    if (!supportsWriteConcern && !wcResult.getValue().usedDefault) {
        // This command doesn't do writes so it should not be passed a writeConcern.
        // If we did not use the default writeConcern, one was provided when it shouldn't have
        // been by the user.
        appendCommandStatus(
            result, Status(ErrorCodes::InvalidOptions, "Command does not support writeConcern"));
        return;
    }

    // attach tracking
    rpc::TrackingMetadata trackingMetadata;
    trackingMetadata.initWithOperName(c->getName());
    rpc::TrackingMetadata::get(txn) = trackingMetadata;

    index_LOG(1) << "Command::execCommandClient run command: " << c->getName() << " ns: " << ns;

    int code = ErrorCodes::OK;
    std::string errmsg;
    bool ok = false;
    Milliseconds execTimeUsed(0);
    Date_t initExecTime = Date_t::now();
    unsigned int retryTimes = 0;

    BSONObj cmd_obj = cmdObj;
    do {
        try {

            index_LOG(1) << "[mongos] start run command: " << c->getName() << "; ns: " << ns;
            if (!supportsWriteConcern) {
                ok = c->run(txn, dbname, cmd_obj, queryOptions, errmsg, result);
            } else {
                // Change the write concern while running the command.
                const auto oldWC = txn->getWriteConcern();
                ON_BLOCK_EXIT([&] { txn->setWriteConcern(oldWC); });
                txn->setWriteConcern(wcResult.getValue());

                ok = c->run(txn, dbname, cmd_obj, queryOptions, errmsg, result);
            }

            code = getStatusCodeOfResult(result, ok);

            if (!isNeedRetryCommand(c)) {
                break;
            }
            if (!isNeedRetryErrCode(ErrorCodes::fromInt(code))) {
                break;
            }

            execTimeUsed = Date_t::now() - initExecTime;
            if (kDefaultClientExecCommandMaxRetryTimeout > execTimeUsed) {
                result.resetToEmpty();
                retryTimes++;
                index_warning() << "retry! name: " << c->getName() << ", dbname: " << dbname
                                << ", execTimeUsed: " << execTimeUsed
                                << ", retryTimes: " << retryTimes << ",code: " << code
                                << ", errCode: "
                                << ErrorCodes::errorString(ErrorCodes::fromInt(code)) << ".";
                if (ErrorCodes::fromInt(code) == ErrorCodes::ChunkChanged ||
                    ErrorCodes::fromInt(code) == ErrorCodes::ShardNotFound ||
                    ErrorCodes::fromInt(code) == ErrorCodes::ShardServerNotFound) {
                    throw StaleConfigException(ns,
                                               "chunk has changed",
                                               code,
                                               ChunkVersion(0, 0, OID()),
                                               ChunkVersion(0, 0, OID()));
                }
                if (retryTimes > 1) {
                    stdx::this_thread::sleep_for(
                        kDefaultClientExecCommandRetryInterval.toSteadyDuration());
                }
                BSONObjBuilder cmd_builder;
                if (!cmd_obj.hasField(StringData("isRetry"))) {
                    if (cmd_obj.hasField(StringData("shardCollection"))) {
                        cmd_builder.appendElements(cmd_obj);
                        cmd_builder.append("isRetry", true);
                        cmd_obj = cmd_builder.obj();
                    }
                }
                continue;
            }
        } catch (const StaleConfigException& e) {
            result.resetToEmpty();
            code = e.getCode();
            execTimeUsed = Date_t::now() - initExecTime;
            if (!isNeedRetryErrCode(ErrorCodes::fromInt(code)) ||
                kDefaultClientExecCommandMaxRetryTimeout <= execTimeUsed) {
                index_err() << "fail! name: " << c->getName() << ", dbname: " << dbname
                            << ", execTimeUsed: " << execTimeUsed
                            << ", maxTime: " << kDefaultClientExecCommandMaxRetryTimeout
                            << ", retryTimes: " << retryTimes << ", code: " << code
                            << ", errCode: " << ErrorCodes::errorString(ErrorCodes::fromInt(code))
                            << ", errmsg: " << errmsg << ".";
                throw e;
            }

            errmsg.clear();
            retryTimes++;
            index_warning() << "StaleConfigException, retry! name: " << c->getName()
                            << ", dbname: " << dbname << ", execTimeUsed: " << execTimeUsed
                            << ", retryTimes: " << retryTimes << ", code: " << code
                            << ", errCode: " << ErrorCodes::errorString(ErrorCodes::fromInt(code))
                            << ", errmsg: " << e.what() << ".";

            // means shard has old version and shard need to reload chunkmap, mongos just retry
            if ((e.getVersionReceived() > e.getVersionWanted()) &&
                (e.getVersionWanted().toLong() != 0 ||
                 e.getVersionWanted() == ChunkVersion::UNSHARDED())) {
                if (retryTimes > kDefaultClientExecCommandSleepAfterTimes) {
                    stdx::this_thread::sleep_for(
                        kDefaultClientExecCommandRetryInterval.toSteadyDuration());
                } else {
                    stdx::this_thread::sleep_for(
                        kDefaultClientExecCommandRetryMinInterval.toSteadyDuration());
                }
                continue;
            }

            // for now, means "chunk not on the shard" or "chunk is splitting" , mongos need to
            // reload
            // for get response as soon as possible, we do not sleep at firt 2 times
            if (e.getVersionReceived() == e.getVersionWanted()) {
                if (retryTimes > kDefaultClientExecCommandSleepAfterTimes) {
                    stdx::this_thread::sleep_for(
                        kDefaultClientExecCommandRetryInterval.toSteadyDuration());
                } else {
                    stdx::this_thread::sleep_for(
                        kDefaultClientExecCommandRetryMinInterval.toSteadyDuration());
                }
            }

            // the ns in the exception may include chunkid, we need to remove it
            NamespaceString nss(e.getns());
            string staleNS = nss.nsFilteredOutChunkId();

            // For legacy reasons, ns may not actually be set in the exception :-(
            if (staleNS.size() == 0) {
                nss = parseNsCollectionRequired(dbname, cmd_obj);
                staleNS = nss.ns();
            }

            versionManager.reloadChunkMapIfNeeded(txn, staleNS);
        } catch (const DBException& e) {
            result.resetToEmpty();
            code = e.getCode();
            execTimeUsed = Date_t::now() - initExecTime;
            if (isNeedRetryErrCode(ErrorCodes::fromInt(code)) &&
                kDefaultClientExecCommandMaxRetryTimeout > execTimeUsed) {
                errmsg.clear();
                retryTimes++;
                index_warning() << "exception, retry! name: " << c->getName()
                                << ", dbname: " << dbname << ", execTimeUsed: " << execTimeUsed
                                << ", retryTimes: " << retryTimes << ", code: " << code
                                << ", errCode: "
                                << ErrorCodes::errorString(ErrorCodes::fromInt(code))
                                << ", errmsg: " << e.what() << ".";
                if (retryTimes > 1) {
                    stdx::this_thread::sleep_for(
                        kDefaultClientExecCommandRetryInterval.toSteadyDuration());
                }
                continue;
            }

            errmsg = e.what();
            result.append("code", code);
            break;
        }
    } while (kDefaultClientExecCommandMaxRetryTimeout > execTimeUsed);

    if (!ok && c->getName().find("replSetGetStatus") == std::string::npos) {
        c->_commandsFailed.increment();
        index_err() << "fail! name: " << c->getName() << ", dbname: " << dbname
                    << ", execTimeUsed: " << execTimeUsed
                    << ", maxTime: " << kDefaultClientExecCommandMaxRetryTimeout
                    << ", retryTimes: " << retryTimes << ", code: " << code
                    << ", errCode: " << ErrorCodes::errorString(ErrorCodes::fromInt(code))
                    << ", errmsg: " << errmsg << ".";
    }

    appendCommandStatus(result, ok, errmsg);
}

void Command::registerError(OperationContext* txn, const DBException& exception) {}

}  // namespace mongo

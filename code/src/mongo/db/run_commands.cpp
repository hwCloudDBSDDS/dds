/*    Copyright 2016 10gen Inc.
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
#include <sstream>

#include "mongo/db/run_commands.h"

#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/curop.h"
#include "mongo/rpc/reply_builder_interface.h"
#include "mongo/rpc/request_interface.h"
#include "mongo/util/log.h"

#include "mongo/s/grid.h"
#include "mongo/util/time_support.h"
#include "mongo/util/util_extend/GlobalConfig.h"
#include "mongo/util/util_extend/default_parameters.h"


namespace mongo {

void runCommands(OperationContext* txn,
                 const rpc::RequestInterface& request,
                 rpc::ReplyBuilderInterface* replyBuilder) {

    if (serverGlobalParams.clusterRole == ClusterRole::ShardServer) {
        // Execute command (except those listed below) only when shard server becomes active
        stdx::lock_guard<stdx::mutex> initLock(serverGlobalParams.shardStateMutex);
        if ((serverGlobalParams.shardState != ShardType::ShardState::kShardActive) &&
            !((request.getCommandName() == "isMaster") ||
              (request.getCommandName() == "ismaster") ||
              (request.getCommandName() == "setShardVersion"))) {
            index_log() << "Command (" << request.getCommandName()
                        << ") is not allowed before shard becomes aware";
            uasserted(ErrorCodes::NotYetInitialized, "shard has not become aware");
        }

        if (!grid.initialized() && (request.getCommandName() == "assignChunk")) {
            index_err() << "Grid is not initialized yet";
            uasserted(ErrorCodes::NotYetInitialized, "shard has not become aware");
        }
    } else if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
        if (((getGlobalServiceContext()->getGlobalStorageEngine() == nullptr) ||
             (txn->recoveryUnit() == nullptr))) {
            if (request.getCommandName() == "find" ||
                request.getCommandName() == "replSetInitiate") {
                uasserted(ErrorCodes::NotYetInitialized,
                          "GlobalStorageEngine has not yet been initialized.");
            }
        }
    }

    std::string role = "[ShardSvr]";
    if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
        role = "[ConfigSvr]";
    }

    try {
        dassert(replyBuilder->getState() == rpc::ReplyBuilderInterface::State::kCommandReply);

        Command* c = nullptr;
        // In the absence of a Command object, no redaction is possible. Therefore
        // to avoid displaying potentially sensitive information in the logs,
        // we restrict the log message to the name of the unrecognized command.
        // However, the complete command object will still be echoed to the client.

        index_LOG(1) << " start run command: " << request.getCommandName();

        if (!(c = Command::findCommand(request.getCommandName()))) {
            Command::unknownCommands.increment();
            std::string msg = str::stream() << "no such command: '" << request.getCommandName()
                                            << "'";
            LOG(2) << msg;
            uasserted(ErrorCodes::CommandNotFound,
                      str::stream() << msg << ", bad cmd: '" << redact(request.getCommandArgs())
                                    << "'");
        }

        index_LOG(1) << "run command " << c->getName() << " " << request.getDatabase();

        {
            // Try to set this as early as possible, as soon as we have figured out the command.
            stdx::lock_guard<Client> lk(*txn->getClient());
            CurOp::get(txn)->setLogicalOp_inlock(c->getLogicalOp());
        }

        const auto start = Date_t::now();
        Command::execCommand(txn, c, request, replyBuilder);
        const Milliseconds totalTime = Date_t::now() - start;
        if (totalTime >= Milliseconds(500)) {
            index_LOG(1) << "time-consuming command " << request.getDatabase()
                         << ", total time: " << totalTime;
        }
    }

    catch (const DBException& ex) {
        Command::generateErrorResponse(txn, replyBuilder, ex, request);
    }

    index_LOG(1) << "role: " << role << " end run command: " << request.getCommandName();
}

}  // namespace mongo

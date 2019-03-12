/**
 * Copyright (C) 2016 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/client/remote_command_retry_scheduler.h"
#include "mongo/db/operation_context.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/write_ops/batched_command_request.h"
#include "mongo/s/write_ops/batched_command_response.h"

#include "mongo/util/log.h"

namespace mongo {

using std::string;

namespace {

const int kOnErrorNumRetries = 3;

Status _getEffectiveCommandStatus(StatusWith<Shard::CommandResponse> cmdResponse) {
    // Make sure the command even received a valid response
    if (!cmdResponse.isOK()) {
        return cmdResponse.getStatus();
    }

    // If the request reached the shard, check if the command itself failed.
    if (!cmdResponse.getValue().commandStatus.isOK()) {
        return cmdResponse.getValue().commandStatus;
    }

    // Finally check if the write concern failed
    if (!cmdResponse.getValue().writeConcernStatus.isOK()) {
        return cmdResponse.getValue().writeConcernStatus;
    }

    return Status::OK();
}

}  // namespace

Status Shard::CommandResponse::processBatchWriteResponse(
    StatusWith<Shard::CommandResponse> response, BatchedCommandResponse* batchResponse) {
    auto status = _getEffectiveCommandStatus(response);
    if (status.isOK()) {
        string errmsg;
        if (!batchResponse->parseBSON(response.getValue().response, &errmsg)) {
            status = Status(ErrorCodes::FailedToParse,
                            str::stream() << "Failed to parse write response: " << errmsg);
        } else {
            status = batchResponse->toStatus();
        }
    }

    if (!status.isOK()) {
        batchResponse->clear();
        batchResponse->setErrCode(status.code());
        batchResponse->setErrMessage(status.reason());
        batchResponse->setOk(false);
    }

    return status;
}

const Milliseconds Shard::kDefaultConfigCommandTimeout = Seconds{60};
const Milliseconds Shard::kDefaultCommandTimeout = Seconds{60};

bool Shard::shouldErrorBePropagated(ErrorCodes::Error code) {
    return std::find(RemoteCommandRetryScheduler::kAllRetriableErrors.begin(),
                     RemoteCommandRetryScheduler::kAllRetriableErrors.end(),
                     code) == RemoteCommandRetryScheduler::kAllRetriableErrors.end() &&
        code != ErrorCodes::ExceededTimeLimit;
}

Shard::Shard(const ShardId& id) : _id(id) {}

const ShardId Shard::getId() const {
    return _id;
}

bool Shard::isConfig() const {
    return _id == "config";
}

StatusWith<Shard::CommandResponse> Shard::runCommand(OperationContext* txn,
                                                     const ReadPreferenceSetting& readPref,
                                                     const std::string& dbName,
                                                     const BSONObj& cmdObj,
                                                     RetryPolicy retryPolicy) {
    return runCommand(txn, readPref, dbName, cmdObj, Milliseconds::max(), retryPolicy);
}

StatusWith<Shard::CommandResponse> Shard::runCommand(OperationContext* txn,
                                                     const ReadPreferenceSetting& readPref,
                                                     const std::string& dbName,
                                                     const BSONObj& cmdObj,
                                                     Milliseconds maxTimeMSOverride,
                                                     RetryPolicy retryPolicy) {
    while (true) {
        auto interruptStatus = txn->checkForInterruptNoAssert();
        if (!interruptStatus.isOK()) {
            return interruptStatus;
        }

        auto hostWithResponse = _runCommand(txn, readPref, dbName, maxTimeMSOverride, cmdObj);
        auto swCmdResponse = std::move(hostWithResponse.commandResponse);
        auto commandStatus = _getEffectiveCommandStatus(swCmdResponse);

        if (isRetriableError(commandStatus.code(), retryPolicy)) {
            LOG(2) << "Command " << redact(cmdObj)
                   << " failed with retriable error and will be retried"
                   << causedBy(redact(commandStatus));
            continue;
        }

        return swCmdResponse;
    }
    MONGO_UNREACHABLE;
}

StatusWith<Shard::CommandResponse> Shard::runCommandWithFixedRetryAttempts(
    OperationContext* txn,
    const ReadPreferenceSetting& readPref,
    const std::string& dbName,
    const BSONObj& cmdObj,
    RetryPolicy retryPolicy) {
    return runCommandWithFixedRetryAttempts(
        txn, readPref, dbName, cmdObj, Milliseconds::max(), retryPolicy);
}

StatusWith<Shard::CommandResponse> Shard::runCommandWithFixedRetryAttempts(
    OperationContext* txn,
    const ReadPreferenceSetting& readPref,
    const std::string& dbName,
    const BSONObj& cmdObj,
    Milliseconds maxTimeMSOverride,
    RetryPolicy retryPolicy) {
    for (int retry = 1; retry <= kOnErrorNumRetries; ++retry) {
        auto interruptStatus = txn->checkForInterruptNoAssert();
        if (!interruptStatus.isOK()) {
            index_LOG(1) << "!interruptStatus.isOK() cause by " << interruptStatus;
            return interruptStatus;
        }

        auto hostWithResponse = _runCommand(txn, readPref, dbName, maxTimeMSOverride, cmdObj);
        auto swCmdResponse = std::move(hostWithResponse.commandResponse);
        auto commandStatus = _getEffectiveCommandStatus(swCmdResponse);

        if (retry < kOnErrorNumRetries && isRetriableError(commandStatus.code(), retryPolicy)) {
            LOG(2) << " failed with retriable error and will be retried"
                   << causedBy(redact(commandStatus));
            continue;
        }

        return swCmdResponse;
    }
    MONGO_UNREACHABLE;
}

BatchedCommandResponse Shard::runBatchWriteCommandOnConfig(
    OperationContext* txn, const BatchedCommandRequest& batchRequest, RetryPolicy retryPolicy) {
    invariant(isConfig());

    const std::string dbname = batchRequest.getNS().db().toString();
    invariant(batchRequest.sizeWriteOps() == 1);

    const BSONObj cmdObj = batchRequest.toBSON();

    for (int retry = 1; retry <= kOnErrorNumRetries; ++retry) {
        auto response = _runCommand(txn,
                                    ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                                    dbname,
                                    kDefaultConfigCommandTimeout,
                                    cmdObj);

        BatchedCommandResponse batchResponse;
        Status writeStatus =
            CommandResponse::processBatchWriteResponse(response.commandResponse, &batchResponse);

        if (!writeStatus.isOK() && response.host) {
            updateReplSetMonitor(response.host.get(), writeStatus);
        }

        if (retry < kOnErrorNumRetries && isRetriableError(writeStatus.code(), retryPolicy)) {
            LOG(2) << "Batch write command failed with retriable error and will be retried"
                   << causedBy(redact(writeStatus));
            continue;
        }

        return batchResponse;
    }
    MONGO_UNREACHABLE;
}

StatusWith<Shard::QueryResponse> Shard::exhaustiveFindOnConfig(
    OperationContext* txn,
    const ReadPreferenceSetting& readPref,
    const repl::ReadConcernLevel& readConcernLevel,
    const NamespaceString& nss,
    const BSONObj& query,
    const BSONObj& sort,
    const boost::optional<long long> limit) {
    const auto start = Date_t::now();

    // Do not allow exhaustive finds to be run against regular shards.
    invariant(isConfig());

    for (int retry = 1; retry <= kOnErrorNumRetries; retry++) {
        auto result =
            _exhaustiveFindOnConfig(txn, readPref, readConcernLevel, nss, query, sort, limit);

        if (retry < kOnErrorNumRetries &&
            isRetriableError(result.getStatus().code(), RetryPolicy::kIdempotent)) {
            continue;
        }

        const Milliseconds totalTime = Date_t::now() - start;
        if (totalTime >= Milliseconds(500)) {
            index_warning() << "time-consuming exhaustiveFindOnConfig (namespace: " << nss
                            << ", query: " << query << "), total time: " << totalTime;
        }

        if (result.isOK()) {
            if (ChunkType::ConfigNS == nss.toString()) {
                for (BSONObj& doc : result.getValue().docs) {
                    BSONObj newDoc;
                    fixupNewDoc(doc, newDoc, true);
                    doc.swap(newDoc);
                }
            }
        }

        return result;
    }
    MONGO_UNREACHABLE;
}

void Shard::fixupNewDoc(const BSONObj& doc, BSONObj& newDoc, bool addDataPath) {
    StringData rootFolder(ChunkType::rootFolder.name());
    if (!doc.hasField(rootFolder) || doc.getField(rootFolder).type() != BSONType::String) {
        newDoc = doc.copy();
        return;
    }

    index_LOG(3) << "fixupNewDoc type: " << doc.getField(rootFolder).type();

    std::string newFolder(doc[rootFolder].valuestrsafe());

    if (!addDataPath) {
        if ((newFolder.find(getDataPath()) != std::string::npos)) {
            int dbpathLen = (int)(getDataPath().length());
            newFolder.assign(newFolder, dbpathLen + 1, newFolder.length() - dbpathLen - 1);
        }
    } else {
        if (newFolder.find(getDataPath()) == std::string::npos) {
            newFolder = getDataPath() + "/" + newFolder;
        }
    }

    BSONObjBuilder b;
    BSONObjIterator i(doc);
    while (i.more()) {
        BSONElement e = i.next();
        const char* fname = e.fieldName();
        if (ChunkType::rootFolder.name() != fname) {
            b.append(e);
        } else {
            b.append(rootFolder.toString(), newFolder);
        }
    }

    newDoc = b.obj();
}

}  // namespace mongo

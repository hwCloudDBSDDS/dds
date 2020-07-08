/**
 *    Copyright (C) 2018 MongoDB Inc.
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

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/op_observer.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/service_context.h"
#include "mongo/db/session_catalog.h"
#include "mongo/s/commands/cluster_commands_helpers.h"
#include "mongo/s/grid.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {

class CmdCommitTransaction : public ErrmsgCommandDeprecated {
public:
    CmdCommitTransaction() : ErrmsgCommandDeprecated("commitTransaction") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    virtual bool adminOnly() const {
        return true;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    std::string help() const override {
        return "Cluster Commits a transaction";
    }

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const std::string& dbname,
                                 const BSONObj& cmdObj) const override {
        return Status::OK();
    }


    bool errmsgRun(OperationContext* opCtx,
                   const std::string& dbName,
                   const BSONObj& cmdObj,
                   std::string& errmsg,
                   BSONObjBuilder& output) override {

        auto session = OperationContextSession::get(opCtx);

        uassert(
            ErrorCodes::CommandFailed, "commitTransaction must be run within a session", session);
        uassert(ErrorCodes::CommandFailed,
                "commitTransaction must have shardId",
                session->shardId != "");

        LOG(1) << cmdObj;

        // commitTransaction is retryable.
        if (session->transactionIsCommitted()) {
            log()
                << "Aborting transaction with txnNumber "
                << " on session with lsid "
                << " because it has been running for longer than 'transactionLifetimeLimitSeconds'";
            return true;
        }

        uassert(ErrorCodes::NoSuchTransaction,
                "Transaction isn't in progress",
                session->inMultiDocumentTransaction());

        session->commitTransaction(opCtx);

        return true;
    }

} commitTransaction;

MONGO_FAIL_POINT_DEFINE(pauseAfterTransactionPrepare);

class CmdPrepareTransaction : public ErrmsgCommandDeprecated {
public:
    CmdPrepareTransaction() : ErrmsgCommandDeprecated("prepareTransaction") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    virtual bool adminOnly() const {
        return true;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    std::string help() const override {
        return "Cluster Preprares a transaction. THIS IS A STUB FOR TESTING.";
    }

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const std::string& dbname,
                                 const BSONObj& cmdObj) const override {
        return Status::OK();
    }

    bool errmsgRun(OperationContext* opCtx,
                   const std::string& dbName,
                   const BSONObj& cmdObj,
                   std::string& errmsg,
                   BSONObjBuilder& output) override {
        auto session = OperationContextSession::get(opCtx);
        uassert(
            ErrorCodes::CommandFailed, "prepareTransaction must be run within a session", session);

        return true;
    }
} prepareTransaction;

class CmdAbortTransaction : public ErrmsgCommandDeprecated {
public:
    CmdAbortTransaction() : ErrmsgCommandDeprecated("abortTransaction") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    virtual bool adminOnly() const {
        return true;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    std::string help() const override {
        return "Cluster Aborts a transaction";
    }

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const std::string& dbname,
                                 const BSONObj& cmdObj) const override {
        return Status::OK();
    }

    bool errmsgRun(OperationContext* opCtx,
                   const std::string& dbName,
                   const BSONObj& cmdObj,
                   std::string& errmsg,
                   BSONObjBuilder& output) override {

        LOG(1) << "transaction abort " << cmdObj;
        auto session = OperationContextSession::get(opCtx);
        uassert(
            ErrorCodes::CommandFailed, "abortTransaction must be run within a session", session);

        uassert(ErrorCodes::CommandFailed,
                "abortTransaction must be have sharId",
                session->shardId != "");

        LOG(2) << "CmdAbortTransaction shardId " << session->shardId;

        uassert(ErrorCodes::NoSuchTransaction,
                "abort Transaction isn't in progress",
                session->inMultiDocumentTransaction());

        session->abortActiveTransaction(opCtx);
        return true;
    }

} abortTransaction;

}  // namespace
}  // namespace mongo

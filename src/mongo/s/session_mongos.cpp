/**
 *    Copyright (C) 2017 MongoDB, Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/s/session_mongos.h"

#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/commands/feature_compatibility_version_documentation.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/concurrency/lock_state.h"
#include "mongo/db/concurrency/locker.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop_failpoint_helpers.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/retryable_writes_stats.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/server_transactions_metrics.h"
#include "mongo/db/stats/fill_locker_info.h"
#include "mongo/db/stats/top.h"
#include "mongo/db/transaction_history_iterator.h"
#include "mongo/s/grid.h"
#include "mongo/s/is_mongos.h"
#include "mongo/stdx/memory.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/socket_utils.h"

namespace mongo {

// Server parameter that dictates the lifetime given to each transaction.
// Transactions must eventually expire to preempt storage cache pressure immobilizing the system.
MONGO_EXPORT_SERVER_PARAMETER(transactionLifetimeLimitSeconds, std::int32_t, 60)
    ->withValidator([](const auto& potentialNewValue) {
        if (potentialNewValue < 1) {
            return Status(ErrorCodes::BadValue,
                          "transactionLifetimeLimitSeconds must be greater than or equal to 1s");
        }

        return Status::OK();
    });


namespace {

// The command names that are allowed in a multi-document transaction.
const StringMap<int> txnCmdWhitelist = {{"abortTransaction", 1},
                                        {"aggregate", 1},
                                        {"commitTransaction", 1},
                                        {"delete", 1},
                                        {"distinct", 1},
                                        {"doTxn", 1},
                                        {"find", 1},
                                        {"findandmodify", 1},
                                        {"findAndModify", 1},
                                        {"geoSearch", 1},
                                        {"getMore", 1},
                                        {"insert", 1},
                                        {"killCursors", 1},
                                        {"prepareTransaction", 1},
                                        {"update", 1}};

// The command names that are allowed in a multi-document transaction only when test commands are
// enabled.
const StringMap<int> txnCmdForTestingWhitelist = {{"dbHash", 1}};

// The commands that can be run on the 'admin' database in multi-document transactions.
const StringMap<int> txnAdminCommands = {
    {"abortTransaction", 1}, {"commitTransaction", 1}, {"doTxn", 1}, {"prepareTransaction", 1}};

}  // namespace

SessionMongoS::SessionMongoS(LogicalSessionId sessionId) {
    _sessionId = std::move(sessionId);
}

void SessionMongoS::setCurrentOperation(OperationContext* currentOperation) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(!_currentOperation);
    _currentOperation = currentOperation;
}

void SessionMongoS::clearCurrentOperation() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(_currentOperation);
    _currentOperation = nullptr;
}

BSONObj SessionMongoS::appendTransactionInfo(OperationContext* opCtx,
                                             const ShardId& shardId_,
                                             BSONObj obj) {
    if (!inMultiDocumentTransaction()) {
        return obj;
    }
    if (shardId == "") {
        shardId = shardId_.toString();
    }
    uassert(90436,
            str::stream() << "Cannot use different shardid in a transaction, old:" << shardId
                          << ", new:"
                          << shardId_.toString(),
            shardId == shardId_);

    boost::optional<bool> oldAutoCommit(boost::none);
    boost::optional<bool> oldStartTxn(boost::none);
    BSONObjBuilder bb;
    for (auto e : obj) {
        if (e.fieldNameStringData() == "autocommit") {
            *oldAutoCommit = e.boolean();
            uassert(
                90437, str::stream() << "autocommit should be false in multiDoc txn", !e.boolean());
        } else if (e.fieldNameStringData() == "startTransaction") {
            *oldStartTxn = e.boolean();
        } else {
            bb.append(e);
        }
    }
    bb.append("autocommit", false);
    if (getStartTransaction()) {
        bb.append("startTransaction", true);
    }
    return bb.obj();
}

void SessionMongoS::refreshFromStorageIfNeeded(OperationContext* opCtx) {
    if (opCtx->getClient()->isInDirectClient()) {
        return;
    }

    invariant(!opCtx->lockState()->isLocked());
    invariant(repl::ReadConcernArgs::get(opCtx).getLevel() ==
              repl::ReadConcernLevel::kLocalReadConcern);

    stdx::unique_lock<stdx::mutex> ul(_mutex);

    while (!_isValid) {
        const int numInvalidations = _numInvalidations;

        // Protect against concurrent refreshes or invalidations
        if (!_isValid && _numInvalidations == numInvalidations) {
            _isValid = true;
            break;
        }
    }
}

void SessionMongoS::beginOrContinueTxn(OperationContext* opCtx,
                                       TxnNumber txnNumber,
                                       boost::optional<bool> autocommit,
                                       boost::optional<bool> startTransaction,
                                       StringData dbName,
                                       StringData cmdName) {
    if (opCtx->getClient()->isInDirectClient()) {
        return;
    }

    invariant(!opCtx->lockState()->isLocked());

    uassert(ErrorCodes::OperationNotSupportedInTransaction,
            "Cannot run 'count' in a multi-document transaction. Please see "
            "http://dochub.mongodb.org/core/transaction-count for a recommended alternative.",
            !autocommit || cmdName != "count"_sd);

    uassert(ErrorCodes::OperationNotSupportedInTransaction,
            str::stream() << "Cannot run '" << cmdName << "' in a multi-document transaction.",
            !autocommit || txnCmdWhitelist.find(cmdName) != txnCmdWhitelist.cend() ||
                (getTestCommandsEnabled() &&
                 txnCmdForTestingWhitelist.find(cmdName) != txnCmdForTestingWhitelist.cend()));

    uassert(ErrorCodes::OperationNotSupportedInTransaction,
            str::stream() << "Cannot run command against the '" << dbName
                          << "' database in a transaction",
            !autocommit || (dbName != "config"_sd && dbName != "local"_sd &&
                            (dbName != "admin"_sd ||
                             txnAdminCommands.find(cmdName) != txnAdminCommands.cend())));

    stdx::lock_guard<stdx::mutex> lg(_mutex);
    _beginOrContinueTxn(opCtx, lg, txnNumber, autocommit, startTransaction);
}

void SessionMongoS::_beginOrContinueTxn(OperationContext* opCtx,
                                        WithLock wl,
                                        TxnNumber txnNumber,
                                        boost::optional<bool> autocommit,
                                        boost::optional<bool> startTransaction) {

    _startTransaction = false;
    // Check whether the session information needs to be refreshed from disk.
    _checkValid(wl);

    // Check if the given transaction number is valid for this session. The transaction number must
    // be >= the active transaction number.
    _checkTxnValid(wl, txnNumber);

    //
    // Continue an active transaction.
    //
    if (txnNumber == _activeTxnNumber) {

        // It is never valid to specify 'startTransaction' on an active transaction.
        uassert(ErrorCodes::ConflictingOperationInProgress,
                str::stream() << "Cannot specify 'startTransaction' on transaction " << txnNumber
                              << " since it is already in progress.",
                startTransaction == boost::none);

        // Continue a retryable write.
        if (_txnState == MultiDocumentTransactionState::kNone) {
            uassert(ErrorCodes::InvalidOptions,
                    "Cannot specify 'autocommit' on an operation not inside a multi-statement "
                    "transaction.",
                    autocommit == boost::none);
            return;
        }

        // Continue a multi-statement transaction. In this case, it is required that
        // autocommit=false be given as an argument on the request. Retryable writes will have
        // _autocommit=true, so that is why we verify that _autocommit=false here.
        if (!_autocommit) {
            uassert(
                ErrorCodes::InvalidOptions,
                "Must specify autocommit=false on all operations of a multi-statement transaction.",
                autocommit == boost::optional<bool>(false));
        }
        return;
    }

    //
    // Start a new transaction.
    //
    // At this point, the given transaction number must be > _activeTxnNumber. Existence of an
    // 'autocommit' field means we interpret this operation as part of a multi-document transaction.
    invariant(txnNumber > _activeTxnNumber);
    if (autocommit) {
        // Start a multi-document transaction.
        invariant(*autocommit == false);
        uassert(ErrorCodes::NoSuchTransaction,
                str::stream() << "Given transaction number " << txnNumber
                              << " does not match any in-progress transactions.",
                startTransaction != boost::none);

        // Check for FCV 4.0. The presence of an autocommit field distiguishes this as a
        // multi-statement transaction vs a retryable write.
        uassert(
            90430,
            str::stream() << "Transactions are only supported in featureCompatibilityVersion 4.0. "
                          << "See "
                          << feature_compatibility_version_documentation::kCompatibilityLink
                          << " for more information.",
            (serverGlobalParams.featureCompatibility.isVersionInitialized() &&
             serverGlobalParams.featureCompatibility.getVersion() ==
                 ServerGlobalParams::FeatureCompatibility::Version::kFullyUpgradedTo40));

        _setActiveTxn(opCtx, wl, txnNumber);
        _autocommit = false;
        _txnState = MultiDocumentTransactionState::kInProgress;
        _startTransaction = true;

        const auto now = curTimeMicros64();
        _transactionExpireDate = Date_t::fromMillisSinceEpoch(now / 1000) +
            stdx::chrono::seconds{transactionLifetimeLimitSeconds.load()};
        // Tracks various transactions metrics.
        {
            stdx::lock_guard<stdx::mutex> ls(_statsMutex);
            _singleTransactionStats.setStartTime(now);
            _singleTransactionStats.setExpireDate(*_transactionExpireDate);
            _singleTransactionStats.setAutoCommit(autocommit);
        }
        ServerTransactionsMetrics::get(getGlobalServiceContext())->incrementTotalStarted();
        // The transaction is considered open here and stays inactive until its first unstash event.
        ServerTransactionsMetrics::get(getGlobalServiceContext())->incrementCurrentOpen();
        ServerTransactionsMetrics::get(getGlobalServiceContext())->incrementCurrentInactive();
    } else {
        // Execute a retryable write.
        invariant(startTransaction == boost::none);
        _setActiveTxn(opCtx, wl, txnNumber);
        _autocommit = true;
        _txnState = MultiDocumentTransactionState::kNone;
    }

    invariant(_transactionOperations.empty());
}

void SessionMongoS::_checkTxnValid(WithLock, TxnNumber txnNumber) const {
    uassert(ErrorCodes::TransactionTooOld,
            str::stream() << "Cannot start transaction " << txnNumber << " on session "
                          << getSessionId()
                          << " because a newer transaction "
                          << _activeTxnNumber
                          << " has already started.",
            txnNumber >= _activeTxnNumber);
}


void SessionMongoS::abortArbitraryTransaction(OperationContext* opCtx) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    if (_txnState != MultiDocumentTransactionState::kInProgress) {
        return;
    }

    _abortTransaction(opCtx, lock);
}

void SessionMongoS::abortArbitraryTransactionIfExpired(OperationContext* opCtx) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    if (_txnState != MultiDocumentTransactionState::kInProgress || !_transactionExpireDate ||
        _transactionExpireDate >= Date_t::now()) {
        return;
    }

    if (_currentOperation) {
        // If an operation is still running for this transaction when it expires, kill the currently
        // running operation.
        stdx::lock_guard<Client> clientLock(*_currentOperation->getClient());
        getGlobalServiceContext()->killOperation(_currentOperation, ErrorCodes::ExceededTimeLimit);
    }

    // Log after killing the current operation because jstests may wait to see this log message to
    // imply that the operation has been killed.
    log() << "Aborting transaction with txnNumber " << _activeTxnNumber << " on session with lsid "
          << _sessionId.getId()
          << " because it has been running for longer than 'transactionLifetimeLimitSeconds'";

    _abortTransaction(opCtx, lock);
}

void SessionMongoS::abortActiveTransaction(OperationContext* opCtx) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    if (_txnState != MultiDocumentTransactionState::kInProgress) {
        return;
    }

    _abortTransaction(opCtx, lock);
    {
        stdx::lock_guard<stdx::mutex> ls(_statsMutex);
        // Add the latest operation stats to the aggregate OpDebug object stored in the
        // SingleTransactionStats instance on the Session.
        _singleTransactionStats.getOpDebug()->additiveMetrics.add(
            CurOp::get(opCtx)->debug().additiveMetrics);

        // Update the LastClientInfo object stored in the SingleTransactionStats instance on the
        // Session with this Client's information.
        _singleTransactionStats.updateLastClientInfo(opCtx->getClient());
    }

    // Log the transaction if its duration is longer than the slowMS command threshold.
    _logSlowTransaction(lock,
                        &(opCtx->lockState()->getLockerInfo())->stats,
                        MultiDocumentTransactionState::kAborted,
                        repl::ReadConcernArgs::get(opCtx));
}

void SessionMongoS::_abortTransaction(OperationContext* opCtx, WithLock wl) {
    // TODO SERVER-33432 Disallow aborting committed transaction after we implement implicit abort.
    // A transaction in kCommitting state will either commit or abort for storage-layer reasons; it
    // is too late to abort externally.
    if (_txnState == MultiDocumentTransactionState::kCommitting ||
        _txnState == MultiDocumentTransactionState::kCommitted) {
        return;
    }

    auto curTime = curTimeMicros64();
    if (_txnState == MultiDocumentTransactionState::kInProgress) {
        stdx::lock_guard<stdx::mutex> ls(_statsMutex);
        _singleTransactionStats.setEndTime(curTime);
        if (_singleTransactionStats.isActive()) {
            _singleTransactionStats.setInactive(curTime);
        }
    }

    // If the transaction is stashed, then we have aborted an inactive transaction.
    ServerTransactionsMetrics::get(getGlobalServiceContext())->decrementCurrentActive();

    _transactionOperationBytes = 0;
    _transactionOperations.clear();
    _txnState = MultiDocumentTransactionState::kAborted;
    ServerTransactionsMetrics::get(getGlobalServiceContext())->incrementTotalAborted();
    ServerTransactionsMetrics::get(getGlobalServiceContext())->decrementCurrentOpen();

    Top::get(getGlobalServiceContext())
        .incrementGlobalTransactionLatencyStats(_singleTransactionStats.getDuration(curTime));

    // call shard
    auto const shardRegistry = Grid::get(opCtx)->shardRegistry();
    const auto shardStatus = shardRegistry->getShard(opCtx, shardId);
    uassert(ErrorCodes::CommandFailed, "shardStatus is not OK", shardStatus.isOK());

    // cmdObj ......
    BSONObjBuilder cmdBuilder;
    cmdBuilder.append("abortTransaction", 1.0);
    cmdBuilder.append("autocommit", false);
    cmdBuilder.append("txnNumber", _activeTxnNumber);
    cmdBuilder.append("lsid", _sessionId.toBSON());

    auto response = uassertStatusOK(
        shardStatus.getValue()->runCommandWithFixedRetryAttempts(opCtx,
                                                                 ReadPreferenceSetting::get(opCtx),
                                                                 "admin",
                                                                 cmdBuilder.obj(),
                                                                 Shard::RetryPolicy::kNoRetry));
    uassertStatusOK(response.commandStatus);
}

void SessionMongoS::_setActiveTxn(OperationContext* opCtx, WithLock wl, TxnNumber txnNumber) {
    // Abort the existing transaction if it's not committed or aborted.
    if (_txnState == MultiDocumentTransactionState::kInProgress) {
        _abortTransaction(opCtx, wl);
    }
    _activeTxnNumber = txnNumber;
    _txnState = MultiDocumentTransactionState::kNone;
    {
        stdx::lock_guard<stdx::mutex> ls(_statsMutex);
        _singleTransactionStats = SingleTransactionStats(txnNumber);
    }
    _multikeyPathInfo.clear();
}

void SessionMongoS::commitTransaction(OperationContext* opCtx) {
    stdx::unique_lock<stdx::mutex> lk(_mutex);

    // Always check '_activeTxnNumber' and '_txnState', since they can be modified by session kill
    // and migration, which do not check out the session.
    _checkIsActiveTransaction(lk, *opCtx->getTxnNumber(), true);

    invariant(_txnState != MultiDocumentTransactionState::kCommitted);
    _commitTransaction(std::move(lk), opCtx);

    // call shard
    auto const shardRegistry = Grid::get(opCtx)->shardRegistry();
    const auto shardStatus = shardRegistry->getShard(opCtx, shardId);
    uassert(ErrorCodes::CommandFailed, "shardStatus is not OK", shardStatus.isOK());

    // cmdObj ......
    BSONObjBuilder cmdBuilder;
    cmdBuilder.append("commitTransaction", 1.0);
    cmdBuilder.append("autocommit", false);
    cmdBuilder.append("txnNumber", _activeTxnNumber);
    cmdBuilder.append("lsid", _sessionId.toBSON());

    auto response = uassertStatusOK(
        shardStatus.getValue()->runCommandWithFixedRetryAttempts(opCtx,
                                                                 ReadPreferenceSetting::get(opCtx),
                                                                 "admin",
                                                                 cmdBuilder.obj(),
                                                                 Shard::RetryPolicy::kNoRetry));
    uassertStatusOK(response.commandStatus);
}

void SessionMongoS::_commitTransaction(stdx::unique_lock<stdx::mutex> lk, OperationContext* opCtx) {
    invariant(_txnState == MultiDocumentTransactionState::kInProgress);
    const bool isMultiDocumentTransaction = _txnState == MultiDocumentTransactionState::kInProgress;
    if (isMultiDocumentTransaction) {
        uassert(ErrorCodes::NoSuchTransaction,
                str::stream() << "Transaction " << opCtx->getTxnNumber()
                              << " aborted while attempting to commit",
                _txnState == MultiDocumentTransactionState::kInProgress &&
                    _activeTxnNumber == opCtx->getTxnNumber());
    }
    _txnState = MultiDocumentTransactionState::kCommitting;
    bool committed = false;
    ON_BLOCK_EXIT([this, &committed, opCtx]() {
        // If we're still "committing", the recovery unit failed to commit, and the lock is not
        // held.  We can't safely use _txnState here, as it is protected by the lock.
        if (!committed) {
            stdx::lock_guard<stdx::mutex> lk(_mutex);
            // Make sure the transaction didn't change because of chunk migration.
            if (opCtx->getTxnNumber() == _activeTxnNumber) {
                _txnState = MultiDocumentTransactionState::kAborted;
                ServerTransactionsMetrics::get(getGlobalServiceContext())->decrementCurrentActive();
                // After the transaction has been aborted, we must update the end time and mark it
                // as inactive.
                auto curTime = curTimeMicros64();
                ServerTransactionsMetrics::get(opCtx)->incrementTotalAborted();
                ServerTransactionsMetrics::get(opCtx)->decrementCurrentOpen();
                {
                    stdx::lock_guard<stdx::mutex> ls(_statsMutex);
                    _singleTransactionStats.setEndTime(curTime);
                    if (_singleTransactionStats.isActive()) {
                        _singleTransactionStats.setInactive(curTime);
                    }
                    // Add the latest operation stats to the aggregate OpDebug object stored in the
                    // SingleTransactionStats instance on the Session.
                    _singleTransactionStats.getOpDebug()->additiveMetrics.add(
                        CurOp::get(opCtx)->debug().additiveMetrics);
                    // Update the LastClientInfo object stored in the SingleTransactionStats
                    // instance on the Session with this Client's information.
                    _singleTransactionStats.updateLastClientInfo(opCtx->getClient());
                }

                // Log the transaction if its duration is longer than the slowMS command threshold.
                _logSlowTransaction(lk,
                                    &(opCtx->lockState()->getLockerInfo())->stats,
                                    MultiDocumentTransactionState::kAborted,
                                    repl::ReadConcernArgs::get(opCtx));
            }
        }
        opCtx->lockState()->unsetMaxLockTimeout();
    });
    committed = true;
    _txnState = MultiDocumentTransactionState::kCommitted;
    // After the transaction has been committed, we must update the end time and mark it as
    // inactive.
    ServerTransactionsMetrics::get(opCtx)->incrementTotalCommitted();
    ServerTransactionsMetrics::get(opCtx)->decrementCurrentOpen();
    ServerTransactionsMetrics::get(getGlobalServiceContext())->decrementCurrentActive();
    auto curTime = curTimeMicros64();
    Top::get(getGlobalServiceContext())
        .incrementGlobalTransactionLatencyStats(_singleTransactionStats.getDuration(curTime));

    {
        stdx::lock_guard<stdx::mutex> ls(_statsMutex);
        _singleTransactionStats.setEndTime(curTime);
        if (_singleTransactionStats.isActive()) {
            _singleTransactionStats.setInactive(curTime);
        }

        // Add the latest operation stats to the aggregate OpDebug object stored in the
        // SingleTransactionStats instance on the Session.
        _singleTransactionStats.getOpDebug()->additiveMetrics.add(
            CurOp::get(opCtx)->debug().additiveMetrics);
        // Update the LastClientInfo object stored in the SingleTransactionStats instance on the
        // Session with this Client's information.
        _singleTransactionStats.updateLastClientInfo(opCtx->getClient());
    }

    // Log the transaction if its duration is longer than the slowMS command threshold.
    _logSlowTransaction(lk,
                        &(opCtx->lockState()->getLockerInfo())->stats,
                        MultiDocumentTransactionState::kCommitted,
                        repl::ReadConcernArgs::get(opCtx));
}


std::string SessionMongoS::_transactionInfoForLog(const SingleThreadedLockStats* lockStats,
                                                  MultiDocumentTransactionState terminationCause,
                                                  repl::ReadConcernArgs /* readConcernArgs */) {
    invariant(lockStats);
    invariant(terminationCause == MultiDocumentTransactionState::kCommitted ||
              terminationCause == MultiDocumentTransactionState::kAborted);

    StringBuilder s;

    // User specified transaction parameters.
    BSONObjBuilder parametersBuilder;
    BSONObjBuilder lsidBuilder(parametersBuilder.subobjStart("lsid"));
    _sessionId.serialize(&lsidBuilder);
    lsidBuilder.doneFast();
    parametersBuilder.append("txnNumber", _activeTxnNumber);
    parametersBuilder.append("autocommit", _autocommit);
    // readConcernArgs.appendInfo(&parametersBuilder);
    s << "parameters:" << parametersBuilder.obj().toString() << ",";

    s << _singleTransactionStats.getOpDebug()->additiveMetrics.report();

    std::string terminationCauseString =
        terminationCause == MultiDocumentTransactionState::kCommitted ? "committed" : "aborted";
    s << " terminationCause:" << terminationCauseString;

    auto curTime = curTimeMicros64();
    s << " timeActiveMicros:"
      << durationCount<Microseconds>(_singleTransactionStats.getTimeActiveMicros(curTime));
    s << " timeInactiveMicros:"
      << durationCount<Microseconds>(_singleTransactionStats.getTimeInactiveMicros(curTime));

    // Number of yields is always 0 in multi-document transactions, but it is included mainly to
    // match the format with other slow operation logging messages.
    s << " numYields:" << 0;

    // Aggregate lock statistics.
    BSONObjBuilder locks;
    lockStats->report(&locks);
    s << " locks:" << locks.obj().toString();

    // Total duration of the transaction.
    s << " "
      << Milliseconds{static_cast<long long>(_singleTransactionStats.getDuration(curTime)) / 1000};

    return s.str();
}

void SessionMongoS::_logSlowTransaction(WithLock wl,
                                        const SingleThreadedLockStats* lockStats,
                                        MultiDocumentTransactionState terminationCause,
                                        repl::ReadConcernArgs readConcernArgs) {
    // Only log multi-document transactions.
    if (_txnState != MultiDocumentTransactionState::kNone) {
        // Log the transaction if its duration is longer than the slowMS command threshold.
        if (_singleTransactionStats.getDuration(curTimeMicros64()) >
            serverGlobalParams.slowMS * 1000ULL) {
            log(logger::LogComponent::kTransaction)
                << "transaction "
                << _transactionInfoForLog(lockStats, terminationCause, readConcernArgs);
        }
    }
}

void SessionMongoS::_checkValid(WithLock) const {
    uassert(ErrorCodes::ConflictingOperationInProgress,
            str::stream() << "Session " << getSessionId()
                          << " was concurrently modified and the operation must be retried.",
            _isValid);
}

void SessionMongoS::_checkIsActiveTransaction(WithLock,
                                              TxnNumber txnNumber,
                                              bool checkAbort) const {
    uassert(ErrorCodes::ConflictingOperationInProgress,
            str::stream() << "Cannot perform operations on transaction " << txnNumber
                          << " on session "
                          << getSessionId()
                          << " because a different transaction "
                          << _activeTxnNumber
                          << " is now active.",
            txnNumber == _activeTxnNumber);
    uassert(ErrorCodes::NoSuchTransaction,
            str::stream() << "Transaction " << txnNumber << " has been aborted.",
            !checkAbort || _txnState != MultiDocumentTransactionState::kAborted);
}

std::unique_ptr<Session> Session::makeOwn(LogicalSessionId lsid) {
    return stdx::make_unique<SessionMongoS>(std::move(lsid));
}

}  // namespace mongo

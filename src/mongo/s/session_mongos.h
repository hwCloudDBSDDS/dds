/*
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

#pragma once

#include <boost/optional.hpp>

#include "mongo/base/disallow_copying.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/concurrency/locker.h"
#include "mongo/db/logical_session_id.h"
#include "mongo/db/multi_key_path_tracker.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/ops/update_request.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/session.h"
#include "mongo/db/session_txn_record_gen.h"
#include "mongo/db/single_transaction_stats.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/concurrency/with_lock.h"

namespace mongo {

extern AtomicInt32 transactionLifetimeLimitSeconds;

class OperationContext;

/**
 * A write through cache for the state of a particular session. All modifications to the underlying
 * session transactions collection must be performed through an object of this class.

 * The cache state can be 'up-to-date' (it is in sync with the persistent contents) or 'needs
 * refresh' (in which case refreshFromStorageIfNeeded needs to be called in order to make it
 * up-to-date).
 */
class SessionMongoS : public Session {
    MONGO_DISALLOW_COPYING(SessionMongoS);

public:
    explicit SessionMongoS(LogicalSessionId sessionId);

    /**
     * Blocking method, which loads the transaction state from storage if it has been marked as
     * needing refresh.
     *
     * In order to avoid the possibility of deadlock, this method must not be called while holding a
     * lock.
     */
    void refreshFromStorageIfNeeded(OperationContext* opCtx);

    /**
     * Starts a new transaction on the session, or continues an already active transaction. In this
     * context, a "transaction" is a sequence of operations associated with a transaction number.
     * This sequence of operations could be a retryable write or multi-statement transaction. Both
     * utilize this method.
     *
     * The 'autocommit' argument represents the value of the field given in the original client
     * request. If it is boost::none, no autocommit parameter was passed into the request. Every
     * operation that is part of a multi statement transaction must specify 'autocommit=false'.
     * 'startTransaction' represents the value of the field given in the original client request,
     * and indicates whether this operation is the beginning of a multi-statement transaction.
     *
     * Throws an exception if:
     *      - An attempt is made to start a transaction with number less than the latest
     *        transaction this session has seen.
     *      - The session has been invalidated.
     *      - The values of 'autocommit' and/or 'startTransaction' are inconsistent with the current
     *        state of the transaction.
     *
     * In order to avoid the possibility of deadlock, this method must not be called while holding a
     * lock. This method must also be called after refreshFromStorageIfNeeded has been called.
     */
    void beginOrContinueTxn(OperationContext* opCtx,
                            TxnNumber txnNumber,
                            boost::optional<bool> autocommit,
                            boost::optional<bool> startTransaction,
                            StringData dbName,
                            StringData cmdName);
    /**
     * Similar to beginOrContinueTxn except it is used specifically for shard migrations and does
     * not check or modify the autocommit parameter.
     */
    void beginOrContinueTxnOnMigration(OperationContext* opCtx, TxnNumber txnNumber) {
        MONGO_UNREACHABLE;
    }

    /**
     * Called for speculative transactions to fix the optime of the snapshot to read from.
     */
    void setSpeculativeTransactionOpTime(OperationContext* opCtx,
                                         SpeculativeTransactionOpTime opTimeChoice) {
        MONGO_UNREACHABLE;
    }
    /**
     * Called after a write under the specified transaction completes while the node is a primary
     * and specifies the statement ids which were written. Must be called while the caller is still
     * in the write's WUOW. Updates the on-disk state of the session to match the specified
     * transaction/opTime and keeps the cached state in sync.
     *
     * Must only be called with the session checked-out.
     *
     * Throws if the session has been invalidated or the active transaction number doesn't match.
     */
    void onWriteOpCompletedOnPrimary(OperationContext* opCtx,
                                     TxnNumber txnNumber,
                                     std::vector<StmtId> stmtIdsWritten,
                                     const repl::OpTime& lastStmtIdWriteOpTime,
                                     Date_t lastStmtIdWriteDate) {
        MONGO_UNREACHABLE;
    }

    /**
     * Helper function to begin a migration on a primary node.
     *
     * Returns whether the specified statement should be migrated at all or skipped.
     */
    bool onMigrateBeginOnPrimary(OperationContext* opCtx, TxnNumber txnNumber, StmtId stmtId) {
        MONGO_UNREACHABLE;
    }

    /**
     * Called after an entry for the specified session and transaction has been written to the oplog
     * during chunk migration, while the node is still primary. Must be called while the caller is
     * still in the oplog write's WUOW. Updates the on-disk state of the session to match the
     * specified transaction/opTime and keeps the cached state in sync.
     *
     * May be called concurrently with onWriteOpCompletedOnPrimary or onMigrateCompletedOnPrimary
     * and doesn't require the session to be checked-out.
     *
     * Throws if the session has been invalidated or the active transaction number is newer than the
     * one specified.
     */
    void onMigrateCompletedOnPrimary(OperationContext* opCtx,
                                     TxnNumber txnNumber,
                                     std::vector<StmtId> stmtIdsWritten,
                                     const repl::OpTime& lastStmtIdWriteOpTime,
                                     Date_t oplogLastStmtIdWriteDate) {
        MONGO_UNREACHABLE;
    }

    /**
     * Marks the session as requiring refresh. Used when the session state has been modified
     * externally, such as through a direct write to the transactions table.
     */
    void invalidate() {
        MONGO_UNREACHABLE;
    }

    /**
     * Returns the op time of the last committed write for this session and transaction. If no write
     * has completed yet, returns an empty timestamp.
     *
     * Throws if the session has been invalidated or the active transaction number doesn't match.
     */
    repl::OpTime getLastWriteOpTime(TxnNumber txnNumber) const {
        MONGO_UNREACHABLE;
    }

    /**
     * Checks whether the given statementId for the specified transaction has already executed and
     * if so, returns the oplog entry which was generated by that write. If the statementId hasn't
     * executed, returns boost::none.
     *
     * Must only be called with the session checked-out.
     *
     * Throws if the session has been invalidated or the active transaction number doesn't match.
     */
    boost::optional<repl::OplogEntry> checkStatementExecuted(OperationContext* opCtx,
                                                             TxnNumber txnNumber,
                                                             StmtId stmtId) const {
        MONGO_UNREACHABLE;
    }

    /**
     * Checks whether the given statementId for the specified transaction has already executed
     * without fetching the oplog entry which was generated by that write.
     *
     * Must only be called with the session checked-out.
     *
     * Throws if the session has been invalidated or the active transaction number doesn't match.
     */
    bool checkStatementExecutedNoOplogEntryFetch(TxnNumber txnNumber, StmtId stmtId) const {
        MONGO_UNREACHABLE;
    }

    /**
     * Transfers management of transaction resources from the OperationContext to the Session.
     */
    void stashTransactionResources(OperationContext* opCtx) {
        MONGO_UNREACHABLE;
    }

    /**
     * Transfers management of transaction resources from the Session to the OperationContext.
     */
    void unstashTransactionResources(OperationContext* opCtx, const std::string& cmdName) {
        MONGO_UNREACHABLE;
    }

    /**
     * Commits the transaction, including committing the write unit of work and updating
     * transaction state.
     */
    void commitTransaction(OperationContext* opCtx);

    /**
     * Aborts the transaction outside the transaction, releasing transaction resources.
     */
    void abortArbitraryTransaction(OperationContext* opCtx);

    /**
     * Same as abortArbitraryTransaction, except only executes if _transactionExpireDate indicates
     * that the transaction has expired.
     */
    void abortArbitraryTransactionIfExpired(OperationContext* opCtx);

    /*
     * Aborts the transaction inside the transaction, releasing transaction resources.
     * We're inside the transaction when we have the Session checked out and 'opCtx' owns the
     * transaction resources.
     */
    void abortActiveTransaction(OperationContext* opCtx);


    /**
     * Returns whether we are in a multi-document transaction, which means we have an active
     * transaction which has autoCommit:false and has not been committed or aborted.
     */
    bool inMultiDocumentTransaction() const {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        return _txnState == MultiDocumentTransactionState::kInProgress;
    };

    bool transactionIsCommitted() const {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        return _txnState == MultiDocumentTransactionState::kCommitted;
    }

    bool transactionIsAborted() const {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        return _txnState == MultiDocumentTransactionState::kAborted;
    }

    /**
     * Returns true if we are in an active multi-document transaction or if the transaction has
     * been aborted. This is used to cover the case where a transaction has been aborted, but the
     * OperationContext state has not been cleared yet.
     */
    bool inActiveOrKilledMultiDocumentTransaction() const {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        return (_txnState == MultiDocumentTransactionState::kInProgress ||
                _txnState == MultiDocumentTransactionState::kAborted);
    }

    /**
     * Adds a stored operation to the list of stored operations for the current multi-document
     * (non-autocommit) transaction.  It is illegal to add operations when no multi-document
     * transaction is in progress.
     */
    void addTransactionOperation(OperationContext* opCtx, const repl::ReplOperation& operation) {
        MONGO_UNREACHABLE;
    }

    /**
     * Returns and clears the stored operations for an multi-document (non-autocommit) transaction,
     * and marks the transaction as closed.  It is illegal to attempt to add operations to the
     * transaction after this is called.
     */
    std::vector<repl::ReplOperation> endTransactionAndRetrieveOperations(OperationContext* opCtx) {
        MONGO_UNREACHABLE;
    }

    const std::vector<repl::ReplOperation>& transactionOperationsForTest() {
        MONGO_UNREACHABLE;
    }

    TxnNumber getActiveTxnNumberForTest() const {
        MONGO_UNREACHABLE;
    }

    boost::optional<SingleTransactionStats> getSingleTransactionStats() const {
        MONGO_UNREACHABLE;
    }

    repl::OpTime getSpeculativeTransactionReadOpTimeForTest() const {
        MONGO_UNREACHABLE;
    }

    const Locker* getTxnResourceStashLockerForTest() const {
        MONGO_UNREACHABLE;
    }

    /**
     * If this session is holding stashed locks in _txnResourceStash, reports the current state of
     * the session using the provided builder. Locks the session object's mutex while running.
     */
    void reportStashedState(BSONObjBuilder* builder) const {
        MONGO_UNREACHABLE;
    }

    /**
     * If this session is not holding stashed locks in _txnResourceStash (transaction is active),
     * reports the current state of the session using the provided builder. Locks the session
     * object's mutex while running.
     */
    void reportUnstashedState(repl::ReadConcernArgs readConcernArgs,
                              BSONObjBuilder* builder) const {
        MONGO_UNREACHABLE;
    }

    /**
     * Convenience method which creates and populates a BSONObj containing the stashed state.
     * Returns an empty BSONObj if this session has no stashed resources.
     */
    BSONObj reportStashedState() const {
        MONGO_UNREACHABLE;
    }


    std::string transactionInfoForLogForTest(const SingleThreadedLockStats* lockStats,
                                             bool committed,
                                             repl::ReadConcernArgs readConcernArgs) {
        MONGO_UNREACHABLE;
    }

    void addMultikeyPathInfo(MultikeyPathInfo info) {
        MONGO_UNREACHABLE;
    }

    const std::vector<MultikeyPathInfo>& getMultikeyPathInfo() const {
        MONGO_UNREACHABLE;
    }

    /**
      * Sets the current operation running on this Session.
      */
    void setCurrentOperation(OperationContext* currentOperation);

    /**
     * Clears the current operation running on this Session.
     */
    void clearCurrentOperation();

    BSONObj appendTransactionInfo(OperationContext* opCtx, const ShardId& shardId, BSONObj obj);

    /**
     * Returns a new oplog entry if the given entry has transaction state embedded within in.
     * The new oplog entry will contain the operation needed to replicate the transaction
     * table.
     * Returns boost::none if the given oplog doesn't have any transaction state or does not
     * support update to the transaction table.
     */
    static boost::optional<repl::OplogEntry> createMatchingTransactionTableUpdate(
        const repl::OplogEntry& entry) {
        MONGO_UNREACHABLE;
    }

protected:
    // Holds function which determines whether the CursorManager has client cursor references for a
    // given transaction.

    void _beginOrContinueTxn(OperationContext* opCtx,
                             WithLock,
                             TxnNumber txnNumber,
                             boost::optional<bool> autocommit,
                             boost::optional<bool> startTransaction);

    void _beginOrContinueTxnOnMigration(OperationContext* opCtx, WithLock, TxnNumber txnNumber) {
        MONGO_UNREACHABLE;
    }

    // Checks if there is a conflicting operation on the current Session
    void _checkValid(WithLock) const;

    // Checks that a new txnNumber is higher than the activeTxnNumber so
    // we don't start a txn that is too old.
    void _checkTxnValid(WithLock, TxnNumber txnNumber) const;

    void _setActiveTxn(OperationContext* opCtx, WithLock, TxnNumber txnNumber);

    void _checkIsActiveTransaction(WithLock, TxnNumber txnNumber, bool checkAbort) const;

    boost::optional<repl::OpTime> _checkStatementExecuted(WithLock,
                                                          TxnNumber txnNumber,
                                                          StmtId stmtId) const {
        MONGO_UNREACHABLE;
    }

    // Returns the write date of the last committed write for this session and transaction. If no
    // write has completed yet, returns an empty date.
    //
    // Throws if the session has been invalidated or the active transaction number doesn't match.
    Date_t _getLastWriteDate(WithLock, TxnNumber txnNumber) const {
        MONGO_UNREACHABLE;
    }

    UpdateRequest _makeUpdateRequest(WithLock,
                                     TxnNumber newTxnNumber,
                                     const repl::OpTime& newLastWriteTs,
                                     Date_t newLastWriteDate) const {
        MONGO_UNREACHABLE;
    }

    void _registerUpdateCacheOnCommit(OperationContext* opCtx,
                                      TxnNumber newTxnNumber,
                                      std::vector<StmtId> stmtIdsWritten,
                                      const repl::OpTime& lastStmtIdWriteTs) {
        MONGO_UNREACHABLE;
    }


    // Releases stashed transaction resources to abort the transaction.
    void _abortTransaction(OperationContext* opCtx, WithLock);

    // Committing a transaction first changes its state to "Committing" and writes to the oplog,
    // then it changes the state to "Committed".
    //
    // When a transaction is in "Committing" state, it's not allowed for other threads to change its
    // state (i.e. abort the transaction), otherwise the on-disk state will diverge from the
    // in-memory state.
    // There are 3 cases where the transaction will be aborted.
    // 1) abortTransaction command. Session check-out mechanism only allows one client to access a
    // transaction.
    // 2) killSession, stepdown, transaction timeout and any thread that aborts the transaction
    // outside of session checkout. They can safely skip the committing transactions.
    // 3) Migration. Should be able to skip committing transactions.
    void _commitTransaction(stdx::unique_lock<stdx::mutex> lk, OperationContext* opCtx);


    // Logs the transaction information if it has run slower than the global parameter slowMS. The
    // transaction must be committed or aborted when this function is called.
    void _logSlowTransaction(WithLock wl,
                             const SingleThreadedLockStats* lockStats,
                             MultiDocumentTransactionState terminationCause,
                             repl::ReadConcernArgs readConcernArgs);

    // This method returns a string with information about a slow transaction. The format of the
    // logging string produced should match the format used for slow operation logging. A
    // transaction must be completed (committed or aborted) and a valid LockStats reference must be
    // passed in order for this method to be called.
    std::string _transactionInfoForLog(const SingleThreadedLockStats* lockStats,
                                       MultiDocumentTransactionState terminationCause,
                                       repl::ReadConcernArgs readConcernArgs);

    // Reports transaction stats for both active and inactive transactions using the provided
    // builder.  The lock may be either a lock on _mutex or a lock on _statsMutex.
    void _reportTransactionStats(WithLock wl,
                                 BSONObjBuilder* builder,
                                 repl::ReadConcernArgs readConcernArgs) const {
        MONGO_UNREACHABLE;
    }
};

}  // namespace mongo

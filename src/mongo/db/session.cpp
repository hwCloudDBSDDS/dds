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

#include "mongo/db/session.h"

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
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/retryable_writes_stats.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/server_transactions_metrics.h"
#include "mongo/db/stats/fill_locker_info.h"
#include "mongo/db/stats/top.h"
#include "mongo/db/transaction_history_iterator.h"
#include "mongo/stdx/memory.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/socket_utils.h"

namespace mongo {

boost::optional<repl::OplogEntry> Session::createMatchingTransactionTableUpdate(
    const repl::OplogEntry& entry) {
    auto sessionInfo = entry.getOperationSessionInfo();
    if (!sessionInfo.getTxnNumber()) {
        return boost::none;
    }

    invariant(sessionInfo.getSessionId());
    invariant(entry.getWallClockTime());

    const auto updateBSON = [&] {
        SessionTxnRecord newTxnRecord;
        newTxnRecord.setSessionId(*sessionInfo.getSessionId());
        newTxnRecord.setTxnNum(*sessionInfo.getTxnNumber());
        newTxnRecord.setLastWriteOpTime(entry.getOpTime());
        newTxnRecord.setLastWriteDate(*entry.getWallClockTime());
        return newTxnRecord.toBSON();
    }();

    return repl::OplogEntry(
        entry.getOpTime(),
        0,  // hash
        repl::OpTypeEnum::kUpdate,
        NamespaceString::kSessionTransactionsTableNamespace,
        boost::none,  // uuid
        false,        // fromMigrate
        repl::OplogEntry::kOplogVersion,
        updateBSON,
        BSON(SessionTxnRecord::kSessionIdFieldName << sessionInfo.getSessionId()->toBSON()),
        {},    // sessionInfo
        true,  // upsert
        *entry.getWallClockTime(),
        boost::none,  // statementId
        boost::none,  // prevWriteOpTime
        boost::none,  // preImangeOpTime
        boost::none   // postImageOpTime
        );
}

}  // namespace mongo

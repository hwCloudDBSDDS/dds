/**
*    Copyright (C) 2008 10gen Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/db/commands/copy_data.h"
#include "mongo/platform/basic.h"

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/background.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/collection_catalog_entry.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/document_validation.h"
#include "mongo/db/catalog/drop_collection.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/catalog/index_create.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/index_builder.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/service_context.h"
#include "mongo/util/scopeguard.h"

#include "mongo/db/commands.h"
#include "mongo/db/ops/insert.h"
#include "mongo/util/log.h"

namespace mongo {

using std::unique_ptr;
using std::string;
using std::stringstream;
using std::endl;

Status copyData(OperationContext* txn,
                const NamespaceString& sourceNS,
                const NamespaceString& targetNS,
                string& errmsg,
                BSONObjBuilder& result) {
    NamespaceString sourceNSWithChunkId = ns2chunkHolder().getNsWithChunkId(sourceNS);
    NamespaceString targetNSWithChunkId = ns2chunkHolder().getNsWithChunkId(targetNS);

    ScopedTransaction transaction(txn, MODE_IX);
    AutoGetDb autoDb(txn, sourceNS.db(), MODE_X);

    Database* const sourceDB = autoDb.getDb();

    Collection* const sourceColl =
        sourceDB ? sourceDB->getCollection(sourceNSWithChunkId.ns()) : nullptr;
    if (!sourceColl) {
        return Status(ErrorCodes::NamespaceNotFound, "source namespace does not exist");
    }

    Collection* const targetColl = sourceDB->getCollection(targetNSWithChunkId.ns());
    if (!targetColl) {
        return Status(ErrorCodes::NamespaceNotFound, "target namespace does not exist");
    }
    index_LOG(1) << "copyData sourceNSWithChunkId " << sourceNSWithChunkId
                 << " targetNSWithChunkId " << targetNSWithChunkId;
    BackgroundOperation::assertNoBgOpInProgForNs(sourceNSWithChunkId.ns());
    // if target collection is not empty
    if (targetColl->numRecords(txn) > 0) {
        index_LOG(1) << "truncate collection " << targetNSWithChunkId;
        Status status = targetColl->truncate(txn);
        if (!status.isOK()) {
            return status;
        }
    }

    {
        auto cursor = sourceColl->getCursor(txn);
        while (auto record = cursor->next()) {
            txn->checkForInterrupt();

            const auto obj = record->data.releaseToBson();

            WriteUnitOfWork wunit(txn);
            bool shouldReplicateWrites = txn->writesAreReplicated();
            txn->setReplicatedWrites(false);
            OpDebug* opDebug = nullptr;
            Status status = targetColl->insertDocument(txn, obj, opDebug, true);
            if (!status.isOK()) {
                return status;
            }

            txn->setReplicatedWrites(shouldReplicateWrites);
            wunit.commit();
        }
    }
    return dropCollection(txn, sourceNSWithChunkId, result, false);
}


Status copyDataForCappedCollection(OperationContext* txn,
                                   const NamespaceString& source,
				   const NamespaceString& target,
				   double size,
				   string& errmsg,
				   BSONObjBuilder& result) {
    StringData dbname = source.db();
    
    NamespaceString sourceNS = ns2chunkHolder().getNsWithChunkId(source);
    NamespaceString targetNS = ns2chunkHolder().getNsWithChunkId(target);

    ScopedTransaction transaction(txn, MODE_IX);
    AutoGetDb autoDb(txn, source.db(), MODE_X);

    bool userInitiatedWritesAndNotPrimary = txn->writesAreReplicated() &&
        !repl::getGlobalReplicationCoordinator()->canAcceptWritesFor(sourceNS);

    if (userInitiatedWritesAndNotPrimary) {
        return Status(ErrorCodes::NotMaster,
	    str::stream() << "Not primary while converting " << sourceNS.ns()
	        <<  " to a capped collection");
    }
    
    Database* const db = autoDb.getDb();
    if (!db) {
        return Status(ErrorCodes::NamespaceNotFound,
	    str::stream() << "database " << dbname << " not found");
    }

    BackgroundOperation::assertNoBgOpInProgForDb(dbname);

    const bool shouldReplicateWrites = txn->writesAreReplicated();
    txn->setReplicatedWrites(false);
    ON_BLOCK_EXIT(&OperationContext::setReplicatedWrites, txn, shouldReplicateWrites);

    string fromNs = sourceNS.ns();
    Collection* fromCollection = db->getCollection(fromNs);
    if (!fromCollection) {
        if (db->getViewCatalog()->lookup(txn, fromNs)) {
	    return Status(ErrorCodes::CommandNotSupportedOnView,
	        str::stream() << "cloneCollectionAsCapped not supported for views: "
		    << fromNs);
	}
	return Status(ErrorCodes::NamespaceNotFound,
	     str::stream() << "source collection " << fromNs << " does not exist");
    }
    
    string toNs = targetNS.ns();
    Collection* toCollection = db->getCollection(toNs);
    invariant(toCollection);

    if (toCollection->numRecords(txn) > 0) {
        index_LOG(1) << "truncate collection " << targetNS;
        Status status = toCollection->truncate(txn);
        if (!status.isOK()) {
            return status;
        }
    }
    {
    long long allocatedSpaceGuess =
        std::max(static_cast<long long>(size * 2),
	    static_cast<long long>(toCollection->getRecordStore()->storageSize(txn) * 2));
    
    long long excessSize = fromCollection->dataSize(txn) - allocatedSpaceGuess;

    std::unique_ptr<PlanExecutor> exec(InternalPlanner::collectionScan(
         txn, fromNs, fromCollection, PlanExecutor::YIELD_MANUAL, InternalPlanner::FORWARD));
    
    exec->setYieldPolicy(PlanExecutor::WRITE_CONFLICT_RETRY_ONLY, fromCollection);

    Snapshotted<BSONObj> objToClone;
    RecordId loc;
    PlanExecutor::ExecState state = PlanExecutor::FAILURE;

    DisableDocumentValidation validationDisabler(txn);

    int retries = 0;
    bool end = false;
    while (true) {
        if (!retries) {
	    state = exec->getNextSnapshotted(&objToClone, &loc);
	}

	switch (state) {
	    case PlanExecutor::IS_EOF:
	        end = true;
	        break;
	    case PlanExecutor::ADVANCED: {
	         if (excessSize > 0) {
		     excessSize -= (4 * objToClone.value().objsize());
		     continue;
		 }
		 break;
	    }
	    default:
	        invariant(false);
	}

	if (end) {
	    break;
	}

	try {
	    if (objToClone.snapshotId() != txn->recoveryUnit()->getSnapshotId() &&
	        !fromCollection->findDoc(txn, loc, &objToClone)) {
		retries = 0;
		continue;
	    }

	    WriteUnitOfWork wunit(txn);
	    OpDebug* const nullOpDebug = nullptr;
	    toCollection->insertDocument(
	        txn, objToClone.value(), nullOpDebug, true, txn->writesAreReplicated());
	    wunit.commit();

	    retries = 0;
	} catch (const WriteConflictException& wce) {
	    CurOp::get(txn)->debug().writeConflicts++;
	    retries++;
	    wce.logAndBackoff(retries, "copyDataForCappedCollection", fromNs);

	    exec->saveState();
	    txn->recoveryUnit()->abandonSnapshot();
	    exec->restoreState(); 
	}
    }
    }
    return dropCollection(txn, sourceNS, result, false);
}

class CmdCopyData : public Command {
public:
    CmdCopyData() : Command("copydata") {}

    virtual bool slaveOk() const {
        return false;
    }


    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    virtual std::string parseNs(const std::string& dbname, const BSONObj& cmdObj) const {
        return parseNsFullyQualified(dbname, cmdObj);
    }

    virtual Status checkAuthForCommand(Client* client,
                                       const std::string& dbname,
                                       const BSONObj& cmdObj) {
        std::string ns = parseNs(dbname, cmdObj);

        ActionSet actions;
        actions.addAction(ActionType::insert);
        actions.addAction(ActionType::createIndex);  // SERVER-11418
        if (shouldBypassDocumentValidationForCommand(cmdObj)) {
            actions.addAction(ActionType::bypassDocumentValidation);
        }

        if (!AuthorizationSession::get(client)->isAuthorizedForActionsOnResource(
                ResourcePattern::forExactNamespace(NamespaceString(ns)), actions)) {
            return Status(ErrorCodes::Unauthorized, "Unauthorized");
        }
        return Status::OK();
    }

    virtual void help(stringstream& help) const {
        help << "{ copydata: <collection1>, target: <collection2> }"
                "\nCopy data from collection1 to collection2. "
                "collection1 and collection2 must be on the same shard, and cannot "
                "have more than one chunk.\n";
    }

    virtual bool run(OperationContext* txn,
                     const string& dbname,
                     BSONObj& cmdObj,
                     int,
                     string& errmsg,
                     BSONObjBuilder& result) {
        index_log() << "copyData dbname " << dbname;
        boost::optional<DisableDocumentValidation> maybeDisableValidation;
        if (shouldBypassDocumentValidationForCommand(cmdObj))
            maybeDisableValidation.emplace(txn);

        string target = cmdObj.getStringField("target");
        if (target.empty()) {
            errmsg = "missing 'target' parameter";
            index_err() << "[copydata] missing target";
            return appendCommandStatus(result,
                                       {ErrorCodes::InvalidNamespace, "missing target collection"});
        }

        string collection = cmdObj.getStringField("copydata");
        if (collection.empty()) {
            errmsg = "missing source";
            index_err() << "[copydata] missing source";
            return appendCommandStatus(result,
                                       {ErrorCodes::InvalidNamespace, "missing source collection"});
        }
        Status allowedWriteStatus = userAllowedWriteNS(collection);
        if (!allowedWriteStatus.isOK()) {
            return appendCommandStatus(result, allowedWriteStatus);
        }

	bool capped = cmdObj["capped"].trueValue();
	double size = cmdObj.getField("size").number();

        // support only copy data from the same db
        NamespaceString sourceNS(collection);
        NamespaceString targetNS(target);

        if (sourceNS.db() != targetNS.db()) {
            errmsg = "source and target are not under same database";
            return appendCommandStatus(
                result,
                {ErrorCodes::IllegalOperation, "source and target are not under same database"});
        }
        if (!capped) {
            return appendCommandStatus(result, copyData(txn, sourceNS, targetNS, errmsg, result));
	}
        
	return appendCommandStatus(result, copyDataForCappedCollection(txn, sourceNS, targetNS, size, errmsg, result));
    }


} cmdCopyData;

}  // namespace mongo

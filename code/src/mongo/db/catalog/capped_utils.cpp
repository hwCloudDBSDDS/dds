/**
 *    Copyright (C) 2015 MongoDB Inc.
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

#include "mongo/db/catalog/capped_utils.h"
#include "mongo/platform/basic.h"
#include <string>
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/background.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/collection_catalog_entry.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/document_validation.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/index_builder.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/query/plan_yield_policy.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/balancer/balancer.h"
#include "mongo/db/service_context.h"
#include "mongo/db/views/view.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/util_extend/GlobalConfig.h"
#include "mongo/db/commands/operation_util.h"

namespace mongo {
using std::vector;
using std::string;
const ReadPreferenceSetting kPrimaryOnlyReadPreference{ReadPreference::PrimaryOnly};
Status emptyCappedOnCfgSrv(OperationContext* txn, const NamespaceString& ns) {
    index_log() << "[capped_util.cpp: emptyCappedOnCfgSrv]  on ConfigServer";
    index_log() << "[capped_util.cpp: emptyCappedOnCfgSrv] ns: " << ns.ns();
    vector<ChunkType> chunks;
    uassertStatusOK(
        grid.catalogClient(txn)->getChunks(txn,
                                           BSON(ChunkType::ns(ns.ns())),
                                           BSONObj(),
                                           0,
                                           &chunks,
                                           nullptr,
                                           repl::ReadConcernLevel::kMajorityReadConcern));
    index_LOG(0) << "[drop collection] chunk.size : " << chunks.size();
    std::map<string, BSONObj> errors;
    auto* shardRegistry = grid.shardRegistry();
    for (ChunkType chunk : chunks) {
        BSONObjBuilder builder;
        auto shardStatus = shardRegistry->getShard(txn, chunk.getShard());
        if (!shardStatus.isOK()) {
            index_log() << "[drop collection] get shard fail";
            return shardStatus.getStatus();
        }
        auto dropResult = shardStatus.getValue()->runCommandWithFixedRetryAttempts(
            txn,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            ns.db().toString(),
            BSON("drop" << ns.coll() << "chunkId" << chunk.getName()
                        << WriteConcernOptions::kWriteConcernField
                        << txn->getWriteConcern().toBSON()),
            Shard::RetryPolicy::kIdempotent);
        if (!dropResult.isOK()) {
            return Status(dropResult.getStatus().code(),
                          dropResult.getStatus().reason() + " at " + chunk.getName());
        }
        auto dropStatus = std::move(dropResult.getValue().commandStatus);
        auto wcStatus = std::move(dropResult.getValue().writeConcernStatus);
        if (!dropStatus.isOK() || !wcStatus.isOK()) {
            if (dropStatus.code() == ErrorCodes::NamespaceNotFound && wcStatus.isOK()) {
                continue;
            }
            errors.emplace(chunk.getName(), std::move(dropResult.getValue().response));
        }
    }
    if (!errors.empty()) {
        StringBuilder sb;
        sb << "Dropping collection failed on the following hosts: ";
        for (auto it = errors.cbegin(); it != errors.cend(); ++it) {
            if (it != errors.cbegin()) {
                sb << ", ";
            }
            sb << it->first << ": " << it->second;
        }
        return {ErrorCodes::OperationFailed, sb.str()};
    }
    index_LOG(0) << "dropCollection " << ns << " shard data deleted";
    for (ChunkType chunk : chunks) {

        auto shardId = chunk.getShard();
        auto assignStatus = Balancer::get(txn)->assignChunk(txn, chunk, true, false, shardId);

        if (!assignStatus.isOK()) {
            index_log() << "[CS_SHARDCOLL]assign fail";
            return assignStatus;
        }
    }
    return Status::OK();
}
Status emptyCapped(OperationContext* txn, const NamespaceString& collectionName) {
    if (ClusterRole::ShardServer == serverGlobalParams.clusterRole) {
        auto configshard = Grid::get(txn)->shardRegistry()->getConfigShard();
        auto cmdResponseStatus =
            configshard->runCommand(txn,
                                    kPrimaryOnlyReadPreference,
                                    collectionName.db().toString(),
                                    BSON("emptycapped" << collectionName.coll()),
                                    Shard::RetryPolicy::kIdempotent);
        if (!cmdResponseStatus.isOK()) {
            cmdResponseStatus.getStatus();
        }
        if (cmdResponseStatus.getValue().commandStatus.isOK()) {
            return Status::OK();
        }
    }

    ScopedTransaction scopedXact(txn, MODE_IX);
    AutoGetDb autoDb(txn, collectionName.db(), MODE_X);

    bool userInitiatedWritesAndNotPrimary = txn->writesAreReplicated() &&
        !repl::getGlobalReplicationCoordinator()->canAcceptWritesFor(collectionName);

    if (userInitiatedWritesAndNotPrimary) {
        return Status(ErrorCodes::NotMaster,
                      str::stream() << "Not primary while truncating collection: "
                                    << collectionName.ns());
    }

    Database* db = autoDb.getDb();
    massert(13429, "no such database", db);

    Collection* collection = db->getCollection(collectionName);
    uassert(ErrorCodes::CommandNotSupportedOnView,
            str::stream() << "emptycapped not supported on view: " << collectionName.ns(),
            collection || !db->getViewCatalog()->lookup(txn, collectionName.ns()));
    massert(28584, "no such collection", collection);

    if (collectionName.isSystem() && !collectionName.isSystemDotProfile()) {
        return Status(ErrorCodes::IllegalOperation,
                      str::stream() << "Cannot truncate a system collection: "
                                    << collectionName.ns());
    }
    /*chunk has '$' and will be treated as vitual collection, so cancel the check.*/
    /*
    if (NamespaceString::virtualized(collectionName.ns())) {
        return Status(ErrorCodes::IllegalOperation,
                      str::stream() << "Cannot truncate a virtual collection: "
                                    << collectionName.ns());
    }
    */
    if ((repl::getGlobalReplicationCoordinator()->getReplicationMode() !=
         repl::ReplicationCoordinator::modeNone) &&
        collectionName.isOplog()) {
        return Status(ErrorCodes::OplogOperationUnsupported,
                      str::stream() << "Cannot truncate a live oplog while replicating: "
                                    << collectionName.ns());
    }

    BackgroundOperation::assertNoBgOpInProgForNs(collectionName.ns());

    WriteUnitOfWork wuow(txn);

    Status status = collection->truncate(txn);
    if (!status.isOK()) {
        return status;
    }

    getGlobalServiceContext()->getOpObserver()->onEmptyCapped(txn, collection->ns());

    wuow.commit();

    return Status::OK();
}

Status cloneCollectionAsCapped(OperationContext* txn,
                               Database* db,
                               const std::string& shortFrom,
                               const std::string& shortTo,
                               double size,
                               bool temp) {
    std::string fromNs = db->name() + "." + shortFrom;
    std::string toNs = db->name() + "." + shortTo;

    if (fromNs == "admin.system.users" || fromNs == "admin.system.roles") {
        return Status(ErrorCodes::Unauthorized,
                      str::stream() << "source collection " << fromNs
                                    << " does not allow to cloneCollectionAsCapped");
    }

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

    if (db->getCollection(toNs))
        return Status(ErrorCodes::NamespaceExists, "to collection already exists");

    // create new collection
    MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
        const auto fromOptions =
            fromCollection->getCatalogEntry()->getCollectionOptions(txn).toBSON();
        OldClientContext ctx(txn, toNs);
        BSONObjBuilder spec;
        spec.appendBool("capped", true);
        spec.append("size", size);
        if (temp)
            spec.appendBool("temp", true);
        spec.appendElementsUnique(fromOptions);

        WriteUnitOfWork wunit(txn);
        Status status = userCreateNS(txn, ctx.db(), toNs, spec.done());
        if (!status.isOK())
            return status;
        wunit.commit();
    }
    MONGO_WRITE_CONFLICT_RETRY_LOOP_END(txn, "cloneCollectionAsCapped", fromNs);

    Collection* toCollection = db->getCollection(toNs);
    invariant(toCollection);  // we created above

    // how much data to ignore because it won't fit anyway
    // datasize and extentSize can't be compared exactly, so add some padding to 'size'

    long long allocatedSpaceGuess =
        std::max(static_cast<long long>(size * 2),
                 static_cast<long long>(toCollection->getRecordStore()->storageSize(txn) * 2));

    long long excessSize = fromCollection->dataSize(txn) - allocatedSpaceGuess;

    std::unique_ptr<PlanExecutor> exec(InternalPlanner::collectionScan(
        txn, fromNs, fromCollection, PlanExecutor::YIELD_MANUAL, InternalPlanner::FORWARD));

    exec->setYieldPolicy(PlanExecutor::WRITE_CONFLICT_RETRY_ONLY, fromCollection);

    Snapshotted<BSONObj> objToClone;
    RecordId loc;
    PlanExecutor::ExecState state = PlanExecutor::FAILURE;  // suppress uninitialized warnings

    DisableDocumentValidation validationDisabler(txn);

    int retries = 0;  // non-zero when retrying our last document.
    while (true) {
        if (!retries) {
            state = exec->getNextSnapshotted(&objToClone, &loc);
        }

        switch (state) {
            case PlanExecutor::IS_EOF:
                return Status::OK();
            case PlanExecutor::ADVANCED: {
                if (excessSize > 0) {
                    // 4x is for padding, power of 2, etc...
                    excessSize -= (4 * objToClone.value().objsize());
                    continue;
                }
                break;
            }
            default:
                // Unreachable as:
                // 1) We require a read lock (at a minimum) on the "from" collection
                //    and won't yield, preventing collection drop and PlanExecutor::DEAD
                // 2) PlanExecutor::FAILURE is only returned on PlanStage::FAILURE. The
                //    CollectionScan PlanStage does not have a FAILURE scenario.
                // 3) All other PlanExecutor states are handled above
                invariant(false);
        }

        try {
            // Make sure we are working with the latest version of the document.
            if (objToClone.snapshotId() != txn->recoveryUnit()->getSnapshotId() &&
                !fromCollection->findDoc(txn, loc, &objToClone)) {
                // doc was deleted so don't clone it.
                retries = 0;
                continue;
            }

            WriteUnitOfWork wunit(txn);
            OpDebug* const nullOpDebug = nullptr;
            toCollection->insertDocument(
                txn, objToClone.value(), nullOpDebug, true, txn->writesAreReplicated());
            wunit.commit();

            // Go to the next document
            retries = 0;
        } catch (const WriteConflictException& wce) {
            CurOp::get(txn)->debug().writeConflicts++;
            retries++;  // logAndBackoff expects this to be 1 on first call.
            wce.logAndBackoff(retries, "cloneCollectionAsCapped", fromNs);

            // Can't use WRITE_CONFLICT_RETRY_LOOP macros since we need to save/restore exec
            // around call to abandonSnapshot.
            exec->saveState();
            txn->recoveryUnit()->abandonSnapshot();
            exec->restoreState();  // Handles any WCEs internally.
        }
    }

    invariant(false);  // unreachable
}

Status convertToCapped(OperationContext* txn, const NamespaceString& collectionName, double size) {
    StringData dbname = collectionName.db();
    StringData shortSource = collectionName.coll();

    ScopedTransaction transaction(txn, MODE_IX);
    AutoGetDb autoDb(txn, collectionName.db(), MODE_X);

    bool userInitiatedWritesAndNotPrimary = txn->writesAreReplicated() &&
        !repl::getGlobalReplicationCoordinator()->canAcceptWritesFor(collectionName);

    if (userInitiatedWritesAndNotPrimary) {
        return Status(ErrorCodes::NotMaster,
                      str::stream() << "Not primary while converting " << collectionName.ns()
                                    << " to a capped collection");
    }

    Database* const db = autoDb.getDb();
    if (!db) {
        return Status(ErrorCodes::NamespaceNotFound,
                      str::stream() << "database " << dbname << " not found");
    }

    BackgroundOperation::assertNoBgOpInProgForDb(dbname);

    std::string shortTmpName = str::stream() << "tmp.convertToCapped." << shortSource;
    std::string longTmpName = str::stream() << dbname << "." << shortTmpName;

    if (db->getCollection(longTmpName)) {
        WriteUnitOfWork wunit(txn);
        Status status = db->dropCollection(txn, longTmpName);
        if (!status.isOK())
            return status;
    }


    const bool shouldReplicateWrites = txn->writesAreReplicated();
    txn->setReplicatedWrites(false);
    ON_BLOCK_EXIT(&OperationContext::setReplicatedWrites, txn, shouldReplicateWrites);
    Status status =
        cloneCollectionAsCapped(txn, db, shortSource.toString(), shortTmpName, size, true);

    txn->recoveryUnit()->abandonSnapshot();

    if (!status.isOK()) {
        return status;
    }

    verify(db->getCollection(longTmpName));

    {
        WriteUnitOfWork wunit(txn);
        status = db->dropCollection(txn, collectionName.ns());
        txn->setReplicatedWrites(shouldReplicateWrites);
        if (!status.isOK())
            return status;

        status = db->renameCollection(txn, longTmpName, collectionName.ns(), false);
        if (!status.isOK())
            return status;

        getGlobalServiceContext()->getOpObserver()->onConvertToCapped(
            txn, NamespaceString(collectionName), size);

        wunit.commit();
    }
    return Status::OK();
}


Status createCappedCollection(OperationContext* txn, const NamespaceString& collectionName, double size) {
    Command* createCmd = Command::findCommand("create");
    BSONObjBuilder cmdBuilder;
    cmdBuilder.append("create", collectionName.coll());
    cmdBuilder.append("capped", true);
    cmdBuilder.append("size", size);

    string errmsg;
    BSONObjBuilder result;
    int options = 0;
    bool ret = false;
    BSONObj cmdObj = cmdBuilder.done();
    try {
        ret = createCmd->run(txn, collectionName.db().toString(), cmdObj, options, errmsg, result);
    } catch(const DBException& e) {
        index_err() << "[createCappedCollection] create collection " << collectionName.ns() << " failed, code: " 
	    << static_cast<int>(e.getCode()) << ", msg: " << e.what();
	return Status(ErrorCodes::OperationFailed, "create capped collection failed");
    }
    if (!ret) {
        BSONObj tmp = result.asTempObj();
        BSONElement code_el = tmp.getField("code");
        if (!code_el.eoo() && code_el.isNumber() && code_el.numberInt() == ErrorCodes::BadValue) {
            return Status(ErrorCodes::BadValue,
                          tmp.hasField("errmsg") ? tmp.getStringField("errmsg")
                                                 : "create capped collection failed");
        }
        return Status(ErrorCodes::OperationFailed, "create capped collection failed");
    }

    return Status::OK();

}

Status rollBackConvertOnConfig(OperationContext* txn,
                               NamespaceString& nss) {
    StringData dbname = nss.db();
    StringData shortSource = nss.coll();
        
    std::string shortTmpName = str::stream() << "tmp.convertToCapped." << shortSource;
    std::string longTmpName = str::stream() << dbname << "." << shortTmpName;
    bool sourceCollExist = false;

    auto tmpCollStatus = grid.catalogClient(txn)->getCollection(txn, longTmpName);
    if (tmpCollStatus == ErrorCodes::NamespaceNotFound) {
	return Status::OK();
    }
    else if (!tmpCollStatus.isOK()) {
	return tmpCollStatus.getStatus();
    }

    auto collStatus = grid.catalogClient(txn)->getCollection(txn, nss.ns());
    if (!collStatus.isOK() && collStatus != ErrorCodes::NamespaceNotFound) {
        return collStatus.getStatus();
    }
    else if (collStatus.isOK()) {
	CollectionType coll = collStatus.getValue().value;
	if (!coll.getDropped() && coll.getCreated()) {
	    sourceCollExist = true;
	}
    }
    //check whether source collection exist
    //if source collection exist, just drop temp collection
    if (sourceCollExist) {
	ChunkType chunk;
	string errmsg;
	bool getRet = operationutil::getChunk(txn, nss.ns(), chunk, errmsg);
	if (!getRet) {
	    return Status(ErrorCodes::OperationFailed, "get chunk failed");
	}

	auto assignStatus = 
	    Balancer::get(txn)->assignChunk(txn, chunk, false, true, chunk.getShard());
	if (!assignStatus.isOK()) {
	    index_err() << "[rollBackConvertOnConfig] assignChunk failed, status " << assignStatus.toString();
	    return assignStatus;
	}
        BSONObjBuilder removeResult;
	auto status = operationutil::removeCollAndIndexMetadata(txn, NamespaceString(longTmpName), removeResult);
	if (!status.isOK()) {
	    index_err() << "[rollBackConvertOnConfig] removeCollAndIndexMetadata failed, ns " << longTmpName 
	        << " status " << status.toString();
	    return status;
	}
    }
    //else drop source collection and rename temp collection to source
    else {
        BSONObjBuilder removeResult;
	auto status = operationutil::removeCollAndIndexMetadata(txn, nss, removeResult);
	if (!status.isOK()) {
	    index_err() << "[rollBackConvertOnConfig] removeCollAndIndexMetadata failed, ns " << longTmpName 
	        << " status " << status.toString();
	    return status;
	}
        BSONObjBuilder result;
	string errmsg;
	bool renameRet = operationutil::renameCollection(txn, longTmpName, nss.ns(), errmsg, result);
	if (!renameRet) {
	    index_err() << "[rollBackConvertOnConfig] renameCollection failed, ns " << longTmpName
	        << " target " << nss.ns();
	    return Status(ErrorCodes::OperationFailed, "rename collection failed");
	}
    }
    return Status::OK();
}



bool copyDataOnCappedCollection(OperationContext* txn, 
                                string& source,
				string& target,
				double size,
				string& errmsg,
				BSONObjBuilder& result) {
    index_LOG(1) << "[copyDataOnCappedCollection] source: " << source << ", target: " << target 
        << ",size: " << size;
    NamespaceString sourceNs(source);
    string dbname = sourceNs.db().toString();
    return operationutil::copyData(txn, source, target, true, size, errmsg, result);
}

Status convertToCappedOnConfig(OperationContext* txn, const NamespaceString& collectionName, double size) {
    StringData dbname = collectionName.db();
    StringData shortSource = collectionName.coll();

    auto collStatus = grid.catalogClient(txn)->getCollection(txn, collectionName.ns());
    if (collStatus == ErrorCodes::NamespaceNotFound) {
        return Status(ErrorCodes::NamespaceNotFound,
	    str::stream() << "source collection " << collectionName.ns() << " does not exist");
    }
    else if (!collStatus.isOK()) {
        return collStatus.getStatus();
    }

    CollectionType coll = collStatus.getValue().value;

    if (coll.getTabType() == CollectionType::TableType::kSharded) {
        return Status(ErrorCodes::IllegalOperation,
	    str::stream() << "can't do command: convertToCapped" << " on sharded collection");
    }

    if (coll.getDropped()) {
        return Status(ErrorCodes::NamespaceNotFound,
	    str::stream() << "source collection " << collectionName.ns() << " does not exist");
    }

    //add dis lock 
    /*auto scopedDistLock = Grid::get(txn)->catalogClient(txn)->getDistLockManager()->lock(
        txn, collectionName.ns(), "convertToCapped", DistLockManager::kDefaultLockTimeout);
    if (!scopedDistLock.isOK()) {
        index_err() << "[convertToCappedOnConfig] getDistLock error, ns:" << collectionName.ns();
	return scopedDistLock.getStatus();
    }

    index_LOG(0) << "[convertToCappedOnConfig] getDistLock ns: " << collectionName.ns();
    */
    //create tmp capped collection
    std::string shortTmpName = str::stream() << "tmp.convertToCapped." << shortSource;
    std::string longTmpName = str::stream() << dbname << "." << shortTmpName;
    NamespaceString tmpCollName(longTmpName);
    auto createStatus = createCappedCollection(txn, tmpCollName, size);
    if (!createStatus.isOK()) {
        return createStatus;
    }

    //create indexes on tm capped collection, this is not need for capped collection
    string errmsg;
    BSONObjBuilder result;
    /*bool ret = operationutil::copyIndexes(txn, collectionName.ns(), longTmpName, errmsg, result);
    if (!ret) {
        index_err() << "[convertToCappedOnConfig] create indexes on capped collection failed, errmsg: "
	    << errmsg;
        return Status(ErrorCodes::OperationFailed, "create indexes on capped collection failed");
    }*/
    bool ret = false;

    //insert data into tmp capped collection
    {
        string source = collectionName.ns();
        ret = copyDataOnCappedCollection(txn, source, longTmpName, size, errmsg, result);
        if (!ret) {
            index_err() << "[convertToCappedOnConfig] copy data to capped collection failed";
            return Status(ErrorCodes::OperationFailed, "copy data to capped collection failed");
        }
    }
    
    //rename tmp cappped collection to source collection
    {
	string errmsg;
        BSONObjBuilder result;
        bool ret = operationutil::renameCollection(txn, longTmpName, collectionName.ns(), errmsg, result);
	if (!ret) {
	    index_err() << "[convertToCappedOnConfig] copy data to capped collection failed, errmsg: "
	        << errmsg;
	    return Status(ErrorCodes::OperationFailed, "rename capped collection failed");
	}
    }

    return Status::OK();
}
}  // namespace mongo

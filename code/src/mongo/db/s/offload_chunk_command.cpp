#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding



#include "mongo/platform/basic.h"

#include "mongo/client/remote_command_targeter.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/background.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/document_validation.h"
#include "mongo/db/catalog/drop_collection.h"
#include "mongo/db/commands.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/modules/rocks/src/gc_common.h"
#include "mongo/db/modules/rocks/src/gc_manager.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/range_deleter_service.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/chunk_move_write_concern_options.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/s/catalog/dist_lock_manager.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/offload_chunk_request.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"


namespace mongo {

using std::string;

namespace {

class OffloadChunkCommand : public Command {
public:
    OffloadChunkCommand() : Command("offloadChunk") {}

    void help(std::stringstream& help) const override {
        help << "should not be calling this directly";
    }

    bool slaveOk() const override {
        return false;
    }

    bool adminOnly() const override {
        return true;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    Status checkAuthForCommand(Client* client,
                               const string& dbname,
                               const BSONObj& cmdObj) override {
        if (!AuthorizationSession::get(client)->isAuthorizedForActionsOnResource(
                ResourcePattern::forClusterResource(), ActionType::internal)) {
            return Status(ErrorCodes::Unauthorized, "Unauthorized");
        }
        return Status::OK();
    }

    string parseNs(const string& dbname, const BSONObj& cmdObj) const override {
        return parseNsFullyQualified(dbname, cmdObj);
    }

    Status renameCollectionCheck(OperationContext* txn,
                                 const NamespaceString& source,
                                 const NamespaceString& target,
                                 bool dropTarget,
                                 bool stayTemp) {
        DisableDocumentValidation validationDisabler(txn);

        ScopedTransaction transaction(txn, MODE_X);
        boost::optional<Lock::GlobalWrite> globalWriteLock;
        boost::optional<Lock::DBLock> dbWriteLock;

        if (source.db() == target.db())
            dbWriteLock.emplace(txn->lockState(), source.db(), MODE_X);
        else
            globalWriteLock.emplace(txn->lockState());

        // We stay in source context the whole time. This is mostly to set the CurOp namespace.
        OldClientContext ctx(txn, source.ns());

        bool userInitiatedWritesAndNotPrimary = txn->writesAreReplicated() &&
            !repl::getGlobalReplicationCoordinator()->canAcceptWritesFor(source);

        if (userInitiatedWritesAndNotPrimary) {
            return Status(ErrorCodes::NotMaster,
                          str::stream() << "Not primary while renaming collection " << source.ns()
                                        << "to "
                                        << target.ns());
        }

        Database* const sourceDB = dbHolder().get(txn, source.db());
        Collection* const sourceColl = sourceDB ? sourceDB->getCollection(source.ns()) : nullptr;
        if (!sourceColl) {
            if (sourceDB && sourceDB->getViewCatalog()->lookup(txn, source.ns()))
                return Status(ErrorCodes::CommandNotSupportedOnView,
                              str::stream() << "cannot rename view: " << source.ns());
            return Status(ErrorCodes::NamespaceNotFound, "source namespace does not exist");
        }

        // Make sure the source collection is not sharded.
        /*if (CollectionShardingState::get(txn, source)->getMetadata()) {
            return {ErrorCodes::IllegalOperation, "source namespace cannot be sharded"};
        }*/

        {
            // Ensure that collection name does not exceed maximum length.
            // Ensure that index names do not push the length over the max.
            // Iterator includes unfinished indexes.
            IndexCatalog::IndexIterator sourceIndIt =
                sourceColl->getIndexCatalog()->getIndexIterator(txn, true);
            int longestIndexNameLength = 0;
            while (sourceIndIt.more()) {
                int thisLength = sourceIndIt.next()->indexName().length();
                if (thisLength > longestIndexNameLength)
                    longestIndexNameLength = thisLength;
            }

            unsigned int longestAllowed = std::min(int(NamespaceString::MaxNsCollectionLen),
                                                   int(NamespaceString::MaxNsLen) -
                                                       2 /*strlen(".$")*/ - longestIndexNameLength);
            if (target.size() > longestAllowed) {
                StringBuilder sb;
                sb << "collection name length of " << target.size() << " exceeds maximum length of "
                   << longestAllowed << ", allowing for index names";
                return Status(ErrorCodes::InvalidLength, sb.str());
            }
        }

        BackgroundOperation::assertNoBgOpInProgForNs(source.ns());
        Database* const targetDB = dbHolder().openDb(txn, target.db());

        {
            if (targetDB->getCollection(target)) {
                if (!dropTarget) {
                    index_err() << "[renameCollection] target namespace " << target.ns()
                                << " exists.";
                    return Status(ErrorCodes::NamespaceExists, "target namespace exists");
                }
            } else if (targetDB->getViewCatalog()->lookup(txn, target.ns())) {
                return Status(ErrorCodes::NamespaceExists,
                              str::stream() << "a view already exists with that name: "
                                            << target.ns());
            }
        }

        return Status::OK();
    }

    bool doRenameCollectionCheck(OperationContext* txn,
                                 const NamespaceString& sourceNS,
                                 const NamespaceString& targetNS,
                                 bool dropTarget,
                                 bool stayTemp,
                                 string& errmsg,
                                 BSONObjBuilder& result) {
        string source = sourceNS.ns();
        string target = targetNS.ns();
        if (!NamespaceString::validCollectionComponent(target.c_str())) {
            errmsg = "invalid collection name: " + target;
            return false;
        }
        if (source.empty() || target.empty()) {
            errmsg = "invalid command syntax";
            return false;
        }

        if ((repl::getGlobalReplicationCoordinator()->getReplicationMode() !=
             repl::ReplicationCoordinator::modeNone)) {
            if (NamespaceString(source).isOplog()) {
                errmsg = "can't rename live oplog while replicating";
                return false;
            }
            if (NamespaceString(target).isOplog()) {
                errmsg = "can't rename to live oplog while replicating";
                return false;
            }
        }

        if (NamespaceString::oplog(source) != NamespaceString::oplog(target)) {
            errmsg = "If either the source or target of a rename is an oplog name, both must be";
            return false;
        }

        Status sourceStatus = userAllowedWriteNS(source);
        if (!sourceStatus.isOK()) {
            errmsg = "error with source namespace: " + sourceStatus.reason();
            return false;
        }

        Status targetStatus = userAllowedWriteNS(target);
        if (!targetStatus.isOK()) {
            errmsg = "error with target namespace: " + targetStatus.reason();
            return false;
        }

        if (NamespaceString(source).coll() == "system.indexes" ||
            NamespaceString(target).coll() == "system.indexes") {
            errmsg = "renaming system.indexes is not allowed";
            return false;
        }

        return appendCommandStatus(
            result, renameCollectionCheck(txn, sourceNS, targetNS, dropTarget, stayTemp));
    }

    bool run(OperationContext* txn,
             const string& dbname,
             BSONObj& cmdObj,
             int,
             string& errmsg,
             BSONObjBuilder& result) override {
        if (serverGlobalParams.clusterRole != ClusterRole::ShardServer) {
            return appendCommandStatus(result,
                                       {ErrorCodes::IllegalOperation, "can only be run on shard"});
        }

        const OffloadChunkRequest offloadChunkRequest =
            uassertStatusOK(OffloadChunkRequest::createFromCommand(cmdObj));
        bool moveChunk = cmdObj["moveChunk"].trueValue();
        bool renameCollection = cmdObj["renameCollection"].trueValue();

        // to generate the collection name for a given chunk
        NamespaceString nss(StringData(offloadChunkRequest.getNss()));

        // create new collection name: ns$chunkID
        NamespaceString nss_with_chunkID(
            StringData(nss.ns() + '$' + offloadChunkRequest.getName()));

        index_LOG(0) << "start offloadChunk: " << nss_with_chunkID;

        {  // db and chunk should be exist in offload flow.
            ScopedTransaction transaction(txn, MODE_IS);
            Lock::DBLock dbLock(txn->lockState(), nss_with_chunkID.db(), MODE_IS);
            auto db = dbHolder().get(txn, nss_with_chunkID.db());
            if (!db) {
                index_err() << "db not exist, offload chunk failed.";
                return appendCommandStatus(result, {ErrorCodes::NamespaceNotFound, "ns not found"});
                // return appendCommandStatus(result,{ErrorCodes::StaleShardVersion,"db not exist
                // when offload chunk"});
            }

            Collection* coll = db->getCollection(nss_with_chunkID, true);
            if (!coll) {
                index_err() << "collection not exist, offload chunk failed.";
                return appendCommandStatus(result, {ErrorCodes::NamespaceNotFound, "ns not found"});
                // return appendCommandStatus(result,{ErrorCodes::StaleShardVersion,"chunk not exist
                // when offload chunk"});
            }
        }

        auto repl_coordinator = repl::getGlobalReplicationCoordinator();
        if (repl_coordinator == nullptr) {
            index_err() << "getGlobalReplicationCoordinator failed.";
            return appendCommandStatus(
                result,
                {ErrorCodes::InternalError,
                 str::stream() << "getGlobalReplicationCoordinator failed."});
        }

        if ((repl_coordinator->getReplicationMode() != repl::ReplicationCoordinator::modeNone) &&
            nss_with_chunkID.isOplog()) {
            errmsg = "can't offload live oplog while replicating";
            return false;
        }

        /*if (moveChunk) {
            // If a chunk is offloaded by multiple threads, only one will get the gcManager
            // instance,
            // because it is protected by db MODE_X lock

            AutoGetDb autoDb(txn, nss_with_chunkID.db().toString(), MODE_IX);
            Lock::CollectionLock collLock(txn->lockState(), nss_with_chunkID.ns(), MODE_X);
            Database* const db = autoDb.getDb();
            Collection* collection = db ? db->getCollection(nss_with_chunkID) : nullptr;

            if (collection) {
                if (moveChunk) {
                    std::set<CursorId> cursorsNow;
                    collection->getCursorManager()->getCursorIds(&cursorsNow);
                    if (cursorsNow.size() > 0) {
                        log() << "[offloadChunk] open cursors exist on collection, can not moveChunk
        " << nss_with_chunkID.toString();
                        errmsg = str::stream() << "open cursors exist on collection " <<
        nss_with_chunkID.toString();
                        return appendCommandStatus(result, {ErrorCodes::CursorInUse, "open cursors
        exist on collection"});
                    }
                }
            }
        }*/
        if (renameCollection) {
            string targetCollection = cmdObj["targetCollection"].valuestrsafe();
            bool dropTarget = cmdObj["dropTarget"].trueValue();
            bool stayTemp = cmdObj["stayTemp"].trueValue();
            NamespaceString targetNSS(targetCollection);
            NamespaceString target = ns2chunkHolder().getNsWithChunkId(targetNSS);
            bool checkResult = doRenameCollectionCheck(
                txn, nss_with_chunkID, target, dropTarget, stayTemp, errmsg, result);
            if (!checkResult) {
                return false;
            }
        }

        // Offload command is now using drop collection command to remove metadata. Modified
        Status offloadStatus = dropCollection(txn, nss_with_chunkID, result, moveChunk);

        index_LOG(0) << "end offloadChunk: " << nss_with_chunkID;

        return appendCommandStatus(result, offloadStatus);
    }
} offloadChunkCmd;

}  // namespace
}  // namespace mongo

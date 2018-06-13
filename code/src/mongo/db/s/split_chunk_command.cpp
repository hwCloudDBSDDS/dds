/**
 *    Copyright (C) 2016 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include <string>
#include <vector>

#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/commands.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/s/operation_sharding_state.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/request_types/split_chunk_request_type.h"
#include "mongo/s/stale_exception.h"
#include "mongo/s/split_chunk_request.h"
#include "mongo/s/confirm_split_request.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/db/storage/storage_options.h"

namespace mongo {

using std::string;
using std::unique_ptr;
using std::vector;

namespace {

const char kChunkVersion[] = "chunkVersion";

const ReadPreferenceSetting kPrimaryOnlyReadPreference{ReadPreference::PrimaryOnly};

bool checkIfSingleDoc(OperationContext* txn,
                      Collection* collection,
                      const IndexDescriptor* idx,
                      const ChunkType* chunk) {
    KeyPattern kp(idx->keyPattern());
    BSONObj newmin = Helpers::toKeyFormat(kp.extendRangeBound(chunk->getMin(), false));
    BSONObj newmax = Helpers::toKeyFormat(kp.extendRangeBound(chunk->getMax(), true));

    unique_ptr<PlanExecutor> exec(InternalPlanner::indexScan(txn,
                                                             collection,
                                                             idx,
                                                             newmin,
                                                             newmax,
                                                             BoundInclusion::kIncludeStartKeyOnly,
                                                             PlanExecutor::YIELD_MANUAL));
    // check if exactly one document found
    PlanExecutor::ExecState state;
    BSONObj obj;
    if (PlanExecutor::ADVANCED == (state = exec->getNext(&obj, NULL))) {
        if (PlanExecutor::IS_EOF == (state = exec->getNext(&obj, NULL))) {
            return true;
        }
    }

    // Non-yielding collection scans from InternalPlanner will never error.
    invariant(PlanExecutor::ADVANCED == state || PlanExecutor::IS_EOF == state);

    return false;
}

//
// Checks the collection's metadata for a successful split on the specified chunkRange
// using the specified splitPoints. Returns false if the metadata's chunks don't match
// the new chunk boundaries exactly.
//
bool _checkMetadataForSuccess(OperationContext* txn,
                              const NamespaceString& nss,
                              const ChunkRange& chunkRange,
                              const std::vector<BSONObj>& splitKeys) {
    ScopedCollectionMetadata metadataAfterSplit;
    {
        AutoGetCollection autoColl(txn, nss, MODE_IS);

        // Get collection metadata
        metadataAfterSplit = CollectionShardingState::get(txn, nss.ns())->getMetadata();
    }

    auto newChunkBounds(splitKeys);
    auto startKey = chunkRange.getMin();
    newChunkBounds.push_back(chunkRange.getMax());

    ChunkType nextChunk;
    for (const auto& endKey : newChunkBounds) {
        // Check that all new chunks fit the new chunk boundaries
        if (!metadataAfterSplit->getNextChunk(startKey, &nextChunk) ||
            nextChunk.getMax().woCompare(endKey)) {
            return false;
        }

        startKey = endKey;
    }

    return true;
}

class SplitChunkCommand : public Command {
public:
    SplitChunkCommand() : Command("splitChunk") {}

    void help(std::stringstream& help) const override {
        help << "internal command usage only\n"
                "example:\n"
                " { splitChunk:\"db.foo\" , keyPattern: {a:1} , min : {a:100} , max: {a:200} { "
                "splitKeys : [ {a:150} , ... ]}";
    }

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    bool slaveOk() const override {
        return false;
    }

    bool adminOnly() const override {
        return true;
    }

    Status checkAuthForCommand(Client* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) override {
        if (!AuthorizationSession::get(client)->isAuthorizedForActionsOnResource(
                ResourcePattern::forClusterResource(), ActionType::internal)) {
            return Status(ErrorCodes::Unauthorized, "Unauthorized");
        }
        return Status::OK();
    }

    std::string parseNs(const std::string& dbname, const BSONObj& cmdObj) const override {
        return parseNsFullyQualified(dbname, cmdObj);
    }

    bool run(OperationContext* txn,
             const std::string& dbname,
             BSONObj& cmdObj,
             int options,
             std::string& errmsg,
             BSONObjBuilder& result) override {
         Date_t initExecTime = Date_t::now();
         log() << "splitChunk db: " << dbname << " cmdobj:" << cmdObj;
         SplitChunkReq splitChunkRequest = uassertStatusOK(SplitChunkReq::createFromCommand(cmdObj));
         if (!splitChunkRequest.validSplitPoint()) {
             log() << "[splitChunk] invalid splitPoint " << cmdObj;
             errmsg = str::stream() << "invlid split point ";
             return false;
         }
         splitChunkRequest.setFullRightDBPath(storageGlobalParams.dbpath + /*'/' + */
                                              splitChunkRequest.getRightChunkName() + '/' +
                                              splitChunkRequest.getRightDBPath());

         index_LOG(1)<<"splitChunk db Request: " << splitChunkRequest;
         NamespaceString nss = splitChunkRequest.getNss();
         NamespaceString nss_with_chunkID (StringData(nss.ns()+'$'+ splitChunkRequest.getName()));

         index_log() << "[splitChunk] shardSvr start nss : " << nss_with_chunkID;

         splitChunkRequest.setNs(nss_with_chunkID);
         index_log() << "[splitChunk] shardSvr get DBLock nss: " << nss_with_chunkID;
         Lock::DBLock dbLock(txn->lockState(), nss.db(), MODE_IS);
         index_log() << "[splitChunk] after geted DBLock nss: " << nss_with_chunkID << "; used Time(ms): " <<
             (Date_t::now()-initExecTime);

         Database* db = dbHolder().get(txn, nss.db());
         if (!db) {
             log() << "splitChunk db not exeist: " << nss.db();
             errmsg = str::stream() << "database not exist " << nss.db();
             return false;
         }

         Collection* collection = db->getCollection(nss_with_chunkID);
         if (!collection) {
             log() << "splitChunk nss not exeist: " << nss_with_chunkID;
             errmsg = str::stream() << "collection not exist " << nss_with_chunkID.toString();
             return false;
         }

         Status commandStatus = collection->split(txn, splitChunkRequest, result);
         if (!commandStatus.isOK()) {
             log() << "splitChunk split fail: " ;
             return appendCommandStatus(result, commandStatus);
         }
         
         index_log() << "[splitChunk] shardSvr end nss : " << nss_with_chunkID <<
             "; used Time(ms): " << (Date_t::now()-initExecTime);
         return true;
    }

} cmdSplitChunk;


class ConfirmSplitCommand : public Command {
public:
    ConfirmSplitCommand() : Command("confirmSplit") {}

    void help(std::stringstream& help) const override {
        help << "internal command usage only\n"
            "example:\n"
            " { confirmSplit:\"db.foo\" , keyPattern: {a:1} , min : {a:100} , max: {a:200} { "
            "splitKeys : [ {a:150} , ... ]}";
    }

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    bool slaveOk() const override {
        return false;
    }

    bool adminOnly() const override {
        return true;
    }

    Status checkAuthForCommand(Client* client,
        const std::string& dbname,
        const BSONObj& cmdObj) override {
            if (!AuthorizationSession::get(client)->isAuthorizedForActionsOnResource(
                ResourcePattern::forClusterResource(), ActionType::internal)) {
                    return Status(ErrorCodes::Unauthorized, "Unauthorized");
            }
            return Status::OK();
    }

    std::string parseNs(const std::string& dbname, const BSONObj& cmdObj) const override {
        return parseNsFullyQualified(dbname, cmdObj);
    }

    bool run(OperationContext* txn,
        const std::string& dbname,
        BSONObj& cmdObj,
        int options,
        std::string& errmsg,
        BSONObjBuilder& result) override {
            Date_t initExecTime = Date_t::now();
            index_log() << "confirmSplit db: " << dbname << " cmdobj:" << cmdObj;
            ConfirmSplitRequest confirmSplitRequest = uassertStatusOK(ConfirmSplitRequest::createFromCommand(cmdObj));
            NamespaceString nss = confirmSplitRequest.getNss();
            NamespaceString nss_with_chunkID (StringData(nss.ns()+'$'+ confirmSplitRequest.getName()));
            index_log() << "[confirmSplit] shardSvr start nss : " << nss_with_chunkID;
            confirmSplitRequest.setNs(nss_with_chunkID);
            {
                index_log() << "[confirmSplit] nss db: " << nss_with_chunkID;
                Lock::DBLock dbLock(txn->lockState(), nss.db(), MODE_IS);
                Lock::CollectionLock collLock(txn->lockState(), nss.ns(), MODE_IS);
                index_log() << "[confirmSplit] nss db after DBLock: " << nss_with_chunkID;
                Database* db = dbHolder().get(txn, nss.db());
                if (!db) {
                    log() << "confirmSplit db not exist: " << nss.db();
                    errmsg = str::stream() << "database not exist " << dbname;
                    return false;
                }

                Collection* collection = db->getCollection(nss_with_chunkID);
                if (!collection) {
                    log() << "confirmSplit nss not exist: " << nss_with_chunkID;
                    errmsg = str::stream() << "collection not exist " << nss_with_chunkID.toString();
                    return false;
                }

                Status commandStatus = collection->confirmSplit(txn, confirmSplitRequest, result);
                if (!commandStatus.isOK()) {
                    log() << "confirmSplit split fail: " ;
                    return appendCommandStatus(result, commandStatus);
                }
            }

            index_log() << "[confirmSplit] shardSvr end nss : " << nss_with_chunkID <<
                "; used Time(ms): " << (Date_t::now()-initExecTime);
            return true;
    }

} cmdConfirmSplit;

}  // namespace
}  // namespace mongo

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

#include "mongo/platform/basic.h"
#include <vector>
#include "mongo/db/catalog/drop_collection.h"

#include "mongo/db/background.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/index_builder.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/server_options.h"
#include "mongo/db/service_context.h"
#include "mongo/db/views/view_catalog.h"
#include "mongo/util/log.h"
#include "mongo/s/grid.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/catalog/dist_lock_manager.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_changelog.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/base/status.h"
#include "mongo/executor/network_interface.h"
#include "mongo/db/catalog/database_holder.h"

namespace mongo {
using std::vector;
using std::string;
Status dropCollectionOnCfgSrv(OperationContext* txn,
                      const NamespaceString& ns,
                      BSONObjBuilder& result) {
    LOG(0) << "[db.catalog.dropCollection] " << ns << "On config Server started";
    //1.send cmd to shardserver to delete collection
    //2.delete config.chunks
    //3.delete config.collections

    //1.
    vector<ChunkType> chunks;
    uassertStatusOK(grid.catalogClient(txn)->getChunks(txn,
                BSON(ChunkType::ns(ns.ns())),
                BSONObj(),
                0,
                &chunks,
                nullptr,
                repl::ReadConcernLevel::kMajorityReadConcern));
    
    LOG(0) << "[drop collection] chunk.szie : "<<chunks.size();
    if(chunks.size() == 0){
         return {ErrorCodes::NamespaceNotFound,"ns not found"};
    }
    std::map<string, BSONObj> errors;
    auto* shardRegistry = grid.shardRegistry();
    for(ChunkType chunk: chunks){
        BSONObjBuilder builder;
        auto shardStatus = shardRegistry->getShard(txn, chunk.getShard());
        if (!shardStatus.isOK()) {
            log()<<"[drop collection] get shard fail";
            return shardStatus.getStatus();
        }
        auto dropResult = shardStatus.getValue()->runCommandWithFixedRetryAttempts(
                txn,
                ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                ns.db().toString(),
                BSON("drop" << ns.coll() <<"chunkId"<<chunk.getName() << WriteConcernOptions::kWriteConcernField
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
                // Generally getting NamespaceNotFound is okay to ignore as it simply means that
                // the collection has already been dropped or doesn't exist on this shard.
                // If, however, we get NamespaceNotFound but also have a write concern error then we
                // can't confirm whether the fact that the namespace doesn't exist is actually
                // committed.  Thus we must still fail on NamespaceNotFound if there is also a write
                // concern error. This can happen if we call drop, it succeeds but with a write
                // concern error, then we retry the drop.
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
    LOG(0) << "dropCollection " << ns << " shard data deleted";    
    // Remove chunk data
    //2.
    Status re =grid.catalogClient(txn)->removeConfigDocuments(txn,
            ChunkType::ConfigNS,
            BSON(ChunkType::ns(ns.ns())),
            ShardingCatalogClient::kMajorityWriteConcern);
    if (!re.isOK()) {
        return re;
    }

    LOG(0) << "dropCollection " << ns << " chunk data deleted";
    //3.
    // Mark the collection as dropped
    CollectionType coll;
    coll.setNs(ns);
    coll.setDropped(true);
    coll.setEpoch(ChunkVersion::DROPPED().epoch());
    coll.setUpdatedAt(Grid::get(txn)->getNetwork()->now());
    coll.setPrefix ((long long)1);
    re = grid.catalogClient(txn)->updateCollection(txn, ns.ns(), coll);
    if (!re.isOK()) {
        return re;
    }

    LOG(0) << "dropCollection " << ns << " collection marked as dropped";
    return Status::OK();     
}

Status dropCollection(OperationContext* txn,
                      const NamespaceString& collectionName,
                      BSONObjBuilder& result) {
    if (!serverGlobalParams.quiet) {
        log() << "CMD: drop " << collectionName;
    }
    if(serverGlobalParams.clusterRole == ClusterRole::ShardServer)
    {
        ns2chunkHolder().del(StringData(collectionName.nsFilteredOutChunkId()));
    } 

    Date_t initExecTime = Date_t::now();
    const std::string dbname = collectionName.db().toString();
    index_log() << "[offloadChunk] shardSvr start stop BackGround nss: " << collectionName;
    {
        Lock::DBLock dbLock(txn->lockState(), dbname, MODE_IS);
        Database* db = dbHolder().get(txn, dbname);
        Collection* coll = db ? db->getCollection(collectionName) : nullptr;
        auto view = db && !coll ? db->getViewCatalog()->lookup(txn, collectionName.ns()) : nullptr;

        if (!db || (!coll && !view)) {
            return Status(ErrorCodes::NamespaceNotFound, "ns not found");
        }

        coll->stopBackGround4Chunk();
    }
    index_log() << "[offloadChunk] shardSvr end stop BackGround nss: " << collectionName << "; used Time(ms): " <<
        (Date_t::now()-initExecTime);

    initExecTime = Date_t::now();
    index_log() << "[offloadChunk] shardSvr start stop nss: " << collectionName;
    MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
        ScopedTransaction transaction(txn, MODE_IX);
        Date_t initExecTime = Date_t::now();
        index_log() << "[offloadChunk] shardSvr dropCollection get DBLock nss: " << collectionName;
        AutoGetDb autoDb(txn, dbname, MODE_IX);
        Lock::CollectionLock collLock(txn->lockState(), collectionName.ns(), MODE_X);
        index_log() << "[offloadChunk] shardSvr dropCollection geted DBLock nss: " << collectionName
            << "; used Time(ms): " << (Date_t::now()-initExecTime);
        Database* const db = autoDb.getDb();
        Collection* coll = db ? db->getCollection(collectionName) : nullptr;
        auto view = db && !coll ? db->getViewCatalog()->lookup(txn, collectionName.ns()) : nullptr;
        index_log() <<" .......1.......";
        if (!db || (!coll && !view)) {
            index_log() <<" .......2.......";
            return Status(ErrorCodes::NamespaceNotFound, "ns not found");
        }

        index_log() <<" .......3.......";
        const bool shardVersionCheck = true;
        OldClientContext context(txn, collectionName.ns(), shardVersionCheck);

        bool userInitiatedWritesAndNotPrimary = txn->writesAreReplicated() &&
            !repl::getGlobalReplicationCoordinator()->canAcceptWritesFor(collectionName);

        if (userInitiatedWritesAndNotPrimary) {
        index_log() <<" .......4.......";
            return Status(ErrorCodes::NotMaster,
                          str::stream() << "Not primary while dropping collection "
                                        << collectionName.ns());
        }

        index_log() <<" .......5.......";
        WriteUnitOfWork wunit(txn);
        result.append("ns", collectionName.ns());

        if (coll) {
            invariant(!view);
            int numIndexes = coll->getIndexCatalog()->numIndexesTotal(txn);

            BackgroundOperation::assertNoBgOpInProgForNs(collectionName.ns());

            index_log() <<" .......6.......";
            Status s = db->dropCollection(txn, collectionName.ns());

            if (!s.isOK()) {
                index_log() <<" .......7......." << s.toString();
                return s;
            }

            result.append("nIndexesWas", numIndexes);
        } else {
            invariant(view);
            Status status = db->dropView(txn, collectionName.ns());
            if (!status.isOK()) {
                return status;
            }
        }
        wunit.commit();
    }
    MONGO_WRITE_CONFLICT_RETRY_LOOP_END(txn, "drop", collectionName.ns());
    index_log() << "[offloadChunk] shardSvr end stop nss: " << collectionName << "; used Time(ms): " <<
        (Date_t::now()-initExecTime);
    return Status::OK();
}

}  // namespace mongo

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



#include "mongo/db/catalog/drop_collection.h"
#include "mongo/platform/basic.h"
#include <vector>

#include "mongo/base/status.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/background.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/create_collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/index_builder.h"
#include "mongo/db/modules/rocks/src/gc_common.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/balancer/balancer.h"
#include "mongo/db/server_options.h"
#include "mongo/db/service_context.h"
#include "mongo/db/views/view_catalog.h"
#include "mongo/executor/network_interface.h"
#include "mongo/s/catalog/dist_lock_manager.h"
#include "mongo/s/catalog/dist_lock_manager.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_changelog.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

using std::vector;
using std::string;

Status getIdentofCollection(OperationContext* txn,
                            const NamespaceString& ns,
                            std::string& ident,
                            bool& created) {
    int retrynum = 3;
    while (retrynum >= 0) {
        auto findResponse =
            Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                txn,
                ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                repl::ReadConcernLevel::kLocalReadConcern,
                NamespaceString(CollectionType::ConfigNS),
                BSON(CollectionType::fullNs(ns.ns())),
                BSONObj(),
                1);

        if (findResponse.isOK()) {
            if (findResponse.getValue().docs.size() != 1) {
                index_warning() << "[dropCollection] get collectionInfo ns: " << ns
                                << "; docs num: " << findResponse.getValue().docs.size();
                retrynum--;
                continue;
            }
            const BSONField<bool> kCreated("created");
            const BSONField<std::string> idents("ident");
            const BSONObj& source = findResponse.getValue().docs[0];
            bool collCreated;
            Status status = bsonExtractBooleanField(source, kCreated.name(), &collCreated);
            if (status.isOK()) {
                created = collCreated;
            } else if (status == ErrorCodes::NoSuchKey) {
                // Dropped can be missing in which case it is presumed false
            } else {
                return status;
            }

            std::string collIdent;
            status = bsonExtractStringField(source, idents.name(), &collIdent);
            if (!status.isOK()) {
                index_err() << "[getIdentofCollection] no ident: " << source;
                return status;
            } else {
                ident = collIdent;
            }
            return Status::OK();

        } else {
            index_err() << "[dropCollection] get collectionInfo ns: " << ns << " error!";
            if (retrynum-- >= 0) {
                continue;
            }
        }
    }

    return {ErrorCodes::FailedToParse,
            str::stream() << " from config.collections find ident fail ns: " << ns.toString()
                          << " retry 3 nums. "};
}

Status rmCollectionDir(OperationContext* txn, const NamespaceString& ns, const std::string& ident) {
    if (ident.empty()) {
        return Status::OK();
    }

    std::string collectionPath = getDataPath() + "/" + DATA_PATH + "/" + ident;
    index_log() << "[dropCollection] rmCollectionDir ns: " << ns << "; path: " << collectionPath;
    return getGlobalServiceContext()->getGlobalStorageEngine()->rmCollectionDir(txn,
                                                                                collectionPath);
}

Status rmCollectionGCInfo(OperationContext* txn,
                          const NamespaceString& ns,
                          const std::string& ident) {
    if (!GLOBAL_CONFIG_GET(enableGlobalGC)) {
        return Status::OK();
    }

    if (ident.empty()) {
        return Status::OK();
    }

    vector<ChunkType> chunks;
    auto status = grid.catalogClient(txn)->getChunks(txn,
                                                     BSON(ChunkType::ns(GcRefNs)),
                                                     BSONObj(),
                                                     0,
                                                     &chunks,
                                                     nullptr,
                                                     repl::ReadConcernLevel::kMajorityReadConcern);

    if (!status.isOK() || chunks.empty()) {
        index_warning() << "[dropCollection] gc not open: " << status;
        return Status::OK();
    }

    const WriteConcernOptions writeConcern(2, WriteConcernOptions::SyncMode::JOURNAL, Milliseconds(20000));
    BSONObj query(BSON(kIdentStr << ident));
    for (ChunkType chunk : chunks) {
        const auto shardStatus = grid.shardRegistry()->getShard(txn, chunk.getShard());
        if (!shardStatus.isOK()) {
            index_err() << "[dropCollection] get shard fail: " << shardStatus.getStatus();
            continue;
        }

        auto deleteDoc(stdx::make_unique<BatchedDeleteDocument>());
        deleteDoc->setQuery(query);
        deleteDoc->setLimit(0);

        auto deleteRequest(stdx::make_unique<BatchedDeleteRequest>());
        deleteRequest->addToDeletes(deleteDoc.release());
        deleteRequest->setWriteConcern(writeConcern.toBSON());

        BatchedCommandRequest request(deleteRequest.release());
        NamespaceString nss = NamespaceString(StringData(GcDbName),
                                              StringData(GcRefCollName + '$' + chunk.getName()));
        request.setNS(nss);

        const BSONObj cmdObj = request.toBSON();
        index_LOG(2) << "[dropCollection] drop collection: " << ns;
        int retryNum = 0;
        while (retryNum < 10) {
            auto cmdResponseStatus =
                shardStatus.getValue()
                    ->runCommand(txn,
                                 ReadPreferenceSetting{ReadPreference::Nearest},
                                 GcDbName,
                                 cmdObj,
                                 Shard::RetryPolicy::kIdempotent);
            if (!cmdResponseStatus.isOK()) {
                index_err() << "[dropCollection] rmCollectionGCInfo runCommand failed, status: " << cmdResponseStatus.getStatus();
                retryNum ++;
                continue;
            }
            auto cmdResponse = cmdResponseStatus.getValue();
            if (!cmdResponse.commandStatus.isOK()) {
                if (cmdResponse.commandStatus.code() == ErrorCodes::ChunkNotAssigned) {
                    index_warning() << "[dropCollection]run cmd fail; fail( "
                                    << cmdResponse.commandStatus << ")";
                    sleepmillis(200);
                    retryNum++;
                    continue;
                }
                index_err() << "[dropCollection] run cmd fail; fail("
                            << cmdResponse.commandStatus << ")";
                break;
            } else {
                break;
            }
        }
    }

    return Status::OK();
    // GcRemoveInfoNs nothing to do;
}

Status offloadChunks(OperationContext* txn, const std::string ns) {
    vector<ChunkType> chunks;
    uassertStatusOK(
        grid.catalogClient(txn)->getChunks(txn,
                                           BSON(ChunkType::ns(ns)),
                                           BSONObj(),
                                           0,
                                           &chunks,
                                           nullptr,
                                           repl::ReadConcernLevel::kMajorityReadConcern));

    LOG(0) << "[drop collection] chunk.size : " << chunks.size();
    unsigned int count = chunks.size();
    while (count > 0) {
        for (ChunkType chunk : chunks) {
            auto status = Balancer::get(txn)->offloadChunk(txn, chunk, true);
            if (!status.isOK()) {
                if (status.code() == ErrorCodes::NamespaceNotFound) {
                    count--;
                    continue;
                }
                const auto shardStatus = grid.shardRegistry()->getShard(txn, chunk.getShard());
                if (!shardStatus.isOK()) {
                    index_err() << "[dropCollectionOnCfgSrv] shard " << chunk.getShard()
                                << "not found";
                    count--;
                    continue;
                }
                index_log() << "[dropCollectionOnCfgSrv] fail to offload chunk " << chunk.getID()
                            << " due to " << status;
                return status;
                // errors[chunk.getName()] = status.reason();
            }
            count--;
        }

        chunks.clear();
        BSONObjBuilder queryBuilder;
        queryBuilder.append(ChunkType::ns(), ns);
        queryBuilder.append(ChunkType::status(),
                            static_cast<int>(ChunkType::ChunkStatus::kAssigned));
        uassertStatusOK(
            grid.catalogClient(txn)->getChunks(txn,
                                               queryBuilder.obj(),
                                               BSONObj(),
                                               0,
                                               &chunks,
                                               nullptr,
                                               repl::ReadConcernLevel::kMajorityReadConcern));
        count = chunks.size();
    }
    return Status::OK();
}

Status dropCollectionOnCfgSrv(OperationContext* txn,
                              const NamespaceString& ns,
                              BSONObjBuilder& result) {
    // 1.delete config.collections
    // 2.send cmd to shardserver to delete collection
    // 3.delete config.chunks
    // 4.delete data from disk
    // 5.delete gc info

    // 1.
    std::string ident;
    bool created = true;
    Status ree = getIdentofCollection(txn, ns, ident, created);
    if (!ree.isOK()) {
        index_err() << "[dropCollection] getIdentofCollection error: " << ree << "; ns: " << ns;
        // return re;
    }
    index_log() << "[dropCollOnCfg] " << ident << " ," << created;
    BSONObjBuilder upCollBuilder;
    upCollBuilder.append(CollectionType::kDropped(), true);
    auto updateStatus =
        grid.catalogClient(txn)->updateConfigDocument(txn,
                                                      CollectionType::ConfigNS,
                                                      BSON(CollectionType::fullNs(ns.ns())),
                                                      BSON("$set" << upCollBuilder.obj()),
                                                      false,
                                                      ShardingCatalogClient::kMajorityWriteConcern);

    if (!updateStatus.isOK()) {
        return updateStatus.getStatus();
    }

    // 2.
    auto re = offloadChunks(txn, ns.ns());
    if (!re.isOK()) {
        return re;
    }
    index_LOG(0) << "dropCollection " << ns << " shard data deleted";
    // Remove chunk data
    // 3.
    re = grid.catalogClient(txn)->removeConfigDocuments(
        txn,
        ChunkType::ConfigNS,
        BSON(ChunkType::ns(ns.ns())),
        ShardingCatalogClient::kMajorityWriteConcern);
    if (!re.isOK()) {
        return re;
    }

    CollectionType coll;
    coll.setNs(ns);
    coll.setDropped(true);
    coll.setCreated(created);
    coll.setEpoch(ChunkVersion::DROPPED().epoch());
    coll.setUpdatedAt(Grid::get(txn)->getNetwork()->now());
    coll.setPrefix((long long)1);
    coll.setIdent(ident);
    re = grid.catalogClient(txn)->updateCollection(txn, ns.ns(), coll);
    if (!re.isOK()) {
        index_log() << "[dropCollection] fail to update collection to dropped due to " << re;
    }

    index_LOG(0) << "dropCollection " << ns << " chunk data deleted";

    // 4. delete data from disk
    index_log() "[dropCollection] delete collection ns: " << ns << "; ident: " << ident;
    re = rmCollectionDir(txn, ns, ident);
    if (!re.isOK()) {
        index_err() << "[dropCollection] rmCollectionDir error: " << re << "; ns: " << ns;
        // not return;
    }

    // 5. delete gc info
    re = rmCollectionGCInfo(txn, ns, ident);
    if (!re.isOK()) {
        index_err() << "dropCollection " << ns << " collection marked as dropped";
        // not return;
    }

    index_log() << "[dropCollection] success finsh: " << ns;
    return Status::OK();
}

Status dropCollection(OperationContext* txn,
                      const NamespaceString& collectionName,
                      BSONObjBuilder& result,
                      bool moveChunk) {
    if (!serverGlobalParams.quiet) {
        index_log() << "CMD: drop " << collectionName;
    }
    if (serverGlobalParams.clusterRole == ClusterRole::ShardServer) {
        ns2chunkHolder().del(StringData(collectionName.nsFilteredOutChunkId()));
    }

    getGlobalServiceContext()->registerProcessStageTime("offloadChunk:" +
                                                        collectionName.toString());

    auto guard = MakeGuard([&] {
        getGlobalServiceContext()->cancelProcessStageTime("offloadChunk:" +
                                                          collectionName.toString());
    });

    getGlobalServiceContext()
        ->getProcessStageTime("offloadChunk:" + collectionName.toString())
        ->noteStageStart("commandStart:" + serverGlobalParams.shardName);

    const std::string dbname = collectionName.db().toString();
    Date_t initExecTime = Date_t::now();
    std::shared_ptr<ViewDefinition> view = nullptr;

    index_log() << "[offloadChunk] shardSvr start stop nss: " << collectionName;
    MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
        ScopedTransaction transaction(txn, MODE_IX);
        Date_t initExecTime = Date_t::now();
        index_log() << "[offloadChunk] shardSvr dropCollection get DBLock nss: " << collectionName;
        AutoGetDb autoDb(txn, dbname, MODE_X);
        index_log() << "[offloadChunk] shardSvr dropCollection geted DBLock nss: " << collectionName
                    << "; used Time(ms): " << (Date_t::now() - initExecTime);
        Database* const db = autoDb.getDb();
        Collection* coll = db ? db->getCollection(collectionName, true) : nullptr;

        if (moveChunk && coll) {
            std::set<CursorId> cursorsNow;
            coll->getCursorManager()->getCursorIds(&cursorsNow);
            if (cursorsNow.size() > 0) {
                index_warning()
                    << "[offloadChunk] open cursors exist on collection, can not moveChunk "
                    << collectionName;
                return Status(ErrorCodes::CursorInUse, "open cursors exist on collection");
            }
        }

        view = db && !coll ? db->getViewCatalog()->lookup(txn, collectionName.ns()) : nullptr;

        if (!db || (!coll && !view)) {
            return Status(ErrorCodes::NamespaceNotFound, "ns not found");
        }

        const bool shardVersionCheck = true;
        OldClientContext context(txn, collectionName.ns(), shardVersionCheck);

        bool userInitiatedWritesAndNotPrimary = txn->writesAreReplicated() &&
            !repl::getGlobalReplicationCoordinator()->canAcceptWritesFor(collectionName);

        if (userInitiatedWritesAndNotPrimary) {
            return Status(ErrorCodes::NotMaster,
                          str::stream() << "Not primary while dropping collection "
                                        << collectionName.ns());
        }

        WriteUnitOfWork wunit(txn);
        result.append("ns", collectionName.ns());

        if (coll) {
            invariant(!view);
            int numIndexes = coll->getIndexCatalog()->numIndexesTotal(txn);

            BackgroundOperation::assertNoBgOpInProgForNs(collectionName.ns());

            Status s = db->dropCollection(txn, collectionName.ns());

            if (!s.isOK()) {
                warning() << "dropCollection failed. status: " << s.toString();
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
        auto css = CollectionShardingState::get(txn, collectionName);
        css->refreshMetadata(txn, nullptr);
    }
    MONGO_WRITE_CONFLICT_RETRY_LOOP_END(txn, "drop", collectionName.ns());

    index_log() << "[offloadChunk] shardSvr end stop nss: " << collectionName
                << "; used Time(ms): " << (Date_t::now() - initExecTime);

    if (!collectionName.isSystemCollection() && !view) {
        getGlobalServiceContext()
            ->getProcessStageTime("offloadChunk:" + collectionName.toString())
            ->noteStageStart("close RocksDB");

        getGlobalServiceContext()->getGlobalStorageEngine()->destroyRocksDB(
            collectionName.toString());
    }

    getGlobalServiceContext()
        ->getProcessStageTime("offloadChunk:" + collectionName.toString())
        ->noteProcessEnd();

    index_LOG(0) << "Time of offloadChunk:" + collectionName.toString() << ": "
                 << getGlobalServiceContext()
                        ->getProcessStageTime("offloadChunk:" + collectionName.toString())
                        ->toString();

    return Status::OK();
}

}  // namespace mongo

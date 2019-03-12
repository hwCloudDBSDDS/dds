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

#include "mongo/db/catalog/create_collection.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/drop_collection.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/balancer/balancer.h"
#include "mongo/s/assign_chunk_request.h"
#include "mongo/s/assign_chunk_request.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/catalog/sharding_catalog_manager.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/config.h"
#include "mongo/s/grid.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/util/log.h"
#include <map>
#include <string>

namespace mongo {
using std::string;

const auto bannedExpressionsInValidators = std::set<StringData>{
    "$geoNear", "$near", "$nearSphere", "$text", "$where",
};

Status checkValidatorForBannedExpressions(const BSONObj& validator) {
    for (auto field : validator) {
        const auto name = field.fieldNameStringData();
        if (name[0] == '$' && bannedExpressionsInValidators.count(name)) {
            return {ErrorCodes::InvalidOptions,
                    str::stream() << name << " is not allowed in collection validators"};
        }

        if (field.type() == Object || field.type() == Array) {
            auto status = checkValidatorForBannedExpressions(field.Obj());
            if (!status.isOK())
                return status;
        }
    }
    return Status::OK();
}

Status createCollection(OperationContext* txn,
                        const std::string& dbName,
                        const BSONObj& cmdObj,
                        const BSONObj& idIndex) {
    BSONObjIterator it(cmdObj);

    // Extract ns from first cmdObj element.
    BSONElement firstElt = it.next();
    uassert(15888, "must pass name of collection to create", firstElt.valuestrsafe()[0] != '\0');

    Status status = userAllowedCreateNS(dbName, firstElt.valuestr());
    if (!status.isOK()) {
        return status;
    }

    NamespaceString nss(dbName, firstElt.valuestrsafe());

    // Build options object from remaining cmdObj elements.
    BSONObjBuilder optionsBuilder;
    while (it.more()) {
        optionsBuilder.append(it.next());
    }

    BSONObj options = optionsBuilder.obj();
    uassert(14832,
            "specify size:<n> when capped is true",
            !options["capped"].trueValue() || options["size"].isNumber() ||
                options.hasField("$nExtents"));

    MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
        ScopedTransaction transaction(txn, MODE_IX);
        Lock::DBLock dbXLock(txn->lockState(), dbName, MODE_X);
        OldClientContext ctx(txn, nss.ns());
        if (txn->writesAreReplicated() &&
            !repl::getGlobalReplicationCoordinator()->canAcceptWritesFor(nss)) {
            return Status(ErrorCodes::NotMaster,
                          str::stream() << "Not primary while creating collection " << nss.ns());
        }

        WriteUnitOfWork wunit(txn);

        // Create collection.
        const bool createDefaultIndexes = true;
        status = userCreateNS(txn, ctx.db(), nss.ns(), options, createDefaultIndexes, idIndex);
        if (!status.isOK()) {
            return status;
        }

        wunit.commit();
    }
    MONGO_WRITE_CONFLICT_RETRY_LOOP_END(txn, "create", nss.ns());
    return Status::OK();
}
static BSONObj _fixIndexKey(const BSONObj& key) {
    BSONObj _idObj = BSON("_id" << 1);
    if (IndexDescriptor::isIdIndexPattern(key)) {
        return _idObj;
    }
    if (key["_id"].type() == Bool && key.nFields() == 1) {
        return _idObj;
    }
    return key;
}

Status removeCollAndChunkMetadata(OperationContext* txn,
                                  const NamespaceString& ns,
                                  BSONObjBuilder& result) {
    /*string errmsg;
    BSONObjBuilder result;
    Command* dropCollCmd = Command::findCommand("drop");
    BSONObjBuilder dropCmdBuilder;
    dropCmdBuilder.append("drop", ns);
    int dropOptions = 0;
    BSONObj dropCmdObj = dropCmdBuilder.done();
    try {
         dropCollCmd->run(txn, "admin", dropCmdObj, dropOptions, errmsg, result);
    } catch (const DBException& e) {
    }*/
    auto status = dropCollectionOnCfgSrv(txn, ns, result);
    if (!status.isOK()) {
        return status;
    }

    status = grid.catalogClient(txn)->removeConfigDocuments(
        txn,
        CollectionType::ConfigNS,
        BSON(CollectionType::fullNs() << ns.ns()),
        ShardingCatalogClient::kMajorityWriteConcern);
    if (!status.isOK()) {
        return status;
    }

    return grid.catalogClient(txn)->removeConfigDocuments(
        txn,
        ChunkType::ConfigNS,
        BSON(ChunkType::ns() << ns.ns()),
        ShardingCatalogClient::kMajorityWriteConcern);
}


// create collection metadata and stores in config.collections
// added en
// create collection metadata and stores in config.collections
Status createCollectionMetadata(OperationContext* txn,
                                const NamespaceString& ns,
                                const BSONObj& cmdObj,
                                BSONObj& indexSpec) {
    Status status = userAllowedCreateNS(ns.db(), ns.coll());
    if (!status.isOK()) {
        return status;
    }

    BSONObjIterator it(cmdObj);
    BSONElement firstElt = it.next();
    uassert(78888, "must pass name of collection to create", firstElt.valuestrsafe()[0] != '\0');
    BSONObjBuilder optionsBuilder;
    while (it.more()) {
        optionsBuilder.append(it.next());
    }
    BSONObj colloptions = optionsBuilder.obj();
    uassert(78832,
            "specify size:<n> when capped is true",
            !colloptions["capped"].trueValue() || colloptions["size"].isNumber() ||
                colloptions.hasField("$nExtents"));

    BSONObj cmd = cmdObj.removeField("create");
    BSONObj rmCreateObj = cmd.removeField("maxTimeMS");  // maxTimeMS not option
    CollectionOptions options;
    Status parse_status = options.parse(rmCreateObj);
    if (!parse_status.isOK()) {
        return parse_status;
    }
    BSONObj validator = options.validator.getOwned();
    {
        auto status = checkValidatorForBannedExpressions(validator);
        if (!status.isOK())
            return status;
    }
    if (!options.collation.isEmpty()) {
        auto collator = CollatorFactoryInterface::get(txn->getServiceContext())
                            ->makeFromBSON(options.collation);
        if (!collator.isOK()) {
            return collator.getStatus();
        }
        options.collation =
            collator.getValue() ? collator.getValue()->getSpec().toBSON() : BSONObj();
    }
    // check if the collection already exists
    auto collStatus = grid.catalogClient(txn)->getCollection(txn, ns.ns());
    if (collStatus.isOK()) {
        CollectionType coll = collStatus.getValue().value;
        if (!coll.getDropped() && coll.getCreated()) {
            index_log() << ns.ns() << "collection already exists";
            return (
                Status(ErrorCodes::NamespaceExists,
                       str::stream() << "a collection '" << ns.toString() << "' already exists"));
        } else {
            BSONObjBuilder removeResult;
            auto status = removeCollAndChunkMetadata(txn, ns, removeResult);
            if (!status.isOK()) {
                return status;
            }
        }
    }

    // generate a new record in config.collections
    CollectionType coll;
    coll.setNs(ns);
    coll.setEpoch(OID::gen());
    coll.setUpdatedAt(Date_t::fromMillisSinceEpoch(1));
    coll.setKeyPattern(BSON("_id" << 1));
    coll.setUnique(true);
    coll.setCreated(false);
    coll.setTabType(CollectionType::TableType::kNonShard);
    // write the max prefix
    coll.setPrefix(1);
    coll.setIdent(grid.catalogClient(txn)->createNewIdent());
    // set options
    if (!rmCreateObj.isEmpty()) {
        BSONObj cla = rmCreateObj;
        if (!options.collation.isEmpty()) {
            BSONObjBuilder out;
            BSONObjBuilder in;
            in.append("collation", options.collation);
            auto Obj = rmCreateObj.removeField("collation");
            out.appendElements(Obj);
            out.appendElements(in.obj());
            cla = out.obj();
        } else {
            if (!rmCreateObj["collation"].eoo()) {
                cla = rmCreateObj.removeField("collation");
            }
        }
        coll.setOptions(cla);
    }
    // create index spec for _id field
    if (!indexSpec.isEmpty()) {
        BSONArrayBuilder bd;
        BSONObjBuilder b;
        auto vElt = indexSpec["v"];
        invariant(vElt);
        b.append("v", vElt.numberInt());
        if (indexSpec["unique"].trueValue())
            b.appendBool("unique", true);
        BSONObj key = _fixIndexKey(indexSpec["key"].Obj());
        b.append("key", key);
        string name = indexSpec["name"].String();
        if (IndexDescriptor::isIdIndexPattern(key)) {
            name = "_id_";
        }
        b.append("name", name);
        {
            BSONObjIterator i(indexSpec);
            while (i.more()) {
                BSONElement e = i.next();
                string s = e.fieldName();
                if (s == "_id") {
                    // skip
                } else if (s == "dropDups") {
                    // dropDups is silently ignored and removed from the spec as of SERVER-14710.
                } else if (s == "v" || s == "unique" || s == "key" || s == "name") {
                    // covered above
                } else {
                    b.append(e);
                }
            }
        }
        b.append("prefix", (long long)1);
        bd.append(b.obj());
        BSONArray ba = bd.arr();
        coll.setIndex(ba);
    } else {
        // generate default index spec
        const auto featureCompatibilityVersion =
            serverGlobalParams.featureCompatibility.version.load();
        const auto indexVersion =
            IndexDescriptor::getDefaultIndexVersion(featureCompatibilityVersion);

        BSONObjBuilder b;
        b.append("v", static_cast<int>(indexVersion));
        b.append("name", "_id_");
        b.append("ns", ns.ns());
        b.append("key", BSON("_id" << 1));
        b.append("prefix", (long long)1);
        if (!rmCreateObj.isEmpty()) {
            if (!options.collation.isEmpty()) {
                b.append("collation", options.collation);
            }
        }
        BSONArrayBuilder indexArrayBuilder;
        indexArrayBuilder.append(b.obj());
        coll.setIndex(indexArrayBuilder.arr());
    }

    uassertStatusOK(grid.catalogClient(txn)->updateCollection(txn, ns.ns(), coll));
    index_log() << "createCollectionMetadata--1 " << ns.ns() << " updateCollection ";
    ChunkType chunk;
    const KeyPattern keypattern = coll.getKeyPattern();
    BSONObj min = keypattern.globalMin();
    BSONObj max = keypattern.globalMax();
    ChunkVersion version(1, 0, OID::gen());
    chunk.setNS(ns.ns());
    chunk.setMin(min);
    chunk.setMax(max);
    chunk.setVersion(version);
    // get database info from config.databases, choose the primary shard for the chunk
    auto dbstatus = grid.catalogClient(txn)->getDatabase(txn, ns.db().toString());
    if (!dbstatus.isOK()) {
        index_err() << "[CS_CREATECOLL] get db fail";
        return dbstatus.getStatus();
    }
    chunk.setShard(dbstatus.getValue().value.getPrimary());

    std::string chunkID;
    auto c_status = grid.catalogClient(txn)->generateNewChunkID(txn, chunkID);
    if (!c_status.isOK()) {
        index_err() << "[CS_CREATECOLL] assign fail";
        return c_status;
    }
    chunk.setName(chunkID);

    {
        // maybe meet corrupt if rootfolder is null.
        std::string chunkRootFolder;
        status = grid.catalogManager()->createRootFolder(
            txn, coll.getIdent(), chunk.getID(), chunkRootFolder);
        if (!status.isOK()) {
            index_err() << "[CS_CREATECOLL] failed to create root folder for chunk:"
                        << chunk.getID();
            return status;
        }
        chunk.setRootFolder(chunkRootFolder);
        index_log() << "[CS_CREATECOLL] create, root folder is " << chunkRootFolder
                    << ",chunk:" << chunk.getID();
    }
    index_log() << "[CS_CREATECOLL] chunk:" << chunk.getID();
    status = grid.catalogClient(txn)->insertConfigDocument(
        txn, ChunkType::ConfigNS, chunk.toBSON(), ShardingCatalogClient::kMajorityWriteConcern);
    if (!status.isOK()) {
        index_err() << "[CS_CREATECOLL] create chunk fail";
        return status;
    }
    auto shardId = chunk.getShard();
    auto assignStatus = Balancer::get(txn)->assignChunk(txn, chunk, true, true, shardId);
    if (!assignStatus.isOK()) {
        BSONObjBuilder removeResult;
        auto status = removeCollAndChunkMetadata(txn, ns, removeResult);
        {
            if (!status.isOK()) {
                return status;
            }
        }
        index_err() << "[CS_CREATECOLL] assign fail";
        return assignStatus;
    }

    coll.setEpoch(version.epoch());
    coll.setCreated(true);
    coll.setUpdatedAt(Date_t::fromMillisSinceEpoch(version.toLong()));
    coll.setKeyPattern(keypattern.toBSON());
    status = grid.catalogClient(txn)->updateCollection(txn, ns.ns(), coll);
    if (!status.isOK()) {
        index_err() << "[CS_CREATECOLL] update coll fail";
        return status;
    }

    return Status::OK();
}

}  // namespace mongo

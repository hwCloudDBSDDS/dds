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
#include <string>
#include "mongo/db/catalog/drop_indexes.h"

#include "mongo/db/background.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/index_builder.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/service_context.h"
#include "mongo/util/log.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/grid.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/db/catalog/index_create.h"
namespace mongo {
/*
extern Status createIndexMetadata(OperationContext* txn,
        const NamespaceString& ns,
        std::vector<BSONObj>& indexSpecs,
        BSONObj& cmdObj,
        const string& dbname);
*/
using std::vector;
using std::string;
namespace {
const StringData kIndexesFieldName = "index"_sd;
const StringData kIdIndexFieldName = "_id_"_sd;
Status _dropIndexByKeyPattern(OperationContext* txn,const std::string& ns,CollectionType& coll,const BSONObj& key){
    BackgroundOperation::assertNoBgOpInProgForNs(ns);
    BSONArrayBuilder indexArrayBuilder;
    BSONObjIterator indexspecs(coll.getIndex());
    while (indexspecs.more()) {
        BSONObj existingIndex = indexspecs.next().Obj();
        log()<<"[_dropIndexByKeyPattern] "<<existingIndex["key"].toString() << " "<<key.toString(); 
        if(existingIndex.getObjectField("key").toString() != key.toString() ){
            indexArrayBuilder.append(existingIndex);
            continue;
        }
    }
    coll.setIndex(indexArrayBuilder.arr());
    return Status::OK();
}
Status _dropIndexByName(OperationContext* txn,CollectionType& coll,const std::string& ns,const std::string& indexToDelete){
    BackgroundOperation::assertNoBgOpInProgForNs(ns);
    BSONArrayBuilder indexArrayBuilder;
    BSONObjIterator indexspecs(coll.getIndex());
    while (indexspecs.more()) {
        BSONObj existingIndex = indexspecs.next().Obj();
        std::string st = existingIndex.getStringField("name");
        if( st != indexToDelete ){
            indexArrayBuilder.append(existingIndex);
            continue;
        }
    }
    coll.setIndex(indexArrayBuilder.arr());
    return Status::OK();
}
bool isIdIndex(const std::string& indexToDelete){
    if( indexToDelete == kIdIndexFieldName )
        return true;
    else
        return false;
}
Status _dropAllIndexes(OperationContext* txn,CollectionType& coll,const std::string& ns,bool includingIdIndex){
    BackgroundOperation::assertNoBgOpInProgForNs(ns);
    BSONArrayBuilder indexArrayBuilder;
    BSONObjIterator indexspecs(coll.getIndex());
    while (indexspecs.more()) {
        BSONObj existingIndex = indexspecs.next().Obj();
        BSONObjIterator i(existingIndex.getObjectField("key"));
        BSONElement e = i.next();
        if((strcmp(e.fieldName(), "_id") == 0 && (e.numberInt() == 1 || e.numberInt() == -1))){
            indexArrayBuilder.append(existingIndex);
            break;
        }
    }
    coll.setIndex(indexArrayBuilder.arr());
    return Status::OK();
}
Status updateCollectionOnCfg(OperationContext* txn,
                  const std::string& toDeleteNs,
                  const BSONObj& jsobj,
                  BSONObjBuilder& anObjBuilder) {
    //1.get collection
    //2.get indexes
    //3.get index specs
    //4.delete indexes
    //5.set indexes
    //6.update collection
   
    //1.
    CollectionType coll;
    auto collStatus = grid.catalogClient(txn)->getCollection(txn,toDeleteNs);
    if(collStatus.isOK()){
        coll = collStatus.getValue().value;
    }else{
        log()<<"[catalog/drop_index.cpp:79] "<< toDeleteNs << " collection not found";
        return Status(ErrorCodes::NamespaceNotFound, "_______collection not found");
    }
    BSONArrayBuilder indexArrayBuilder;
    //2.
    BSONObjIterator indexspecs(coll.getIndex());
    //3.
    BSONElement f = jsobj.getField(kIndexesFieldName);
    if(f.type() == String){
        std::string indexToDelete = f.valuestr();
        if (indexToDelete == "*"){
            //4.5.6  not delete id index default
            Status s = _dropAllIndexes(txn,coll,toDeleteNs,false);
            if(!s.isOK()){
                return s;
            }
            anObjBuilder.append("msg", "non-_id indexes dropped for collection");
        }else{
            log()<<"[catalog/drop_index.cpp:98] " << indexToDelete;
            if(isIdIndex(indexToDelete)){
                return Status(ErrorCodes::InvalidOptions, "_____cannot drop _id index");
            }
            //4.5.6
            Status ss = _dropIndexByName(txn,coll,toDeleteNs,indexToDelete);
            if(!ss.isOK()){
                return Status(ErrorCodes::IndexNotFound,
                              str::stream() << "index not found with name [" << indexToDelete << "]");
            }
       }
    }else if(f.type() == Object){
        //4.5.6
        Status ss = _dropIndexByKeyPattern(txn,toDeleteNs,coll,f.embeddedObject());
        if( !ss.isOK() )
           return ss;
    }
    log()<<coll.getIndex();
    uassertStatusOK(grid.catalogClient(txn)->updateCollection(txn,toDeleteNs, coll));
    return Status::OK();
}
Status wrappedRun(OperationContext* txn,
                  const StringData& dbname,
                  const std::string& toDeleteNs,
                  Database* const db,
                  const BSONObj& jsobj,
                  BSONObjBuilder* anObjBuilder) {
    if (!serverGlobalParams.quiet) {
        LOG(0) << "CMD: dropIndexes " << toDeleteNs;
    }
    Collection* collection = db ? db->getCollection(toDeleteNs) : nullptr;

    // If db/collection does not exist, short circuit and return.
    if (!db || !collection) {
        if (db && db->getViewCatalog()->lookup(txn, toDeleteNs)) {
            return {ErrorCodes::CommandNotSupportedOnView,
                    str::stream() << "Cannot drop indexes on view " << toDeleteNs};
        }

        return Status(ErrorCodes::NamespaceNotFound, "ns not found");
    }

    OldClientContext ctx(txn, toDeleteNs);
    BackgroundOperation::assertNoBgOpInProgForNs(toDeleteNs);

    IndexCatalog* indexCatalog = collection->getIndexCatalog();
    anObjBuilder->appendNumber("nIndexesWas", indexCatalog->numIndexesTotal(txn));


    BSONElement f = jsobj.getField("index");
    if (f.type() == String) {
        std::string indexToDelete = f.valuestr();

        if (indexToDelete == "*") {
            Status s = indexCatalog->dropAllIndexes(txn, false);
            if (!s.isOK()) {
                return s;
            }
            anObjBuilder->append("msg", "non-_id indexes dropped for collection");
            return Status::OK();
        }

        IndexDescriptor* desc = collection->getIndexCatalog()->findIndexByName(txn, indexToDelete);
        if (desc == NULL) {
            return Status(ErrorCodes::IndexNotFound,
                          str::stream() << "index not found with name [" << indexToDelete << "]");
        }

        if (desc->isIdIndex()) {
            return Status(ErrorCodes::InvalidOptions, "cannot drop _id index");
        }

        Status s = indexCatalog->dropIndex(txn, desc);
        if (!s.isOK()) {
            return s;
        }

        return Status::OK();
    }

    if (f.type() == Object) {
        std::vector<IndexDescriptor*> indexes;
        collection->getIndexCatalog()->findIndexesByKeyPattern(
            txn, f.embeddedObject(), false, &indexes);
        if (indexes.empty()) {
            return Status(ErrorCodes::IndexNotFound,
                          str::stream() << "can't find index with key: " << f.embeddedObject());
        } else if (indexes.size() > 1) {
            return Status(ErrorCodes::AmbiguousIndexKeyPattern,
                          str::stream() << indexes.size() << " indexes found for key: "
                                        << f.embeddedObject()
                                        << ", identify by name instead."
                                        << " Conflicting indexes: "
                                        << indexes[0]->infoObj()
                                        << ", "
                                        << indexes[1]->infoObj());
        }

        IndexDescriptor* desc = indexes[0];
        if (desc->isIdIndex()) {
            return Status(ErrorCodes::InvalidOptions, "cannot drop _id index");
        }

        Status s = indexCatalog->dropIndex(txn, desc);
        if (!s.isOK()) {
            return s;
        }

        return Status::OK();
    }

    return Status(ErrorCodes::IndexNotFound, "invalid index name spec");
}
}  // namespace
const ReadPreferenceSetting kPrimaryOnlyReadPreference{ReadPreference::PrimaryOnly};
Status dropIndexesOnCfgSrv(OperationContext* txn,
                   const string& dbname,
                   const NamespaceString& nss,
                   const BSONObj& idxDescriptor,
                   BSONObjBuilder& result) {
    //1.get chunks
    //2.send cmd to every chunk
    //3.deal error
    //4.delete index on configServer
    log()<<"nss: "<<nss<<" cmd: "<<idxDescriptor.toString();
    //1.
    vector<ChunkType> chunks;
    vector<Status> statuses;
    uassertStatusOK(grid.catalogClient(txn)->getChunks(txn,
                BSON(ChunkType::ns(nss.ns())),
                BSONObj(),
                0,
                &chunks,
                nullptr,
                repl::ReadConcernLevel::kMajorityReadConcern));
    //2.
    for(ChunkType chunk: chunks){
        log()<<"[dropIndexesOnCfgSrv :]"<<chunk;
        BSONObjBuilder builder;
        for(BSONElement elm:idxDescriptor){
            builder.append(elm);
        }
        builder.append("chunkId",chunk.getName());
        const auto shardStatus = grid.shardRegistry()->getShard(txn, chunk.getShard());
        if( !shardStatus.isOK()){
                log()<<"[dropIndexesOnCfgSrv ] get shard fail";
                return Status(ErrorCodes::InternalError, "cannot get chunk");
        }
        const auto createindexShard = shardStatus.getValue();
	BSONObj cmdObj = builder.obj();
	auto cmdResponseStatus = uassertStatusOK(createindexShard->runCommand(txn,kPrimaryOnlyReadPreference,dbname,cmdObj,Shard::RetryPolicy::kIdempotent));
	if (!cmdResponseStatus.commandStatus.isOK()){
	    log()<<"[dropIndexesOnCfgSrv ]run cmd fail,cmdObj:" << cmdObj;
	    statuses.push_back(cmdResponseStatus.commandStatus);
	}
    }
    //3.
    if( !statuses.empty() ){
        return statuses[0];
    }
    //4.
    //Lock::CollectionLock colLock(txn->lockState(), nss.ns(), MODE_IX);
    if (dbname != "admin" && dbname != "config")
    {
        Status  status = updateCollectionOnCfg(txn, nss.ns(), idxDescriptor, result);
        if(!status.isOK()) {
            return status;
        }
    }
    return Status::OK();
}

Status dropIndexes(OperationContext* txn,
                   const NamespaceString& nss,
                   const BSONObj& idxDescriptor,
                   BSONObjBuilder* result) {
    StringData dbName = nss.db();
    MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
        ScopedTransaction transaction(txn, MODE_IX);
        AutoGetDb autoDb(txn, dbName, MODE_IX);
        Lock::CollectionLock collLock(txn->lockState(), nss.ns(), MODE_X);

        bool userInitiatedWritesAndNotPrimary = txn->writesAreReplicated() &&
            !repl::getGlobalReplicationCoordinator()->canAcceptWritesFor(nss);

        if (userInitiatedWritesAndNotPrimary) {
            return {ErrorCodes::NotMaster,
                    str::stream() << "Not primary while dropping indexes in " << nss.ns()};
        }

        WriteUnitOfWork wunit(txn);
        Status status = wrappedRun(txn, dbName, nss.ns(), autoDb.getDb(), idxDescriptor, result);
        if (!status.isOK()) {
            return status;
        }

        auto opObserver = getGlobalServiceContext()->getOpObserver();
        if (opObserver)
            opObserver->onDropIndex(txn, dbName.toString() + ".$cmd", idxDescriptor);
          
        //get all indexes
        BSONArray indexes;
        Database* db = dbHolder().get(txn, nss.db());
        Collection* collection = db->getCollection(nss.ns());
        collection->getAllIndexes(txn,indexes);
        //update chunkMeatadata
        uassert(90553, "database dropped during index build", db);
        uassert(90554, "collection dropped during index build", db->getCollection(nss.ns()));
        db->toUpdateChunkMetadata(txn,nss.ns(),indexes);
        //end

        wunit.commit();
    }
    MONGO_WRITE_CONFLICT_RETRY_LOOP_END(txn, "dropIndexes", dbName);
    return Status::OK();
}

Status reIndexesOnCfgSrv(OperationContext* txn,
                   const string& dbname,
                   const NamespaceString& nss,
                   const BSONObj& idxDescriptor,
                   BSONObjBuilder& result) {
    //1. get collection
    CollectionType coll;
    auto collStatus = grid.catalogClient(txn)->getCollection(txn,nss.ns());
    if(collStatus.isOK()){
        coll = collStatus.getValue().value;
    }else{
        log()<<"[catalog/drop_index.cpp:79] "<< nss.ns() << " collection not found";
        return Status(ErrorCodes::NamespaceNotFound, "_______collection not found");
    }
    BSONArrayBuilder indexArrayBuilder;
    std::vector<BSONObj> indexSpecs;
    BSONObjIterator indexspecs(coll.getIndex());
    while (indexspecs.more()) {
        BSONObj existingIndex = indexspecs.next().Obj();
        auto indexDescriptores = existingIndex.removeField("prefix").removeField("ns").removeField("v");
        indexSpecs.push_back(indexDescriptores);
        indexArrayBuilder.append(indexDescriptores);
    }
    //3. delete all indexes
    BSONObjBuilder cmdBuilder;
    cmdBuilder.append("deleteIndexes",nss.coll());
    cmdBuilder.append("index","*");
    Status dropstatus = dropIndexesOnCfgSrv(txn,dbname,nss,cmdBuilder.obj(),result);
    if( !dropstatus.isOK() )
        return dropstatus;
    //4. create new indexes
    BSONObjBuilder cmd;
    cmd.append("createIndexes",nss.coll().toString());
    cmd.append("indexes",indexArrayBuilder.arr());
    BSONObj cmdObj = cmd.obj();
    auto createstatus = createIndexMetadata(txn,nss,indexSpecs,cmdObj,dbname,result);
    return createstatus;
}
}  // namespace mongo

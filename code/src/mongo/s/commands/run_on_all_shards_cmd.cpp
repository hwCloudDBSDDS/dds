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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include "mongo/s/commands/run_on_all_shards_cmd.h"

#include <list>
#include <set>

#include "mongo/db/jsobj.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/commands/cluster_commands_common.h"
#include "mongo/s/commands/sharded_command_processing.h"
#include "mongo/s/grid.h"
#include "mongo/s/sharding_raii.h"
#include "mongo/util/log.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/s/config.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/bson/bsonobjbuilder.h"

namespace mongo {

struct CollModRQ{
    BSONElement collValidator = {};
    BSONElement collValidationAction = {};
    BSONElement collValidationLevel = {};
    BSONElement indexExpireAfterSeconds = {};
    BSONElement noPadding = {};
};


RunOnAllShardsCommand::RunOnAllShardsCommand(const char* name,
                                             const char* oldName,
                                             bool useShardConn,
                                             bool implicitCreateDb)
    : Command(name, false, oldName),
      _useShardConn(useShardConn),
      _implicitCreateDb(implicitCreateDb) {}

void RunOnAllShardsCommand::aggregateResults(const std::vector<ShardAndReply>& results,
                                             BSONObjBuilder& output) {}

BSONObj RunOnAllShardsCommand::specialErrorHandler(const std::string& server,
                                                   const std::string& db,
                                                   const BSONObj& cmdObj,
                                                   const BSONObj& originalResult) const {
    return originalResult;
}

void RunOnAllShardsCommand::getShardIds(OperationContext* txn,
                                        const std::string& db,
                                        BSONObj& cmdObj,
                                        std::vector<ShardId>& shardIds) {
    grid.shardRegistry()->getAllShardIds(&shardIds);
}

bool RunOnAllShardsCommand::run(OperationContext* txn,
                                const std::string& dbName,
                                BSONObj& cmdObj,
                                int options,
                                std::string& errmsg,
                                BSONObjBuilder& output) {
    LOG(0) << "RunOnAllShardsCommand db: " << dbName << " cmd:" << cmdObj;

    if (_implicitCreateDb) {
        uassertStatusOK(ScopedShardDatabase::getOrCreate(txn, dbName));
    }
    const NamespaceString nss(parseNs(dbName, cmdObj));
    std::vector<ShardId> shardIds;
    getShardIds(txn, dbName, cmdObj, shardIds);

    std::list<std::shared_ptr<Future::CommandResult>> futures;
    if( dbName == parseNs(dbName, cmdObj) ){
        for (const ShardId& shardId : shardIds) {
            const auto shardStatus = grid.shardRegistry()->getShard(txn, shardId);
            if (!shardStatus.isOK()) {
                continue;
            }

            futures.push_back(Future::spawnCommand(shardStatus.getValue()->getConnString().toString(),
                                               dbName,
                                               cmdObj,
                                               0,
                                               NULL,
                                               _useShardConn));
        }
    }else{
        std::vector<ChunkType> Chunks;
        uassertStatusOK(grid.catalogClient(txn)->getChunks(txn,
                                           BSON(ChunkType::ns(nss.ns())),
                                           BSONObj(),
                                           boost::none,
                                           &Chunks,
                                           nullptr,
                                           repl::ReadConcernLevel::kMajorityReadConcern));
     
        auto status = grid.catalogCache()->getDatabase(txn, dbName);
        if (!status.isOK()) {
            if (status == ErrorCodes::NamespaceNotFound) {
                output.append("info", "database does not exist");
                return true;
            }
            return appendCommandStatus(output, status.getStatus());
        }
        auto conf = status.getValue();
        for (ChunkType &chunk : Chunks) {
            BSONObjBuilder newCmd;
            const auto shardStatus = grid.shardRegistry()->getShard(txn, chunk.getShard());
            if (!shardStatus.isOK()) {
                continue;
            }
            newCmd.appendElements(cmdObj);
            newCmd.append("chunkId",chunk.getName());
            futures.push_back(Future::spawnCommand(shardStatus.getValue()->getConnString().toString(),
                                               dbName,
                                               newCmd.obj(),
                                               0,
                                               NULL,
                                               _useShardConn));
        }
    }
    std::vector<ShardAndReply> results;
    BSONObjBuilder subobj(output.subobjStart("raw"));
    BSONObjBuilder errors;
    int commonErrCode = -1;

    std::list<std::shared_ptr<Future::CommandResult>>::iterator futuresit;
    std::vector<ShardId>::const_iterator shardIdsIt;

    BSONElement wcErrorElem;
    ShardId wcErrorShardId;
    bool hasWCError = false;

    // We iterate over the set of shard ids and their corresponding futures in parallel.
    // TODO: replace with zip iterator if we ever decide to use one from Boost or elsewhere
    for (futuresit = futures.begin(), shardIdsIt = shardIds.cbegin();
         futuresit != futures.end() && shardIdsIt != shardIds.end();
         ++futuresit, ++shardIdsIt) {
        std::shared_ptr<Future::CommandResult> res = *futuresit;

        if (res->join(txn)) {
            // success :)
            BSONObj result = res->result();
            results.emplace_back(shardIdsIt->toString(), result);
            subobj.append(res->getServer(), result);

            if (!hasWCError) {
                if ((wcErrorElem = result["writeConcernError"])) {
                    wcErrorShardId = *shardIdsIt;
                    hasWCError = true;
                }
            }
            continue;
        }

        BSONObj result = res->result();

        if (!hasWCError) {
            if ((wcErrorElem = result["writeConcernError"])) {
                wcErrorShardId = *shardIdsIt;
                hasWCError = true;
            }
        }

        if (result["errmsg"].type() || result["code"].numberInt() != 0) {
            result = specialErrorHandler(res->getServer(), dbName, cmdObj, result);

            BSONElement errmsgObj = result["errmsg"];
            if (errmsgObj.eoo() || errmsgObj.String().empty()) {
                // it was fixed!
                results.emplace_back(shardIdsIt->toString(), result);
                subobj.append(res->getServer(), result);
                continue;
            }
        }

        // Handle "errmsg".
        if (!result["errmsg"].eoo()) {
            errors.appendAs(result["errmsg"], res->getServer());
        } else {
            // Can happen if message is empty, for some reason
            errors.append(res->getServer(),
                          str::stream() << "result without error message returned : " << result);
        }

        // Handle "code".
        int errCode = result["code"].numberInt();
        if (commonErrCode == -1) {
            commonErrCode = errCode;
        } else if (commonErrCode != errCode) {
            commonErrCode = 0;
        }
        results.emplace_back(shardIdsIt->toString(), result);
        subobj.append(res->getServer(), result);
    }

    subobj.done();

    if (hasWCError) {
        appendWriteConcernErrorToCmdResponse(wcErrorShardId, wcErrorElem, output);
    }

    BSONObj errobj = errors.done();

    if (!errobj.isEmpty()) {
        errmsg = errobj.toString();

        // If every error has a code, and the code for all errors is the same, then add
        // a top-level field "code" with this value to the output object.
        if (commonErrCode > 0) {
            output.append("code", commonErrCode);
        }

        return false;
    }

    aggregateResults(results, output);
    //update configServer collections
    auto collmod = cmdObj.getField("collMod");
    BSONElement indexName;
    BSONObj keyPattern;
    bool flag = false;
    if( !collmod.eoo()){
        CollModRQ cmr;
        for (BSONElement e : cmdObj){
            if (str::equals("collMod", e.fieldName())) {
                continue;  
            }else if(str::equals("validator", e.fieldName())){
                 flag = true;
                 cmr.collValidator = e;
            }else if(str::equals("validationLevel", e.fieldName())){
                 flag = true;
                 cmr.collValidationLevel = e;
            }else if(str::equals("validationAction", e.fieldName())){
                 flag = true;
                 cmr.collValidationAction = e;
            }else if(str::equals("index", e.fieldName())){ //we ensure that coll is not view
                 BSONObj indexObj = e.Obj();
                 BSONElement nameElem = indexObj["name"];
                 BSONElement keyPatternElem = indexObj["keyPattern"];
                 if(nameElem){
                     indexName = nameElem;
                 }
                 if (keyPatternElem){
                     keyPattern = keyPatternElem.embeddedObject();
                 }
                 cmr.indexExpireAfterSeconds = indexObj["expireAfterSeconds"];
                 
            }else{
                 const StringData name = e.fieldNameStringData();
                 if (name == "noPadding"){
                     cmr.noPadding = e;
                 }
            }
       }
       auto status = Grid::get(txn)->catalogClient(txn)->getCollection(txn,nss.ns());
       if (status.isOK()) {
            CollectionType coll = status.getValue().value;
            BSONObjBuilder newOptions;
            BSONObj option = coll.getOptions();
            BSONObj index = coll.getIndex();
            if (!cmr.collValidator.eoo()){
                option = option.removeField("validator");
                newOptions.append(cmr.collValidator);
            }
            if (!cmr.collValidationAction.eoo()){
                option = option.removeField("validationAction");
                newOptions.append(cmr.collValidationAction);
            }else{
                if( option.getObjectField("validationAction").isEmpty() && flag){
                     newOptions.append("validationAction","strict");
                }
            }
            if(!cmr.collValidationLevel.eoo()){
                option = option.removeField("validationLevel");
                newOptions.append(cmr.collValidationLevel);
            }else{
                if(option.getObjectField("validationLevel").isEmpty() && flag ){
                      newOptions.append("validationLevel","error");
                }
            }
            if(!cmr.indexExpireAfterSeconds.eoo()){
                 BSONElement& newExpireSecs = cmr.indexExpireAfterSeconds;
                 if (!indexName.eoo()){
                     BSONObjIterator indexspecs(index);
                     BSONArrayBuilder indexArrayBuilder;
                     while(indexspecs.more()){
                         BSONObj existingIndex = indexspecs.next().Obj();
                         BSONObjBuilder build;
                         if( existingIndex["name"].toString() == indexName.toString() ){
                             build.appendElements(existingIndex.removeField("expireAfterSeconds"));
                             build.append(newExpireSecs);
                             indexArrayBuilder.append(build.obj());
                         }else{
                             indexArrayBuilder.append(existingIndex);  
                         }
                     }
                     coll.setIndex(indexArrayBuilder.arr());
                 }else{
                     BSONObjIterator indexspecs(index);
                     BSONArrayBuilder indexArrayBuilder;
                     while(indexspecs.more()){
                         BSONObj existingIndex = indexspecs.next().Obj();
                         BSONObjBuilder build;
                         LOG(0)<<"existingIndex :"<<existingIndex["key"].toString()<<","<<keyPattern.toString();
                         if( existingIndex.getObjectField("key").toString() == keyPattern.toString() ){
                             build.appendElements(existingIndex.removeField("expireAfterSeconds"));
                             build.append(newExpireSecs);
                             indexArrayBuilder.append(build.obj());
                         }else{
                             indexArrayBuilder.append(existingIndex);
                         }
                     }
                     coll.setIndex(indexArrayBuilder.arr());
                 }
            }
            if(!cmr.noPadding.eoo()){
                const StringData name = cmr.noPadding.fieldNameStringData();
                const bool newSetting = cmr.noPadding.trueValue();
                if( newSetting ){
                    newOptions.appendNumber("flags",2);
                }else{
                    newOptions.appendNumber("flags",0);
                }
            } 
            newOptions.appendElements(option);
            coll.setOptions(newOptions.obj());
            uassertStatusOK(grid.catalogClient(txn)->updateCollection(txn,nss.ns(),coll));  
       }
    }
    return true;
}
}

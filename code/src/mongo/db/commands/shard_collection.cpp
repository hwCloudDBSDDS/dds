/******************************************************************************
                Copyright 1999 - 2017, Huawei Tech. Co., Ltd.
                           ALL RIGHTS RESERVED
  File Name     : shard_collection.cpp
  Version       : Initial Draft
  Author        : 
  Created       : 2017/6/20
  Description   : add shardcollection cmd for mongod     -  CR rebalancing
  History       :
  1.Date        : 2017/6/20
    Author      : 
    Modification: Created file

******************************************************************************/


#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include <string>
#include <vector>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include "mongo/base/string_data.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/index_create.h"
#include "mongo/db/catalog/index_key_validate.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/op_observer.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/server_options.h"
#include "mongo/db/views/view_catalog.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/log.h"
#include "mongo/s/grid.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/assign_chunk_request.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/s/catalog/sharding_catalog_manager.h"
#include "mongo/db/s/balancer/balancer.h"
#include "mongo/db/field_parser.h"
#include "mongo/s/config.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/client/connpool.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsonobj.h"


namespace mongo {

const ReadPreferenceSetting kPrimaryOnlyReadPreference{ReadPreference::PrimaryOnly};

using std::string;
using std::vector;
Status _createIndexMetadata(OperationContext* txn,
                        const NamespaceString& ns,
                        BSONObj &keypatternProperty,
                        bool unique){
        int flag = 0;

        CollectionType coll;
        auto collStatus = grid.catalogClient(txn)->getCollection(txn, ns.ns());
        if (! collStatus.isOK()) {
            // return error
            return Status(ErrorCodes::NamespaceNotFound, "collection not found");
        }

        coll = collStatus.getValue().value;
        BSONArrayBuilder indexArrayBuilder;
        BSONObjIterator indexspecs(coll.getIndex());
        long long ll_max_prefix = coll.getPrefix();

        //check index is existed or not?
        while (indexspecs.more()) {
            BSONObj existingIndex = indexspecs.next().Obj();
            indexArrayBuilder.append(existingIndex);
            if (keypatternProperty.toString() == existingIndex["key"].toString()){
                LOG(0) << ns.ns() << "index have Exist:" << existingIndex["key"].toString();
                flag = 1;
                break;
            }
        }
        if(flag){
            return Status(ErrorCodes::IndexAlreadyExists, "IndexAlreadyExists");
        }
        BSONObjBuilder indexBuilder;
        std::string str= keypatternProperty.toString();
        char name[512]={0};
        int m=0;
        std::string::iterator  it;
        for(it =str.begin(); it != str.end(); ++it)
        {
            if(*it ==' '|| *it == '{' || *it == '.' || *it == '}' || *it == '"' )
            {
                if (*it == '.') {
                    ++ it;
                }
                continue;
            }else if( *it == ':'){
                name[m++]='_';
            }else{
                name[m++]=*it;
            }
        }

        const auto featureCompatibilityVersion = serverGlobalParams.featureCompatibility.version.load();
        const auto indexVersion = IndexDescriptor::getDefaultIndexVersion(featureCompatibilityVersion);

        BSONObjIterator insp(coll.getIndex());
        BSONObj index = insp.next().Obj();
        indexBuilder.append("key",keypatternProperty);
        indexBuilder.append("prefix", ll_max_prefix+1);
        indexBuilder.append("ns",ns.ns());
        indexBuilder.append("v",static_cast<int>(indexVersion));
        indexBuilder.append("name",name);
        if( unique ){
            indexBuilder.append("unique",true);
        }
        indexArrayBuilder.append (indexBuilder.obj());

        coll.setIndex (indexArrayBuilder.arr());
        coll.setPrefix(ll_max_prefix+1);

        uassertStatusOK(grid.catalogClient(txn)->updateCollection(txn,ns.ns(),coll));
        return Status::OK();
    }


namespace {

    class CmdShardCollection : public Command {
        private:
            Status _createFirstChunks(OperationContext* txn,
                    const ShardId& primaryShardId,
                    const vector<BSONObj>& allSplits,
                    const std::string &ns,
                    BSONObj &cmdObj) {
                int count = allSplits.size();
                //int count = 1;
                ChunkVersion version(1, 0, OID::gen());
                ShardKeyPattern keypattern(cmdObj.getObjectField("key").getOwned());
                BSONObj min,max;

                std::vector<ShardId> allShards;
                //auto& prng = txn->getClient()->getPrng();
                grid.shardRegistry()->getAllShardIds(&allShards);
                if (0 == allShards.size()) { 
                    return Status(ErrorCodes::ShardNotFound, "no shard available");
                }
                
                int chunk_num = 0;
                for(int i=0;i<=count;i++){
                    if( i==0 && count==0){
                        min = keypattern.getKeyPattern().globalMin();
                        max = keypattern.getKeyPattern().globalMax();
                    }else{
                        if( i==0 ){
                            min = keypattern.getKeyPattern().globalMin();
                            max = allSplits[i];
                        }else if( i>0 && i <= count-1 ){
                            min = allSplits[i-1];
                            max = allSplits[i];
                        }else if( i== count){
                            min = allSplits[i-1];
                            max = keypattern.getKeyPattern().globalMax();
                        }
                    }

                    ChunkType chunk;
                    chunk.setNS(ns);
                    chunk.setMin(min);
                    chunk.setMax(max);
                    //chunk.setShard(primaryShardId);
                    chunk.setVersion(version);
                    std::string chunkID;
                    auto c_status = grid.catalogClient(txn)->generateNewChunkID(txn, chunkID);
                    if (!c_status.isOK()) {
                        log()<<"[CS_SHARDCOLL]assign fail";
                        return c_status;
                    }
                    chunk.setName(chunkID);
                    //chunk.setShard(allShards[prng.nextInt32(allShards.size())]);
                    chunk.setShard(allShards[i%(allShards.size())]);
                    // update chunk status if this is a new chunk
                    chunk.setStatus(ChunkType::ChunkStatus::kOffloaded);
                {
                    //maybe meet corrupt if rootfolder is null.
                    std::string chunkRootFolder;
                    Status status = grid.catalogManager()->createRootFolder(txn, chunk.getID(), chunkRootFolder);
                    if (!status.isOK()) {
                        log() << "[CS_SHARDCOLL] failed to create root folder for chunk:"<< chunk.getID();
                        return status;
                    }
                    chunk.setRootFolder(chunkRootFolder);
                }      
                    #if 0 
                    //not insert config.chunks, assign flow will insert the config.chunks record
                    
                    Status status = grid.catalogClient(txn)->insertConfigDocument(
                            txn, ChunkType::ConfigNS, chunk.toBSON(), ShardingCatalogClient::kMajorityWriteConcern);
                    if (!status.isOK()) {
                        const string errMsg = str::stream() << "Creating first chunks failed: "<< redact(status.reason());
                        error() << errMsg;
                        return Status(status.code(), errMsg);
                    }
                    #endif
                    auto collstatus = grid.catalogClient(txn)->getCollection(txn, chunk.getNS());
                    if (!collstatus.isOK()) {
                        log()<<"[CS_SHARDCOLL]update coll fail";
                        return collstatus.getStatus();
                    }
                    CollectionType coll = collstatus.getValue().value;
                    coll.setEpoch(version.epoch());
                    coll.setUpdatedAt(Date_t::fromMillisSinceEpoch(version.toLong()));
                    coll.setKeyPattern(keypattern.toBSON());
                    auto status = grid.catalogClient(txn)->updateCollection(txn, ns, coll);
                    if (!status.isOK()) {
                        log()<<"[CS_SHARDCOLL]update coll fail";
                        return status;
                    }

                    auto shardId = chunk.getShard();
                    auto assignStatus = Balancer::get(txn)->assignChunk(txn, chunk, true, true, shardId,true);

                    if (!assignStatus.isOK()) {
                        log()<<"[CS_SHARDCOLL]assign fail";
                        return assignStatus;
                    }
                    chunk_num++;
                    if(chunk_num >= 5){
                        chunk_num = 0;
                        Status assign_status = Balancer::get(txn)->getResults(txn,ns);
                        if (!assign_status.isOK()) {
                            return assign_status;
                        } 
                    }

                }

                if(chunk_num > 0){
                    chunk_num = 0;
                    Status assign_status = Balancer::get(txn)->getResults(txn,ns);
                    if (!assign_status.isOK()) {
                        return assign_status;
                    } 
                }
                
                return Status::OK(); 
            }

                    
            Status _createCollectionMetadata(OperationContext *txn,
                                             const NamespaceString& ns){


                Status status = userAllowedCreateNS(ns.db(), ns.coll());
                if (!status.isOK()) {
                    return status;
                }  
                long long ll_max_prefix =0;

                CollectionType coll;
                coll.setNs(ns);
                coll.setEpoch(OID::gen());
                coll.setUpdatedAt(Date_t::fromMillisSinceEpoch(1));
                coll.setKeyPattern (BSON("a" << 1));
                coll.setPrefix(ll_max_prefix +1);
                coll.setTabType(CollectionType::TableType::kSharded);

                const auto featureCompatibilityVersion =
                    serverGlobalParams.featureCompatibility.version.load();
                const auto indexVersion = IndexDescriptor::getDefaultIndexVersion(featureCompatibilityVersion);

                BSONObjBuilder b;
                b.append("v", static_cast<int>(indexVersion));
                b.append("name", "_id_");
                b.append("ns", ns.ns());
                b.append("key", BSON("_id" << 1));
                b.append("prefix", ll_max_prefix +1);
                BSONArrayBuilder indexArrayBuilder;
                indexArrayBuilder.append (b.obj());
                coll.setIndex (indexArrayBuilder.arr());

                uassertStatusOK(grid.catalogClient(txn)->updateCollection(txn, ns.ns(), coll));
                return Status::OK();
            }
public:
    CmdShardCollection() : Command("shardCollection", false, "shardcollection") {}

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }
    virtual bool slaveOk() const {
        return false;
    }

    virtual Status checkAuthForCommand(Client* client,
                                       const std::string& dbname,
                                       const BSONObj& cmdObj) {
        ActionSet actions;
        actions.addAction(ActionType::createIndex);
        Privilege p(parseResourcePattern(dbname, cmdObj), actions);
        if (AuthorizationSession::get(client)->isAuthorizedForPrivilege(p))
            return Status::OK();
        return Status(ErrorCodes::Unauthorized, "Unauthorized");
    }

    virtual std::string parseNs(const std::string& dbname, const BSONObj& cmdObj) const {
        return parseNsFullyQualified(dbname, cmdObj);
    }
    virtual bool run(OperationContext* txn,
                     const string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     string& errmsg,
                     BSONObjBuilder& result) {
        const NamespaceString nss(parseNs(dbname, cmdObj));
        return run_normal(txn, dbname, cmdObj, options, errmsg, result);

    }

    static bool isIdIndexPattern(const BSONObj& pattern) {
        BSONObjIterator i(pattern);
        BSONElement e = i.next();
        //_id index must have form exactly {_id : 1} or {_id : -1}.
        // Allows an index of form {_id : "hashed"} to exist but
        // do not consider it to be the primary _id index
        if (!(strcmp(e.fieldName(), "_id") == 0 && (e.numberInt() == 1 || e.numberInt() == -1)))
            return false;
        return i.next().eoo();
    }

    virtual bool run_normal(OperationContext* txn,
                                const string& dbname,
                                BSONObj& cmdObj,
                                int options,
                                string& errmsg,
                                BSONObjBuilder& result){
        const NamespaceString nss(parseNs(dbname, cmdObj));
        bool unique = false;
        BSONElement e = cmdObj.getField("unique");
        if( !e.eoo() ){
            unique = e.type() == Bool ? e.boolean() : false;
        }
        // 1. createCollection
        // 1.1 check if the Collection already exists
        auto collStatus = grid.catalogClient(txn)->getCollection(txn,nss.ns());
        if( !collStatus.isOK() ){
            log()<<"[CS_SHARDCOLL] "<<nss.ns()<<" not exist,need to be created";
            // 1.2 createCollection Metadata
            Status st = _createCollectionMetadata(txn,nss);//maybe carry options to save into config.collections
            if( !st.isOK() ){
                return appendCommandStatus(result, st);
            }
        }else{
            CollectionType coll;
            coll = collStatus.getValue().value;
            if( coll.getDropped() ){
                Status st = _createCollectionMetadata(txn,nss);
                if( !st.isOK() ){
                   return appendCommandStatus(result, st);
                }
            }
        }
        auto property = cmdObj.getObjectField("key").getOwned();
        if (property.isEmpty()) {
            errmsg = "no shard key";
            return false;
        }
        ShardKeyPattern keypattern(property);
        if (!keypattern.isValid()) {
            errmsg = str::stream() << "Unsupported shard key pattern. Pattern must"
                << " either be a single hashed field, or a list"
                << " of ascending fields.";
            return false;
        }

        
        //2. create index metadata on config server
        if(serverGlobalParams.clusterRole == ClusterRole::ConfigServer 
            && dbname != "config" && dbname != "system"){
            // if succeed to create index metadata, then pass the commands to 
            // all the shard servers
            if( !isIdIndexPattern(cmdObj.getObjectField("key"))){ 
                if (!cmdObj.getObjectField("usefulIndex").isEmpty()) {
                    auto usefulIndex = cmdObj.getObjectField("usefulIndex").getOwned();
                    ShardKeyPattern keypattern(usefulIndex);
                    bool usefulIndexUniq =  cmdObj["usefulIndexUniq"].trueValue();
                    if(!_createIndexMetadata(txn, nss, usefulIndex,usefulIndexUniq).isOK()){
                        log()<<" CreateIndexMetadata Error ..........................";    
                        return false; 
                    }
                
                }
                else if(!_createIndexMetadata(txn, nss, property,unique).isOK()){
                    //grid.catalogClient(txn)->createIndexOnShards(txn, ns, cmdObj);
                    log()<<" CreateIndexMetadata Error ..........................";    
                    return false; 
                }
            }
        }

            
        bool isHashedShardKey = keypattern.isHashedPattern();
        if (isHashedShardKey && cmdObj["unique"].trueValue()) {
            dassert(property.nFields() == 1);
            errmsg = "hashed shard keys cannot be declared unique.";
            return false;
        }
        uassert(ErrorCodes::IllegalOperation, "can't shard system namespaces", !nss.isSystem());
        bool simpleCollationSpecified = false;
        {
            BSONElement collationElement;
            Status collationStatus =
                bsonExtractTypedField(cmdObj, "collation", BSONType::Object, &collationElement);
            if (collationStatus.isOK()) {
                // Ensure that the collation is valid. Currently we only allow the simple collation.
                auto collator = CollatorFactoryInterface::get(txn->getServiceContext())
                                    ->makeFromBSON(collationElement.Obj());
                if (!collator.getStatus().isOK()) {
                    return appendCommandStatus(result, collator.getStatus());
                }

                if (collator.getValue()) {
                    return appendCommandStatus(
                        result,
                        {ErrorCodes::BadValue,
                         str::stream()
                             << "The collation for shardCollection must be {locale: 'simple'}, "
                             << "but found: "
                             << collationElement.Obj()});
                }

                simpleCollationSpecified = true;
            } else if (collationStatus != ErrorCodes::NoSuchKey) {
                return appendCommandStatus(result, collationStatus);
            }
        }
        vector<ShardId> shardIds;
        grid.shardRegistry()->getAllShardIds(&shardIds);
        int numShards = shardIds.size();

        const int maxNumInitialChunksForShards = numShards * 8192;
        const int maxNumInitialChunksTotal = 1000 * 1000;
        int numChunks = cmdObj["numInitialChunks"].numberInt();
        if (numChunks > maxNumInitialChunksForShards || numChunks > maxNumInitialChunksTotal) {
            errmsg = str::stream()
                << "numInitialChunks cannot be more than either: " << maxNumInitialChunksForShards
                << ", 8192 * number of shards; or " << maxNumInitialChunksTotal;
            return false;
        }

        // 0. collection should be empty.
        auto config = uassertStatusOK(grid.catalogCache()->getDatabase(txn, nss.db().toString()));
        ConnectionString shardConnString;
        {
            const auto shard =
                uassertStatusOK(grid.shardRegistry()->getShard(txn, config->getPrimaryId()));
            shardConnString = shard->getConnString();
        }

        ScopedDbConnection conn(shardConnString);
        //?? ns on cfg has not chunkid, but ns shard server has chunkid, what's the followed return value?
        bool isEmpty = (conn->count(nss.ns()) == 0);
        conn.done();
        bool hasData = cmdObj["hasData"].trueValue();

        vector<BSONObj> initSplits;
        vector<BSONObj> allSplits;        
        if(isHashedShardKey && isEmpty && !hasData){
            if (numChunks <= 0) {
                // default number of initial chunks
                numChunks = 2 * numShards;
            }
            long long intervalSize = (std::numeric_limits<long long>::max() / numChunks) * 2;

            long long current = 0;
            auto proposedKey =cmdObj.getObjectField("key").getOwned();
            if (numChunks % 2 == 0) {
                allSplits.push_back(BSON(proposedKey.firstElementFieldName() << current));
                current += intervalSize;
            } else {
                current += intervalSize / 2;
            }

            for (int i = 0; i < (numChunks - 1) / 2; i++) {
                allSplits.push_back(BSON(proposedKey.firstElementFieldName() << current));
                allSplits.push_back(BSON(proposedKey.firstElementFieldName() << -current));
                current += intervalSize;
            }

            sort(allSplits.begin(),
                    allSplits.end(),
                    SimpleBSONObjComparator::kInstance.makeLessThan());
            auto ss = grid.catalogClient(txn)->getDatabase(txn, nss.db().toString());
            if (!ss.isOK()) {
                log()<<"[CS_SHARDCOLL] get db fail";
                return false;
            }
            Status createFirstChunksStatus = _createFirstChunks(txn, ss.getValue().value.getPrimary(),allSplits,nss.ns(),cmdObj);
            if (!createFirstChunksStatus.isOK()) {
                return false;
            }
            log()<<".......... create chunk over";
       }
       else{
           if( !_create_one_chunk(keypattern,nss,txn,result,cmdObj)){
               return false;
           }
       }
    return true;    
    }

    bool _create_one_chunk(const ShardKeyPattern &keypattern,
            const NamespaceString &nss,
            OperationContext* txn,
            BSONObjBuilder& result,BSONObj &cmdObj){
        // 1. build the first chunk
        ChunkType    chunk;
        BSONObj min = keypattern.getKeyPattern().globalMin();
        BSONObj max = keypattern.getKeyPattern().globalMax();
        ChunkVersion version(1, 0, OID::gen());
        chunk.setNS(nss.ns());
        chunk.setMin(min);
        chunk.setMax(max);
        chunk.setVersion(version);
        std::string chunkID;
            auto c_status = grid.catalogClient(txn)->generateNewChunkID(txn, chunkID);
            if (!c_status.isOK()) {
                log()<<"[CS_SHARDCOLL]assign fail";
                return appendCommandStatus(result, c_status);
            }
        chunk.setName(chunkID);
        // get database info from config.databases, choose the primary shard for the chunk 
        auto dbstatus = grid.catalogClient(txn)->getDatabase(txn, nss.db().toString());
        if (!dbstatus.isOK()) {
            log()<<"[CS_SHARDCOLL] get db fail";
            return appendCommandStatus(result, dbstatus.getStatus());
        }

        std::vector<ShardId> allShards;
        grid.shardRegistry()->getAllShardIds(&allShards);
        if (0 == allShards.size()) { 
            return appendCommandStatus(result, {ErrorCodes::ShardNotFound, str::stream() << "no shard available"});
        }
        //chunk.setShard(allShards[prng.nextInt32(allShards.size())]);
        chunk.setShard(dbstatus.getValue().value.getPrimary());
        {
            //maybe meet corrupt if rootfolder is null.
            std::string chunkRootFolder;
            Status status = grid.catalogManager()->createRootFolder(txn, chunk.getID(), chunkRootFolder);
            if (!status.isOK()) {
                log() << "[CS_SHARDCOLL] failed to create root folder for chunk:"<< chunk.getID();
                return appendCommandStatus(result, status);
            }
            chunk.setRootFolder(chunkRootFolder);
            log() << "_create_one_chunk, root folder is "
                  << chunkRootFolder<<",chunk:"<< chunk.getID();
            
        }
        // 2. insert the chunk doc into config.chunks 
        Status status = grid.catalogClient(txn)->insertConfigDocument(
                txn, ChunkType::ConfigNS, chunk.toBSON(), ShardingCatalogClient::kMajorityWriteConcern);
        if (!status.isOK()) {
            log()<<"[CS_SHARDCOLL]create chunk fail";
            return appendCommandStatus(result, status);
        }

        // 3. update the collection info : epoch, keypattern
        auto collstatus = grid.catalogClient(txn)->getCollection(txn, chunk.getNS());
        if (!collstatus.isOK()) {
            log()<<"[CS_SHARDCOLL]update coll fail";
            return appendCommandStatus(result, collstatus.getStatus());
        }

        CollectionType coll = collstatus.getValue().value;
        coll.setEpoch(version.epoch());
        coll.setUpdatedAt(Date_t::fromMillisSinceEpoch(version.toLong()));
        coll.setKeyPattern(keypattern.toBSON());
        status = grid.catalogClient(txn)->updateCollection(txn, nss.ns(), coll);
        if (!status.isOK()) {
            log()<<"[CS_SHARDCOLL]update coll fail";
            return appendCommandStatus(result, status);
        }


        // 4. assign the new chunk
        auto shardId = chunk.getShard();
        auto assignStatus = Balancer::get(txn)->assignChunk(txn, chunk, true, true, shardId);

        if (!assignStatus.isOK()) {
            log()<<"[CS_SHARDCOLL]assign fail";
            return appendCommandStatus(result, assignStatus);
        }
        return true;
    }

                   
} CmdShardCollection;
}  // namespace
}  // namespace mongo

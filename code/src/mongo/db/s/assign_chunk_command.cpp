/******************************************************************************
                Copyright 1999 - 2017, Huawei Tech. Co., Ltd.
                           ALL RIGHTS RESERVED
  File Name     : assign_chunk_command.cpp
  Version       : Initial Draft
  Author        : 
  Created       : 2017/6/20
  Description   : assign chunk cmd
  History       :
  1.Date        : 2017/6/20
    Author      : 
    Modification: Created file

******************************************************************************/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/client/remote_command_targeter.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/drop_collection.h"
#include "mongo/db/commands.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/range_deleter_service.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/chunk_move_write_concern_options.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/s/catalog/dist_lock_manager.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/assign_chunk_request.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/db/catalog/index_key_validate.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/views/durable_view_catalog.h"
#include "mongo/db/modules/rocks/src/GlobalConfig.h"
#include "mongo/db/s/metadata_loader.h"

namespace mongo {

using std::string;

class AssignChunkCommand : public Command {
public:
    AssignChunkCommand() : Command("assignChunk") {}

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

    Status validateTargetInfo(const string& shardName, const string& processIdentity) {
        if (GLOBAL_CONFIG_GET(IsCoreTest)) {
            return Status::OK();
        }

        if ((0 != shardName.compare(serverGlobalParams.shardName)) ||
           (0 != processIdentity.compare(serverGlobalParams.processIdentity))) {
            return {ErrorCodes::TargetShardMismatch,
                    str::stream() << "targetShard Info(shardName: " << shardName
                                  << ", processIdentity: " << processIdentity
                                  << "), self info(shardName: " << serverGlobalParams.shardName
                                  << ", processIdentity: " << serverGlobalParams.processIdentity
                                  << ")"};
        }
        return Status::OK();
    }

    bool run(OperationContext* txn,
             const string& dbname,
             BSONObj& cmdObj,
             int ,
             string& errmsg,
             BSONObjBuilder& result) override {
    Date_t initExecTime = Date_t::now();
     // for Verify the parameters

    const NamespaceString ns(parseNs(dbname, cmdObj));
    BSONObj options1 = cmdObj.getObjectField("collection");
    BSONObj options = options1.getObjectField("options");

    if( !options.isEmpty() ){
        if (options.hasField("autoIndexId")) {
            const char* deprecationWarning =
                 "the autoIndexId option is deprecated and will be removed in a future release";
                 warning() << deprecationWarning;
                 result.append("note", deprecationWarning);
        }

        auto featureCompatibilityVersion = serverGlobalParams.featureCompatibility.version.load();
        auto validateFeaturesAsMaster =
                serverGlobalParams.featureCompatibility.validateFeaturesAsMaster.load();
        if (ServerGlobalParams::FeatureCompatibility::Version::k32 == featureCompatibilityVersion &&
            validateFeaturesAsMaster && options.hasField("collation")) {
            return appendCommandStatus(
                   result,
                   {ErrorCodes::InvalidOptions,
                   "The featureCompatibilityVersion must be 3.4 to create a collection or "
                   "view with a default collation. See "
                   "http://dochub.mongodb.org/core/3.4-feature-compatibility."});
        }
  
    // Validate _id index spec and fill in missing fields.
        if (auto idIndexElem = options["idIndex"]) {
            if (options["viewOn"]) {
                return appendCommandStatus(
                       result,
                       {ErrorCodes::InvalidOptions,
                       str::stream() << "'idIndex' is not allowed with 'viewOn': " << idIndexElem});
            }
            if (options["autoIndexId"]) {
                return appendCommandStatus(result,
                       {ErrorCodes::InvalidOptions,
                       str::stream()
                       << "'idIndex' is not allowed with 'autoIndexId': "
                       << idIndexElem});
            }

            if (idIndexElem.type() != BSONType::Object) {
                log() << " AssignChunkCommand::run()-> idIndexElem.type()-> " << (int)idIndexElem.type();
                 return appendCommandStatus(
                       result,
                       {ErrorCodes::TypeMismatch,
                       str::stream() << "'idIndex' has to be a document: " << idIndexElem});
            }

            auto idIndexSpec = idIndexElem.Obj();
        // Perform index spec validation.
            idIndexSpec = uassertStatusOK(index_key_validate::validateIndexSpec(
                idIndexSpec, ns, serverGlobalParams.featureCompatibility));
            uassertStatusOK(index_key_validate::validateIdIndexSpec(idIndexSpec));

        // Validate or fill in _id index collation.
            std::unique_ptr<CollatorInterface> defaultCollator;
            if (auto collationElem = options["collation"]) {
                if (collationElem.type() != BSONType::Object) {
                    return appendCommandStatus(
                           result,
                           {ErrorCodes::TypeMismatch,
                           str::stream() << "'collation' has to be a document: " << collationElem});
                }
                auto collatorStatus = CollatorFactoryInterface::get(txn->getServiceContext())
                      ->makeFromBSON(collationElem.Obj());
                if (!collatorStatus.isOK()) {
                    return appendCommandStatus(result, collatorStatus.getStatus());
                }
                defaultCollator = std::move(collatorStatus.getValue());
            }
            idIndexSpec = uassertStatusOK(index_key_validate::validateIndexSpecCollation(
                 txn, idIndexSpec, defaultCollator.get()));
            std::unique_ptr<CollatorInterface> idIndexCollator;
            if (auto collationElem = idIndexSpec["collation"]) {
                auto collatorStatus = CollatorFactoryInterface::get(txn->getServiceContext())
                       ->makeFromBSON(collationElem.Obj());
            // validateIndexSpecCollation() should have checked that the _id index collation
            // spec is valid.
                invariant(collatorStatus.isOK());
                idIndexCollator = std::move(collatorStatus.getValue());
            }
            if (!CollatorInterface::collatorsMatch(defaultCollator.get(), idIndexCollator.get())) {
                return appendCommandStatus(
                       result,
                       {ErrorCodes::BadValue,
                       "'idIndex' must have the same collation as the collection."});
            }

            // Remove "idIndex" field from command.
            auto resolvedCmdObj = options.removeField("idIndex");
            options = resolvedCmdObj;
            
        }
    }

        AssignChunkRequest assignChunkRequest = uassertStatusOK(AssignChunkRequest::createFromCommand(cmdObj));
        std::string shardName = assignChunkRequest.getShardName();
        std::string processIdentity = assignChunkRequest.getProcessIdentity();

        Status validateTargetRes = validateTargetInfo(shardName, processIdentity);
        if (!validateTargetRes.isOK()) {
            LOG(0) << "!validateTargetRes.isOK() cause by " << validateTargetRes;
            return appendCommandStatus(result, validateTargetRes);
        }

        // to generate the collection name for a given chunk
        NamespaceString nss = assignChunkRequest.getNss();
        // create new collection name: ns$chunkID

        NamespaceString nss_with_chunkID;
        if (GLOBAL_CONFIG_GET(IsCoreTest)) {
            nss_with_chunkID = nss;
        } else {
            nss_with_chunkID = NamespaceString(StringData(nss.ns()+'$'+ assignChunkRequest.getName()));
        }

        index_log() << "[assignChunk] shardSvr start nss: " << nss_with_chunkID;
        assignChunkRequest.setNs(nss_with_chunkID);

        getGlobalServiceContext()->registerProcessStageTime(
            "assignChunk:"+assignChunkRequest.getChunk().getName());
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+assignChunkRequest.getChunk().getName())->noteStageStart(
            "commandStart:"+serverGlobalParams.shardName);


        Database* db = nullptr;
        
        // Assign chunk. 
        MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
            ScopedTransaction transaction(txn, MODE_IX);
            Date_t initExecTime = Date_t::now();
            index_log() << "[assignChunk] shardSvr get DBLock nss: " << nss_with_chunkID;
            Lock::DBLock dbLock(txn->lockState(), nss_with_chunkID.db(), MODE_IX);
            if (!dbHolder().get(txn, nss.db())) {
                dbLock.relockWithMode(MODE_X);
                invariant(dbHolder().openDb(txn, nss_with_chunkID.ns()));
                dbLock.relockWithMode(MODE_IX);
            }
            Lock::CollectionLock collLock(txn->lockState(), nss_with_chunkID.ns(), MODE_X);

            index_log() << "[assignChunk] shardSvr geted DBLock nss: " << nss_with_chunkID <<
                "; used Time(ms): " << (Date_t::now()-initExecTime);  
            OldClientContext ctx(txn, nss_with_chunkID.ns()); //here will create new  database_catalog_entry if not exist
            if (txn->writesAreReplicated() &&
                !repl::getGlobalReplicationCoordinator()->canAcceptWritesFor(nss_with_chunkID)) {
                log() << "assignChunk failed: "
                      << dbname
                      << " cmdobj:"<<cmdObj
                      << " Request:"
                      << assignChunkRequest
                      << "Not primary while creating collection.";
                return appendCommandStatus(
                    result,
                    {ErrorCodes::NotMaster,
                              str::stream() << "Not primary while creating collection " << nss_with_chunkID.ns()});
            }

            WriteUnitOfWork wunit(txn);

            invariant(ctx.db());
            getGlobalServiceContext()->getProcessStageTime(
                "assignChunk:"+assignChunkRequest.getChunk().getName())->noteStageStart("assignChunk");

            Date_t initExecTime1 = Date_t::now();
            index_log() << "[assignChunk] shardSvr DataBase::assignChunk() nss : " << nss_with_chunkID;
            Status status = ctx.db()->assignChunk(txn, nss_with_chunkID.ns(), options, assignChunkRequest);
            index_log() << "[assignChunk] shardSvr DataBase::assignChunk() end nss : " << nss_with_chunkID <<
                "; used Time(ms): " << (Date_t::now()-initExecTime1);  

            if(status.code() == ErrorCodes::NamespaceExists){
                result.append("shardName", serverGlobalParams.shardName);
                result.append("processIdentity", serverGlobalParams.processIdentity);
                return true;
            }
            if (!status.isOK()) {
                log() << "assignChunk failed: "
                      << dbname
                      << " cmdobj:"<<cmdObj
                      << " Request:"
                      << assignChunkRequest
                      << "status:" + status.reason();
                return appendCommandStatus(result, status);
            }

            //invalidate view to reload
            /*NamespaceString ns_without_chunkid(nss.nsFilteredOutChunkId());
            if (ns_without_chunkid.coll() == DurableViewCatalog::viewsCollectionName()) {
                DurableViewCatalog::onExternalChange(txn, nss);
            }*/
            getGlobalServiceContext()->getProcessStageTime(
                "assignChunk:"+assignChunkRequest.getChunk().getName())->noteStageStart("wunit.commit");    
            wunit.commit();
            db = ctx.db();

            {

                Date_t initExecTime1 = Date_t::now();
                index_log() << "[assignChunk] shardSvr config::refreshMetadata() nss: " << nss_with_chunkID;
                if (!GLOBAL_CONFIG_GET(IsCoreTest)) {
                    std::unique_ptr<CollectionMetadata> remoteMetadata(stdx::make_unique<CollectionMetadata>());
                    status = MetadataLoader::makeCollectionMetadata(txn,
                        grid.catalogClient(txn),
                        nss_with_chunkID.ns(),
                        shardName,
                        nullptr,
                        remoteMetadata.get());
                    if (status.isOK()) {
                        auto css = CollectionShardingState::get(txn, nss_with_chunkID);
                        css->refreshMetadata(txn, std::move(remoteMetadata));
                    }
                }
                index_log() << "[assignChunk] shardSvr config::refreshMetadata() end nss: " << nss_with_chunkID<<
                    "; used Time(ms): " << (Date_t::now()-initExecTime1);  
            }

            
            LOG(1) << "db assignChunk complete: " << dbname;
        }
        MONGO_WRITE_CONFLICT_RETRY_LOOP_END(txn, "assignChunk", nss_with_chunkID.ns());
        {
            NamespaceString ns_without_chunkid(nss.nsFilteredOutChunkId());
            if (ns_without_chunkid.coll() == DurableViewCatalog::viewsCollectionName()) {
                Lock::DBLock dbXLock(txn->lockState(), nss_with_chunkID.db(), MODE_IS);
                DurableViewCatalog::onExternalChange(txn, nss);
            }
        }

        //OpenRocksDB
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+assignChunkRequest.getChunk().getName())->noteStageStart("assignChunkFinalize");

        Date_t initExecTime2 = Date_t::now();
        index_log() << "[assignChunk] shardSvr assignChunkFinalize nss : " << nss_with_chunkID;
        db->assignChunkFinalize(txn, nss_with_chunkID.ns(), assignChunkRequest);
        index_log() << "[assignChunk] shardSvr assignChunkFinalize end nss : " << nss_with_chunkID<<
            "; used Time(ms): " << (Date_t::now()-initExecTime2);  
     
        result.append("shardName", serverGlobalParams.shardName);
        result.append("processIdentity", serverGlobalParams.processIdentity);
        log() << "assignChunk complete: " << dbname <<" cmdobj:" << cmdObj;
        getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+assignChunkRequest.getChunk().getName())->noteProcessEnd();
        /*log() << "Time of " << "assignChunk:"+assignChunkRequest.getChunk().getName() << ": " 
            << getGlobalServiceContext()->getProcessStageTime(
            "assignChunk:"+assignChunkRequest.getChunk().getName())->toString();
        getGlobalServiceContext()->cancelProcessStageTime(
            "assignChunk:"+assignChunkRequest.getChunk().getName());*/
        index_log() << "[assignChunk] shardSvr end nss: " << nss_with_chunkID << "; used Time(ms): " <<
            (Date_t::now()-initExecTime);
        return true;
    }
} assignChunkCmd;

}  // namespace mongo

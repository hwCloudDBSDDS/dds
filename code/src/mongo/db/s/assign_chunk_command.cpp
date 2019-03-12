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
#include "mongo/db/catalog/index_key_validate.h"
#include "mongo/db/commands.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/range_deleter_service.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/chunk_move_write_concern_options.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/metadata_loader.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/views/durable_view_catalog.h"
#include "mongo/s/assign_chunk_request.h"
#include "mongo/s/catalog/dist_lock_manager.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/util_extend/GlobalConfig.h"
#include "mongo/util/util_extend/config_reader.h"


namespace mongo {

using std::string;

class AssignChunkCommand : public Command {
private:
    stdx::unordered_map<std::string, std::unique_ptr<stdx::mutex>> assignStateMap;
    stdx::mutex assignStateMapMutex;

    sem_t _sem;
    int semNum;
public:
    AssignChunkCommand() : Command("assignChunk") {
        semNum = ConfigReader::getInstance()->getDecimalNumber<int>(
            "PublicOptions", "max_chunk_count_in_one_shard") / 2;

        index_log()<<"semNum: "<< semNum<<", max_chunk_count_in_one_shard: "<< ConfigReader::getInstance()->getDecimalNumber<int>(
            "PublicOptions", "max_chunk_count_in_one_shard");
        if (0 != sem_init(&_sem, 0, semNum)) {
            index_err() << "init sem error: " << errnoWithDescription(errno);
            invariant(false);
        }     
    }

    ~AssignChunkCommand() {
        if(0 != sem_destroy(&_sem)) {
             index_err() << "destroy sem error: " << errnoWithDescription(errno);
        }
    }

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
        auto replMode = repl::getGlobalReplicationCoordinator()->getReplicationMode();
        auto single_mongod= ClusterRole::None == serverGlobalParams.clusterRole && 
                            replMode != repl::ReplicationCoordinator::Mode::modeReplSet;
        if (single_mongod) {
            return Status::OK();
        }

        if ((0 != shardName.compare(serverGlobalParams.shardName)) ||
            (0 != processIdentity.compare(serverGlobalParams.processIdentity))) {
            return {ErrorCodes::NeedChangeShard,
                    str::stream() << "Target shard mismatch, targetShard Info(shardName: "
                                  << shardName
                                  << ", processIdentity: "
                                  << processIdentity
                                  << "), self info(shardName: "
                                  << serverGlobalParams.shardName
                                  << ", processIdentity: "
                                  << serverGlobalParams.processIdentity
                                  << ")"};
        }
        return Status::OK();
    }

    Status openChunkDbInstance(OperationContext* txn,
                               StringData ns,
                               AssignChunkRequest& assignChunkRequest) {
       CollectionOptions coll_options;
       coll_options.dbPath = assignChunkRequest.getChunk().getRootFolder();
       coll_options.toCreate = assignChunkRequest.getNewChunkFlag();
       // create rockdb instance when assign the chunk first time, prepare the chunkmetadata
       coll_options.collection = assignChunkRequest.getCollection();
       coll_options.chunk = assignChunkRequest.getChunk();
       
       auto status = getGlobalServiceContext()->getGlobalStorageEngine()->openChunkDbInstance(txn, ns, coll_options);
       if (status.isOK()){
           //update assignChunkRequest to modify indexs info according to chunkmeta
           AssignChunkRequest request(coll_options.collection, coll_options.chunk, assignChunkRequest.getNewChunkFlag()
                               , assignChunkRequest.getShardName(), assignChunkRequest.getProcessIdentity());
           assignChunkRequest = request;
       }
       
       return status;
    }

    bool run(OperationContext* txn,
             const string& dbname,
             BSONObj& cmdObj,
             int,
             string& errmsg,
             BSONObjBuilder& result) override {
        Date_t initExecTime = Date_t::now();
        // add for Verify the parameters

        const NamespaceString ns(parseNs(dbname, cmdObj));
        BSONObj options1 = cmdObj.getObjectField("collection");
        BSONObj options = options1.getObjectField("options");
        index_LOG(0) << "start assignchunk, dbname: " << dbname << ",semNum: "<<semNum <<", cmd: " << cmdObj;

        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 20;
        if (0 != sem_timedwait(&_sem, &ts)) {
            index_err() << "assignChunk sem_timedwait error: " << errnoWithDescription(errno);
            return appendCommandStatus(result,
                       {ErrorCodes::InternalError, "sem_timewait error! please retry!"});
        }

        auto guardSem = MakeGuard([&] {
            if (0 != sem_post(&_sem)) {
                index_err() << "assignChunk sem_post error: " << errnoWithDescription(errno);;
            }
        });

        if (!options.isEmpty()) {
            if (options.hasField("autoIndexId")) {
                const char* deprecationWarning =
                    "the autoIndexId option is deprecated and will be removed in a future release";
                index_warning() << "autoIndexId: " << deprecationWarning;
                result.append("note", deprecationWarning);
            }

            auto featureCompatibilityVersion =
                serverGlobalParams.featureCompatibility.version.load();
            auto validateFeaturesAsMaster =
                serverGlobalParams.featureCompatibility.validateFeaturesAsMaster.load();
            if (ServerGlobalParams::FeatureCompatibility::Version::k32 ==
                    featureCompatibilityVersion &&
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
                    return appendCommandStatus(result,
                                               {ErrorCodes::InvalidOptions,
                                                str::stream()
                                                    << "'idIndex' is not allowed with 'viewOn': "
                                                    << idIndexElem});
                }
                if (options["autoIndexId"]) {
                    return appendCommandStatus(
                        result,
                        {ErrorCodes::InvalidOptions,
                         str::stream() << "'idIndex' is not allowed with 'autoIndexId': "
                                       << idIndexElem});
                }

                if (idIndexElem.type() != BSONType::Object) {
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
                        return appendCommandStatus(result,
                                                   {ErrorCodes::TypeMismatch,
                                                    str::stream()
                                                        << "'collation' has to be a document: "
                                                        << collationElem});
                    }

                    auto s_ctx = txn->getServiceContext();
                    if (s_ctx == nullptr) {
                        index_err() << "getServiceContext failed.";
                        return appendCommandStatus(result,
                                                   {ErrorCodes::InternalError,
                                                    str::stream() << "getServiceContext failed."});
                    }
                    auto collatorStatus =
                        CollatorFactoryInterface::get(s_ctx)->makeFromBSON(collationElem.Obj());
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
                if (!CollatorInterface::collatorsMatch(defaultCollator.get(),
                                                       idIndexCollator.get())) {
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

        auto replMode = repl::getGlobalReplicationCoordinator()->getReplicationMode();
        auto single_mongod= ClusterRole::None == serverGlobalParams.clusterRole && 
                            replMode != repl::ReplicationCoordinator::Mode::modeReplSet;

        if (serverGlobalParams.clusterRole != ClusterRole::ShardServer && !single_mongod) {
            return appendCommandStatus(result,
                                       {ErrorCodes::IllegalOperation, "can only be run on shard"});
        }
        AssignChunkRequest assignChunkRequest =
            uassertStatusOK(AssignChunkRequest::createFromCommand(cmdObj));
        std::string shardName = assignChunkRequest.getShardName();
        std::string processIdentity = assignChunkRequest.getProcessIdentity();

        Status validateTargetRes = validateTargetInfo(shardName, processIdentity);
        if (!validateTargetRes.isOK()) {
            index_warning() << "!validateTargetRes.isOK() cause by " << validateTargetRes;
            return appendCommandStatus(result, validateTargetRes);
        }

        // to generate the collection name for a given chunk
        NamespaceString nss = assignChunkRequest.getNss();
        // create new collection name: ns$chunkID
        NamespaceString nss_with_chunkID(StringData(nss.ns() + '$' + assignChunkRequest.getName()));
        assignChunkRequest.setNs(nss_with_chunkID);

        if (single_mongod) {
            nss_with_chunkID = nss;
        }
        getGlobalServiceContext()->registerProcessStageTime(
            "assignChunk:" + assignChunkRequest.getChunk().getName());
        auto guard = MakeGuard([&] {
            getGlobalServiceContext()->cancelProcessStageTime(
                "assignChunk:" + assignChunkRequest.getChunk().getName());
        });

        /*************assign chunk step1 --open rocksdb instance out of DB x
         * LOCK************************/
        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + assignChunkRequest.getChunk().getName())
            ->noteStageStart("open RocksDB");

        stdx::mutex* nsMutex;
        {
            stdx::lock_guard<stdx::mutex> assignStatusMapLock(assignStateMapMutex);
            std::string chunkNs = nss_with_chunkID.ns();
            auto it = assignStateMap.find(chunkNs);
            if (it == assignStateMap.end()) {
                it = assignStateMap
                         .emplace(std::make_pair(chunkNs, stdx::make_unique<stdx::mutex>()))
                         .first;
            }
            nsMutex = it->second.get();
        }

        stdx::lock_guard<stdx::mutex> nsMutexLock(*nsMutex);
        {
            AutoGetCollectionOrViewForRead ctx(txn, nss_with_chunkID);
            Collection* collection = ctx.getCollection();
            if (collection) {
                result.append("shardName", serverGlobalParams.shardName);
                result.append("processIdentity", serverGlobalParams.processIdentity);
                return true;
            }
        }

        {
            auto o_status = openChunkDbInstance(txn, nss_with_chunkID.ns(), assignChunkRequest);
            if (!o_status.isOK()) {
                index_err() << "openChunkDbInstance failed: " << dbname
                            << " Request:" << assignChunkRequest << "status:" << o_status;
                return appendCommandStatus(result, o_status);
            }
        }

        bool suc = false;
        auto guard2 = MakeGuard([&] {
            if (!suc) {
                try {
                    // NamespaceString nss(ns);
                    BSONObjBuilder res;
                    auto dropStatus = mongo::dropCollection(txn, nss_with_chunkID, res);
                    if (dropStatus == ErrorCodes::NamespaceNotFound) {
                        index_log() << "drop collection failed cause by " << dropStatus;
                        getGlobalServiceContext()->getGlobalStorageEngine()->destroyRocksDB(
                            nss_with_chunkID.toString());
                    }
                    // invariant(mongo::dropCollection(txn, nss_with_chunkID, res).isOK());

                    // make sure the chunk is cleaned
                    // invariant(!this->getCollection(ns, true));
                    index_log() << "assignChunkFinalize roll back sucess";
                } catch (const std::exception& exc) {
                    index_log() << "assign rollback failed" << exc.what();
                    // invariant(false);
                }
            }
        });

        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + assignChunkRequest.getChunk().getName())
            ->noteStageStart("start fetch DBLock");

        /*************assign chunk step2 --assign chunk in DB x LOCK************************/
        ScopedTransaction transaction(txn, MODE_IX);
        Lock::DBLock dbXLock(txn->lockState(), nss_with_chunkID.db(), MODE_X);
        OldClientContext ctx(txn, nss_with_chunkID.ns());

        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + assignChunkRequest.getChunk().getName())
            ->noteStageStart("end fetch DBLock");

        // Assign chunk.
        MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
            auto repl_coordinator = repl::getGlobalReplicationCoordinator();
            if (repl_coordinator == nullptr) {
                index_err() << "getGlobalReplicationCoordinator failed.";
                return appendCommandStatus(
                    result,
                    {ErrorCodes::InternalError,
                     str::stream() << "getGlobalReplicationCoordinator failed."});
            }

            if (txn->writesAreReplicated() &&
                !repl_coordinator->canAcceptWritesFor(nss_with_chunkID)) {
                index_err() << "assignChunk failed: " << dbname << " Request:" << assignChunkRequest
                            << "Not primary while creating collection.";
                return appendCommandStatus(result,
                                           {ErrorCodes::NotMaster,
                                            str::stream()
                                                << "Not primary while creating collection "
                                                << nss_with_chunkID.ns()});
            }

            WriteUnitOfWork wunit(txn);
            getGlobalServiceContext()
                ->getProcessStageTime("assignChunk:" + assignChunkRequest.getChunk().getName())
                ->noteStageStart("assignChunk");

            Status status =
                ctx.db()->assignChunk(txn, nss_with_chunkID.ns(), options, assignChunkRequest);
            if (status.code() == ErrorCodes::NamespaceExists) {
                result.append("shardName", serverGlobalParams.shardName);
                result.append("processIdentity", serverGlobalParams.processIdentity);
                return true;
            }
            if (!status.isOK()) {
                index_err() << "assignChunk failed: " << dbname << " Request:" << assignChunkRequest
                            << "status:" + status.reason();
                return appendCommandStatus(result, status);
            }

            // invalidate view to reload
            getGlobalServiceContext()
                ->getProcessStageTime("assignChunk:" + assignChunkRequest.getChunk().getName())
                ->noteStageStart("wunit.commit");
            wunit.commit();
            {

                Date_t initExecTime1 = Date_t::now();
                index_LOG(1) << "[assignChunk] shardSvr config::refreshMetadata() nss: "
                             << nss_with_chunkID;
                if (!single_mongod) {
                    std::unique_ptr<CollectionMetadata> remoteMetadata(
                        stdx::make_unique<CollectionMetadata>());
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
                index_LOG(1) << "[assignChunk] shardSvr config::refreshMetadata() end nss: "
                             << nss_with_chunkID
                             << "; used Time(ms): " << (Date_t::now() - initExecTime1);
            }


            index_LOG(1) << "db assignChunk complete: " << dbname;
        }
        MONGO_WRITE_CONFLICT_RETRY_LOOP_END(txn, "assignChunk", nss_with_chunkID.ns());
        {
            NamespaceString ns_without_chunkid(nss.nsFilteredOutChunkId());
            if (ns_without_chunkid.coll() == DurableViewCatalog::viewsCollectionName()) {
                DurableViewCatalog::onExternalChange(txn, nss);
            }
        }

        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + assignChunkRequest.getChunk().getName())
            ->noteStageStart("assignChunkFinalize");

        /*************assign chunk step3--createindex in DB x LOCK************************/
        auto status = ctx.db()->assignChunkFinalize(txn, nss_with_chunkID.ns(), assignChunkRequest);
        if (!status.isOK()) {
            result.append("shardName", serverGlobalParams.shardName);
            result.append("processIdentity", serverGlobalParams.processIdentity);
            return appendCommandStatus(result, status);
        }

        result.append("shardName", serverGlobalParams.shardName);
        result.append("processIdentity", serverGlobalParams.processIdentity);

        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + assignChunkRequest.getChunk().getName())
            ->noteProcessEnd();

        index_LOG(0) << "Time of "
                     << "assignChunk:" + assignChunkRequest.getChunk().getName() << ": "
                     << getGlobalServiceContext()
                            ->getProcessStageTime("assignChunk:" +
                                                  assignChunkRequest.getChunk().getName())
                            ->toString();
        index_LOG(1) << "shard end assignchunk, dbname: " << dbname;

        suc = true;
        return true;
    }
} assignChunkCmd;

}  // namespace mongo

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <string>
#include <vector>

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/client/connpool.h"
#include "mongo/client/remote_command_targeter.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/drop_collection.h"
#include "mongo/db/catalog/drop_indexes.h"
#include "mongo/db/catalog/index_create.h"
#include "mongo/db/catalog/index_key_validate.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/field_parser.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/op_observer.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/balancer/balancer.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/server_options.h"
#include "mongo/db/views/view_catalog.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/rpc/get_status_from_command_result.h"
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
#include "mongo/util/scopeguard.h"
#include "mongo/db/commands/operation_util.h"
#include "mongo/util/util_extend/GlobalConfig.h"

namespace mongo {

const ReadPreferenceSetting kPrimaryOnlyReadPreference{ReadPreference::PrimaryOnly};

using std::string;
using std::vector;

class CommandResult;

using CallbackHandle = executor::TaskExecutor::CallbackHandle;
using RemoteCommandCallbackArgs = executor::TaskExecutor::RemoteCommandCallbackArgs;
using RemoteCommandCallbackFn = executor::TaskExecutor::RemoteCommandCallbackFn;
using RemoteCommandRequest = executor::RemoteCommandRequest;
using RemoteCommandResponse = executor::RemoteCommandResponse;

namespace {

class CmdShardCollection : public Command {
private:
    // CollectionType _coll;
private:
    Status _createIndexMetadata(OperationContext* txn,
                                const NamespaceString& ns,
                                BSONObj& keypatternProperty,
                                bool unique,
                                std::string idxName = "") {
        int flag = 0;
        CollectionType _coll;
        auto collStatus = grid.catalogClient(txn)->getCollection(txn, ns.ns());
        if (!collStatus.isOK()) {
            return Status(ErrorCodes::NamespaceNotFound, "collection not found");
        }
        _coll = collStatus.getValue().value;
        // for bug "shardKey is too long"
        {
            BSONObjBuilder bob;
            bob.append("key", keypatternProperty);
            BSONObjIterator i(keypatternProperty);
            BSONElement e = i.next();
            bob.append("ns", ns.toString());
            bob.append("name", e.fieldName());
            bob.append("v", 2);
            Status status = mongo::prepareSpecForCreate(txn, ns, bob.obj(), NULL).getStatus();
            if (!status.isOK()) {
                return status;
            }
        }


        BSONArrayBuilder indexArrayBuilder;
        BSONObjIterator indexspecs(_coll.getIndex());
        long long ll_max_prefix = _coll.getPrefix();

        // check index is existed or not?
        while (indexspecs.more()) {
            BSONObj existingIndex = indexspecs.next().Obj();
            indexArrayBuilder.append(existingIndex);
            if (keypatternProperty.toString() == existingIndex.getField("key").toString(false)) {
                index_LOG(0) << "" << ns.ns() << "index have Exist:";
                flag = 1;
                break;
            }
        }
        if (flag) {
            return Status(ErrorCodes::IndexAlreadyExists, "IndexAlreadyExists");
        }
        BSONObjBuilder indexBuilder;
        std::stringstream indexName;
        bool isFirstKey = true;
        for (BSONObjIterator keyIter(keypatternProperty); keyIter.more();) {
            BSONElement currentKey = keyIter.next();

            if (isFirstKey) {
                isFirstKey = false;
            } else {
                indexName << "_";
            }

            indexName << currentKey.fieldName() << "_";
            if (currentKey.isNumber()) {
                indexName << currentKey.numberInt();
            } else {
                indexName << currentKey.str();  // this should match up with shell command
            }
        }

        const auto featureCompatibilityVersion =
            serverGlobalParams.featureCompatibility.version.load();
        const auto indexVersion =
            IndexDescriptor::getDefaultIndexVersion(featureCompatibilityVersion);

        BSONObjIterator insp(_coll.getIndex());
        BSONObj index = insp.next().Obj();
        indexBuilder.append("key", keypatternProperty);
        indexBuilder.append("prefix", ll_max_prefix + 1);
        indexBuilder.append("ns", ns.ns());
        indexBuilder.append("v", static_cast<int>(indexVersion));
        if (idxName.length() < 1) {
            indexBuilder.append("name", indexName.str());
        } else {
            indexBuilder.append("name", idxName);
        }
        if (unique) {
            indexBuilder.append("unique", true);
        }
        indexArrayBuilder.append(indexBuilder.obj());

        _coll.setIndex(indexArrayBuilder.arr());
        _coll.setPrefix(ll_max_prefix + 1);
        uassertStatusOK(grid.catalogClient(txn)->updateCollection(txn, ns.ns(), _coll));
        return Status::OK();
    }
    Status _writeCollAndIndexMetadata(OperationContext* txn,
                                      const std::string& ns,
                                      const CollectionType& coll) {
        return grid.catalogClient(txn)->updateCollection(txn, ns, coll);
    }

    Status _removeCollAndIndexMetadata(OperationContext* txn,
                                       const NamespaceString& nss,
                                       BSONObjBuilder& result) {
        auto status = dropCollectionOnCfgSrv(txn, nss, result);
        if (!status.isOK()) {
            index_log() << "[CS_SHARDCOLL] dropCollectionOnCfgSrv "
                        << static_cast<int>(status.code()) << " " << status.codeString();
            return status;
        }
        status = grid.catalogClient(txn)->removeConfigDocuments(
            txn,
            CollectionType::ConfigNS,
            BSON(CollectionType::fullNs() << nss.ns()),
            ShardingCatalogClient::kMajorityWriteConcern);
        if (!status.isOK()) {
            return status;
        }

        return grid.catalogClient(txn)->removeConfigDocuments(
            txn,
            ChunkType::ConfigNS,
            BSON(ChunkType::ns() << nss.ns()),
            ShardingCatalogClient::kMajorityWriteConcern);
    }

    Status _createFirstChunks(OperationContext* txn,
                              const ShardId& primaryShardId,
                              const vector<BSONObj>& allSplits,
                              const std::string& ns,
                              BSONObj& cmdObj) {
        int count = allSplits.size();
        // int count = 1;
        ChunkVersion version(1, 0, OID::gen());
        ShardKeyPattern keypattern(cmdObj.getObjectField("key").getOwned());
        BSONObj min, max;

        std::vector<ShardId> allShards;
        // auto& prng = txn->getClient()->getPrng();
        grid.shardRegistry()->getAllShardIds(&allShards);
        if (0 == allShards.size()) {
            return Status(ErrorCodes::ShardNotFound, "no shard available");
        }
        auto collstatus = grid.catalogClient(txn)->getCollection(txn, ns);
        if (!collstatus.isOK()) {
            index_log() << "[CS_SHARDCOLL]get coll failed";
            return collstatus.getStatus();
        }
        CollectionType _coll = collstatus.getValue().value;
        _coll.setEpoch(version.epoch());
        _coll.setUpdatedAt(Date_t::fromMillisSinceEpoch(version.toLong()));
        _coll.setKeyPattern(keypattern.toBSON());

        Status st = _writeCollAndIndexMetadata(txn, ns, _coll);
        if (!st.isOK()) {
            index_err() << "[CS_SHARDCOLL] write coll and index metadata failed";
            return st;
        }

        for (int i = 0; i <= count; i++) {
            if (i == 0 && count == 0) {
                min = keypattern.getKeyPattern().globalMin();
                max = keypattern.getKeyPattern().globalMax();
            } else {
                if (i == 0) {
                    min = keypattern.getKeyPattern().globalMin();
                    max = allSplits[i];
                } else if (i > 0 && i <= count - 1) {
                    min = allSplits[i - 1];
                    max = allSplits[i];
                } else if (i == count) {
                    min = allSplits[i - 1];
                    max = keypattern.getKeyPattern().globalMax();
                }
            }

            ChunkType chunk;
            chunk.setNS(ns);
            chunk.setMin(min);
            chunk.setMax(max);
            if (count == 0) {
                chunk.setShard(primaryShardId);
            } else {
                chunk.setShard(allShards[i % (allShards.size())]);
            }
            chunk.setVersion(version);
            std::string chunkID;
            auto c_status = grid.catalogClient(txn)->generateNewChunkID(txn, chunkID);
            if (!c_status.isOK()) {
                index_err() << "[CS_SHARDCOLL] generateNewChunkID fail";
                (void)Balancer::get(txn)->getResults(txn, ns);
                return c_status;
            }
            chunk.setName(chunkID);
            // chunk.setShard(allShards[prng.nextInt32(allShards.size())]);
            // update chunk status if this is a new chunk
            chunk.setStatus(ChunkType::ChunkStatus::kOffloaded);

            auto shardId = chunk.getShard();
            auto assignStatus =
                Balancer::get(txn)->assignChunk(txn, chunk, true, true, shardId, true);

            if (!assignStatus.isOK()) {
                index_err() << "[CS_SHARDCOLL] assign fail, chunk: " << chunk;
                (void)Balancer::get(txn)->getResults(txn, ns);
                return assignStatus;
            }
        }

        Status assign_status = Balancer::get(txn)->getResults(txn, ns);
        if (!assign_status.isOK()) {
            return assign_status;
        }

        _coll.setCreated(true);
        Status status = grid.catalogClient(txn)->updateCollection(txn, ns, _coll);
        if (!status.isOK()) {
            index_log() << "[CS_SHARDCOLL]update coll fail, coll: ";
            return status;
        }
        return Status::OK();
    }

    Status _createCollectionMetadata(OperationContext* txn,
                                     const NamespaceString& ns,
                                     ShardKeyPattern& keypattern,
                                     bool unique,
                                     bool exist) {
        Status status = userAllowedCreateNS(ns.db(), ns.coll());
        if (!status.isOK()) {
            return status;
        }
        long long ll_max_prefix = 0;
        CollectionType _coll;

        _coll.setNs(ns);
        _coll.setEpoch(OID::gen());
        _coll.setUpdatedAt(Date_t::fromMillisSinceEpoch(1));
        _coll.setKeyPattern(keypattern.toBSON());
        _coll.setUnique(unique);
        _coll.setPrefix(ll_max_prefix + 1);
        _coll.setTabType(CollectionType::TableType::kSharded);
        _coll.setCreated(false);
        if (exist) {
            string tmpNS = ns.ns();
            std::size_t pos = tmpNS.find('.');
            string dbname = tmpNS.substr(0, pos);
            string extra = "tmp.shardCollection.";
            string collname = tmpNS.substr(pos + extra.size() + 1);
            NamespaceString sourceNS(dbname, collname);

            auto collStatus = grid.catalogClient(txn)->getCollection(txn, sourceNS.ns());
            if (!collStatus.isOK()) {
                return collStatus.getStatus();
            }
            CollectionType sourceColl = collStatus.getValue().value;
            _coll.setDefaultCollation(sourceColl.getDefaultCollation());
            _coll.setOptions(sourceColl.getOptions());
            _coll.setAllowBalance(false);
        }

        BSONObj options = _coll.getOptions();

        const auto featureCompatibilityVersion =
            serverGlobalParams.featureCompatibility.version.load();
        auto validateFeaturesAsMaster =
            serverGlobalParams.featureCompatibility.validateFeaturesAsMaster.load();
        const auto indexVersion =
            IndexDescriptor::getDefaultIndexVersion(featureCompatibilityVersion);
        if (ServerGlobalParams::FeatureCompatibility::Version::k32 == featureCompatibilityVersion &&
            validateFeaturesAsMaster && options.hasField("collation")) {
            return Status(ErrorCodes::InvalidOptions,
                          "The featureCompatibilityVersion must be 3.4 to create a collection or "
                          "view with a default collation. See "
                          "http://dochub.mongodb.org/core/3.4-feature-compatibility.");
        }

        BSONObjBuilder b;
        b.append("v", static_cast<int>(indexVersion));
        b.append("name", "_id_");
        b.append("ns", ns.ns());
        b.append("key", BSON("_id" << 1));
        b.append("prefix", ll_max_prefix + 1);
        BSONArrayBuilder indexArrayBuilder;
        indexArrayBuilder.append(b.obj());
        _coll.setIndex(indexArrayBuilder.arr());
        _coll.setIdent(grid.catalogClient(txn)->createNewIdent());
        uassertStatusOK(grid.catalogClient(txn)->updateCollection(txn, ns.ns(), _coll));
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

    bool getCollection(OperationContext* txn,
                       const string& nss,
                       bool& exist,
                       bool& dropped,
                       bool& created,
                       string& errmsg,
                       BSONObjBuilder& result) {
        auto collStatus = grid.catalogClient(txn)->getCollection(txn, nss);
        if (collStatus.isOK()) {
            CollectionType coll = collStatus.getValue().value;
            exist = true;
            dropped = coll.getDropped();
            created = coll.getCreated();
            return true;
        } else if (collStatus != ErrorCodes::NamespaceNotFound) {
            return appendCommandStatus(result, collStatus.getStatus());
        }
        exist = false;
        return true;
    }

    virtual bool run(OperationContext* txn,
                     const string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     string& errmsg,
                     BSONObjBuilder& result) {
        const NamespaceString nss(parseNs(dbname, cmdObj));
        BSONObj cmd_obj = cmdObj.removeField("maxTimeMS");

        auto property = cmdObj.getObjectField("key").getOwned();
        if (property.isEmpty()) {
            errmsg = "no shard key";
            index_err() << "[CS_SHARDCOLL] property.isEmpty() :" << errmsg;
            return false;
        }
        ShardKeyPattern keypattern(property);
        if (!keypattern.isValid()) {
            errmsg = str::stream() << "Unsupported shard key pattern. Pattern must"
                                   << " either be a single hashed field, or a list"
                                   << " of ascending fields.";
            index_err() << "[CS_SHARDCOLL] !keypattern.isValid :" << errmsg;
            return false;
        }

        bool isEmpty = cmdObj["isEmpty"].trueValue();

        bool tmpCollExist = false;
        bool tmpCollDropped = false;
        bool tmpCreated = false;
        bool collExist = false;
        bool newCollection = false;
        CollectionType coll;
        std::string shortTmpName = str::stream() << "tmp.shardCollection." << nss.coll();
        std::string longTmpName = str::stream() << nss.db() << "." << shortTmpName;

        // whether tmp collection exists
        bool ret = getCollection(
            txn, longTmpName, tmpCollExist, tmpCollDropped, tmpCreated, errmsg, result);
        if (!ret) {
            index_err() << "get collection " << longTmpName << " failed";
            return false;
        }
        if (tmpCollExist) {
            tmpCollExist = (tmpCollExist && !tmpCollDropped);
        }

        auto collStatus = grid.catalogClient(txn)->getCollection(txn, nss.ns());
        if (collStatus == ErrorCodes::NamespaceNotFound) {
            if (!tmpCollExist) {
                newCollection = true;
            }
        } else if (collStatus.isOK()) {
            coll = collStatus.getValue().value;
            if (coll.getDropped() && coll.getCreated()) {
                if (!tmpCollExist) {
                    newCollection = true;
                }
            } else if (!coll.getDropped() && coll.getCreated()) {
                // coll already exist, check tabType and shardKey
                index_log() << "[CS_SHARDCOLL] collection exist";
                if (coll.getTabType() == CollectionType::TableType::kSharded) {
                    return true;
                }
                collExist = true;
            } else if (!coll.getCreated()) {
                // last cmd run failed
                /*BSONObjBuilder removeResult;
                auto status = _removeCollAndIndexMetadata(txn, nss, removeResult);
                if (!status.isOK()) {
                    index_err() << "[CS_SHARDCOLL] removecollectionmetadata fail "
                                << static_cast<int>(status.code());
                    return appendCommandStatus(result, status);
                }*/
                newCollection = true;
            }
        } else {
	    index_err() << "get collection failed, nss: " << nss;
	    return appendCommandStatus(result, collStatus.getStatus());
	}

        if (newCollection) {
            BSONObjBuilder removeResult;
            auto status = _removeCollAndIndexMetadata(txn, nss, removeResult);
            if (!status.isOK()) {
                index_err() << "[CS_SHARDCOLL] removecollectionmetadata fail "
                                << static_cast<int>(status.code());
                return appendCommandStatus(result, status);
            }
            
            return run_normal(txn, dbname, cmd_obj, options, errmsg, result);
        }

	if (!isEmpty && !GLOBAL_CONFIG_GET(IsShardingTest)) {
	    return appendCommandStatus(result, 
	        Status(ErrorCodes::CommandNotSupported, 
		str::stream() << "shardCollection command for non-empty collection is not supported"));
	}

        // rollback last failed result
        bool rollBackRet =
            rollBackShardCollExist(txn, collExist, tmpCollExist, nss, longTmpName, errmsg, result);
        if (!rollBackRet) {
            index_err() << "rollBackShardCollExist failed";
            return appendCommandStatus(
                result, Status(ErrorCodes::OperationFailed, "shardCollection run failed."));
        }
        // whether shardColl success after rollback
        {
            auto collStatus = grid.catalogClient(txn)->getCollection(txn, nss.ns());
            if (collStatus.isOK()) {
                CollectionType coll = collStatus.getValue().value;
                if (!coll.getDropped() && coll.getCreated()) {
                    if (coll.getTabType() == CollectionType::TableType::kSharded) {
                        return true;
                    }
                }
            }
        }
        bool shardCollRet = shardCollectionExist2(
            txn, dbname, cmd_obj, tmpCollExist, isEmpty, options, errmsg, result);
        if (!shardCollRet) {
            bool ret = getCollection(
                txn, longTmpName, tmpCollExist, tmpCollDropped, tmpCreated, errmsg, result);
            if (!ret) {
                index_err() << "get collection " << longTmpName << " failed";
                return false;
            }
            tmpCollExist = (tmpCollExist && !tmpCollDropped);
            bool collExist = false;
            bool collDropped = false;
            ret = getCollection(txn, nss.ns(), collExist, collDropped, tmpCreated, errmsg, result);
            if (!ret) {
                index_err() << "get collection " << nss.ns() << " failed";
                return false;
            }
            collExist = (collExist && !collDropped);
            rollBackShardCollExist(txn, collExist, tmpCollExist, nss, longTmpName, errmsg, result);
            return appendCommandStatus(
                result, Status(ErrorCodes::OperationFailed, "shardCollection run failed."));
        }
        return shardCollRet;
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

    Status isShardkeyUnique(OperationContext* txn,
                            const string& source,
                            BSONObj& keyPattern,
                            bool& unique) {
        auto sourceCollStatus = Grid::get(txn)->catalogClient(txn)->getCollection(txn, source);
        if (!sourceCollStatus.isOK()) {
            index_err() << "[shardCollectionExist] isShardkeyUnique get sourceColl: " << source
                        << " failed.";
            return sourceCollStatus.getStatus();
        }

        CollectionType sourceColl = sourceCollStatus.getValue().value;
        for (const BSONElement& elm : sourceColl.getIndex()) {
            BSONObj newIndex = elm.Obj();
            BSONObj curKeyPattern = IndexDescriptor::getKeyPattern(newIndex);
            if (curKeyPattern.toString() == keyPattern.toString()) {
                unique = IndexDescriptor::hasUniqueModifier(newIndex);
                return Status::OK();
            }
        }
        unique = false;
        return Status::OK();
    }

    bool checkNumInitialChunks(int numChunks, int numShards, string& errmsg) {
        int maxNumInitialChunksForShards = numShards * 8192;
        const int maxNumInitialChunksTotal = 1000 * 1000;
        if (numChunks > maxNumInitialChunksForShards || numChunks > maxNumInitialChunksTotal) {
            errmsg = str::stream()
                << "numInitialChunks cannot be more than either: " << maxNumInitialChunksForShards
                << " or " << maxNumInitialChunksTotal;
            return false;
        }

        return true;
    }

    bool shardCollectionExist2(OperationContext* txn,
                               const string& dbname,
                               BSONObj& cmdObj,
                               bool tmpExist,
                               bool isEmpty,
                               int options,
                               string& errmsg,
                               BSONObjBuilder& result) {
        index_LOG(1) << "shardCollectionExist2 dbname " << dbname;
        const NamespaceString nss(parseNs(dbname, cmdObj));
        std::string source = nss.ns();
        std::string shortTmpName = str::stream() << "tmp.shardCollection." << nss.coll();
        std::string longTmpName = str::stream() << nss.db() << "." << shortTmpName;

        ChunkType chunk;
        ShardId primaryShardId;

        vector<ShardId> shardIds;
        grid.shardRegistry()->getAllShardIds(&shardIds);
        int numShards = shardIds.size();

        auto property = cmdObj.getObjectField("key").getOwned();
        int numChunks = cmdObj["numInitialChunks"].numberInt();
        ShardKeyPattern keypattern(property);

        bool isUnique = false;

        auto status = isShardkeyUnique(txn, source, property, isUnique);
        if (!status.isOK()) {
            return appendCommandStatus(result, status);
        }

        BSONObjBuilder newCmdObjBuilder;
        BSONObj newCmdObj;
        newCmdObj = cmdObj.removeField("shardCollection");
        newCmdObjBuilder.append("shardCollection", longTmpName);

        if (keypattern.isHashedPattern()) {
            bool checkRet = checkNumInitialChunks(numChunks, numShards, errmsg);
            if (!checkRet) {
                return false;
            }
            newCmdObj = newCmdObj.removeField("numInitialChunks");
            newCmdObjBuilder.appendElements(newCmdObj);
            newCmdObjBuilder.append("numInitialChunks", 1);
        } else {
            if (isUnique) {
                newCmdObj = newCmdObj.removeField("unique");
                newCmdObjBuilder.appendElements(newCmdObj);
                newCmdObjBuilder.append("unique", true);
            } else {
                newCmdObjBuilder.appendElements(newCmdObj);
            }
        }
        newCmdObjBuilder.append("exist", true);

        newCmdObj = newCmdObjBuilder.obj();
        bool cmdRet = run_normal(txn, dbname, newCmdObj, options, errmsg, result);
        if (!cmdRet) {
	    index_err() << "[shardCollectionExist] run_normal failed, err: " << errmsg;
            return false;
        }
        // create indexes
        {
            bool createindexesRet = operationutil::copyIndexes(txn, source, longTmpName, errmsg, result);
            if (!createindexesRet) {
	        index_err() << "[shardCollectionExist] copyIndexes failed, err: " << errmsg;
                return false;
            }
        }
        // copy data
        {
            bool copyRet = operationutil::copyData(
                txn, nss.ns(), longTmpName, false, 0, errmsg, result);
            if (!copyRet) {
	        index_err() << "[shardCollectionExist] copyData failed, err: " << errmsg;
                return false;
            }
        }

        // rename collection
        {
            bool renameRet = operationutil::renameCollection(txn, longTmpName, source, errmsg, result);
            if (!renameRet) {
	        index_err() << "[shardCollectionExist] renameCollection failed, err: " << errmsg;
                return false;
            }
        }
        // to open balancer
        {
            auto ret = updateAllowBalance(txn, source, true);
            if (!ret) {
	        index_err() << "[shardCollectionExist] updateAllowBalance failed";
                return false;
            }
        }

        if (keypattern.isHashedPattern() && isEmpty) {
            vector<BSONObj> allSplits;
            if (numChunks <= 0) {
                numChunks = 2 * numShards;
            }
            long long intervalSize = (std::numeric_limits<long long>::max() / numChunks) * 2;
            long long current = 0;
            if (numChunks % 2 == 0) {
                allSplits.push_back(BSON(property.firstElementFieldName() << current));
                current += intervalSize;
            } else {
                current += intervalSize / 2;
            }

            for (int i = 0; i < (numChunks - 1) / 2; i++) {
                allSplits.push_back(BSON(property.firstElementFieldName() << current));
                allSplits.push_back(BSON(property.firstElementFieldName() << -current));
                current += intervalSize;
            }

            sort(allSplits.begin(),
                 allSplits.end(),
                 SimpleBSONObjComparator::kInstance.makeLessThan());
            // split chunk
            int splitPointCount = allSplits.size();
            BSONObj min = keypattern.getKeyPattern().globalMin();
            BSONObj max = keypattern.getKeyPattern().globalMax();

            string chunkNS = source;
            std::vector<ChunkType> Chunks;

            BSONObjBuilder queryBuilder;
            queryBuilder.append(ChunkType::ns(), chunkNS);
            queryBuilder.append(ChunkType::min(), min);
            for (int i = splitPointCount - 1; i >= 0; i--) {
                uassertStatusOK(grid.catalogClient(txn)->getChunks(
                    txn,
                    queryBuilder.done(),
                    BSONObj(),
                    boost::none,
                    &Chunks,
                    nullptr,
                    repl::ReadConcernLevel::kMajorityReadConcern));

                Status status =
                    Balancer::get(txn)->splitChunk(txn, Chunks[0], allSplits[i], true, true);
                if (!status.isOK()) {
                    index_err() << "[shardCollectionExist] split failed at " << allSplits[i];
                    continue;
                }
            }
        }
        return true;
    }

    bool updateAllowBalance(OperationContext* txn, const string& nss, bool allowBalance) {
        auto collstatus = grid.catalogClient(txn)->getCollection(txn, nss);
        if (!collstatus.isOK()) {
            return false;
        }
        CollectionType coll = collstatus.getValue().value;
        coll.setAllowBalance(allowBalance);
        auto status = grid.catalogClient(txn)->updateCollection(txn, nss, coll);
        if (!status.isOK()) {
            return false;
        }
        return true;
    }

    bool rollBackShardCollExist(OperationContext* txn,
                                bool collExist,
                                bool tmpCollExist,
                                const NamespaceString& nss,
                                const string& tmpNss,
                                string& errmsg,
                                BSONObjBuilder& result) {
        if (!tmpCollExist) {
            return true;
        }
        index_log() << "[rollBackShardCollExist] start, collExist: " << collExist << " tmpCollExist: "
	    << tmpCollExist << " nss: " << nss << " tmpNss: " << tmpNss;
        {
            if (collExist) {
                ChunkType chunk;
                BSONObjBuilder removeResult;
                bool getRet = operationutil::getChunk(txn, nss.ns(), chunk, errmsg);
                if (!getRet) {
                    return false;
                }
                auto assignStatus =
                    Balancer::get(txn)->assignChunk(txn, chunk, false, true, chunk.getShard());
                if (!assignStatus.isOK()) {
                    return appendCommandStatus(result, assignStatus);
                }

                Status status =
                    _removeCollAndIndexMetadata(txn, NamespaceString(tmpNss), removeResult);
                if (!status.isOK()) {
                    return appendCommandStatus(result, status);
                }
                return true;
            } else {
	        BSONObjBuilder removeResult;
	        Status status = 
		    _removeCollAndIndexMetadata(txn, nss, removeResult);
		if (!status.isOK()) {
		    return appendCommandStatus(result, status);
		}
                //ChunkType chunk;
                bool renameRet = operationutil::renameCollection(txn, tmpNss, nss.ns(), errmsg, result);
                if (!renameRet) {
                    return false;
                }
                return updateAllowBalance(txn, nss.ns(), true);
            }
        }
        return true;
    }


    virtual bool run_normal(OperationContext* txn,
                            const string& dbname,
                            BSONObj& cmdObj,
                            int options,
                            string& errmsg,
                            BSONObjBuilder& result) {
        const NamespaceString nss(parseNs(dbname, cmdObj));
        bool unique = false;
        BSONElement e = cmdObj.getField("unique");
        if (!e.eoo()) {
            unique = e.type() == Bool ? e.boolean() : false;
        }

        bool exist = cmdObj["exist"].trueValue();
        // 1. createCollection
        // 1.1 check if the Collection already exists
        index_log() << "[CS_SHARDCOLL] ns:" << nss.ns()
                    << "*****************step1***************** createCollection ";
        auto property = cmdObj.getObjectField("key").getOwned();
        if (property.isEmpty()) {
            errmsg = "no shard key";
            index_err() << "[CS_SHARDCOLL] property.isEmpty() :" << errmsg;
            return false;
        }
        ShardKeyPattern keypattern(property);
        if (!keypattern.isValid()) {
            errmsg = str::stream() << "Unsupported shard key pattern. Pattern must"
                                   << " either be a single hashed field, or a list"
                                   << " of ascending fields.";
            index_err() << "[CS_SHARDCOLL] !keypattern.isValid :" << errmsg;
            return false;
        }
        Status st = _createCollectionMetadata(txn, nss, keypattern, unique, exist);
        if (!st.isOK()) {
            index_err() << "[CS_SHARDCOLL] _createCollectionMetadata -2 failed. status:" << st;
            return appendCommandStatus(result, st);
        }

        index_LOG(2) << "[CS_SHARDCOLL] ns:" << nss.ns()
                     << "****************step2***************** create index metadata ";
        // 2. create index metadata on config server
        if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer && dbname != "config" &&
            dbname != "system") {
            // if succeed to create index metadata, then pass the commands to
            // all the shard servers
            if (!isIdIndexPattern(cmdObj.getObjectField("key"))) {
                if (!cmdObj.getObjectField("usefulIndex").isEmpty()) {
                    auto usefulIndex = cmdObj.getObjectField("usefulIndex").getOwned();
                    ShardKeyPattern keypattern(usefulIndex);
                    bool usefulIndexUniq = cmdObj["usefulIndexUniq"].trueValue();
                    string usefulIndexName = cmdObj["usefulIndexName"].str();
                    Status st = _createIndexMetadata(
                        txn, nss, usefulIndex, usefulIndexUniq, usefulIndexName);
                    if (!st.isOK()) {
                        //_create_one_chunk(ShardKeyPattern(BSON("_id"<<1)),nss,txn,result,cmdObj);
                        index_err() << "CreateIndexMetadata Error ......................... 1";
                        return appendCommandStatus(result, st);
                    }

                } else {
                    Status st = _createIndexMetadata(txn, nss, property, unique);
                    if (!st.isOK()) {
                        //_create_one_chunk(ShardKeyPattern(BSON("_id"<<1)),nss,txn,result,cmdObj);
                        index_err() << "CreateIndexMetadata Error ..........................";
                        return appendCommandStatus(result, st);
                    }
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
        // added end
        vector<ShardId> shardIds;
        grid.shardRegistry()->getAllShardIds(&shardIds);
        int numShards = shardIds.size();

        int maxNumInitialChunksForShards = numShards * 8192;
        const int maxNumInitialChunksTotal = 1000 * 1000;
        int numChunks = cmdObj["numInitialChunks"].numberInt();
        if (numChunks > maxNumInitialChunksForShards || numChunks > maxNumInitialChunksTotal) {
            errmsg = str::stream()
                << "numInitialChunks cannot be more than either: " << maxNumInitialChunksForShards
                << " or " << maxNumInitialChunksTotal;
            return false;
        }

        bool isEmpty = cmdObj["isEmpty"].trueValue();

        vector<BSONObj> initSplits;
        vector<BSONObj> allSplits;
        if (isHashedShardKey && isEmpty) {
            if (numChunks <= 0) {
                // default number of initial chunks
                numChunks = 2 * numShards;
            }
            long long intervalSize = (std::numeric_limits<long long>::max() / numChunks) * 2;

            long long current = 0;
            auto proposedKey = cmdObj.getObjectField("key").getOwned();
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
                index_err() << "[CS_SHARDCOLL] get db fail,status: " << ss.getStatus();
                return false;
            }
            index_LOG(3) << "[CS_SHARDCOLL] ns:" << nss.ns()
                         << "****************step3***************** _createFirstChunks ";
            Status c_status = _createFirstChunks(
                txn, ss.getValue().value.getPrimary(), allSplits, nss.ns(), cmdObj);
            if (!c_status.isOK()) {
                index_err() << "[CS_SHARDCOLL] _createFirstChunks failed";
                return appendCommandStatus(result, c_status);
            }

        } else {

            index_LOG(3) << "[CS_SHARDCOLL] ns:" << nss.ns()
                         << "****************step3***************** _create_one_chunk ";
            if (!_create_one_chunk(keypattern, nss, txn, result, cmdObj)) {
                index_err() << "[CS_SHARDCOLL] _create_one_chunk failed";
                return false;
            }
        }

        return true;
    }

    bool _create_one_chunk(const ShardKeyPattern& keypattern,
                           const NamespaceString& nss,
                           OperationContext* txn,
                           BSONObjBuilder& result,
                           BSONObj& cmdObj) {
        // 1. build the first chunk
        ChunkType chunk;
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
            index_err() << "[CS_SHARDCOLL] generateNewChunkID fail";
            return appendCommandStatus(result, c_status);
        }
        chunk.setName(chunkID);
        // get database info from config.databases, choose the primary shard for the chunk
        auto dbstatus = grid.catalogClient(txn)->getDatabase(txn, nss.db().toString());
        if (!dbstatus.isOK()) {
            index_err() << "[CS_SHARDCOLL] get db fail";
            return appendCommandStatus(result, dbstatus.getStatus());
        }

        std::vector<ShardId> allShards;
        grid.shardRegistry()->getAllShardIds(&allShards);
        if (0 == allShards.size()) {
            index_err() << "[CS_SHARDCOLL] not find shards";
            return appendCommandStatus(
                result, {ErrorCodes::ShardNotFound, str::stream() << "no shard available"});
        }
        // chunk.setShard(allShards[prng.nextInt32(allShards.size())]);
        chunk.setShard(dbstatus.getValue().value.getPrimary());
        auto collstatus = grid.catalogClient(txn)->getCollection(txn, chunk.getNS());
        if (!collstatus.isOK()) {
            index_err() << "[CS_SHARDCOLL] get coll failed";
            return appendCommandStatus(result, collstatus.getStatus());
        }
        CollectionType _coll = collstatus.getValue().value;
        _coll.setEpoch(version.epoch());
        _coll.setUpdatedAt(Date_t::fromMillisSinceEpoch(version.toLong()));
        _coll.setKeyPattern(keypattern.toBSON());
        // write coll and index metadata
        Status st = _writeCollAndIndexMetadata(txn, nss.ns(), _coll);
        if (!st.isOK()) {
            index_err() << "[CS_SHARDCOLL] write coll and index metadata failed";
            return appendCommandStatus(result, st);
        }

        // 4. assign the new chunk
        auto shardId = chunk.getShard();
        auto assignStatus = Balancer::get(txn)->assignChunk(txn, chunk, true, true, shardId);

        if (!assignStatus.isOK()) {
            index_err() << "[CS_SHARDCOLL] assign fail";
            return appendCommandStatus(result, assignStatus);
        }
        // update collection metadata
        _coll.setCreated(true);
        Status status = grid.catalogClient(txn)->updateCollection(txn, nss.ns(), _coll);
        if (!status.isOK()) {
            index_err() << "[CS_SHARDCOLL] update coll fail";
            return appendCommandStatus(result, status);
        }

        return true;
    }


} CmdShardCollection;
}  // namespace


}  // namespace mongo

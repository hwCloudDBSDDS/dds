#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/db/commands/operation_util.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/catalog/index_create.h"
#include "mongo/db/commands.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/client/remote_command_targeter.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/db/s/balancer/balancer.h"
#include "mongo/db/catalog/drop_collection.h"

#include <vector>

namespace mongo {

using std::vector;
using std::shared_ptr;

using CallbackHandle = executor::TaskExecutor::CallbackHandle;
using RemoteCommandCallbackArgs = executor::TaskExecutor::RemoteCommandCallbackArgs;
using RemoteCommandCallbackFn = executor::TaskExecutor::RemoteCommandCallbackFn;
using RemoteCommandRequest = executor::RemoteCommandRequest;
using RemoteCommandResponse = executor::RemoteCommandResponse;

class CommandResult;

namespace operationutil{

void handleCopyDataRequestResponse(const RemoteCommandCallbackArgs& cbArgs,
                                   std::string server,
                                   std::string shardId,
                                   std::list<std::shared_ptr<CommandResult>>* futures) {
    auto response = cbArgs.response;
    shared_ptr<CommandResult> res(new CommandResult());

    if (!response.isOK()) {
        index_err() << "[handleCopyDataRequestResponse] copyData error, " << server
                    << "; code: " << static_cast<int>(response.status.code())
                    << "; errMsg: " << response.status.reason();
        res->setOK(false);
    } else {
        auto status = getStatusFromCommandResult(response.data);
        if (!status.isOK()) {
            res->setOK(false);
        } else {
            res->setOK(true);
        }
    }
    res->setServer(server);
    res->setShardId(shardId);
    res->setCode(response.status.code());
    res->setErrmsg(response.status.reason());
    res->setResult(response.data);
    futures->push_back(res);
}

    bool getChunk(OperationContext* txn, const string& ns, ChunkType& chunk, string& errmsg) {
        index_LOG(1) << "getChunk ns: " << ns;
        std::vector<ChunkType> Chunks;
        uassertStatusOK(
            grid.catalogClient(txn)->getChunks(txn,
                                               BSON(ChunkType::ns(ns)),
                                               BSONObj(),
                                               boost::none,
                                               &Chunks,
                                               nullptr,
                                               repl::ReadConcernLevel::kMajorityReadConcern));
        if (Chunks.size() == 0) {
            errmsg = str::stream() << "No chunk found for collection: " << ns;
            return false;
        } else if (Chunks.size() != 1) {
            errmsg = str::stream() << "More than 1 chunk found for collection: " << ns;
            return false;
        }

        chunk = Chunks[0];
        return true;
    }

    StatusWith<BSONObj> getCollation(const BSONObj& cmdObj) {
        BSONElement collationElement;
        auto status =
            bsonExtractTypedField(cmdObj, "collation", BSONType::Object, &collationElement);
        if (status.isOK()) {
            return collationElement.Obj();
        }
        if (status != ErrorCodes::NoSuchKey) {
            return status;
        }
        return BSONObj();
    }

    bool copyData(OperationContext* txn,
                  const string& source,
                  const string& target,
		  bool capped,
		  double size,
                  string& errmsg,
                  BSONObjBuilder& result) {
        // copy data from tmp to source collection
        NamespaceString nss(source);
        NamespaceString targetNss(target);
	string dbname = nss.db().toString();

        index_LOG(1) << "copyData dbname: " << dbname
                     << " source: " << source << " target: " << target;
        {
            ChunkType chunk;
            bool getRet = getChunk(txn, source, chunk, errmsg);
            if (!getRet) {
                index_err() << "copyData getChunk failed: " << nss;
                return false;
            }

            const auto shardStatus = grid.shardRegistry()->getShard(txn, chunk.getShard());
            if (!shardStatus.isOK()) {
                index_err() << "shard: " << chunk.getShard() << " not found";
                return Command::appendCommandStatus(result, shardStatus.getStatus());
            }

            BSONObjBuilder builder;
            builder.append("copydata", nss.ns());
            builder.append("target", targetNss.ns());
	    builder.append("capped", capped);
            builder.append("size", size);

            BSONObj copyDataCmdObj = builder.obj();

            std::list<std::shared_ptr<CommandResult>> features;
            auto primaryShard = shardStatus.getValue();
            executor::TaskExecutor* const executor =
                Grid::get(txn)->getExecutorPool()->getFixedExecutor();
            auto targeter = primaryShard->getTargeter();
            auto hostAndport =
                targeter->findHost(txn, ReadPreferenceSetting{ReadPreference::PrimaryOnly})
                    .getValue();
            const RemoteCommandRequest request(hostAndport,
                                               nss.db().toString(),
                                               copyDataCmdObj,
                                               txn,
                                               executor::RemoteCommandRequest::kNoTimeout);
            const RemoteCommandCallbackFn callback = stdx::bind(&handleCopyDataRequestResponse,
                                                                stdx::placeholders::_1,
                                                                hostAndport.toString(),
                                                                primaryShard->getId().toString(),
                                                                &features);
            StatusWith<CallbackHandle> callbackHandleWithStatus =
                executor->scheduleRemoteCommand(request, callback);
            if (!callbackHandleWithStatus.isOK()) {
                index_err() << "failed to schedule command "
                            << redact(callbackHandleWithStatus.getStatus());
                return false;
            }
            // wait for response from shard
            while (features.size() < 1) {
                usleep(10);
            }

            std::list<std::shared_ptr<CommandResult>>::iterator featureit = features.begin();
            std::shared_ptr<CommandResult> res = *featureit;
            BSONObj resu = res->result();
            if (!res->isOK()) {
                index_err() << "copydata error: " << resu;
                return false;
            }
            return true;
        }
    }

    bool copyIndexes(OperationContext* txn,
                     const std::string& source,
                     const std::string& target,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
        NamespaceString targetNS(target);

        auto sourceCollStatus = Grid::get(txn)->catalogClient(txn)->getCollection(txn, source);
        if (!sourceCollStatus.isOK()) {
            index_err() << "copyIndexes get sourceColl: " << source
                        << " failed.";
            return false;
        }

        auto targetCollStatus = Grid::get(txn)->catalogClient(txn)->getCollection(txn, target);
        if (!targetCollStatus.isOK()) {
            index_err() << "copyIndexes get targetColl: " << target
                        << " failed.";
            return false;
        }

        CollectionType sourceColl = sourceCollStatus.getValue().value;

        CollectionType targetColl = targetCollStatus.getValue().value;

        BSONArrayBuilder indexes;
        bool needCopyIndex = false;
        int longestIndexNameLength = 0;

        for (const BSONElement& elm : sourceColl.getIndex()) {
            BSONObj newIndex = elm.Obj();
            BSONObjBuilder toCreateIndex;
            BSONObj newKeyPattern = IndexDescriptor::getKeyPattern(newIndex);
            auto statusFrom = getCollation(newIndex);
            if (!statusFrom.isOK()) {
                return false;
            }

            bool indexExist = false;
            for (const BSONElement& elmExsit : targetColl.getIndex()) {
                BSONObj existingIndex = elmExsit.Obj();
                BSONObj keyPattern = IndexDescriptor::getKeyPattern(existingIndex);
                auto statusTo = getCollation(existingIndex);
                if (!statusTo.isOK()) {
                    return false;
                }

                if (keyPattern.toString() == newKeyPattern.toString()) {
                    if (KeyPattern::isIdKeyPattern(keyPattern) ||
                        statusTo.getValue().woCompare(statusFrom.getValue()) == 0) {
                        indexExist = true;
                        break;
                    }
                }
            }

            for (auto&& item : newIndex) {
                if (item.fieldNameStringData() == "name") {
                    int thisLength = item.valuestrsize() - 1;
                    if (thisLength > longestIndexNameLength) {
                        longestIndexNameLength = thisLength;
                    }
                }
                if (item.fieldNameStringData() == "ns") {
                    toCreateIndex.append("ns", target);
                } else {
                    toCreateIndex.append(item);
                }
            }
            if (indexExist) {
                continue;
            }
            needCopyIndex = true;
            indexes.append(toCreateIndex.done());
        }

        if (needCopyIndex) {
            unsigned int longestAllowed = std::min(int(NamespaceString::MaxNsCollectionLen),
                                                   int(NamespaceString::MaxNsLen) -
                                                       2 /*strlen(".$")*/ - longestIndexNameLength);
            if (target.size() > longestAllowed) {
                StringBuilder sb;
                sb << "collection name length of " << target.size() << " exceeds maximum length of "
                   << longestAllowed << ", allowing for index names";
                result.append("code", ErrorCodes::InvalidLength);
                result.append("codeName", ErrorCodes::errorString(ErrorCodes::InvalidLength));
                errmsg = sb.str();
                return false;
            }
            Command* createIndexCmd = Command::findCommand("createIndexes");
            BSONObjBuilder cmdBuilder;
            cmdBuilder << "createIndexes" << targetNS.coll();
            cmdBuilder << "indexes" << indexes.arr();
            BSONObj createCmdObj = cmdBuilder.done();
            bool createRet = false;
            try {
                createRet = createIndexCmd->run(
                    txn, targetNS.db().toString(), createCmdObj, 0, errmsg, result);
            } catch (const DBException& e) {
                return false;
            }

            return createRet;
        }
        return true;
    }

    bool renameCollection(OperationContext* txn,
                          const string& source,
                          const string& target,
                          string& errmsg,
                          BSONObjBuilder& result) {
        ChunkType chunk;
        bool getRet = getChunk(txn, source, chunk, errmsg);
        if (!getRet) {
            index_err() << "getChunk failed, err: " << errmsg;
            return false;
        }
        auto primaryShardId = chunk.getShard();
        index_LOG(1) << "renameCollection " << source
                     << " target: " << target;
        Status renameStatus =
            Balancer::get(txn)->renameCollection(txn, chunk, target, true, false, true);
        if (!renameStatus.isOK()) {
            return Command::appendCommandStatus(result, renameStatus);
        }
        return true;
    }

    Status removeCollAndIndexMetadata(OperationContext* txn,
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

}
}

// index_create.cpp

/**
*    Copyright (C) 2008-2014 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kIndex

#include "mongo/db/catalog/index_create.h"
#include "mongo/platform/basic.h"
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/client/remote_command_targeter.h"
#include "mongo/db/audit.h"
#include "mongo/db/background.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"

#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/create_collection.h"
#include "mongo/db/catalog/index_key_validate.h"
#include "mongo/db/client.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/index/index_description_onCfg.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/index_legacy.h"
#include "mongo/db/index_names.h"
#include "mongo/db/matcher/extensions_callback_disallow_extensions.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/server_parameters.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/client/shard_remote.h"
#include "mongo/s/grid.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/progress_meter.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/represent_as.h"
#include "mongo/util/util_extend/GlobalConfig.h"


namespace mongo {

using std::shared_ptr;
using std::unique_ptr;
using std::string;
using std::endl;
using std::vector;
using IndexVersion = IndexDescriptor::IndexVersion;

/*
const StringData kIndexesFieldName = "indexes"_sd;
const StringData kCommandName = "createIndexes"_sd;
const StringData kWriteConcern = "writeConcern"_sd;
const StringData kchunkIdFieldName = "chunkId"_sd;
*/

using CallbackHandle = executor::TaskExecutor::CallbackHandle;
using CallbackArgs = executor::TaskExecutor::CallbackArgs;
using RemoteCommandCallbackArgs = executor::TaskExecutor::RemoteCommandCallbackArgs;
using RemoteCommandCallbackFn = executor::TaskExecutor::RemoteCommandCallbackFn;
using RemoteCommandRequest = executor::RemoteCommandRequest;
using RemoteCommandResponse = executor::RemoteCommandResponse;

/*class CommandResult {
private:
    std::string _server;
    std::string _shardId;
    BSONObj _result;
    bool _isOK;
    string _errmsg;
    ErrorCodes::Error _code;

public:
    CommandResult(){};
    ~CommandResult(){};

    ErrorCodes::Error code() {
        return _code;
    }
    std::string& getServer() {
        return _server;
    }
    std::string& getShardId() {
        return _shardId;
    }
    void setServer(std::string& ser) {
        _server = ser;
    }
    void setShardId(std::string& shardId) {
        _shardId = shardId;
    }
    BSONObj& result() {
        return _result;
    }
    bool isOK() {
        return _isOK;
    }
    std::string& errmsg() {
        return _errmsg;
    }
    void setResult(BSONObj& res) {
        _result = res;
    }
    void setOK(bool ok) {

        _isOK = ok;
    }
    void setErrmsg(std::string err) {
        _errmsg = err;
    }
    void setCode(ErrorCodes::Error code) {
        _code = code;
    }
};*/

void handleCreateIndexesRequestResponse(const RemoteCommandCallbackArgs& cbArgs,
                                        std::string server,
                                        std::string shardId,
                                        std::list<std::shared_ptr<CommandResult>>* futures) {
    auto response = cbArgs.response;
    shared_ptr<CommandResult> res(new CommandResult());

    if (!response.isOK()) {
        index_err() << "[CONFIG] createIndexes error, server: " << server
                    << "; code: " << static_cast<int>(response.status.code())
                    << "; errMsg: " << response.status.reason();
        res->setOK(false);
        res->setShardId(shardId);
        res->setServer(server);
        res->setCode(response.status.code());
        res->setErrmsg(response.status.reason());
        res->setResult(response.data);
        futures->push_back(res);
    } else {
        res->setOK(true);
        res->setServer(server);
        res->setShardId(shardId);
        res->setCode(response.status.code());
        res->setErrmsg(response.status.reason());
        res->setResult(response.data);
        futures->push_back(res);
    }
}

const ReadPreferenceSetting kPrimaryOnlyReadPreference{ReadPreference::PrimaryOnly};
Status passCreateIndexCMDtoEveryChunk(OperationContext* txn,
                                      const NamespaceString& ns,
                                      BSONArray& toCreateCmdBuilder,
                                      std::string& s,
                                      const std::string& dbname,
                                      BSONObjBuilder& result,
                                      bool& isUpdate) {

    vector<ChunkType> chunks;
    uassertStatusOK(
        grid.catalogClient(txn)->getChunks(txn,
                                           BSON(ChunkType::ns(ns.ns())),
                                           BSONObj(),
                                           0,
                                           &chunks,
                                           nullptr,
                                           repl::ReadConcernLevel::kMajorityReadConcern));

    std::vector<std::shared_ptr<Shard>> shards;
    std::list<std::shared_ptr<CommandResult>> futures;

    for (ChunkType chunk : chunks) {
        index_log() << "chunk: " << chunk.getID();
        BSONObjBuilder builder;
        builder.append("createIndexes", s);
        builder.append("indexes", toCreateCmdBuilder);
        builder.append("chunkId", chunk.getName());
        // builder.append("isCreateColl", isCreateCollection);
        const auto shardStatus = grid.shardRegistry()->getShard(txn, chunk.getShard());
        if (!shardStatus.isOK()) {
            index_err()
                << "[CONFIG] createIndexes passCreateIndexCMDtoEveryChunk get shard faied, shard: "
                << chunk.getShard();
            continue;
        }
        BSONObj cmdObj = builder.obj();
        const auto createindexShard = shardStatus.getValue();

        executor::TaskExecutor* const executor =
            Grid::get(txn)->getExecutorPool()->getFixedExecutor();
        auto targeter = createindexShard->getTargeter();
        auto hostAndport =
            targeter->findHost(txn, ReadPreferenceSetting{ReadPreference::PrimaryOnly}).getValue();

        const RemoteCommandRequest request(
            hostAndport, dbname, cmdObj, txn, executor::RemoteCommandRequest::kNoTimeout);
        const RemoteCommandCallbackFn callback = stdx::bind(&handleCreateIndexesRequestResponse,
                                                            stdx::placeholders::_1,
                                                            hostAndport.toString(),
                                                            createindexShard->getId().toString(),
                                                            &futures);
        StatusWith<CallbackHandle> callbackHandleWithStatus =
            executor->scheduleRemoteCommand(request, callback);
        if (!callbackHandleWithStatus.isOK()) {
            index_err() << "[CONFIG] createIndexes failed to schedule command "
                        << redact(callbackHandleWithStatus.getStatus());
            continue;
        } else {
            index_LOG(0) << "[CONFIG] createIndexes succeed";
        }
    }
    while (futures.size() != chunks.size()) {
        usleep(10);
    }
    BSONObjBuilder errors;
    int commonErrCode = -1;
    size_t ErrCount = 0;
    std::list<std::shared_ptr<CommandResult>>::iterator futuresit;
    for (futuresit = futures.begin(); futuresit != futures.end(); ++futuresit) {
        std::shared_ptr<CommandResult> res = *futuresit;
        BSONObj resu = res->result();
        if (res->isOK()) {
            // result.append(res->getServer(), resu);
            result.append(res->getShardId(), resu);
            Status status = getStatusFromCommandResult(resu);
            if (!status.isOK()) {
                index_err() << "[CONFIG] createIndexes failed, server: " << res->getServer()
                            << "; reason: " << status.reason()
                            << "; code: " << static_cast<int>(status.code());
                ErrCount++;
                errors.append(res->getShardId(), status.reason());
                commonErrCode = static_cast<int>(status.code());
            }
            continue;
        } else {
            ErrCount++;
            index_log() << "errcode:" << static_cast<int>(res->code())
                        << " ,errmsg:" << res->errmsg();
            commonErrCode = 70;
            index_err() << "[CONFIG] createIndexes failed to receive response, server: "
                        << res->getServer() << "; errMsg: " << res->errmsg()
                        << "; code: " << static_cast<int>(res->code());
            if (resu.isEmpty())
                errors.append(res->getShardId(),
                              str::stream() << "result without error message returned");
        }
    }
    if (ErrCount == chunks.size()) {
        isUpdate = false;
    }
    BSONObj errobj = errors.done();
    if (!errobj.isEmpty()) {
        return {ErrorCodes::fromInt(commonErrCode), errobj.toString()};
    }
    return Status::OK();
}

Status createNewCollection(OperationContext* txn, const NamespaceString& ns) {
    BSONObj idIndexSpec;
    BSONObjBuilder cmd;
    cmd.append("create", ns.coll());
    return createCollectionMetadata(txn, ns, cmd.obj(), idIndexSpec);
}

std::unique_ptr<CollatorInterface> parseCollation(OperationContext* txn,
                                                  const NamespaceString& nss,
                                                  BSONObj collationSpec) {
    if (collationSpec.isEmpty()) {
        return {nullptr};
    }
    auto collator =
        CollatorFactoryInterface::get(txn->getServiceContext())->makeFromBSON(collationSpec);
    if (collator == ErrorCodes::IncompatibleCollationVersion) {
        index_log() << "Collection " << nss
                    << " has a default collation which is incompatible with this version: ";
        fassertFailedNoTrace(40145);
    }
    invariantOK(collator.getStatus());

    return std::move(collator.getValue());
}

StatusWith<std::vector<BSONObj>> resolveCollectionDefaultProperties(
    OperationContext* txn, const NamespaceString& nss, std::vector<BSONObj> indexSpecs) {
    std::vector<BSONObj> indexSpecsWithDefaults = std::move(indexSpecs);
    CollectionType coll;
    auto collStatus = grid.catalogClient(txn)->getCollection(txn, nss.ns());
    coll = collStatus.getValue().value;
    BSONObj option = coll.getOptions();
    BSONObj defaultcollation;
    if (!option.isEmpty() && !option["collation"].eoo()) {
        defaultcollation = option["collation"].Obj();
    }
    auto Collator = parseCollation(txn, nss, defaultcollation);
    for (size_t i = 0, numIndexSpecs = indexSpecsWithDefaults.size(); i < numIndexSpecs; ++i) {
        auto indexSpecStatus = index_key_validate::validateIndexSpecCollation(
            txn, indexSpecsWithDefaults[i], Collator.get());
        if (!indexSpecStatus.isOK()) {
            return indexSpecStatus.getStatus();
        }
        auto indexSpec = indexSpecStatus.getValue();
        if (IndexDescriptor::isIdIndexPattern(
                indexSpec[IndexDescriptor::kKeyPatternFieldName].Obj())) {
            std::unique_ptr<CollatorInterface> indexCollator;
            if (auto collationElem = indexSpec[IndexDescriptor::kCollationFieldName]) {
                auto collatorStatus = CollatorFactoryInterface::get(txn->getServiceContext())
                                          ->makeFromBSON(collationElem.Obj());
                invariantOK(collatorStatus.getStatus());
                indexCollator = std::move(collatorStatus.getValue());
            }
            if (!CollatorInterface::collatorsMatch(Collator.get(), indexCollator.get())) {
                return {ErrorCodes::BadValue,
                        str::stream() << "The _id index must have the same collation as the "
                                         "collection. Index collation: "
                                      << (indexCollator.get() ? indexCollator->getSpec().toBSON()
                                                              : CollationSpec::kSimpleSpec)
                                      << ", collection collation: "
                                      << (Collator.get() ? Collator.get()->getSpec().toBSON()
                                                         : CollationSpec::kSimpleSpec)};
            }
        }
        indexSpecsWithDefaults[i] = indexSpec;
    }
    return indexSpecsWithDefaults;
}

Status _checkValidFilterExpressions(MatchExpression* expression, int level = 0) {
    if (!expression)
        return Status::OK();

    switch (expression->matchType()) {
        case MatchExpression::AND:
            if (level > 0)
                return Status(ErrorCodes::CannotCreateIndex,
                              "$and only supported in partialFilterExpression at top level");
            for (size_t i = 0; i < expression->numChildren(); i++) {
                Status status = _checkValidFilterExpressions(expression->getChild(i), level + 1);
                if (!status.isOK())
                    return status;
            }
            return Status::OK();
        case MatchExpression::EQ:
        case MatchExpression::LT:
        case MatchExpression::LTE:
        case MatchExpression::GT:
        case MatchExpression::GTE:
        case MatchExpression::EXISTS:
        case MatchExpression::TYPE_OPERATOR:
            return Status::OK();
        default:
            return Status(ErrorCodes::CannotCreateIndex,
                          str::stream() << "unsupported expression in partial index: "
                                        << expression->toString());
    }
}

Status isSpecOk(OperationContext* txn, const NamespaceString& nss, const BSONObj& spec) {
    BSONElement vElt = spec["v"];
    if (!vElt) {
        return {ErrorCodes::InternalError,
                str::stream()
                    << "An internal operation failed to specify the 'v' field, which is a required "
                       "property of an index specification: "
                    << spec};
    }
    if (!vElt.isNumber()) {
        return Status(ErrorCodes::CannotCreateIndex,
                      str::stream() << "non-numeric value for \"v\" field: " << vElt);
    }
    auto vEltAsInt = representAs<int>(vElt.number());
    if (!vEltAsInt) {
        return {ErrorCodes::CannotCreateIndex,
                str::stream() << "Index version must be representable as a 32-bit integer, but got "
                              << vElt.toString(false, false)};
    }
    auto indexVersion = static_cast<IndexVersion>(*vEltAsInt);

    if (indexVersion >= IndexVersion::kV2) {
        auto status = index_key_validate::validateIndexSpecFieldNames(spec);
        if (!status.isOK()) {
            return status;
        }
    }
    if (indexVersion == IndexVersion::kV0 &&
        !txn->getServiceContext()->getGlobalStorageEngine()->isMmapV1()) {
        return Status(ErrorCodes::CannotCreateIndex,
                      str::stream() << "use of v0 indexes is only allowed with the "
                                    << "mmapv1 storage engine");
    }
    if (!IndexDescriptor::isIndexVersionSupported(indexVersion)) {
        return Status(ErrorCodes::CannotCreateIndex,
                      str::stream() << "this version of mongod cannot build new indexes "
                                    << "of version number "
                                    << static_cast<int>(indexVersion));
    }

    if (nss.isSystemDotIndexes())
        return Status(ErrorCodes::CannotCreateIndex,
                      "cannot have an index on the system.indexes collection");
    if (nss.isOplog())
        return Status(ErrorCodes::CannotCreateIndex, "cannot have an index on the oplog");

    if (nss.coll() == "$freelist") {
        return Status(ErrorCodes::IndexAlreadyExists, "cannot index freelist");
    }
    const BSONElement specNamespace = spec["ns"];
    if (specNamespace.type() != String)
        return Status(ErrorCodes::CannotCreateIndex,
                      "the index spec is missing a \"ns\" string field");
    const BSONElement nameElem = spec["name"];
    if (nameElem.type() != String)
        return Status(ErrorCodes::CannotCreateIndex, "index name must be specified as a string");

    const StringData name = nameElem.valueStringData();
    if (name.find('\0') != std::string::npos)
        return Status(ErrorCodes::CannotCreateIndex, "index name cannot contain NUL bytes");

    if (name.empty())
        return Status(ErrorCodes::CannotCreateIndex, "index name cannot be empty");

    const std::string indexNamespace = IndexDescriptionOnCfg::makeIndexNamespace(nss.ns(), name);
    if (indexNamespace.length() > NamespaceString::MaxNsLen)
        return Status(ErrorCodes::CannotCreateIndex,
                      str::stream() << "namespace name generated from index name \""
                                    << indexNamespace
                                    << "\" is too long (127 byte max)");

    const BSONObj key = spec.getObjectField("key");
    const Status keyStatus = index_key_validate::validateKeyPattern(key, indexVersion);
    if (!keyStatus.isOK()) {
        return Status(ErrorCodes::CannotCreateIndex,
                      str::stream() << "bad index key pattern " << key << ": "
                                    << keyStatus.reason());
    }
    std::unique_ptr<CollatorInterface> collator;
    BSONElement collationElement = spec.getField("collation");
    if (collationElement) {
        if (collationElement.type() != BSONType::Object) {
            return Status(ErrorCodes::CannotCreateIndex,
                          "\"collation\" for an index must be a document");
        }
        auto statusWithCollator = CollatorFactoryInterface::get(txn->getServiceContext())
                                      ->makeFromBSON(collationElement.Obj());
        if (!statusWithCollator.isOK()) {
            return statusWithCollator.getStatus();
        }
        collator = std::move(statusWithCollator.getValue());
        if (!collator) {
            return {ErrorCodes::InternalError,
                    str::stream() << "An internal operation specified the collation "
                                  << CollationSpec::kSimpleSpec
                                  << " explicitly, which should instead be implied by omitting the "
                                     "'collation' field from the index specification"};
        }

        if (static_cast<IndexVersion>(vElt.numberInt()) < IndexVersion::kV2) {
            return {ErrorCodes::CannotCreateIndex,
                    str::stream() << "Index version " << vElt.fieldNameStringData() << "="
                                  << vElt.numberInt()
                                  << " does not support the '"
                                  << collationElement.fieldNameStringData()
                                  << "' option"};
        }

        string pluginName = IndexNames::findPluginName(key);
        if ((pluginName != IndexNames::BTREE) && (pluginName != IndexNames::GEO_2DSPHERE) &&
            (pluginName != IndexNames::HASHED)) {
            return Status(ErrorCodes::CannotCreateIndex,
                          str::stream() << "Index type '" << pluginName
                                        << "' does not support collation: "
                                        << collator->getSpec().toBSON());
        }
    }
    const bool isSparse = spec["sparse"].trueValue();
    BSONElement filterElement = spec.getField("partialFilterExpression");
    if (filterElement) {
        if (isSparse) {
            return Status(ErrorCodes::CannotCreateIndex,
                          "cannot mix \"partialFilterExpression\" and \"sparse\" options");
        }

        if (filterElement.type() != Object) {
            return Status(ErrorCodes::CannotCreateIndex,
                          "\"partialFilterExpression\" for an index must be a document");
        }
        StatusWithMatchExpression statusWithMatcher = MatchExpressionParser::parse(
            filterElement.Obj(), ExtensionsCallbackDisallowExtensions(), collator.get());
        if (!statusWithMatcher.isOK()) {
            return statusWithMatcher.getStatus();
        }
        const std::unique_ptr<MatchExpression> filterExpr = std::move(statusWithMatcher.getValue());

        Status status = _checkValidFilterExpressions(filterExpr.get());
        if (!status.isOK()) {
            return status;
        }
    }
    if (IndexDescriptionOnCfg::isIdIndexPattern(key)) {
        BSONElement uniqueElt = spec["unique"];
        if (uniqueElt && !uniqueElt.trueValue()) {
            return Status(ErrorCodes::CannotCreateIndex, "_id index cannot be non-unique");
        }

        if (filterElement) {
            return Status(ErrorCodes::CannotCreateIndex, "_id index cannot be a partial index");
        }

        if (isSparse) {
            return Status(ErrorCodes::CannotCreateIndex, "_id index cannot be sparse");
        }

        CollectionType coll;
        auto collStatus = grid.catalogClient(txn)->getCollection(txn, nss.ns());
        coll = collStatus.getValue().value;
        BSONObj option = coll.getOptions();
        BSONObj defaultcollation;
        if (!option.isEmpty() && !option["collation"].eoo()) {
            defaultcollation = option["collation"].Obj();
        }
        auto Collator = parseCollation(txn, nss, defaultcollation);
        if (collationElement &&
            !CollatorInterface::collatorsMatch(collator.get(), Collator.get())) {
            return Status(ErrorCodes::CannotCreateIndex,
                          "_id index must have the collection default collation");
        }
    } else {
        if (!repl::getGlobalReplicationCoordinator()->buildsIndexes()) {
            return Status(ErrorCodes::IndexAlreadyExists, "no indexes per repl");
        }
    }
    BSONElement storageEngineElement = spec.getField("storageEngine");
    if (storageEngineElement.eoo()) {
        return Status::OK();
    }
    if (storageEngineElement.type() != mongo::Object) {
        return Status(ErrorCodes::CannotCreateIndex,
                      "\"storageEngine\" options must be a document if present");
    }
    BSONObj storageEngineOptions = storageEngineElement.Obj();
    if (storageEngineOptions.isEmpty()) {
        return Status(ErrorCodes::CannotCreateIndex,
                      "Empty \"storageEngine\" options are invalid. "
                      "Please remove the field or include valid options.");
    }
    Status storageEngineStatus =
        validateStorageOptions(storageEngineOptions,
                               stdx::bind(&StorageEngine::Factory::validateIndexStorageOptions,
                                          stdx::placeholders::_1,
                                          stdx::placeholders::_2));
    if (!storageEngineStatus.isOK()) {
        return storageEngineStatus;
    }
    return Status::OK();
}

IndexDescriptionOnCfg* findIndexByName(OperationContext* txn,
                                       StringData name,
                                       const NamespaceString& ns) {
    CollectionType coll;
    auto collStatus = grid.catalogClient(txn)->getCollection(txn, ns.ns());
    if (collStatus.isOK()) {
        coll = collStatus.getValue().value;
        if (coll.getDropped()) {
            return nullptr;
        }
        coll = collStatus.getValue().value;
    } else {
        return nullptr;
    }
    BSONObjIterator indexspecs(coll.getIndex());
    while (indexspecs.more()) {
        BSONObj existingIndex = indexspecs.next().Obj();
        const char* indexName = existingIndex.getStringField("name");
        if (StringData(indexName) == name) {
            return new IndexDescriptionOnCfg(existingIndex);
        }
    }
    return nullptr;
}


IndexDescriptionOnCfg* findIndexByKeyPatternAndCollationSpec(OperationContext* txn,
                                                             const BSONObj& key,
                                                             const BSONObj& collationSpec,
                                                             const NamespaceString& ns) {
    CollectionType coll;
    auto collStatus = grid.catalogClient(txn)->getCollection(txn, ns.ns());
    if (collStatus.isOK()) {
        coll = collStatus.getValue().value;
        if (coll.getDropped()) {
            return nullptr;
        }
        coll = collStatus.getValue().value;
    } else {
        return nullptr;
    }
    BSONObjIterator indexspecs(coll.getIndex());
    while (indexspecs.more()) {
        BSONObj existingIndex = indexspecs.next().Obj();
        BSONObj keyPattern = existingIndex.getObjectField("key");
        if ((SimpleBSONObjComparator::kInstance.evaluate(keyPattern == key)) &&
            (SimpleBSONObjComparator::kInstance.evaluate(
                existingIndex.getObjectField("collation") == collationSpec))) {
            return new IndexDescriptionOnCfg(existingIndex);
        }
    }
    return nullptr;
}


void findIndexesByKeyPattern(OperationContext* txn,
                             const BSONObj& key,
                             const NamespaceString& ns,
                             std::vector<IndexDescriptionOnCfg*>* matches) {
    CollectionType coll;
    auto collStatus = grid.catalogClient(txn)->getCollection(txn, ns.ns());
    if (collStatus.isOK()) {
        coll = collStatus.getValue().value;
        if (coll.getDropped()) {
            index_LOG(0) << "findIndexesByKeyPattern() " << ns << " has been droped!";
            return;
        }
        coll = collStatus.getValue().value;
    } else {
        return;
    }
    BSONObjIterator indexspecs(coll.getIndex());
    while (indexspecs.more()) {
        BSONObj existingIndex = indexspecs.next().Obj();
        BSONObj keyPattern = existingIndex.getObjectField("key");
        if (SimpleBSONObjComparator::kInstance.evaluate(keyPattern == key)) {
            matches->push_back(new IndexDescriptionOnCfg(existingIndex));
        }
    }
}
BSONObj fixIndexKey(const BSONObj& key) {
    if (IndexDescriptionOnCfg::isIdIndexPattern(key)) {
        return BSON("_id" << 1);
    }
    if (key["_id"].type() == Bool && key.nFields() == 1) {
        return BSON("_id" << 1);
    }
    return key;
}

StatusWith<BSONObj> fixIndexSpec(OperationContext* txn,
                                 const NamespaceString& ns,
                                 const BSONObj& spec) {

    auto statusWithSpec = IndexLegacy::adjustIndexSpecObject(spec);
    if (!statusWithSpec.isOK()) {
        return statusWithSpec;
    }
    BSONObj o = statusWithSpec.getValue();

    BSONObjBuilder b;
    auto vElt = o["v"];
    invariant(vElt);

    b.append("v", vElt.numberInt());

    if (o["unique"].trueValue())
        b.appendBool("unique", true);
    BSONObj key = fixIndexKey(o["key"].Obj());
    b.append("key", key);
    string name = o["name"].String();
    if (IndexDescriptionOnCfg::isIdIndexPattern(key)) {
        name = "_id_";
    }
    b.append("name", name);
    {
        BSONObjIterator i(o);
        while (i.more()) {
            BSONElement e = i.next();
            string s = e.fieldName();

            if (s == "_id") {

            } else if (s == "dropDups") {

            } else if (s == "v" || s == "unique" || s == "key" || s == "name") {

            } else {
                b.append(e);
            }
        }
    }
    return b.obj();
}

void findIndexByType(OperationContext* txn,
                     const string& type,
                     std::vector<IndexDescriptionOnCfg*>& matches,
                     const NamespaceString& ns) {
    CollectionType coll;
    auto collStatus = grid.catalogClient(txn)->getCollection(txn, ns.ns());
    if (collStatus.isOK()) {
        coll = collStatus.getValue().value;
        if (coll.getDropped()) {
            index_LOG(0) << "findIndexByType() " << ns << " has been droped";
            return;
        }
        coll = collStatus.getValue().value;
    } else {
        index_LOG(0) << "findIndexByType() " << ns << " has not exist";
        return;
    }
    BSONObjIterator indexspecs(coll.getIndex());
    while (indexspecs.more()) {
        BSONObj existingIndex = indexspecs.next().Obj();
        IndexDescriptionOnCfg* desc = new IndexDescriptionOnCfg(existingIndex);
        if (IndexNames::findPluginName(desc->keyPattern()) == type) {
            matches.push_back(desc);
        }
    }
}

Status doesSpecConflictWithExisting(OperationContext* txn,
                                    const BSONObj& spec,
                                    const NamespaceString& ns,
                                    std::vector<int64_t>* prefixes) {
    const char* name = spec.getStringField("name");
    invariant(name[0]);
    const BSONObj key = spec.getObjectField("key");
    const BSONObj collation = spec.getObjectField("collation");
    {
        std::unique_ptr<IndexDescriptionOnCfg> desc(findIndexByName(txn, name, ns));
        if (desc) {
            if (SimpleBSONObjComparator::kInstance.evaluate(desc->keyPattern() == key) &&
                SimpleBSONObjComparator::kInstance.evaluate(
                    desc->infoObj().getObjectField("collation") != collation)) {
                return Status(ErrorCodes::IndexOptionsConflict,
                              str::stream()
                                  << "An index with the same key pattern, but a different "
                                  << "collation already exists with the same name.  Try again with "
                                  << "a unique name. "
                                  << "Existing index: "
                                  << desc->infoObj()
                                  << " Requested index: "
                                  << spec);
            }
            if (SimpleBSONObjComparator::kInstance.evaluate(desc->keyPattern() != key) ||
                SimpleBSONObjComparator::kInstance.evaluate(
                    desc->infoObj().getObjectField("collation") != collation)) {
                return Status(ErrorCodes::IndexKeySpecsConflict,
                              str::stream() << "Index must have unique name."
                                            << "The existing index: "
                                            << desc->infoObj()
                                            << " has the same name as the requested index: "
                                            << spec);
            }
            IndexDescriptionOnCfg temp(spec);
            if (!desc->areIndexOptionsEquivalent(&temp)) {
                return Status(ErrorCodes::IndexOptionsConflict,
                              str::stream() << "Index with name: " << name
                                            << " already exists with different options");
            }
            if (prefixes)
                prefixes->push_back(desc->getPrefix());
            return Status(ErrorCodes::IndexAlreadyExists,
                          str::stream() << "Identical index already exists: " << name);
        }
    }
    {
        const std::unique_ptr<IndexDescriptionOnCfg> desc(
            findIndexByKeyPatternAndCollationSpec(txn, key, collation, ns));
        if (desc) {
            IndexDescriptionOnCfg temp(spec);
            if (!desc->areIndexOptionsEquivalent(&temp))
                return Status(ErrorCodes::IndexOptionsConflict,
                              str::stream() << "Index: " << spec
                                            << " already exists with different options: "
                                            << desc->infoObj());
            if (prefixes)
                prefixes->push_back(desc->getPrefix());
            return Status(ErrorCodes::IndexAlreadyExists,
                          str::stream() << "index already exists with different name: " << name);
        }
    }
    return Status::OK();
}

StatusWith<BSONObj> prepareSpecForCreate(OperationContext* txn,
                                         const NamespaceString& ns,
                                         const BSONObj& original,
                                         std::vector<int64_t>* prefixes) {
    Status status = isSpecOk(txn, ns, original);
    if (!status.isOK())
        return StatusWith<BSONObj>(status);
    auto fixed = fixIndexSpec(txn, ns, original);
    if (!fixed.isOK()) {
        return fixed;
    }
    // we double check with new index spec
    status = isSpecOk(txn, ns, fixed.getValue());
    if (!status.isOK())
        return StatusWith<BSONObj>(status);
    status = doesSpecConflictWithExisting(txn, fixed.getValue(), ns, prefixes);
    if (!status.isOK())
        return StatusWith<BSONObj>(status);
    return fixed;
}

void generateResult(OperationContext* txn,
                    BSONObjBuilder& res,
                    CollectionType& coll,
                    const NamespaceString& ns,
                    bool iscreateCollAuto) {
    BSONObjIterator indexspecs(coll.getIndex());
    int count = 0;
    while (indexspecs.more()) {
        count++;
        indexspecs++;
    }
    BSONObjBuilder builder;
    builder.append("createdCollectionAutomatically", iscreateCollAuto);
    builder.append("numIndexesBefore", count);
    builder.append("numIndexesAfter", count);
    builder.append("note", "all indexes already exist");
    builder.append("ok", 1);
    vector<ChunkType> chunks;
    uassertStatusOK(
        grid.catalogClient(txn)->getChunks(txn,
                                           BSON(ChunkType::ns(ns.ns())),
                                           BSONObj(),
                                           0,
                                           &chunks,
                                           nullptr,
                                           repl::ReadConcernLevel::kMajorityReadConcern));
    for (ChunkType chunk : chunks) {
        const auto shardStatus = grid.shardRegistry()->getShard(txn, chunk.getShard());
        const auto createindexShard = shardStatus.getValue();
        auto targeter = createindexShard->getTargeter();
        auto re = targeter->findHost(txn, ReadPreferenceSetting{ReadPreference::PrimaryOnly});
        res.append(StringData(re.getValue().toString()), builder.obj());
    }
}

void generateResult(OperationContext* txn,
                    BSONObjBuilder& res,
                    Status& status,
                    const NamespaceString& ns) {
    vector<ChunkType> chunks;
    uassertStatusOK(
        grid.catalogClient(txn)->getChunks(txn,
                                           BSON(ChunkType::ns(ns.ns())),
                                           BSONObj(),
                                           0,
                                           &chunks,
                                           nullptr,
                                           repl::ReadConcernLevel::kMajorityReadConcern));
    BSONObjBuilder builder;
    builder.append("ok", 0);
    builder.append("errmsg", status.reason());
    builder.append("code", status.code());
    builder.append("codeName", status.codeString());
    for (ChunkType chunk : chunks) {
        const auto shardStatus = grid.shardRegistry()->getShard(txn, chunk.getShard());
        const auto createindexShard = shardStatus.getValue();
        auto targeter = createindexShard->getTargeter();
        auto re = targeter->findHost(txn, ReadPreferenceSetting{ReadPreference::PrimaryOnly});
        res.append(StringData(re.getValue().toString()), builder.obj());
    }
}

bool isInTheRange(std::vector<size_t>* location, size_t i) {
    for (auto m : (*location)) {
        if (m == i) {
            return true;
        }
    }
    return false;
}

Status removeExistingIndexes(OperationContext* txn,
                             const NamespaceString& ns,
                             std::vector<BSONObj>* specs,
                             std::vector<size_t>* origin,
                             std::vector<int64_t>* prefixes,
                             BSONObjBuilder& res,
                             CollectionType& coll,
                             bool& flag) {
    size_t t = -1;
    for (size_t i = 0; i < specs->size(); i++) {
        t++;
        Status status = prepareSpecForCreate(txn, ns, (*specs)[i], prefixes).getStatus();
        BSONObj index = (*specs)[i];
        if (status.code() == ErrorCodes::IndexAlreadyExists) {
            if (IndexDescriptor::isIdIndexPattern(index["key"].Obj())) {
                BSONObj option = coll.getOptions();
                if (!option.isEmpty()) {
                    if (!option["autoIndexId"].eoo() && !option["autoIndexId"].boolean()) {
                        option = option.removeField("autoIndexId");
                        coll.setOptions(option);
                        flag = true;
                    }
                }
            }
            specs->erase(specs->begin() + i);
            origin->push_back(t);
            i--;
        } else if (!status.isOK()) {
            generateResult(txn, res, status, ns);
            return status;
        }
    }
    return Status::OK();
}


Status createIndexMetadata(OperationContext* txn,
                           const NamespaceString& ns,
                           std::vector<BSONObj>& indexSpecs,
                           BSONObj& cmdObj,
                           const std::string& dbname,
                           BSONObjBuilder& result,
                           const bool isReIndex) {
    // if coll not exist, to create it!
    BSONObjBuilder subobj(result.subobjStart("raw"));
    CollectionType coll;
    auto collStatus = grid.catalogClient(txn)->getCollection(txn, ns.ns());
    if (collStatus.isOK()) {
        coll = collStatus.getValue().value;
        if (coll.getDropped()) {
            Status status = createNewCollection(txn, ns);
            if (!status.isOK()) {
                if (status.code() != ErrorCodes::NamespaceExists) {
                    return status;
                }
            }
            auto cs = grid.catalogClient(txn)->getCollection(txn, ns.ns());
            coll = cs.getValue().value;
        }
    } else {
        Status status = createNewCollection(txn, ns);
        if (!status.isOK()) {
            if (status.code() != ErrorCodes::NamespaceExists) {
                return status;
            }
        }
        auto cs = grid.catalogClient(txn)->getCollection(txn, ns.ns());
        coll = cs.getValue().value;
    }
    std::vector<BSONObj> indexes(indexSpecs);
    std::vector<size_t> remove_location;
    std::vector<long long> prefixes;
    std::vector<int64_t> AllreadyExistPrefixes;
    std::vector<BSONObj> tmpIndexSpecs = std::move(indexSpecs);
    for (size_t k = 0, numIndexSpecs = tmpIndexSpecs.size(); k < numIndexSpecs; k++) {
        auto one = tmpIndexSpecs[k];
        if (isReIndex && one["collation"].eoo()) {
            BSONObjBuilder b;
            b.appendElements(one);
            b.append("collation",
                     BSON("locale"
                          << "simple"));
            tmpIndexSpecs[k] = b.obj();
        }
    }
    auto indexSpecsWithDefaults =
        resolveCollectionDefaultProperties(txn, ns, std::move(tmpIndexSpecs));
    if (!indexSpecsWithDefaults.isOK()) {
        return indexSpecsWithDefaults.getStatus();
    }
    auto specs = std::move(indexSpecsWithDefaults.getValue());
    bool flag = false;
    Status sst = removeExistingIndexes(
        txn, ns, &specs, &remove_location, &AllreadyExistPrefixes, subobj, coll, flag);
    if (!sst.isOK()) {
        return sst;
    }
    if (specs.size() == 0) {
        index_log() << "createIndexMetadata all indexes exist";
        if (flag)
            uassertStatusOK(grid.catalogClient(txn)->updateCollection(txn, ns.ns(), coll));
    }
    BSONArrayBuilder indexArrayBuilder;
    BSONArrayBuilder toCreateCmdBuilder;
    BSONObjIterator indexspecs(coll.getIndex());
    while (indexspecs.more()) {
        BSONObj existingIndex = indexspecs.next().Obj();
        indexArrayBuilder.append(existingIndex);
    }
    long long ll_max_prefix = coll.getPrefix();
    for (size_t i = 0; i < specs.size(); i++) {
        StatusWith<BSONObj> statusWithInfo = fixIndexSpec(txn, ns, specs[i]);
        BSONObj info = statusWithInfo.getValue();
        BSONObjBuilder build;
        build.appendElements(info);
        build.append("prefix", ++ll_max_prefix);
        prefixes.push_back(ll_max_prefix);
        BSONObj index = build.obj();
        auto ele = index.getField("dropDups");
        if (!ele.eoo()) {
            indexArrayBuilder.append(index.removeField("dropDups"));
        } else {
            indexArrayBuilder.append(index);
        }
    }

    {
        size_t k = 0;
        size_t j = 0;
        invariant(indexes.size() == (remove_location.size() + prefixes.size()));
        for (size_t i = 0; i < indexes.size(); i++) {
            BSONObjBuilder build;
            build.appendElements(indexes[i]);
            if (isInTheRange(&remove_location, i)) {
                // there is no need to create in the vector remove_location. so we set prefix to
                // zero.
                // prefix must be required by shardServer. and indexes must not be empty,
                // because configServer just generate indexMetadata,not return the
                // result of createIndex to user.
                build.append("prefix", AllreadyExistPrefixes[j++]);
            } else {
                build.append("prefix", prefixes[k++]);
            }
            toCreateCmdBuilder.append(build.obj());
        }
    }


    BSONArray temp = indexArrayBuilder.arr();
    if (specs.size() != 0) {
        coll.setIndex(temp);
    }
    coll.setPrefix(ll_max_prefix);
    std::string s = cmdObj.getField("createIndexes").String();
    BSONArray ba = toCreateCmdBuilder.arr();
    bool isUpdate = true;
    Status st = passCreateIndexCMDtoEveryChunk(txn, ns, ba, s, dbname, subobj, isUpdate);
    subobj.done();

    if (isUpdate) {
        uassertStatusOK(grid.catalogClient(txn)->updateCollection(txn, ns.ns(), coll));
    }
    if (!st.isOK()) {
        return st;
    }
    return Status::OK();
}


MONGO_FP_DECLARE(crashAfterStartingIndexBuild);
MONGO_FP_DECLARE(hangAfterStartingIndexBuild);
MONGO_FP_DECLARE(hangAfterStartingIndexBuildUnlocked);

std::atomic<std::int32_t> maxIndexBuildMemoryUsageMegabytes(500);  // NOLINT

class ExportedMaxIndexBuildMemoryUsageParameter
    : public ExportedServerParameter<std::int32_t, ServerParameterType::kStartupAndRuntime> {
public:
    ExportedMaxIndexBuildMemoryUsageParameter()
        : ExportedServerParameter<std::int32_t, ServerParameterType::kStartupAndRuntime>(
              ServerParameterSet::getGlobal(),
              "maxIndexBuildMemoryUsageMegabytes",
              &maxIndexBuildMemoryUsageMegabytes) {}

    virtual Status validate(const std::int32_t& potentialNewValue) {
        if (potentialNewValue < 100) {
            return Status(
                ErrorCodes::BadValue,
                "maxIndexBuildMemoryUsageMegabytes must be greater than or equal to 100 MB");
        }

        return Status::OK();
    }

} exportedMaxIndexBuildMemoryUsageParameter;


/**
 * On rollback sets MultiIndexBlock::_needToCleanup to true.
 */
class MultiIndexBlock::SetNeedToCleanupOnRollback : public RecoveryUnit::Change {
public:
    explicit SetNeedToCleanupOnRollback(MultiIndexBlock* indexer) : _indexer(indexer) {}

    virtual void commit() {}
    virtual void rollback() {
        _indexer->_needToCleanup = true;
    }

private:
    MultiIndexBlock* const _indexer;
};

/**
 * On rollback in init(), cleans up _indexes so that ~MultiIndexBlock doesn't try to clean
 * up _indexes manually (since the changes were already rolled back).
 * Due to this, it is thus legal to call init() again after it fails.
 */
class MultiIndexBlock::CleanupIndexesVectorOnRollback : public RecoveryUnit::Change {
public:
    explicit CleanupIndexesVectorOnRollback(MultiIndexBlock* indexer) : _indexer(indexer) {}

    virtual void commit() {}
    virtual void rollback() {
        _indexer->_indexes.clear();
    }

private:
    MultiIndexBlock* const _indexer;
};

MultiIndexBlock::MultiIndexBlock(OperationContext* txn, Collection* collection)
    : _collection(collection),
      _txn(txn),
      _buildInBackground(false),
      _allowInterruption(false),
      _ignoreUnique(false),
      _needToCleanup(true) {}

MultiIndexBlock::~MultiIndexBlock() {
    if (!_needToCleanup || _indexes.empty())
        return;
    while (true) {
        try {
            WriteUnitOfWork wunit(_txn);
            // This cleans up all index builds.
            // Because that may need to write, it is done inside
            // of a WUOW. Nothing inside this block can fail, and it is made fatal if it does.
            for (size_t i = 0; i < _indexes.size(); i++) {
                _indexes[i].block->fail();
            }
            wunit.commit();
            return;
        } catch (const WriteConflictException& e) {
            continue;
        } catch (const DBException& e) {
            if (e.toStatus() == ErrorCodes::ExceededMemoryLimit)
                continue;
            error() << "Caught exception while cleaning up partially built indexes: " << redact(e);
        } catch (const std::exception& e) {
            error() << "Caught exception while cleaning up partially built indexes: " << e.what();
        } catch (...) {
            error() << "Caught unknown exception while cleaning up partially built indexes.";
        }
        fassertFailed(18644);
    }
}

void MultiIndexBlock::removeExistingIndexes(std::vector<BSONObj>* specs) const {
    for (size_t i = 0; i < specs->size(); i++) {
        Status status =
            _collection->getIndexCatalog()->prepareSpecForCreate(_txn, (*specs)[i]).getStatus();
        if (status.code() == ErrorCodes::IndexAlreadyExists) {
            specs->erase(specs->begin() + i);
            i--;
        }
        // intentionally ignoring other error codes
    }
}

StatusWith<std::vector<BSONObj>> MultiIndexBlock::init(const BSONObj& spec) {
    const auto indexes = std::vector<BSONObj>(1, spec);
    return init(indexes);
}

StatusWith<std::vector<BSONObj>> MultiIndexBlock::init(const std::vector<BSONObj>& indexSpecs) {
    WriteUnitOfWork wunit(_txn);

    Database* db = dbHolder().get(_txn, _collection->ns().db());

    invariant(_indexes.empty());
    _txn->recoveryUnit()->registerChange(new CleanupIndexesVectorOnRollback(this));

    const string& ns = _collection->ns().ns();

    const auto idxCat = _collection->getIndexCatalog();
    invariant(idxCat);
    invariant(idxCat->ok());
    Status status = idxCat->checkUnfinished();
    if (!status.isOK())
        return status;

    for (size_t i = 0; i < indexSpecs.size(); i++) {
        BSONObj info = indexSpecs[i].getOwned();
        string pluginName = IndexNames::findPluginName(info["key"].Obj());
        if (pluginName.size()) {
            Status s = _collection->getIndexCatalog()->_upgradeDatabaseMinorVersionIfNeeded(
                _txn, pluginName);
            if (!s.isOK())
                return s;
        }

        // Any foreground indexes make all indexes be built in the foreground.
        _buildInBackground = (_buildInBackground && info["background"].trueValue());
    }

    std::vector<BSONObj> indexInfoObjs;
    index_log() << "[createIndex] MultiIndexBlock::init() indexSpecs size:" << indexSpecs.size();
    indexInfoObjs.reserve(indexSpecs.size());
    std::size_t eachIndexBuildMaxMemoryUsageBytes = 0;
    auto collCount = 1;
    if(db){
        collCount = db->getCollectionCount()+1;
    }
    
    if (!indexSpecs.empty()) {
        eachIndexBuildMaxMemoryUsageBytes = std::size_t(maxIndexBuildMemoryUsageMegabytes) * 1024 *
            1024 / (indexSpecs.size() * collCount);
    }

    for (size_t i = 0; i < indexSpecs.size(); i++) {
        BSONObj info = indexSpecs[i].getOwned();

        if (!info.hasField("prefix")) {
            NamespaceString nss = _collection->ns();
            auto replMode = repl::getGlobalReplicationCoordinator()->getReplicationMode();
            auto single_mongod= ClusterRole::None == serverGlobalParams.clusterRole && 
                                replMode != repl::ReplicationCoordinator::Mode::modeReplSet;
            if (single_mongod && !nss.isSystemCollection()) {
                static long long prefix = 10000;
                BSONObjBuilder builder;

                BSONObjIterator it(info);
                while (it.more()) {
                    builder.append(*it);
                    it++;
                }

                builder.append("prefix", prefix++);
                info = builder.obj();
            }
        }

        StatusWith<BSONObj> statusWithInfo =
            _collection->getIndexCatalog()->prepareSpecForCreate(_txn, info);
        Status status = statusWithInfo.getStatus();
        if (!status.isOK())
            return status;
        info = statusWithInfo.getValue();
        indexInfoObjs.push_back(info);

        IndexToBuild index;
        index.block.reset(new IndexCatalog::IndexBuildBlock(_txn, _collection, info));
        status = index.block->init();
        if (!status.isOK())
            return status;

        index.real = index.block->getEntry()->accessMethod();
        status = index.real->initializeAsEmpty(_txn);
        if (!status.isOK())
            return status;

        if (!_buildInBackground) {
            // Bulk build process requires foreground building as it assumes nothing is changing
            // under it.
            index.bulk = index.real->initiateBulk(eachIndexBuildMaxMemoryUsageBytes);
        }

        const IndexDescriptor* descriptor = index.block->getEntry()->descriptor();

        IndexCatalog::prepareInsertDeleteOptions(_txn, descriptor, &index.options);
        index.options.dupsAllowed = index.options.dupsAllowed || _ignoreUnique;
        if (_ignoreUnique) {
            index.options.getKeysMode = IndexAccessMethod::GetKeysMode::kRelaxConstraints;
        }

        log() << "build index on: " << ns;
        if (index.bulk)
            log() << "\t building index using bulk method; build may temporarily use up to "
                  << eachIndexBuildMaxMemoryUsageBytes / 1024 / 1024 << " megabytes of RAM";

        index.filterExpression = index.block->getEntry()->getFilterExpression();

        // TODO SERVER-14888 Suppress this in cases we don't want to audit.
        audit::logCreateIndex(_txn->getClient(), &info, descriptor->indexName(), ns);

        _indexes.push_back(std::move(index));
    }

    if (_buildInBackground)
        _backgroundOperation.reset(new BackgroundOperation(ns));
    index_log() << "MultiIndexBlock::init before wunit.commit().";
    wunit.commit();

    if (MONGO_FAIL_POINT(crashAfterStartingIndexBuild)) {
        log() << "Index build interrupted due to 'crashAfterStartingIndexBuild' failpoint. Exiting "
                 "after waiting for changes to become durable.";
        Locker::LockSnapshot lockInfo;
        _txn->lockState()->saveLockStateAndUnlock(&lockInfo);
        if (_txn->recoveryUnit()->waitUntilDurable()) {
            quickExit(EXIT_TEST);
        }
    }
    index_log() << "MultiIndexBlock::init after wunit.commit().";

    return indexInfoObjs;
}

Status MultiIndexBlock::insertAllDocumentsInCollection(std::set<RecordId>* dupsOut) {
    const char* curopMessage = _buildInBackground ? "Index Build (background)" : "Index Build";
    const auto numRecords = _collection->numRecords(_txn);
    stdx::unique_lock<Client> lk(*_txn->getClient());
    ProgressMeterHolder progress(*_txn->setMessage_inlock(curopMessage, curopMessage, numRecords));
    lk.unlock();

    Timer t;

    unsigned long long n = 0;

    unique_ptr<PlanExecutor> exec(InternalPlanner::collectionScan(
        _txn, _collection->ns().ns(), _collection, PlanExecutor::YIELD_MANUAL));
    if (_buildInBackground) {
        invariant(_allowInterruption);
        exec->setYieldPolicy(PlanExecutor::YIELD_AUTO, _collection);
    } else {
        exec->setYieldPolicy(PlanExecutor::WRITE_CONFLICT_RETRY_ONLY, _collection);
    }

    Snapshotted<BSONObj> objToIndex;
    RecordId loc;
    PlanExecutor::ExecState state;
    int retries = 0;  // non-zero when retrying our last document.
    while (retries ||
           (PlanExecutor::ADVANCED == (state = exec->getNextSnapshotted(&objToIndex, &loc)))) {
        try {
            if (_allowInterruption)
                _txn->checkForInterrupt();

            // Make sure we are working with the latest version of the document.
            if (objToIndex.snapshotId() != _txn->recoveryUnit()->getSnapshotId() &&
                !_collection->findDoc(_txn, loc, &objToIndex)) {
                // doc was deleted so don't index it.
                retries = 0;
                continue;
            }

            // Done before insert so we can retry document if it WCEs.
            progress->setTotalWhileRunning(_collection->numRecords(_txn));

            WriteUnitOfWork wunit(_txn);
            Status ret = insert(objToIndex.value(), loc);
            if (_buildInBackground)
                exec->saveState();
            if (ret.isOK()) {
                wunit.commit();
            } else if (dupsOut && ret.code() == ErrorCodes::DuplicateKey) {
                // If dupsOut is non-null, we should only fail the specific insert that
                // led to a DuplicateKey rather than the whole index build.
                dupsOut->insert(loc);
            } else {
                // Fail the index build hard.
                return ret;
            }
            if (_buildInBackground)
                exec->restoreState();  // Handles any WCEs internally.

            // Go to the next document
            progress->hit();
            n++;
            retries = 0;
        } catch (const WriteConflictException& wce) {
            CurOp::get(_txn)->debug().writeConflicts++;
            retries++;  // logAndBackoff expects this to be 1 on first call.
            wce.logAndBackoff(retries, "index creation", _collection->ns().ns());

            // Can't use WRITE_CONFLICT_RETRY_LOOP macros since we need to save/restore exec
            // around call to abandonSnapshot.
            exec->saveState();
            _txn->recoveryUnit()->abandonSnapshot();
            exec->restoreState();  // Handles any WCEs internally.
        }
    }

    uassert(28550,
            "Unable to complete index build due to collection scan failure: " +
                WorkingSetCommon::toStatusString(objToIndex.value()),
            state == PlanExecutor::IS_EOF);

    if (MONGO_FAIL_POINT(hangAfterStartingIndexBuild)) {
        // Need the index build to hang before the progress meter is marked as finished so we can
        // reliably check that the index build has actually started in js tests.
        while (MONGO_FAIL_POINT(hangAfterStartingIndexBuild)) {
            log() << "Hanging index build due to 'hangAfterStartingIndexBuild' failpoint";
            sleepmillis(1000);
        }

        // Check for interrupt to allow for killop prior to index build completion.
        _txn->checkForInterrupt();
    }

    if (MONGO_FAIL_POINT(hangAfterStartingIndexBuildUnlocked)) {
        // Unlock before hanging so replication recognizes we've completed.
        Locker::LockSnapshot lockInfo;
        _txn->lockState()->saveLockStateAndUnlock(&lockInfo);
        while (MONGO_FAIL_POINT(hangAfterStartingIndexBuildUnlocked)) {
            log() << "Hanging index build with no locks due to "
                     "'hangAfterStartingIndexBuildUnlocked' failpoint";
            sleepmillis(1000);
        }
        // If we want to support this, we'd need to regrab the lock and be sure that all callers are
        // ok with us yielding. They should be for BG indexes, but not for foreground.
        invariant(!"the hangAfterStartingIndexBuildUnlocked failpoint can't be turned off");
    }

    progress->finished();

    Status ret = doneInserting(dupsOut);
    if (!ret.isOK())
        return ret;

    log() << "build index done.  scanned " << n << " total records. " << t.seconds() << " secs";

    return Status::OK();
}

Status MultiIndexBlock::insert(const BSONObj& doc, const RecordId& loc) {
    for (size_t i = 0; i < _indexes.size(); i++) {
        if (_indexes[i].filterExpression && !_indexes[i].filterExpression->matchesBSON(doc)) {
            continue;
        }

        int64_t unused;
        Status idxStatus(ErrorCodes::InternalError, "");
        if (_indexes[i].bulk) {
            idxStatus = _indexes[i].bulk->insert(_txn, doc, loc, _indexes[i].options, &unused);
        } else {
            idxStatus = _indexes[i].real->insert(_txn, doc, loc, _indexes[i].options, &unused);
        }

        if (!idxStatus.isOK())
            return idxStatus;
    }
    return Status::OK();
}

Status MultiIndexBlock::doneInserting(std::set<RecordId>* dupsOut) {
    for (size_t i = 0; i < _indexes.size(); i++) {
        if (_indexes[i].bulk == NULL)
            continue;
        LOG(1) << "\t bulk commit starting for index: "
               << _indexes[i].block->getEntry()->descriptor()->indexName();
        invariant(!_buildInBackground);
        Status status = _indexes[i].real->commitBulk(_txn,
                                                     std::move(_indexes[i].bulk),
                                                     _allowInterruption,
                                                     _indexes[i].options.dupsAllowed,
                                                     dupsOut);
        if (!status.isOK()) {
            return status;
        }
    }

    return Status::OK();
}

void MultiIndexBlock::abortWithoutCleanup() {
    _indexes.clear();
    _needToCleanup = false;
}

void MultiIndexBlock::commit() {
    for (size_t i = 0; i < _indexes.size(); i++) {
        _indexes[i].block->success();
    }

    _txn->recoveryUnit()->registerChange(new SetNeedToCleanupOnRollback(this));
    _needToCleanup = false;
}

}  // namespace mongo

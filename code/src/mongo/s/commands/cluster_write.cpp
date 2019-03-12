/**
 *    Copyright (C) 2013 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/s/commands/cluster_write.h"

#include <string>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/commands.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/s/balancer_configuration.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/chunk_manager.h"
#include "mongo/s/chunk_manager_targeter.h"
#include "mongo/s/client/dbclient_multi_command.h"
#include "mongo/s/commands/checkView.h"
#include "mongo/s/config.h"
#include "mongo/s/grid.h"
#include "mongo/s/mongos_options.h"
#include "mongo/s/sharding_raii.h"
#include "mongo/s/write_ops/batch_write_exec.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

using std::shared_ptr;
using std::unique_ptr;
using std::vector;
using std::map;
using std::string;
using std::stringstream;

using IndexVersion = IndexDescriptor::IndexVersion;

namespace {

/**
 * Constructs the BSON specification document for the given namespace, index key
 * and options.
 */
BSONObj createIndexDoc(const string& ns,
                       const BSONObj& keys,
                       const BSONObj& collation,
                       bool unique) {
    BSONObjBuilder indexDoc;
    indexDoc.append("ns", ns);
    indexDoc.append("key", keys);

    stringstream indexName;

    bool isFirstKey = true;
    for (BSONObjIterator keyIter(keys); keyIter.more();) {
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

    indexDoc.append("name", indexName.str());

    if (!collation.isEmpty()) {
        // Creating an index with the "collation" option requires a v=2 index.
        indexDoc.append("v", static_cast<int>(IndexVersion::kV2));
        indexDoc.append("collation", collation);
    }

    if (unique && !IndexDescriptor::isIdIndexPattern(keys)) {
        indexDoc.appendBool("unique", unique);
    }

    return indexDoc.obj();
}

void toBatchError(const Status& status, BatchedCommandResponse* response, bool flag = false) {
    response->clear();
    if (flag) {
        response->clear();
        WriteErrorDetail* error = new WriteErrorDetail();
        error->setIndex(0);
        error->setErrCode(status.code());
        error->setErrMessage(status.reason());
        response->setOk(true);
        response->setN(0);
        response->addToErrDetails(error);
    } else {
        response->setErrCode(status.code());
        response->setErrMessage(status.reason());
        response->setOk(false);
    }
    dassert(response->isValid(NULL));
}
/**
* Splits the chunks touched based from the targeter stats if needed.
*/
void splitIfNeeded(OperationContext* txn, const NamespaceString& nss, const TargeterStats& stats) {
    auto status = grid.catalogCache()->getDatabase(txn, nss.db().toString());
    if (!status.isOK()) {
        warning() << "failed to get database config for " << nss
                  << " while checking for auto-split: " << status.getStatus();
        return;
    }

    shared_ptr<DBConfig> config = status.getValue();

    shared_ptr<ChunkManager> chunkManager;
    shared_ptr<Shard> dummyShard;
    auto cmOrPrimaryStatus =
        config->getChunkManagerOrPrimary(txn, nss.ns(), chunkManager, dummyShard);
    if (!cmOrPrimaryStatus.isOK()) {
        return;
    }

    if (!chunkManager) {
        return;
    }

    for (auto it = stats.chunkSizeDelta.cbegin(); it != stats.chunkSizeDelta.cend(); ++it) {
        shared_ptr<Chunk> chunk;
        try {
            chunk = chunkManager->findIntersectingChunkWithSimpleCollation(txn, it->first);
        } catch (const AssertionException& ex) {
            warning() << "could not find chunk while checking for auto-split: "
                      << causedBy(redact(ex));
            return;
        }

        chunk->splitIfShould(txn, it->second);
    }
}

}  // namespace

Status clusterCreateIndex(
    OperationContext* txn, const string& ns, BSONObj keys, BSONObj collation, bool unique) {
    const NamespaceString nss(ns);
    const std::string dbName = nss.db().toString();

    BSONObj indexDoc = createIndexDoc(ns, keys, collation, unique);

    // Go through the shard insert path
    std::unique_ptr<BatchedInsertRequest> insert(new BatchedInsertRequest());
    insert->addToDocuments(indexDoc);

    BatchedCommandRequest request(insert.release());
    request.setNS(NamespaceString(nss.getSystemIndexesCollection()));
    request.setWriteConcern(WriteConcernOptions::Acknowledged);

    BatchedCommandResponse response;

    ClusterWriter writer(false, 0);
    writer.write(txn, request, &response);

    return response.toStatus();
}

Status ClusterWriter::checkDocs(const BatchedCommandRequest* request) {
    BatchedInsertRequest* insertRequest = request->getInsertRequest();
    invariant(insertRequest != NULL);
    std::vector<BSONObj> docs = insertRequest->getDocuments();
    int validDocSize = int(docs.size());
    Status status = Status::OK();
    for (BSONObj& doc : docs) {
        if (doc.objsize() > BSONObjMaxUserSize) {
            status = Status(ErrorCodes::BadValue,
                            str::stream() << "object to insert too large"
                                          << ". size in bytes: "
                                          << doc.objsize()
                                          << ", max size: "
                                          << BSONObjMaxUserSize);
            validDocSize--;
            continue;
        }

        int idNum = 0;
        BSONObjIterator i(doc);
        for (bool isFirstElement = true; i.more(); isFirstElement = false) {
            BSONElement e = i.next();
            auto fieldName = e.fieldNameStringData();
            if (fieldName[0] == '$') {
                status = Status(ErrorCodes::BadValue,
                                str::stream() << "Document can't have $ prefixed field names: "
                                              << fieldName);
                validDocSize--;
                break;
            }
            if (fieldName == "_id") {
                if (e.type() == RegEx) {
                    status = Status(ErrorCodes::BadValue, "can't use a regex for _id");
                    validDocSize--;
                    break;
                } else if (e.type() == Undefined) {
                    status = Status(ErrorCodes::BadValue, "can't use a undefined for _id");
                    validDocSize--;
                    break;
                } else if (e.type() == Array) {
                    status = Status(ErrorCodes::BadValue, "can't use an array for _id");
                    validDocSize--;
                    break;
                } else if (e.type() == Object) {
                    BSONObj o = e.Obj();
                    Status s = o.storageValidEmbedded();
                    if (!s.isOK()) {
                        status = s;
                        validDocSize--;
                        break;
                    }
                }
                idNum++;
            }
        }

        if (idNum > 1) {
            status = Status(ErrorCodes::BadValue, "can't have multiple _id fields in one document");
            validDocSize--;
        }
    }

    if (0 == validDocSize) {
        index_err() << "invalid document: " << status;
        return status;
    } else {
        return Status::OK();
    }
}

void ClusterWriter::write(OperationContext* txn,
                          const BatchedCommandRequest& origRequest,
                          BatchedCommandResponse* response) {
    // Add _ids to insert request if req'd
    unique_ptr<BatchedCommandRequest> idRequest(BatchedCommandRequest::cloneWithIds(origRequest));
    const BatchedCommandRequest* request = NULL != idRequest.get() ? idRequest.get() : &origRequest;

    const NamespaceString& nss = request->getNS();
    if (!nss.isValid()) {
        toBatchError(Status(ErrorCodes::InvalidNamespace, nss.ns() + " is not a valid namespace"),
                     response);
        return;
    }

    if (!NamespaceString::validCollectionName(nss.coll())) {
        toBatchError(
            Status(ErrorCodes::BadValue, str::stream() << "invalid collection name " << nss.coll()),
            response);
        return;
    }

    if (request->sizeWriteOps() == 0u) {
        toBatchError(Status(ErrorCodes::InvalidLength, "no write ops were included in the batch"),
                     response);
        return;
    }

    if (request->sizeWriteOps() > BatchedCommandRequest::kMaxWriteBatchSize) {
        toBatchError(Status(ErrorCodes::InvalidLength,
                            str::stream() << "exceeded maximum write batch size of "
                                          << BatchedCommandRequest::kMaxWriteBatchSize),
                     response);
        return;
    }

    string errMsg;
    if (request->isInsertIndexRequest() && !request->isValidIndexRequest(&errMsg)) {
        toBatchError(Status(ErrorCodes::InvalidOptions, errMsg), response);
        return;
    }
    const string dbName = nss.db().toString();
    // Config writes and shard writes are done differently
    unique_ptr<BatchedCommandRequest> requestWithWriteConcern;
    if (dbName == "config" || dbName == "admin") {
        // w:majority is the only valid write concern for writes to the config servers.
        // We also allow w:1 to come in on a user-initiated write, though we convert it here to
        // w:majority before sending it to the config servers.
        bool rewriteCmdWithWriteConcern = false;
        WriteConcernOptions writeConcern;
        if (request->isWriteConcernSet()) {
            Status status = writeConcern.parse(request->getWriteConcern());
            if (!status.isOK()) {
                toBatchError(status, response);
                return;
            }
            if (!writeConcern.validForConfigServers()) {
                toBatchError(Status(ErrorCodes::InvalidOptions,
                                    "Invalid replication write concern.  Writes to config servers "
                                    "must use w:'majority'"),
                             response);
                return;
            }
            if (writeConcern.wMode == "") {
                invariant(writeConcern.wNumNodes == 1);
                rewriteCmdWithWriteConcern = true;
            }
        } else {
            rewriteCmdWithWriteConcern = true;
        }

        if (rewriteCmdWithWriteConcern) {
            requestWithWriteConcern.reset(new BatchedCommandRequest(request->getBatchType()));
            request->cloneTo(requestWithWriteConcern.get());
            writeConcern.wMode = WriteConcernOptions::kMajority;
            writeConcern.wNumNodes = 0;
            requestWithWriteConcern->setWriteConcern(writeConcern.toBSON());
            request = requestWithWriteConcern.get();
        }

        grid.catalogClient(txn)->writeConfigServerDirect(txn, *request, response);
    } else {
        // TODO: if nss exists go on, else create collections and then run insert
        auto dbStatus = ScopedShardDatabase::getOrCreate(txn, dbName);
        if (!dbStatus.isOK()) {
            Status st = dbStatus.getStatus();
            toBatchError(Status(st.code(),
                                str::stream() << "unable to target"
                                              << (request->isInsertIndexRequest() ? " index" : "")
                                              << " write op for collection "
                                              << request->getTargetingNS()
                                              << causedBy(st)),
                         response);
            return;
        }

        auto conf = uassertStatusOK(grid.catalogCache()->getDatabase(txn, dbName));
        bool isExist;
        auto res = conf->isCollectionExist(txn, dbName + "." + nss.coll());
        if (res.isOK()) {
            isExist = res.getValue();
        } else {
            toBatchError(res.getStatus(), response);
            return;
        }
        if (!isExist) {
            conf->reload(txn);
        }

        // bool isExist = config->isCollectionExist(txn,dbName + "." + nss.coll());
        if (nss.coll() == "system.profile") {
            toBatchError(
                Status(ErrorCodes::BadValue,
                       str::stream() << "cannot write to '" << nss.db() << ".system.profile'"),
                response);
            return;
        }
        // for insert2.js:t.insert({z: 1, $inc: {x: 1}}, 0, true) begin
        {
            if (!isExist) {
                auto res = conf->isCollectionExist(txn, dbName + "." + nss.coll());
                if (res.isOK()) {
                    isExist = res.getValue();
                } else {
                    toBatchError(res.getStatus(), response);
                    return;
                }
            }
            if ((!isExist) &&
                (request->getBatchType() == BatchedCommandRequest::BatchType_Insert)) {
                auto status = checkDocs(request);
                if (!status.isOK()) {
                    toBatchError(status, response, true);
                    return;
                }
            }
        }
        NamespaceString _nss(dbName, nss.coll());
        if (_nss.isSystemDotIndexes()) {
            vector<BSONObj> documents;
            if (origRequest.getBatchType() == BatchedCommandRequest::BatchType_Insert) {
                const BatchedInsertRequest* irq = origRequest.getInsertRequest();
                for (auto doc : irq->getDocuments()) {
                    documents.push_back(doc);
                }
                uassert(ErrorCodes::InvalidLength,
                        "Insert commands to system.indexes are limited to a single insert",
                        documents.size() == 1);
                Command* createIndexCmd = Command::findCommand("createIndexes");
                string errmsg;
                BSONObj spec = documents[0];
                BSONElement nsElement = spec["ns"];
                const NamespaceString new_ns(nsElement.valueStringData());
                uassert(ErrorCodes::InvalidOptions,
                        str::stream() << "Cannot create an index on " << new_ns.ns()
                                      << " with an insert to "
                                      << _nss.ns(),
                        new_ns.db() == _nss.db());
                BSONObjBuilder cmdBuilder;
                cmdBuilder << "createIndexes" << new_ns.coll();
                cmdBuilder << "indexes" << BSON_ARRAY(spec);
                BSONObj cmd = cmdBuilder.done();
                BSONObjBuilder result;
                int queryOptions = 0;
                bool ok = false;
                try {
                    ok = createIndexCmd->run(txn, dbName, cmd, queryOptions, errmsg, result);
                    BSONObj res = result.done();
                    response->setOk(true);
                    response->setN(1);
                    return;
                } catch (const DBException& e) {
                    response->clear();
                    WriteErrorDetail* error = new WriteErrorDetail();
                    error->setIndex(0);
                    error->setErrCode(e.getCode());
                    error->setErrMessage(e.what());
                    response->setN(0);
                    response->setOk(true);
                    response->addToErrDetails(error);
                    return;
                }
                if (!ok) {
                    return;
                }
            }
        }

        int count = 0;
        while (true) {
            if (count++ > 10) {
                return;
            }
            if (!isExist) {
                auto status = isView(txn, dbName, nss);
                bool Exist = false;
                if (status.isOK()) {
                    Exist = status.getValue();
                } else {
                    response->clear();
                    response->setErrCode(status.getStatus().code());
                    response->setErrMessage(status.getStatus().reason());
                    response->setOk(false);
                    return;
                }
                if (Exist) {
                    response->clear();
                    response->setN(0);
                    response->setErrCode(ErrorCodes::CommandNotSupportedOnView);
                    response->setErrMessage("Namespace " + nss.ns() +
                                            " is a view, not a collection");
                    response->setOk(false);
                    return;
                }
                Command* createCmd = Command::findCommand("create");
                int queryOptions = 0;
                string errmsg;
                BSONObjBuilder cmdBob;
                cmdBob.append("create", nss.coll());
                auto createCmdObj = cmdBob.obj();
                BSONObjBuilder result;
                int code = 0;
                bool ok = false;
                try {
                    ok = createCmd->run(txn, dbName, createCmdObj, queryOptions, errmsg, result);
                } catch (const DBException& e) {
                    if (e.getCode() != ErrorCodes::NamespaceExists) {
                        response->clear();
                        response->setErrCode(e.getCode());
                        response->setErrMessage(e.what());
                        response->setOk(false);
                        return;
                    } else {
                        code = ErrorCodes::NamespaceExists;
                        break;
                    }
                    continue;
                }
                if (!ok && (code != ErrorCodes::NamespaceExists)) {
                    continue;
                }
                break;
            }
            break;
        }
        TargeterStats targeterStats;
        {
            ChunkManagerTargeter targeter(request->getTargetingNSS(), &targeterStats);
            Status targetInitStatus = targeter.init(txn);
            if (!targetInitStatus.isOK()) {
                toBatchError(Status(targetInitStatus.code(),
                                    str::stream()
                                        << "unable to target"
                                        << (request->isInsertIndexRequest() ? " index" : "")
                                        << " write op for collection "
                                        << request->getTargetingNS()
                                        << causedBy(targetInitStatus)),
                             response);
                return;
            }
            DBClientMultiCommand dispatcher;
            BatchWriteExec exec(&targeter, &dispatcher);
            exec.executeBatch(txn, *request, response, &_stats);
        }

        if (_autoSplit) {
            splitIfNeeded(txn, request->getNS(), targeterStats);
        }
    }
}

ClusterWriter::ClusterWriter(bool autoSplit, int timeoutMillis)
    : _autoSplit(autoSplit), _timeoutMillis(timeoutMillis) {}

const BatchWriteExecStats& ClusterWriter::getStats() {
    return _stats;
}

}  // namespace mongo

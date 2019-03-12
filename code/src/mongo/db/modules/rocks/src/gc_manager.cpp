#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"
#include <algorithm>
#include <string>

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/client/read_preference.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"


#include "gc_manager.h"

#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_catalog_entry.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/s/commands/cluster_write.h"
#include "mongo/s/write_ops/batched_command_request.h"
#include "mongo/s/write_ops/batched_command_response.h"
#include "mongo/util/log.h"

#include "GlobalConfig.h"

namespace mongo {

const std::string GcNS = "gc.references";

const Seconds kGCRoundDefaultInterval(10);
// Sleep between GC rounds in the case where the last round found some shared resourced which needed to
// be GCed. 
const Seconds kShortGCRoundInterval(2);

const int maxBatchMaxSize = 1000;


//GCManager::GCManager(PartitionedRocksEngine* engine): _engine(path){
//
//   stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
//    _state = kStopped;
//}

GCManager::~GCManager() {
    // The GCManager thread must have been stopped
    invariant(_state == kStopped);
}

bool GCManager::_stopRequested() {
    stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
    return (_state != kRunning);
}

void GCManager::Start() {
    if (!GLOBAL_CONFIG_GET(enableGlobalGC))
        return;
    
    log() << "GC manager is starting";
    stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
    invariant(_state == kStopped);
    _state = kRunning;

    invariant(!_thread.joinable());
    invariant(!_threadOperationContext);
    _thread = stdx::thread([this] { _mainThread(); });
    log() << "GC manager is started";
}

void GCManager::Stop() {
    stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
    if (_state != kRunning)
        return;

    _state = kStopping;
    _condVar.notify_all();

    // Interrupt the GCManager thread if it has been started. We are guaranteed that the operation
    // context of that thread is still alive, because we hold the GC mutex.
    if (_threadOperationContext) {
        stdx::lock_guard<Client> scopedClientLock(*_threadOperationContext->getClient());
        _threadOperationContext->markKilled(ErrorCodes::InterruptedDueToReplStateChange);
    }

    invariant(_state == kStopping);
    invariant(_thread.joinable());
 
    _thread.join();

    _state = kStopped;
    _thread = {};
}

void GCManager::_beginRound(OperationContext* txn) {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    _inGCRound = true;
    _condVar.notify_all();
}

void GCManager::_endRound(OperationContext* txn, Seconds waitTimeout) {
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _inGCRound = false;
        _condVar.notify_all();
    }

    _sleepFor(txn, waitTimeout);
}

void GCManager::setEngine (PartitionedRocksEngine* engine){
    _engine = engine;
}

void GCManager::_sleepFor(OperationContext* txn, Seconds waitTimeout) {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    //stdx::this_thread::sleep_for(waitTimeout.toSystemDuration());
    _condVar.wait_for(lock, waitTimeout.toSystemDuration(), [&] { return _state != kRunning; });
}


Status GCManager::sendSharedResourceRefUpdate (OperationContext* txn, const uint64_t chunkID, std::vector<mongo::TransactionLog::SharedResourceReferenceOperation> unprocessedRecords, uint64_t lastPrcossedLSN){

    BatchedInsertRequest* addRefInsertRequest = new BatchedInsertRequest();
    addRefInsertRequest->setNS(NamespaceString(StringData(GcNS)));
    addRefInsertRequest->setOrdered(true);

    BatchedUpdateRequest* removeRefUpdateRequest = new BatchedUpdateRequest();
    removeRefUpdateRequest->setNS(NamespaceString(GcNS));
    removeRefUpdateRequest->setOrdered(true);

    lastPrcossedLSN = 0;
    for (auto operation : unprocessedRecords){
        if (lastPrcossedLSN < operation.GetLsn()){
            lastPrcossedLSN = operation.GetLsn();
        } 
        
        if (operation.GetType() == mongo::TransactionLog::SharedResourceOperationType::AddReference){ 
            BSONObjBuilder indexDoc;
            indexDoc.append("chunkid", (long long)operation.GetParentChunkId());
            indexDoc.append("resourceid", std::string(operation.GetResource().id));
            addRefInsertRequest->addToDocuments(indexDoc.obj());

            BSONObjBuilder indexDoc2;
            indexDoc2.append("chunkid", (long long)operation.GetChildChunkId());
            indexDoc2.append("resourceid", std::string(operation.GetResource().id));
            addRefInsertRequest->addToDocuments(indexDoc2.obj());
        }
        else if (operation.GetType() == mongo::TransactionLog::SharedResourceOperationType::RemoveReference){
            BatchedUpdateDocument* updateDoc = new BatchedUpdateDocument();
            BSONObjBuilder query;
            //query.append ("ResourceID", operation.resource);
            query.append ("ChunkID", (long long)chunkID);
            updateDoc->setQuery(query.obj());
            updateDoc->setUpdateExpr(BSON("$set" << BSON("deleted" << true)));
            removeRefUpdateRequest->addToUpdates(updateDoc);

        }
    }
    BatchedCommandRequest addRefRequest(addRefInsertRequest);
    MDB_RIF(writeReferenceLog(txn, addRefRequest));
    

    BatchedCommandRequest removeRefRequest(removeRefUpdateRequest);
    MDB_RET(writeReferenceLog(txn, removeRefRequest));
}



// for example, batchWriteObj should be like:
// 
// {insert: "references", documents: [ {plogid: 100, cnt: 1}, {plogid: 200, cnt: 2}, {plogid: 300, cnt: 3} ], ordered: false}
// {update: "references", updates: [ { q: { plogid: { $gt: 10.0 } }, u: { $set: { cnt: 100.0 } }, multi: true, upsert: false } ], ordered: false}
// {update: "references", updates: [ { q: { plogid: 200 }, u: { $set: { cnt: 150.0 } }, multi: true, upsert: true } ], ordered: false}
// {delete: "references", deletes: [ { q: { plogid: { $gt: 0.0 } }, limit: 0.0 } ], ordered: false }

Status GCManager::writeReferenceLog(
    OperationContext* txn,
    const BatchedCommandRequest& request) {
    
    BatchedCommandResponse response;
    ClusterWriter writer(true, 0);
    writer.write(txn, request, &response);

    return response.toStatus();
}

Status GCManager::readReferenceLog(
    OperationContext* txn,
    const BSONObj& query,
    std::vector<BSONObj>& refdocs) {

    NamespaceString nss(GcNS);

    // fisrt we should konw which chunks on this shard server
    Lock::DBLock dbLock(txn->lockState(), nss.db(), MODE_IS);
    Database* db = dbHolder().get(txn, nss.db());
    if (!db)
        return Status(ErrorCodes::NamespaceNotFound,
                      str::stream() << "db is not on this shard " << nss.db());

    DBDirectClient client(txn);
    std::vector<Collection*> collections;
    db->listCollections(collections);
    for (auto collection : collections) {
        if (collection->ns().nsFilteredOutChunkId() != GcNS)
            continue;

        std::unique_ptr<DBClientCursor> c = client.query(collection->ns().ns(), Query(query));
        while (c->more()) {
            refdocs.push_back(c->nextSafe());
        }
    }

    return Status::OK();
}


void GCManager::_mainThread() {
    Client::initThread("GC Manager");
    auto txn = cc().makeOperationContext();

    log() << "GC manager is starting3";

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        _threadOperationContext = txn.get();
    }

    while (!_stopRequested()) {
        _beginRound(txn.get());
        log() << "GC manager is starting4";
        try {
            std::shared_ptr<mongo::TransactionLog::SharedResourceModule> provider;
            std::vector<mongo::TransactionLog::SharedResourceReferenceOperation> unprocessedRecords;
            uint64_t chunkID;
            bool toGC = getEngine()->GetSharedResourcesOperations(chunkID, provider, unprocessedRecords, maxBatchMaxSize);
            if (toGC){
                uint64_t lastProcessLSN;
                if (sendSharedResourceRefUpdate(_threadOperationContext, chunkID, unprocessedRecords, lastProcessLSN).isOK()){
                    provider->SetLastProcessedOperationLSN(lastProcessLSN);
                }
            }

            _endRound(txn.get(),
                      toGC ? kShortGCRoundInterval: kGCRoundDefaultInterval);
        } catch (const std::exception& e) {
            log() << "caught exception while doing GC : " << e.what();

            // Just to match the opening statement if in log level 1
            LOG(1) << "*** End of GC round";

            //TODO alarm
            
            // Sleep a fair amount before retrying because of the error
            _endRound(txn.get(), kGCRoundDefaultInterval);
        }
    }

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        _threadOperationContext = nullptr;
    }


}
}  // namespace mongo


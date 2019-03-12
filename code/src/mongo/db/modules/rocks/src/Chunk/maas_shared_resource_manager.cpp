#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/util/log.h"
#include "maas_shared_resource_manager.h"
#include "../rocks_engine.h"

namespace mongo
{

namespace TransactionLog
{

MaaSSharedResourceManager::MaaSSharedResourceManager(const std::string& dbPath)
{
    _dbPath = dbPath;
    _dbPathWithOutChunkId.assign(dbPath, 0, dbPath.rfind("/"));
    _chunkId = MaaSSharedResourceModule::ParseChunkId(dbPath);
    index_LOG(0) << "[transLog] MaaSSharedResourceManager::MaaSSharedResourceManager() dbPath: " << dbPath 
        << "; _dbPathWithOutChunkId: " << _dbPathWithOutChunkId << "; ChunkId: " << _chunkId;
}

rocksdb::Status MaaSSharedResourceManager::WriteCheckpoint()
{
    //nothing todo
    return rocksdb::Status::OK();
}

void MaaSSharedResourceManager::AddSharedResources(const SharedResourceIds& resources)
{
    SharedResourceManager::AddSharedResources(resources);
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    for(auto resource : resources) {
        invariant(resource.toString().find("/") != std::string::npos);
        std::string id = MaaSSharedResourceModule::ParseSSTFileName(resource);
        index_LOG(0)  << "[transLog] MaaSSharedResourceManager::AddSharedResources(): resource " << resource.toString()
            << "; id: " << id;
        _sharedResource[id] = resource;
    }
}

rocksdb::Status MaaSSharedResourceManager::OnResourcesDeletion(std::vector<SharedResourceRemoveDescription>& resources)
{
    //todo  Global GC
    //RES_RIF(SharedResourceManager::OnResourcesDeletion(resources));

      index_LOG(0) << "[transLog] MaaSSharedResourceManager::OnResourcesDeletion()";
    for (auto resource : _sharedResource) {
        index_LOG(0) << "[transLog] MaaSSharedResourceManager::OnResourcesDeletion()-> all sharedid: " << resource.first
            << "; : " << resource.second.toString();
    }

    stdx::unique_lock<stdx::mutex> lock(_mutex);
    for(auto& resource : resources)
    {
        std::string id = MaaSSharedResourceModule::ParseSSTFileName(resource.id);
        if (_sharedResource.end() == _sharedResource.find(id)) {
            resource.shared_flag = false;
            resource.remove_result = 0;
        } else {
            resource.shared_flag = true;
            resource.remove_result = 0;
        }
        index_log() << "[transLog] MaaSSharedResourceManager::OnResourcesDeletion(): resource: " << resource.id.toString()
            << "; id: " << id << "; shard: " << resource.shared_flag; 
    }

    return rocksdb::Status::OK();
}

rocksdb::Status MaaSSharedResourceManager::ReplayLogRecord(const LogRecord& record, bool& needMoreRecords) 
{
     RES_RIF(SharedResourceManager::ReplayLogRecord(record, needMoreRecords));

     SharedResourceFilterOperation* oper = SharedResourceFilterOperation::DecodeFrom(record);
     const SharedResourceOperationType type = oper->GetType();

     std::string id = MaaSSharedResourceModule::ParseSSTFileName(oper->GetResource());
     log() << "[transLog] MaaSSharedResourceManager::ReplayLogRecord(): resource " << oper->GetResource().toString()
         << "; id: " << id << "; type: " << (int)type;

     stdx::unique_lock<stdx::mutex> lock(_mutex);
     if (type == SharedResourceOperationType::AddFilter) {
         _sharedResource[id] = oper->GetResource();
     }else {
          invariant(type == SharedResourceOperationType::RemoveFilter);
          //_sharedResource.erase(id);
     }

     return rocksdb::Status::OK();
}

bool MaaSSharedResourceManager::CheckSharedResource(const SharedResourceId &id, std::string &path)  
{
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    for (auto resource : _sharedResource) {
        index_LOG(1) << "[transLog] MaaSSharedResourceManager::CheckSharedResource()-> all sharedid: " << resource.first
            << "; : " << resource.second.toString();
    }

    auto it = _sharedResource.find(id.toString());
    if (it != _sharedResource.end()) {
        path = _dbPathWithOutChunkId + "/" + it->second.toString();
        index_LOG(0)  << "[transLog] MaaSSharedResourceManager::CheckSharedResource()-> shared id: " << id.toString() << "; path: " << path;
        return true;
    } else {
        path = _dbPath + ROCKSDB_PATH + "/" + id.toString();
        index_LOG(0)  << "[transLog] MaaSSharedResourceManager::CheckSharedResource()-> no-shared id: " << id.toString() << "; path: " << path;
        return false;
    }
}

bool MaaSSharedResourceManager::getSharedResourcePath(const SharedResourceId &id, std::string &path)  
{
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    auto it = _sharedResource.find(id.toString());
    if (it != _sharedResource.end()) {
        path = it->second.toString();
        index_LOG(1)  << "[transLog] MaaSSharedResourceManager::getSharedResourcePath()-> id: " << id.toString() << "; path: " << path;
        return true;
    } else {
        index_LOG(1)  << "[transLog] MaaSSharedResourceManager::getSharedResourcePath()-> id 2: " << id.toString();
        return false;
    }
}

rocksdb::Status MaaSSharedResourceModule::Init(rocksdb::Env* env,
                                               const std::string& db_path,
                                               ChunkMetadata* chunk_meta_data)
{
    _dbPath = db_path;
    _chunkId = ParseChunkId(db_path);
    log() << "[transLog] MaaSSharedResourceModule::Init(): _dbPath: " << _dbPath << "; _chunkId: " << _chunkId;
    return SharedResourceModule::Init(env, db_path, chunk_meta_data);
}

int MaaSSharedResourceModule::RegisterSharedResource(std::vector<SharedResourceId>& resource_id_list)
{
    auto resources = resource_id_list;
    for (auto& resource : resources) {
        index_LOG(1)  << "[transLog] MaaSSharedResourceModule::RegisterSharedResource()-> resource: " << resource;
        addRootChunkId(resource);
    }

    for (auto resource : resources) {
        index_LOG(0)  << "[transLog] MaaSSharedResourceModule::RegisterSharedResource()-> resource addRootChunkId: " << resource;
    }

    return SharedResourceModule::RegisterSharedResource(resources);
}

int MaaSSharedResourceModule::RemoveSharedResource(std::vector<SharedResourceRemoveDescription>& list)
{
    for (auto& resource : list) {
        index_LOG(0) << "[transLog] MaaSSharedResourceModule::RemoveSharedResource()-> resource: " << resource.id.toString();
        addRootChunkId(resource.id);
        index_LOG(0)  << "[transLog] MaaSSharedResourceModule::RemoveSharedResource()-> resource addRootChunkId: " << resource.id.toString();
    }

    return SharedResourceModule::RemoveSharedResource(list);
}

bool MaaSSharedResourceModule::CheckSharedResource(const SharedResourceId &id, std::string &path) 
{
    return filter_manager_->CheckSharedResource(id, path);
}

void MaaSSharedResourceModule::addRootChunkId(SharedResourceId& resource)
{
    index_LOG(1)  << "[transLog] MaaSSharedResourceModule::addRootChunkId: resource: " << resource.toString();
    std::string path;
    if (filter_manager_->getSharedResourcePath(resource, path)) {
        resource = path;
    } else {
        resource = _chunkId + ROCKSDB_PATH + "/" + resource.toString();
    }
    index_LOG(1)  << "[transLog] MaaSSharedResourceModule::addRootChunkId: resource: " << resource.toString()
        <<" ; path: " << path;
}

void MaaSSharedResourceModule::delRootChunkId(SharedResourceId& resource)
{
    index_LOG(1)  << "[transLog] MaaSSharedResourceModule::delRootChunkId()-> resourece: " << resource.toString();
    std::string id = ParseSSTFileName(resource);
    resource = id;
    index_LOG(1) << "[transLog] MaaSSharedResourceModule::delRootChunkId()-> resourece: " << resource.toString()
        << "; id: " << id;
}

void MaaSSharedResourceModule::GetUnprocessedOperations(uint32_t maxBatchMaxSize,
                                                        std::vector<SharedResourceReferenceOperation>& unprocessedRecords)
{
    SharedResourceModule::GetUnprocessedOperations(maxBatchMaxSize, unprocessedRecords);
    for(auto& record : unprocessedRecords) {
         SharedResourceId resource = record.resource_;
         index_LOG(1) << "[transLog] MaaSSharedResourceModule::GetUnprocessedOperations()-> resource: " << resource.toString();
         delRootChunkId(resource);
         record.resource_ = resource;
         index_LOG(1) << "[transLog]MaaSSharedResourceModule::GetUnprocessedOperations()-> delRootChunkId resource: " << resource.toString();;
    }

    for(auto record : unprocessedRecords) {
        index_LOG(0) << "[transLog] MaaSSharedResourceModule::GetUnprocessedOperations()-> delRootChunkId resource: " << record.resource_.toString();
    }
}

std::string MaaSSharedResourceModule::ParseSSTFileName(const SharedResourceId& resource) 
{
    std::string id = ParseChunkId(resource.toString());
    index_LOG(1)  << "[transLog] MaaSSharedResourceModule::ParseSSTFileName()-> id: " << id << "; resource: " << resource.toString();
    return id;
}

std::string MaaSSharedResourceModule::ParseChunkId(const std::string& path) 
{
    std::string chunkid;
    chunkid.assign(path, path.rfind("/") + 1, path.size() - path.rfind("/") -1);
    index_LOG(1)  << "[transLog] MaaSSharedResourceModule::ParseChunkId()-> chunkid: " << chunkid << "; path: " << path;
    return chunkid;
}

}   //  namespace TransactionLog

}   //  namespace mongo
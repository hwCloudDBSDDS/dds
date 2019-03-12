#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "maas_shared_resource_manager.h"
#include "../rocks_engine.h"
#include "mongo/com/include/error_code.h"
#include "mongo/util/log.h"

namespace mongo {

    namespace TransactionLog {

#define transLog_log() index_log() << "[transLog][" << getChunkId() << "] "

#define transLog_err() index_err() << "[transLog][" << getChunkId() << "] "

#define transLog_LOG(x) index_LOG(x) << "[transLog][" << getChunkId() << "] "

        MaaSSharedResourceManager::MaaSSharedResourceManager(SharedResourceModule* srm_module,
                                                             const std::string& dbPath)
            : SharedResourceManager(srm_module) {
            _dbPath = dbPath;
            _dbPathWithOutChunkId.assign(dbPath, 0, dbPath.rfind(SEPARATOR));
            _chunkId = MaaSSharedResourceModule::ParseChunkId(dbPath);
            transLog_log() << "init MaaSSharedResourceManager dbPath: " << dbPath
                           << "; _dbPathWithOutChunkId: " << _dbPathWithOutChunkId
                           << "; ChunkId: " << _chunkId;
        }

        void MaaSSharedResourceManager::AddSharedResources(const SharedResourceIds& resources) {
            SharedResourceManager::AddSharedResources(resources);
            stdx::unique_lock<stdx::mutex> lock(_mutex);
            for (auto resource : resources) {
                invariant(resource.toString().find(SEPARATOR) != std::string::npos);
                std::string id = MaaSSharedResourceModule::ParseSSTFileName(resource);
                transLog_LOG(1) << "AddSharedResources resource " << resource.toString()
                                << "; id: " << id;
                _sharedResource[id] = resource;
            }
        }

        rocksdb::Status MaaSSharedResourceManager::OnResourcesDeletion(
            std::vector<SharedResourceRemoveDescription>& resources) {
            transLog_LOG(2) << "OnResourcesDeletion sharedResource num: " << _sharedResource.size();
            for (auto resource : _sharedResource) {
                transLog_LOG(2) << "OnResourcesDeletion()-> all sharedid: " << resource.first
                                << "; : " << resource.second.toString();
            }

            transLog_LOG(2) << "OnResourcesDeletion resource num: " << resources.size();
            for (auto resource : resources) {
                transLog_LOG(2) << "OnResourcesDeletion resource: " << resource.id;
            }

            RES_RIF(SharedResourceManager::OnResourcesDeletion(resources));

            stdx::unique_lock<stdx::mutex> lock(_mutex);
            for (auto& resource : resources) {
                std::string id = MaaSSharedResourceModule::ParseSSTFileName(resource.id);
                if (_sharedResource.end() == _sharedResource.find(id)) {
                    transLog_LOG(0) << "OnResourcesDeletion not shared id: "
                                    << resource.id.toString();
                    if (resource.shared_flag) {
                        index_err() << "not shared id : " << resource.id.toString();
                    }
                    resource.shared_flag = false;
                    resource.remove_result = 0;
                } else {
                    transLog_LOG(0) << "OnResourcesDeletion shared id: " << resource.id.toString();
                    if (!resource.shared_flag) {
                        index_err() << "shared id : " << resource.id.toString();
                    }
                    resource.shared_flag = true;
                    resource.remove_result = 0;
                }
            }

            return rocksdb::Status::OK();
        }

        rocksdb::Status MaaSSharedResourceManager::ReplayLogRecord(const LogRecord& record,
                                                                   bool& needMoreRecords) {
            RES_RIF(SharedResourceManager::ReplayLogRecord(record, needMoreRecords));

            SharedResourceFilterOperation* oper = SharedResourceFilterOperation::DecodeFrom(record);
            const SharedResourceOperationType type = oper->GetType();
            std::string id = MaaSSharedResourceModule::ParseSSTFileName(oper->GetResource());

            {
                stdx::unique_lock<stdx::mutex> lock(_mutex);
                if (type == SharedResourceOperationType::AddFilter) {
                    transLog_LOG(0) << "ReplayLogRecord: resource "
                                    << oper->GetResource().toString() << "; id: " << id
                                    << "; type: " << (int)type;
                    _sharedResource[id] = oper->GetResource();
                } else {
                    invariant(type == SharedResourceOperationType::RemoveFilter);
                }
            }

            delete oper;
            oper = nullptr;
            return rocksdb::Status::OK();
        }

        bool MaaSSharedResourceManager::CheckSharedResource(const SharedResourceId& id,
                                                            std::string& path) {
            stdx::unique_lock<stdx::mutex> lock(_mutex);
            for (auto resource : _sharedResource) {
                transLog_LOG(1) << "CheckSharedResource()-> all sharedid: " << resource.first
                                << "; : " << resource.second.toString();
            }

            auto it = _sharedResource.find(id.toString());
            if (it != _sharedResource.end()) {
                path = _dbPathWithOutChunkId + SEPARATOR + it->second.toString();
                transLog_LOG(0) << "CheckSharedResource shared id: " << id.toString()
                                << "; path: " << path;
                return true;
            } else {
                path = _dbPath + ROCKSDB_PATH + SEPARATOR + id.toString();
                transLog_LOG(1) << "CheckSharedResource no-shared id: " << id.toString()
                                << "; path: " << path;
                return false;
            }
        }

        bool MaaSSharedResourceManager::getSharedResourcePath(const SharedResourceId& id,
                                                              std::string& path) {
            stdx::unique_lock<stdx::mutex> lock(_mutex);
            auto it = _sharedResource.find(id.toString());
            if (it != _sharedResource.end()) {
                path = it->second.toString();
                transLog_LOG(1) << "getSharedResourcePath no shard id: " << id.toString()
                                << "; path: " << path;
                return true;
            } else {
                transLog_LOG(1) << "getSharedResourcePath shard id: " << id.toString();
                return false;
            }
        }

        rocksdb::Status MaaSSharedResourceModule::Init(rocksdb::Env* env,
                                                       const std::string& db_path,
                                                       ChunkMetadata* chunk_meta_data) {
            _dbPath = db_path;
            _chunkId = ParseChunkId(db_path);
            transLog_LOG(0) << "[transLog] MaaSSharedResourceModule::Init(): _dbPath: " << _dbPath
                            << "; _chunkId: " << _chunkId;
            return SharedResourceModule::Init(env, db_path, chunk_meta_data);
        }

        int MaaSSharedResourceModule::RegisterSharedResource(
            std::vector<SharedResourceId>& resource_id_list) {
            auto resources = resource_id_list;
            for (auto& resource : resources) {
                addRootChunkId(resource);
                transLog_LOG(0) << "RegisterSharedResource resource: " << resource;
            }

            int ret = SharedResourceModule::RegisterSharedResource(resources);
            if (RET_OK == ret) {
                filter_manager_->AddSharedResources(resources);
            } else {
                transLog_err() << "RegisterSharedResource faild ret: " << ret;
            }
            return ret;
        }

        int MaaSSharedResourceModule::RemoveSharedResource(
            std::vector<SharedResourceRemoveDescription>& list) {
            for (auto it = list.begin(); it != list.end(); it++) {
                it->shared_flag = false;
                it->remove_result = -1;
            }

            for (auto& resource : list) {
                transLog_LOG(0) << "RemoveSharedResource resource: " << resource.id.toString();
                addRootChunkId(resource.id);
                transLog_LOG(1) << "RemoveSharedResource()-> resource addRootChunkId: "
                                << resource.id.toString();
            }

            return SharedResourceModule::RemoveSharedResource(list);
        }

        bool MaaSSharedResourceModule::CheckSharedResource(const SharedResourceId& id,
                                                           std::string& path) {
            return filter_manager_->CheckSharedResource(id, path);
        }

        void MaaSSharedResourceModule::addRootChunkId(SharedResourceId& resource) const {
            transLog_LOG(1) << "addRootChunkId: resource: " << resource.toString();
            std::string path;
            if (filter_manager_->getSharedResourcePath(resource, path)) {
                resource = path;
            } else {
                resource = _chunkId + ROCKSDB_PATH + SEPARATOR + resource.toString();
            }
            transLog_LOG(1) << "addRootChunkId resource: " << resource.toString()
                            << " ; path: " << path;
        }

        void MaaSSharedResourceModule::delRootChunkId(SharedResourceId& resource) const {
            transLog_LOG(1) << "delRootChunkId() resourece: " << resource.toString();
            std::string id = ParseSSTFileName(resource);
            resource = id;
            transLog_LOG(1) << "delRootChunkId() resourece: " << resource.toString()
                            << "; id: " << id;
        }

        std::string MaaSSharedResourceModule::ParseSSTFileName(const SharedResourceId& resource) {
            std::string id = ParseChunkId(resource.toString());
            index_LOG(1) << "ParseSSTFileName id: " << id << "; resource: " << resource.toString();
            return id;
        }

        std::string MaaSSharedResourceModule::ParseChunkId(const std::string& path) {
            std::string chunkid;
            chunkid.assign(path, path.rfind(SEPARATOR) + 1,
                           path.size() - path.rfind(SEPARATOR) - 1);
            index_LOG(1) << "ParseChunkId chunkid: " << chunkid << "; path: " << path;
            return chunkid;
        }

    }  //  namespace TransactionLog

}  //  namespace mongo


#pragma once

#include <map>
#include <string>
#include "i_shared_resource_manager.h"

namespace mongo {

    namespace TransactionLog {

        const std::string SEPARATOR = "/";

        class MaaSSharedResourceManager : public SharedResourceManager {
        public:
            MaaSSharedResourceManager(SharedResourceModule* srm_module, const std::string& dbPath);

            virtual void AddSharedResources(const SharedResourceIds& resources) override;

            virtual rocksdb::Status OnResourcesDeletion(
                std::vector<SharedResourceRemoveDescription>& list) override;

            virtual rocksdb::Status ReplayLogRecord(const LogRecord& record,
                                                    bool& needMoreRecords) override;

            virtual bool CheckSharedResource(const SharedResourceId& id,
                                             std::string& path) override;
            virtual bool getSharedResourcePath(const SharedResourceId& id,
                                               std::string& path) override;

        private:
            std::string getChunkId(void) const { return _chunkId; }

        private:
            std::map<std::string, SharedResourceId> _sharedResource;
            stdx::mutex _mutex;
            std::string _dbPath;
            std::string _dbPathWithOutChunkId;
            std::string _chunkId;
        };

        class MaaSSharedResourceModule : public SharedResourceModule {
        public:
            virtual rocksdb::Status Init(rocksdb::Env* env, const std::string& db_path,
                                         ChunkMetadata* chunk_meta_data) override;

            // add Root ChunkId of SharedResourceId
            virtual int RegisterSharedResource(
                std::vector<SharedResourceId>& resource_id_list) override;
            virtual int RemoveSharedResource(
                std::vector<SharedResourceRemoveDescription>& list) override;
            virtual bool CheckSharedResource(const SharedResourceId& id,
                                             std::string& path) override;

            static std::string ParseSSTFileName(const SharedResourceId& resource);
            static std::string ParseChunkId(const std::string& path);

        private:
            void addRootChunkId(SharedResourceId& resource) const;
            void delRootChunkId(SharedResourceId& resource) const;
            std::string getChunkId(void) const { return _chunkId; }

        private:
            std::string _dbPath;
            std::string _chunkId;
        };

        class MaasSharedResourceOperationLogRecordProvider
            : public SharedResourceOperationLogRecordProvider {
        public:
            // rollback split (delete sharedFile)
            // virtual rocksdb::Status RollbackSplitOperations() override;
        };

    }  //  namespace TransactionLog

}  //  namespace mongo

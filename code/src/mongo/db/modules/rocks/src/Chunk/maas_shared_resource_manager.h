
#pragma once

#include <map>
#include <string>
#include "i_shared_resource_manager.h"

namespace mongo
{

namespace TransactionLog
{

class MaaSSharedResourceManager : public SharedResourceManager
{
public:
    MaaSSharedResourceManager(const std::string& dbPath);

    virtual rocksdb::Status WriteCheckpoint() override;

    virtual void AddSharedResources(const SharedResourceIds& resources) override;
    virtual rocksdb::Status OnResourcesDeletion(std::vector<SharedResourceRemoveDescription>& list)override;

    virtual rocksdb::Status ReplayLogRecord(const LogRecord& record, bool& needMoreRecords) override;

    virtual bool CheckSharedResource(const SharedResourceId &id, std::string &path) override;
    virtual bool getSharedResourcePath(const SharedResourceId &id, std::string &path) override;

private:

    std::map< std::string, SharedResourceId> _sharedResource;
    stdx::mutex _mutex;
    std::string _dbPath;
    std::string _dbPathWithOutChunkId;
    std::string _chunkId;
};

class MaaSSharedResourceModule : public SharedResourceModule {
public:

    virtual rocksdb::Status Init(rocksdb::Env* env,
                                 const std::string& db_path,
                                 ChunkMetadata* chunk_meta_data)override;

    //add Root ChunkId of SharedResourceId
    virtual int RegisterSharedResource(std::vector<SharedResourceId>& resource_id_list) override;
    virtual int RemoveSharedResource(std::vector<SharedResourceRemoveDescription>& list) override;
    virtual bool CheckSharedResource(const SharedResourceId &id, std::string &path) override;

    // del root ChunkId of SharedResourceId
    virtual void GetUnprocessedOperations(uint32_t maxBatchMaxSize,
        std::vector<SharedResourceReferenceOperation>& unprocessedRecords)override;

    static std::string ParseSSTFileName(const SharedResourceId& resource);
    static std::string ParseChunkId(const std::string& path);

private:
    void addRootChunkId(SharedResourceId& resource);
    void delRootChunkId(SharedResourceId& resource);

private:
    std::string _dbPath;
    std::string _chunkId;
};


}   //  namespace TransactionLog

}   //  namespace mongo

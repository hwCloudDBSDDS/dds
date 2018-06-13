#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "mongo/platform/basic.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/connection_string.h"
#include "mongo/client/read_preference.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_connection.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/config.h"
#include "mongo/s/grid.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/s/catalog/catalog_extend/root_folder_manager.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/util/log.h"

namespace mongo
{

    const ReadPreferenceSetting kConfigReadSelector(ReadPreference::Nearest, TagSet {});
    const ReadPreferenceSetting kConfigPrimarySelector(ReadPreference::PrimaryOnly);
    const WriteConcernOptions kNoWaitWriteConcern(1, WriteConcernOptions::SyncMode::UNSET, Seconds(0));

    RootFolderManager::RootFolderManager()
    {
        // parameters for plog allocation
        //_shardServerRootPlogMap = new std::map<std::string, RootPlogItem*>();
    }
    RootFolderManager::~RootFolderManager()
    {

    }

     Status RootFolderManager::createChunkRootFolder(
        OperationContext* txn,
        const std::string& chunkId,
        std::string& chunkRootFolder) {

        invariant(chunkRootFolder.empty());

        log() << "Create a new root folder for chunk " << chunkId;
        chunkRootFolder.append(storageGlobalParams.dbpath + '/');
        chunkRootFolder.append(chunkId);
        //char cmd[1024];
        //sprintf(cmd,"mkdir -p %s", chunkRootFolder.c_str());
        //system(cmd);
        
        return Status::OK();
    }

    Status RootFolderManager::updateChunkRootFolder(
        OperationContext* txn,
        const std::string& chunkId,
        std::string& chunkRootFolder) {

        return Status::OK();
    }

    Status RootFolderManager::deleteChunkRootFolder(
        OperationContext* txn,
        const std::string& chunkId) {

        log() << "Delete root folder for chunk " << chunkId;
        // get root folder from chunk view
 
        return Status::OK();
    }
    Status RootFolderManager::deleteChunkRootFolder(
        const std::string& chunkRootFolder) {

        log() << "Delete chunkRootFolder:" << chunkRootFolder;

        // check chunkRootFolder isEmpty()
        if(chunkRootFolder.size() == 0) {
            log() << "chunkRootFolder is NULL";
            return Status::OK();
        }

         log() << "Delete root folder for chunkRootFolder: " << chunkRootFolder;
         //char cmd[1024];
         //sprintf(cmd,"rm -rf %s", chunkRootFolder.c_str());
         //system(cmd);

         return Status::OK();
    }


}  // namespace mongo

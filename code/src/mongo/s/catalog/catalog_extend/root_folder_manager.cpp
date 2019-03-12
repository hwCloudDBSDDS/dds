#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/connection_string.h"
#include "mongo/client/read_preference.h"
#include "mongo/platform/basic.h"
#include "mongo/s/catalog/catalog_extend/root_folder_manager.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_connection.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/config.h"
#include "mongo/s/grid.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/util_extend/GlobalConfig.h"

namespace mongo {

const ReadPreferenceSetting kConfigReadSelector(ReadPreference::Nearest, TagSet{});
const ReadPreferenceSetting kConfigPrimarySelector(ReadPreference::PrimaryOnly);
const WriteConcernOptions kNoWaitWriteConcern(1, WriteConcernOptions::SyncMode::UNSET, Seconds(0));

RootFolderManager::RootFolderManager() {
    // parameters for plog allocation
    //_shardServerRootPlogMap = new std::map<std::string, RootPlogItem*>();
}
RootFolderManager::~RootFolderManager() {}

std::string& RootFolderManager::escapeCharacter(std::string& nss) {
    std::string specialCharacters;
    specialCharacters = "*{[: ?";

    char ch[] = {'\\'};
    size_t pos = nss.find_first_of(specialCharacters);
    while (pos != std::string::npos) {
        nss.insert(pos, ch);
        pos = nss.find_first_of(specialCharacters, pos + 2);
    }

    return nss;
}

Status RootFolderManager::createChunkRootFolder(OperationContext* txn,
                                                const std::string& ident,
                                                const std::string& chunkId,
                                                std::string& chunkRootFolder) {

    invariant(chunkRootFolder.empty());
    invariant(!ident.empty());
    invariant(!chunkId.empty());
    chunkRootFolder = getDataPath() + "/" + DATA_PATH + "/" + ident + "/" + chunkId;

    index_LOG(0) << "Create a new root folder for chunk " << chunkId << "; ident: " << ident
                 << "; storageGlobalParams.dbpath: " << storageGlobalParams.dbpath
                 << "; datapath: " << getDataPath() << "; chunkRootFolder: " << chunkRootFolder;

    return Status::OK();
}

}  // namespace mongo

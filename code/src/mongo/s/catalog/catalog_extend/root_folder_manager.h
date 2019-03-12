#pragma once

#include <boost/optional.hpp>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "mongo/base/disallow_copying.h"
#include "mongo/client/connection_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/s/catalog/catalog_extend/root_folder.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/shard_id.h"
#include "mongo/util/net/hostandport.h"

namespace mongo {
class RootFolderManager : public ChunkRootFolder {
public:
    RootFolderManager();

    virtual ~RootFolderManager();

    /*
     * Implementation of unified interface for chunk root folder
     */
    Status createChunkRootFolder(OperationContext* txn,
                                 const std::string& ident,
                                 const std::string& chunkId,
                                 std::string& chunkRootFolder);

private:
    std::string& escapeCharacter(std::string& nss);
};
}  // namespace mongo

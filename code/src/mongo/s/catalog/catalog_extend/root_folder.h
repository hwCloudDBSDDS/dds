#pragma once

#include <boost/optional.hpp>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "mongo/db/operation_context.h"

namespace mongo {

// Presents an interface for manipulating the root folder of chunk, regardless of
// whether the underlying storage is plog or distributed file system.
class ChunkRootFolder {
public:
    ChunkRootFolder(){};

    virtual ~ChunkRootFolder() = default;

    // Creates a root folder for chunk which is specified by chunkId, return error
    // if there already exists a root folder.
    virtual Status createChunkRootFolder(OperationContext* txn,
                                         const std::string& ident,
                                         const std::string& chunkId,
                                         std::string& chunkRootFolder) = 0;

};

}  // namespace mongo

#pragma once

#include <boost/optional.hpp>
#include <string>
#include <vector>
#include <map>
#include <mutex>

#include "mongo/db/operation_context.h"

namespace mongo
{

// Presents an interface for manipulating the root folder of chunk, regardless of
// whether the underlying storage is plog or distributed file system.
class ChunkRootFolder {
public:
    ChunkRootFolder() {};

    virtual ~ChunkRootFolder() = default;

    // Creates a root folder for chunk which is specified by chunkId, return error
    // if there already exists a root folder.
    virtual Status createChunkRootFolder(
        OperationContext* txn,
        const std::string& chunkId,
        std::string& chunkRootFolder) = 0;

    // Currently, this interface is only used for plog storage as the folder will not
    // get changed once it is created for distributed file system storage.
    virtual Status updateChunkRootFolder(
        OperationContext* txn,
        const std::string& chunkId,
        std::string& chunkRootFolder) = 0;

    // Deletes a root folder for chunk which is specified by chunkId, return error
    // if no folder is found.
    virtual Status deleteChunkRootFolder(
        OperationContext* txn,
        const std::string& chunkId) = 0;

    virtual Status deleteChunkRootFolder(
        const std::string& chunkRootFolder) = 0;
};

}  // namespace mongo

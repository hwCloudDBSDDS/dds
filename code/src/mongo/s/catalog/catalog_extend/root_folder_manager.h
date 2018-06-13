#pragma once

#include <boost/optional.hpp>
#include <string>
#include <vector>
#include <map>
#include <mutex>

#include "mongo/base/disallow_copying.h"
#include "mongo/client/connection_string.h"
#include "mongo/s/shard_id.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/catalog_extend/root_folder.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/db/operation_context.h"

namespace mongo
{

#define UINT8_MAX_I           0xFF
#define UINT16_MAX_I          0xFFFFU
#define UINT32_MAX_I          0xFFFFFFFFU
#define UINT64_MAX_I          0xFFFFFFFFFFFFFFFFULL


    struct BSONArray;
    class BSONObj;
    class Status;
    class ConnectionString;
    class ShardId;
    class ShardType;


    const uint32_t      PLOG_MAX_PATH_LEN           = 512;
    const uint32_t      FALSE                       = 0;
    const uint32_t      TRUE                        = 1;
    const uint32_t      RET_OK                      = 0;
    const uint32_t      RET_ERROR                   = 1;
    const uint32_t      __GB__                      = 1024 * 1024 * 1024;
    const uint32_t      __MB__                      = 1024 * 1024;
    const uint32_t      __KB__                      = 1024;
    const uint32_t      PLOG_BLOCK_SIZE             = 4 * __KB__;
    const uint32_t      PLOG_CREATE_TRY_MAX_TIMES   = 100000;
    const uint32_t      MAX_PATH_LEN                = 256;
    const uint32_t      MAX_FILE_NAME_LEN           = 25;
    const uint32_t      MAX_COMMAND_LEN             = 1 * __KB__;



    class RootFolderManager : public ChunkRootFolder {
    public:
        RootFolderManager();

        virtual ~RootFolderManager();
#if 0
        Status applyRootPlog(
            OperationContext* txn,
            const ShardId& shardId,
            const ConnectionString& shardConnectionString,
            RootPlogList& rootPlogList);
#endif
        /*
         * Implementation of unified interface for chunk root folder
         */
        Status createChunkRootFolder(
            OperationContext* txn,
            const std::string& chunkId,
            std::string& chunkRootFolder);

        Status updateChunkRootFolder(
            OperationContext* txn,
            const std::string& chunkId,
            std::string& chunkRootFolder);

        Status deleteChunkRootFolder(
            OperationContext* txn,
            const std::string& chunkId);

        Status deleteChunkRootFolder(
            const std::string& chunkRootFolder);

    private:
#if 0		
        Status _plogAlloc(
            const trace_id_t   trace_id,
            const PlogCreateOptions   options,
            uint32&         chunk_size_out,
            uint32&         block_size,
            PLogId&         plog_id);

        uint32 _getPlogChunkSize8Type(chunk_size_e chunkSizeType);

        Status _getRootPlogListFromShardView(
            OperationContext* txn,
            const ShardId& shardId,
            ShardType& shardType,
            RootPlogList& rootPlogList,
            bool& shardExisting);

        Status _getRootFolderFromChunkView(
            OperationContext* txn,
            const std::string& chunkId,
            ChunkType& chunkType,
            std::string& chunkRootFolder);

        // parameters for plog allocation
        trace_id_t _traceID;
        PlogCreateOptions _options;
#endif		
    };
}  // namespace mongo

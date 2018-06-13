/******************************************************************************
                Copyright 1999 - 2017, Huawei Tech. Co., Ltd.
                           ALL RIGHTS RESERVED
  File Name     : dump_chunk_stats_command.cpp
  Version       : Initial Draft
  Author        : 
  Created       : 2017/9/22
  Description   : dump chunk stats cmd
  History       :
  1.Date        : 2017/9/22
    Author      : 
    Modification: Created file

******************************************************************************/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdarg.h>
#include <sys/time.h>
#include <fcntl.h>
#include <map>

#include "mongo/platform/basic.h"

#include "mongo/client/remote_command_targeter.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/range_deleter_service.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/chunk_move_write_concern_options.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/s/catalog/dist_lock_manager.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/db/storage/kv/kv_storage_engine.h"
//#include "mongo/db/modules/rocks/src/Chunk/PartitionedRocksEngine.h"
//#include "mongo/db/modules/rocks/src/GlobalConfig.h"
//#include "mongo/db/modules/rocks/src/Chunk/i_shared_resource_manager.h"


namespace mongo {

using std::string;

const char kChunkId[] = "chunkId";
const char statFilePath[] = "/var/log/index_layer/shard/27021/mongo/chunkstat.log";

void log_stat(bool isPrintHeader, const char *fmt,...) {
    va_list ap;
    char headBuf[100];
    char logName[100];
    FILE *fp = nullptr;

    struct timeval now_tv;
    gettimeofday(&now_tv, nullptr);
    const time_t seconds = now_tv.tv_sec;

    struct tm t;
    localtime_r(&seconds, &t);

    sprintf(headBuf,"[%04d/%02d/%02d-%02d:%02d:%02d.%06d] ",
                t.tm_year + 1900,
                t.tm_mon + 1,
                t.tm_mday,
                t.tm_hour,
                t.tm_min,
                t.tm_sec,
                static_cast<int>(now_tv.tv_usec));
    
    sprintf(logName, "%s", statFilePath);

    va_start(ap, fmt);
    fp = fopen(logName, "a+");
    if (nullptr == fp) {
        return;
    }
    if (isPrintHeader) {
        fprintf(fp, "%s", headBuf);
    }
    vfprintf(fp, fmt, ap);
    fclose(fp);
    va_end(ap);
}


class DumpChunkStatsCommand : public Command {
public:
    DumpChunkStatsCommand() : Command("dumpChunkStats") {}

    void help(std::stringstream& help) const override {
        help << "internal command usage only\n"
                "example:\n"
                " { dumpChunkStats:\"db.foo\" , chunkId:\"1\" } ";
    }

    bool slaveOk() const override {
        return false;
    }

    bool adminOnly() const override {
        return true;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    string parseNs(const string& dbname, const BSONObj& cmdObj) const override {
        return parseNsFullyQualified(dbname, cmdObj);
    }

    bool run(OperationContext* txn,
             const string& dbname,
             BSONObj& cmdObj,
             int ,
             string& errmsg,
             BSONObjBuilder& result) override {
/*        const NamespaceString nss = NamespaceString(parseNs(dbname, cmdObj));
        if (!nss.isValid()) {
            errmsg = str::stream() << "invalid namespace '" << nss.toString()
                                   << "' specified for command";
            return false;
        }

        //log() << "dumpChunkStats command:" << std::endl
        //      << "cmdObj:" << cmdObj << std::endl
        //      << "nss:" << nss.toString() << std::endl;

        string chunkId;
        {
            BSONElement chunkIdElem;
            auto chunkIdStatus =
                bsonExtractTypedField(cmdObj, kChunkId, BSONType::String, &chunkIdElem);

            if (!chunkIdStatus.isOK()) {
                errmsg = str::stream() << "need to specify chunkId";
                return false;
            }
            chunkId = chunkIdElem.String();
        }

        log() << "dumpChunkStats command:" << std::endl
              << "cmdObj:" << cmdObj << std::endl
              << "nss:" << nss.toString() << std::endl
              << "dbname:" << dbname << std::endl
              << "chunkId:" << chunkId << std::endl;

        StorageEngine* storageEngine = txn->getServiceContext()->getGlobalStorageEngine();
        KVStorageEngine* kvStorageEngine = dynamic_cast<KVStorageEngine*>(storageEngine);
        bool useMultiInstance =
                    GLOBAL_CONFIG_GET(UseMultiRocksDBInstanceEngine) && 
                    serverGlobalParams.clusterRole == ClusterRole::ShardServer;

        if (!useMultiInstance) {
            errmsg = str::stream() << "need to use multiInstance";
            return false;
        }
        
        PartitionedRocksEngine* partitionedRocksEngine = dynamic_cast<PartitionedRocksEngine*>(kvStorageEngine->getEngine());
        ShardId shardId;

        // Get shard id
        partitionedRocksEngine->GetShardId(shardId);

        log_stat(false, "\n\n****************************************************************************************************************************\n");
        log_stat(true, "[NewCommand][shardId=%s] %s\n", shardId.toString().c_str(), cmdObj.toString().c_str());


        // Dump chunk metadata
        std::map<std::string, std::string> chunkMetadataVector;
        {
            if (chunkId == "All") {
                partitionedRocksEngine->GetChunkMetadata(chunkId, chunkMetadataVector, true);
            } else {
                partitionedRocksEngine->GetChunkMetadata(chunkId, chunkMetadataVector, false);
            }

            for (auto chunkMetadata : chunkMetadataVector) {
                log_stat(true, "[ChunkMetadata][shardId=%s][chunkId=%s]\n%s\n\n", 
                    shardId.toString().c_str(), chunkMetadata.first.c_str(), chunkMetadata.second.c_str());
            }
        }

        // Dump rocksdb stats
        std::map<std::string, std::string> dbStatsMap;
        {
            if (chunkId == "All") {
                partitionedRocksEngine->GetDbStats(chunkId, dbStatsMap, true);
            } else {
                partitionedRocksEngine->GetDbStats(chunkId, dbStatsMap, false);
            }

            for (auto dbStats : dbStatsMap) {
                log_stat(true, "[RocksdbMetadata][shardId=%s][chunkId=%s] %s\n", 
                    shardId.toString().c_str(), dbStats.first.c_str(), dbStats.second.c_str());
            }
        }

        // Dump shared resource stats
        {
            if (chunkId == "All") {
                partitionedRocksEngine->DumpSharedResourceStats(chunkId, statFilePath, true);
            } else {
                partitionedRocksEngine->DumpSharedResourceStats(chunkId, statFilePath, false);
            }
        }
  */            
        return true;
    }
} dumpChunkStatsCmd;

}  // namespace mongo

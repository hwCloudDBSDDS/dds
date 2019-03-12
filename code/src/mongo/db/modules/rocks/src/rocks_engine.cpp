/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "rocks_engine.h"

#include <algorithm>
#include <mutex>

#include <boost/filesystem/operations.hpp>

#include <rocksdb/cache.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/experimental.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/table.h>
#include <rocksdb/convenience.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>

#include "mongo/db/client.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/storage/journal_listener.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/platform/endian.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/background.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"
#include "rocks_counter_manager.h"
#include "rocks_global_options.h"
#include "rocks_record_store.h"
#include "rocks_recovery_unit.h"
#include "rocks_index.h"
#include "rocks_util.h"

#include "config/config_reader.h"
#include "GlobalConfig.h"
#include <stdio.h>
#include <iostream>

using namespace std;

#define ROCKS_TRACE log()

// Memory Config for shard and config svr
// TODO We will get this config from deploy yml soon
#define M_MIN_MEMORY_SIZEMB              (1024ULL)
#define M_SHARDSVR_MEMORY_CONFMB         (128*1024ULL)
#define M_CONFIGSVR_MEMORY_CONFMB        (16*1024ULL)
#define M_SHARDSVR_PLOGCLIENT_CONFMB     (10*1024ULL)

// Memory Model for shard svr and config svr
// TODO We will make following Macro into Config file
#define M_SHARDSVR_SYSTEM_OVERHEAD_MAXSIZEMB   (16*1024ULL)
#define M_SHARDSVR_SYSTEM_OVERHEAD_PERMILLAGE  (130ULL)
#define M_SHARDSVR_SYSTEM_DYNAMIC_MAXSIZEMB    (32*1024ULL)
#define M_SHARDSVR_SYSTEM_DYNAMIC_PERMILLAGE   (260ULL)
#define M_SHARDSVR_READAHEADBUFF_MAXSIZEMB     (1536ULL) //(1.5*1024)
#define M_SHARDSVR_READAHEADBUFF_PERMILLAGE    (15ULL)
#define M_SHARDSVR_TABLECACHE_PERMILLAGE       (220ULL)
#define M_SHARDSVR_BLOCKCACHE_PERMILLAGE       (330ULL)
#define M_SHARDSVR_ZBLOCKCACHE_PERMILLAGE      (45ULL)

#define M_CONFIGSVR_SYSTEM_OVERHEAD_MAXSIZEMB   (4*1024ULL)
#define M_CONFIGSVR_SYSTEM_OVERHEAD_PERMILLAGE  (250ULL)
#define M_CONFIGSVR_SYSTEM_DYNAMIC_MAXSIZEMB    (2*1024ULL)
#define M_CONFIGSVR_SYSTEM_DYNAMIC_PERMILLAGE   (130ULL)
#define M_CONFIGSVR_READAHEADBUFF_MAXSIZEMB     (0ULL)
#define M_CONFIGSVR_READAHEADBUFF_PERMILLAGE    (0ULL)
#define M_CONFIGSVR_TABLECACHE_PERMILLAGE       (130ULL)
#define M_CONFIGSVR_BLOCKCACHE_PERMILLAGE       (430ULL)
#define M_CONFIGSVR_ZBLOCKCACHE_PERMILLAGE      (60ULL)

namespace mongo {
const uint32_t      RET_OK                      = 0;
const uint32_t      RET_ERROR                   = 1;


//
//    class ShardsvrService : public UpLayerService {
//        public:
//         ShardsvrService() {}
//         virtual ~ShardsvrService() {}
//
//         virtual int AllocRootPlog(void* root_plog_id) {
//            return _createRootFolderFunc(_chunkId, root_plog_id);
//         }
//
//         virtual void SetRootPlogFunc(void* pFunc) {
//            _createRootFolderFunc = (shardsvrCreateRootFolder)pFunc;
//         }
//
//         virtual void SetChunkId(std::string chunkId) {
//            _chunkId = chunkId;
//         }
//
//        private:
//         shardsvrCreateRootFolder _createRootFolderFunc;
//         std::string _chunkId;
//    };

bool extractPrefix(const rocksdb::Slice& slice, uint32_t* prefix) {
    if (slice.size() < sizeof(uint32_t)) {
        return false;
    }
    *prefix = endian::bigToNative(*reinterpret_cast<const uint32_t*>(slice.data()));
    return true;
}

    namespace {
        // we encode prefixes in big endian because we want to quickly jump to the max prefix
        // (iter->SeekToLast())


        class PrefixDeletingCompactionFilter : public rocksdb::CompactionFilter {
        public:
            explicit PrefixDeletingCompactionFilter(std::unordered_set<uint32_t> droppedPrefixes)
                : _droppedPrefixes(std::move(droppedPrefixes)),
                  _prefixCache(0),
                  _droppedCache(false) {}

            // filter is not called from multiple threads simultaneously
            virtual bool Filter(int level, const rocksdb::Slice& key,
                                const rocksdb::Slice& existing_value, std::string* new_value,
                                bool* value_changed) const {
                uint32_t prefix = 0;
                if (!extractPrefix(key, &prefix)) {
                    // this means there is a key in the database that's shorter than 4 bytes. this
                    // should never happen and this is a corruption. however, it's not compaction
                    // filter's job to report corruption, so we just silently continue
                    return false;
                }
                if (prefix == _prefixCache) {
                    return _droppedCache;
                }
                _prefixCache = prefix;
                _droppedCache = _droppedPrefixes.find(prefix) != _droppedPrefixes.end();
                return _droppedCache;
            }

            virtual const char* Name() const { return "PrefixDeletingCompactionFilter"; }

        private:
            std::unordered_set<uint32_t> _droppedPrefixes;
            mutable uint32_t _prefixCache;
            mutable bool _droppedCache;
        };

        class PrefixDeletingCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
        public:
            explicit
            PrefixDeletingCompactionFilterFactory(const RocksEngine* engine) : _engine(engine) {}

            virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
                const rocksdb::CompactionFilter::Context& context) override {

                //  Make sure we do it only for our own column family
                if(context.column_family_id > 0)
                    return nullptr;

                auto droppedPrefixes = _engine->getDroppedPrefixes();
                if (droppedPrefixes.size() == 0) {
                    // no compaction filter needed
                    return std::unique_ptr<rocksdb::CompactionFilter>(nullptr);
                } else {
                    return std::unique_ptr<rocksdb::CompactionFilter>(
                        new PrefixDeletingCompactionFilter(std::move(droppedPrefixes)));
                }
            }

            virtual const char* Name() const override {
                return "PrefixDeletingCompactionFilterFactory";
            }

        private:
            const RocksEngine* _engine;
        };

    }  // anonymous namespace

    // first four bytes are the default prefix 0
    const std::string RocksEngine::kMetadataPrefix("\0\0\0\0metadata-", 12);
    const std::string RocksEngine::kDroppedPrefix("\0\0\0\0droppedprefix-", 18);

    RocksEngine::RocksEngine(const std::string& path, bool durable, int formatVersion)
        : _path(path), _durable(durable), _formatVersion(formatVersion), _maxPrefix(0) {
        {  // create block cache
            unsigned long long memSizeMB = 0;
            uint64_t cacheSizeGB = rocksGlobalOptions.cacheSizeGB;
            if (cacheSizeGB == 0) {
                ProcessInfo pi;
                memSizeMB = pi.getMemSizeMB();
                if (memSizeMB > 0) {
                    // reserve 1GB for system and binaries, and use 30% of the rest
                    double cacheMB = (memSizeMB - 1024) * 0.3; //RocksCreateCompressedCache by Xiexiaoqin
                    cacheSizeGB = static_cast<uint64_t>(cacheMB / 1024);
                }
                if (cacheSizeGB < 1) {
                    cacheSizeGB = 1;
                }
            }
            _block_cache = rocksdb::NewLRUCache(cacheSizeGB * 1024 * 1024 * 1024LL, 6);
        }
        _maxWriteMBPerSec = rocksGlobalOptions.maxWriteMBPerSec;
        _rateLimiter.reset(
            rocksdb::NewGenericRateLimiter(static_cast<int64_t>(_maxWriteMBPerSec) * 1024 * 1024));
        if (rocksGlobalOptions.counters) {
            _statistics = rocksdb::CreateDBStatistics();
        }

        log() << "RocksEngine::RocksEngine() path: " << path << "; _path: " << _path;
        Init();
    }

    RocksEngine::~RocksEngine() {
         cleanShutdown();
         _db.reset();
    }

    void RocksEngine::_initMemPlan() {
        // get system memory info
        ProcessInfo pi;
        unsigned long long systemSizeMB = 0;
        systemSizeMB = pi.getMemSizeMB();

        if (serverGlobalParams.clusterRole != ClusterRole::ShardServer
            && serverGlobalParams.clusterRole != ClusterRole::ConfigServer) {
            // default behavior
            invariantRocksOKWithNoCore(rocksdb::Status::Aborted());

            return;
        }

        _initMemPlanForShardConfigSvr(systemSizeMB);
        return;
    }

    void RocksEngine::_initMemPlanForShardConfigSvr(unsigned long long systemTotalSizeMB) {
        //// macro array
        //static unsigned long long memoryConfigTable[E_PROCESS_ROLE_BUTT][E_MEM_CONFIG_BUTT] = {
        //    // shard svr config
        //  { M_SHARDSVR_MEMORY_CONFMB,
        //    M_SHARDSVR_SYSTEM_OVERHEAD_MAXSIZEMB,
        //    M_SHARDSVR_SYSTEM_OVERHEAD_PERMILLAGE,
        //    M_SHARDSVR_SYSTEM_DYNAMIC_MAXSIZEMB,
        //    M_SHARDSVR_SYSTEM_DYNAMIC_PERMILLAGE,
        //    M_SHARDSVR_READAHEADBUFF_MAXSIZEMB,
        //    M_SHARDSVR_READAHEADBUFF_PERMILLAGE,
        //    M_SHARDSVR_TABLECACHE_PERMILLAGE,
        //    M_SHARDSVR_BLOCKCACHE_PERMILLAGE,
        //    M_SHARDSVR_ZBLOCKCACHE_PERMILLAGE},

        //  // config svr
        //  { M_CONFIGSVR_MEMORY_CONFMB,
        //    M_CONFIGSVR_SYSTEM_OVERHEAD_MAXSIZEMB,
        //    M_CONFIGSVR_SYSTEM_OVERHEAD_PERMILLAGE,
        //    M_CONFIGSVR_SYSTEM_DYNAMIC_MAXSIZEMB,
        //    M_CONFIGSVR_SYSTEM_DYNAMIC_PERMILLAGE,
        //    M_CONFIGSVR_READAHEADBUFF_MAXSIZEMB,
        //    M_CONFIGSVR_READAHEADBUFF_PERMILLAGE,
        //    M_CONFIGSVR_TABLECACHE_PERMILLAGE,
        //    M_CONFIGSVR_BLOCKCACHE_PERMILLAGE,
        //    M_CONFIGSVR_ZBLOCKCACHE_PERMILLAGE}
        //};

        //uint64_t indexAvaiableSizeMB = 0;
        //uint64_t leftAvaiableSize  = 0;

        //uint64_t systemOverHeadSize = 0;
        //uint64_t systemDynamicSize = 0;
        //uint64_t readAheadBufferSize = 0;

        //uint64_t cacheTotalSize  = 0;
        //uint64_t cachePermillageBase = 0;
        //uint64_t tableCacheSize  = 0;
        //uint64_t blockCacheSize  = 0;
        //uint64_t zblockCacheSize = 0;

        //eProcessType svrType = E_PROCESS_ROLE_BUTT;

        //// get and check svr type
        //if (serverGlobalParams.clusterRole == ClusterRole::ShardServer) {
        //    svrType = E_PROCESS_ROLE_SHARD;
        //} else if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
        //    svrType = E_PROCESS_ROLE_CONFIG;
        //} else {
        //    // default behavior
        //    index_err() << "unknown svr type, init mem plan failed";
        //    invariantRocksOKWithNoCore(rocksdb::Status::Aborted());
        //    return;
        //}

        //// get and check M_MIN_MEMORY_SIZEMB
        //if (systemTotalSizeMB <= M_MIN_MEMORY_SIZEMB)
        //{
        //    // default behavior
        //    index_err() << "systemTotalSizeMB("
        //          << systemTotalSizeMB
        //          << ") < ("
        //          << M_MIN_MEMORY_SIZEMB
        //          << ") init mem plan failed";
        //    invariantRocksOKWithNoCore(rocksdb::Status::Aborted());
        //    return;
        //}

        //indexAvaiableSizeMB = std::min((systemTotalSizeMB - M_MIN_MEMORY_SIZEMB),
        //                              memoryConfigTable[svrType][E_MEM_CONFIG_CONFMB]);
        //leftAvaiableSize  = M_INDEX_MB*indexAvaiableSizeMB;

        //// (1) assign operating system overhead resources, has max size upper limit
        //systemOverHeadSize = M_INDEX_MB*
        //    std::min(indexAvaiableSizeMB*memoryConfigTable[svrType][E_SYSTEM_OVERHEAD_PERMILLAGE]/M_PERMILLAGE_BASE,
        //             memoryConfigTable[svrType][E_SYSTEM_OVERHEAD_MAXSIZEMB]);
        //leftAvaiableSize -= systemOverHeadSize;

        //// (2) assign dynamic allocation memory resources, has max size upper limit
        //systemDynamicSize = M_INDEX_MB*
        //    std::min(indexAvaiableSizeMB*memoryConfigTable[svrType][E_SYSTEM_DYNAMIC_PERMILLAGE]/M_PERMILLAGE_BASE,
        //            memoryConfigTable[svrType][E_SYSTEM_DYNAMIC_MAXSIZEMB]);
        //leftAvaiableSize -= systemDynamicSize;

        //// (3) assign compaction read ahead buffer resources, has max size upper limit
        //readAheadBufferSize = M_INDEX_MB*
        //    std::min(indexAvaiableSizeMB*memoryConfigTable[svrType][E_READAHEADBUFF_PERMILLAGE]/M_PERMILLAGE_BASE,
        //            memoryConfigTable[svrType][E_READAHEADBUFF_MAXSIZEMB]);
        //leftAvaiableSize -= readAheadBufferSize;

        //// (4) assign TableCache/BlockCache/ZBlockCache resources, no max size upper limit
        //cacheTotalSize = leftAvaiableSize;
        //cachePermillageBase = memoryConfigTable[svrType][E_TABLECACHE_PERMILLAGE] +
        //                      memoryConfigTable[svrType][E_BLOCKCACHE_PERMILLAGE] +
        //                      memoryConfigTable[svrType][E_ZBLOCKCACHE_PERMILLAGE];

        //tableCacheSize  = static_cast<uint64_t>((static_cast<double>(cacheTotalSize) *
        //                            memoryConfigTable[svrType][E_TABLECACHE_PERMILLAGE])/cachePermillageBase);
        //blockCacheSize  = static_cast<uint64_t>((static_cast<double>(cacheTotalSize) *
        //                            memoryConfigTable[svrType][E_BLOCKCACHE_PERMILLAGE])/cachePermillageBase);
        //zblockCacheSize = static_cast<uint64_t>((static_cast<double>(cacheTotalSize) *
        //                            memoryConfigTable[svrType][E_ZBLOCKCACHE_PERMILLAGE])/cachePermillageBase);

        //// create read ahead buffer
        //_bufferSizeCtrl = NewBufferSizeCtrl(readAheadBufferSize);
        //if (!_bufferSizeCtrl
        //    && serverGlobalParams.clusterRole != ClusterRole::ConfigServer) {
        //    _bufferSizeCtrl = nullptr;
        //    index_err() << "new BufferSizeCtrl failed";
        //    invariantRocksOKWithNoCore(rocksdb::Status::Aborted());
        //}

        //// create cache
        //if (0 != tableCacheSize) {
        //    _table_cache = rocksdb::NewLRUCache(tableCacheSize, M_CACHE_NUMSHARDBITS, true, 0.0, rocksdb::E_ROCKS_CACHE_TABLE);
        //    if (!_table_cache) {
        //        _table_cache = nullptr;
        //        index_err() << "new NewLRUCache for TableCache failed";
        //        invariantRocksOKWithNoCore(rocksdb::Status::Aborted());
        //    }
        //} else {
        //    _table_cache = nullptr;
        //    index_err() << "tableCacheSize == 0, NoSharedTableCache";
        //    invariantRocksOKWithNoCore(rocksdb::Status::Aborted());
        //}

        //if (0 != blockCacheSize) {
        //    _block_cache = rocksdb::NewLRUCache(blockCacheSize, M_CACHE_NUMSHARDBITS, true, 0.0, rocksdb::E_ROCKS_CACHE_BLOCK);
        //    if (!_block_cache) {
        //        _block_cache = nullptr;
        //        index_err() << "new NewLRUCache for BlockCache failed";
        //        invariantRocksOKWithNoCore(rocksdb::Status::Aborted());
        //    }
        //} else {
        //    _block_cache = nullptr;
        //    index_err() << "blockCacheSize == 0, NoSharedBlockCache";
        //    invariantRocksOKWithNoCore(rocksdb::Status::Aborted());
        //}

        //#if (defined(__SWITCH_PERF_ADDCOMPRESSED_CACHE___) && (__SWITCH_PERF_ADDCOMPRESSED_CACHE___))
        //if (0 != zblockCacheSize) {
        //    _block_cache_compressed = rocksdb::NewLRUCache(zblockCacheSize,M_CACHE_NUMSHARDBITS, true, 0.0, rocksdb::E_ROCKS_CACHE_COMPRESSED_BLOCK);
        //    if (!_block_cache_compressed) {
        //        _block_cache_compressed = nullptr;
        //        index_err() << "new NewLRUCache for ZBlockCache failed";
        //        invariantRocksOKWithNoCore(rocksdb::Status::Aborted());
        //    }
        //} else {
        //    _block_cache_compressed = nullptr;
        //    index_err() << "zblockCacheSize == 0, NoSharedZBlockCache";
        //    invariantRocksOKWithNoCore(rocksdb::Status::Aborted());
        //}
        //#else
        //_block_cache_compressed = nullptr;
        //#endif

        //index_err() << "M_SVR_MEMORY_CONFBYTE("
        //      << memoryConfigTable[svrType][E_MEM_CONFIG_CONFMB]
        //      << ")M_SVR_SYSTEM_OVERHEAD_MAXSIZEBYTE("
        //      << memoryConfigTable[svrType][E_SYSTEM_OVERHEAD_MAXSIZEMB]
        //      << ")M_SVR_SYSTEM_OVERHEAD_PERMILLAGE("
        //      << memoryConfigTable[svrType][E_SYSTEM_OVERHEAD_PERMILLAGE]
        //      << ")M_SVR_SYSTEM_DYNAMIC_MAXSIZEBYTE("
        //      << memoryConfigTable[svrType][E_SYSTEM_DYNAMIC_MAXSIZEMB]
        //      << ")M_SVR_SYSTEM_DYNAMIC_PERMILLAGE("
        //      << memoryConfigTable[svrType][E_SYSTEM_DYNAMIC_PERMILLAGE]
        //      << ")M_SVR_READAHEADBUFF_MAXSIZEBYTE("
        //      << memoryConfigTable[svrType][E_READAHEADBUFF_MAXSIZEMB]
        //      << ")M_SVR_READAHEADBUFF_PERMILLAGE("
        //      << memoryConfigTable[svrType][E_READAHEADBUFF_PERMILLAGE]
        //      << ")M_SVR_TABLECACHE_PERMILLAGE("
        //      << memoryConfigTable[svrType][E_TABLECACHE_PERMILLAGE]
        //      << ")M_SVR_BLOCKCACHE_PERMILLAGE("
        //      << memoryConfigTable[svrType][E_BLOCKCACHE_PERMILLAGE]
        //      << ")M_SVR_ZBLOCKCACHE_PERMILLAGE("
        //      << memoryConfigTable[svrType][E_ZBLOCKCACHE_PERMILLAGE]
        //      << ")";

        //index_err() << "systemTotalSizeMB("  << systemTotalSizeMB
        //      << ")systemReservedSizeMB("  << M_MIN_MEMORY_SIZEMB
        //      << ")indexAvaiableSizeMB("  << indexAvaiableSizeMB
        //      << ")systemOverHeadSize("  << systemOverHeadSize
        //      << ")systemDynamicSize("  << systemDynamicSize
        //      << ")readAheadBufferSize("  << readAheadBufferSize
        //      << ")TableCacheSize("  << tableCacheSize
        //      << ")BlockCacheSize(" << blockCacheSize
        //      << ")ZBlockCacheSize(" << zblockCacheSize
        //      << ")";

        return;
    }

    void RocksEngine::Init()
    {
        index_log() << "[MongoRocks] Start initialization of RocksEngine....";

        if (!GLOBAL_CONFIG_GET(IsCoreTest) && 
            ClusterRole::ConfigServer != serverGlobalParams.clusterRole) {
            std::string dbpath = _path + ROCKSDB_PATH;
            deleteDir(dbpath);
        } 
        //  Always should be able to open system DB to start
        invariantOK(OpenDB(_path, _db));
        LoadMetadata();

        index_log() << "[MongoRocks] RocksEngine initialized";
    }
    rocksdb::CompressionType RocksEngine::getCompressLevelByName(const std::string cmprsLvl)
    {
        if (0 == cmprsLvl.compare("kSnappyCompression"))
        {
            return rocksdb::kSnappyCompression;
        }
        else if (0 == cmprsLvl.compare("kZlibCompression"))
        {
            return rocksdb::kZlibCompression;
        }
        else if (0 == cmprsLvl.compare("kBZip2Compression"))
        {
            return rocksdb::kBZip2Compression;
        }
        else if (0 == cmprsLvl.compare("kLZ4Compression"))
        {
            return rocksdb::kLZ4Compression;
        }
        else if (0 == cmprsLvl.compare("kLZ4HCCompression"))
        {
            return rocksdb::kLZ4HCCompression;
        }
        else if (0 == cmprsLvl.compare("kXpressCompression"))
        {
            return rocksdb::kXpressCompression;
        }
        else if (0 == cmprsLvl.compare("kZSTD"))
        {
            return rocksdb::kZSTD;
        }
        else if (0 == cmprsLvl.compare("kDisableCompressionOption"))
        {
            return rocksdb::kDisableCompressionOption;
        }
        else
        {
            return rocksdb::kNoCompression;
        }
    }

    uint32_t RocksEngine::getAllConfig(rocksdb::Options &option, rocksdb::BlockBasedTableOptions & blkBasedTblOption)
    {
        std::string val;

        val = ConfigReader::getInstance()->getString("DBOptions", "base_background_compactions");
        option.base_background_compactions = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "max_background_compactions");
        option.max_background_compactions = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "max_subcompactions");
        option.max_subcompactions = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "max_background_flushes");
        option.max_background_flushes = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "max_manifest_file_size");
        option.max_manifest_file_size = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "manifest_preallocation_size");
        option.manifest_preallocation_size = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "stats_dump_period_sec");
        option.stats_dump_period_sec = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "new_table_reader_for_compaction_inputs");
        option.new_table_reader_for_compaction_inputs = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "compaction_readahead_size");
        option.compaction_readahead_size = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "writable_file_max_buffer_size");
        option.writable_file_max_buffer_size = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "use_adaptive_mutex");
        option.use_adaptive_mutex = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "write_thread_max_yield_usec");
        option.write_thread_max_yield_usec = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "write_thread_slow_yield_usec");
        option.write_thread_slow_yield_usec = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "skip_stats_update_on_db_open");
        option.skip_stats_update_on_db_open = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("DBOptions", "dump_malloc_stats");
        option.dump_malloc_stats = atoi(val.c_str());

        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "write_buffer_size");
        option.write_buffer_size = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "max_write_buffer_number");
        option.max_write_buffer_number = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "min_write_buffer_number_to_merge");
        option.min_write_buffer_number_to_merge = atoi(val.c_str());
#if 0
        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "compression");
        option.compression = getCompressLevelByName(val);
        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "compression_per_level0");
        option.compression_per_level[0] = getCompressLevelByName(val);
        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "compression_per_level1");
        option.compression_per_level[1] = getCompressLevelByName(val);
        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "compression_per_level2");
        option.compression_per_level[2] = getCompressLevelByName(val);
        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "bottommost_compression");
        option.bottommost_compression = getCompressLevelByName(val);
#endif
        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "num_levels");
        option.num_levels = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "level0_stop_writes_trigger");
        option.level0_stop_writes_trigger = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "target_file_size_base");
        option.target_file_size_base = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "max_bytes_for_level_base");
        option.max_bytes_for_level_base = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "max_compaction_bytes");
        option.max_compaction_bytes = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("ColumnFamilyOptions", "report_bg_io_stats");
        option.report_bg_io_stats = atoi(val.c_str());

        val = ConfigReader::getInstance()->getString("BlockBasedTableOptions", "cache_index_and_filter_blocks");
        blkBasedTblOption.cache_index_and_filter_blocks = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("BlockBasedTableOptions", "cache_index_and_filter_blocks_with_high_priority");
        blkBasedTblOption.cache_index_and_filter_blocks_with_high_priority = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("BlockBasedTableOptions", "pin_l0_filter_and_index_blocks_in_cache");
        blkBasedTblOption.pin_l0_filter_and_index_blocks_in_cache = atoi(val.c_str());
        val = ConfigReader::getInstance()->getString("BlockBasedTableOptions", "block_size");
        blkBasedTblOption.block_size = atoi(val.c_str());

        return RET_OK;
    }


    mongo::Status RocksEngine::OpenDB(const std::string& path, std::unique_ptr<mongo::ChunkRocksDBInstance>& dbInstance) const
    {
        std::unique_ptr<mongo::ChunkRocksDBInstance> db(new mongo::ChunkRocksDBInstance(path));

        //TODO remove after the dbpath is no longer used by RocksDB
        std::string pathWithoutPlog = path;
        if (pathWithoutPlog.find("plogcnt")  != std::string::npos ){
            pathWithoutPlog.assign(pathWithoutPlog, 0, pathWithoutPlog.find_last_of('/'));
        }
        MDB_RIF(db->Open(_options(path), pathWithoutPlog, _durable));

        dbInstance = std::move(db);
        return mongo::Status::OK();
    }

    void RocksEngine::LoadMetadata()
    {
        // open iterator
        std::unique_ptr<rocksdb::Iterator> iter(getMetadataDB()->NewIterator(rocksdb::ReadOptions()));

        // find maxPrefix
        iter->SeekToLast();
        if (iter->Valid()) {
            // otherwise the DB is empty, so we just keep it at 0
            bool ok = extractPrefix(iter->key(), &_maxPrefix);
            // this is DB corruption here
            invariant(ok);
        }

        // load ident to prefix map. also update _maxPrefix if there's any prefix bigger than
        // current _maxPrefix
        {
            stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
            for (iter->Seek(kMetadataPrefix);
                 iter->Valid() && iter->key().starts_with(kMetadataPrefix); iter->Next()) {
                invariantRocksOK(iter->status());
                rocksdb::Slice ident(iter->key());
                ident.remove_prefix(kMetadataPrefix.size());
                // this could throw DBException, which then means DB corruption. We just let it fly
                // to the caller
                BSONObj identConfig(iter->value().data());
                BSONElement element = identConfig.getField("prefix");

                if (element.eoo() || !element.isNumber()) {
                    log() << "Mongo metadata in RocksDB database is corrupted.";
                    invariant(false);
                }
                uint32_t identPrefix = static_cast<uint32_t>(element.numberInt());

                _identMap[StringData(ident.data(), ident.size())] =
                    std::move(identConfig.getOwned());

                log() << "add one collection to _identmap,ident:" << ident.data() << ",value:" << identConfig << ".";
                _maxPrefix = std::max(_maxPrefix, identPrefix);
            }
        }

        // just to be extra sure. we need this if last collection is oplog -- in that case we
        // reserve prefix+1 for oplog key tracker
        ++_maxPrefix;

        // load dropped prefixes
        {
            rocksdb::WriteBatch wb;
            // we will use this iter to check if prefixes are still alive
            std::unique_ptr<rocksdb::Iterator> prefixIter(getMetadataDB()->NewIterator(rocksdb::ReadOptions()));
            for (iter->Seek(kDroppedPrefix);
                 iter->Valid() && iter->key().starts_with(kDroppedPrefix); iter->Next()) {
                invariantRocksOK(iter->status());
                rocksdb::Slice prefix(iter->key());
                prefix.remove_prefix(kDroppedPrefix.size());
                prefixIter->Seek(prefix);
                invariantRocksOK(iter->status());
                if (prefixIter->Valid() && prefixIter->key().starts_with(prefix)) {
                    // prefix is still alive, let's instruct the compaction filter to clear it up
                    uint32_t int_prefix;
                    bool ok = extractPrefix(prefix, &int_prefix);
                    invariant(ok);
                    {
                        stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
                        _droppedPrefixes.insert(int_prefix);
                    }
                } else {
                    // prefix is no longer alive. let's remove the prefix from our dropped prefixes
                    // list
                    wb.Delete(iter->key());
                }
            }
            if (wb.Count() > 0) {
                auto s = getMetadataDB()->Write(rocksdb::WriteOptions(), &wb);
                invariantRocksOK(s);
            }
        }
    }

    RecoveryUnit* RocksEngine::newRecoveryUnit() {
        return new RocksRecoveryUnit(&_snapshotManager, _db.get());
    }

    Status RocksEngine::createRecordStore(OperationContext* opCtx, StringData ns, StringData ident,
                                          const CollectionOptions& options) {
        BSONObjBuilder configBuilder;
        auto s = _createIdent(ident, &configBuilder);
        if (s.isOK() && NamespaceString::oplog(ns)) {
            _oplogIdent = ident.toString();
            // oplog needs two prefixes, so we also reserve the next one
            uint64_t oplogTrackerPrefix = 0;
            {
                stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
                oplogTrackerPrefix = ++_maxPrefix;
            }
            // we also need to write out the new prefix to the database. this is just an
            // optimization
            std::string encodedPrefix(encodePrefix(oplogTrackerPrefix));
            s = rocksToMongoStatus(
                getMetadataDB()->Put(rocksdb::WriteOptions(), encodedPrefix, rocksdb::Slice()));
        }

        LOG(5) << "create Record return " << s.codeString()
              << ",reason:" << s.reason() <<",ns: " << ns.toString() << ", ident:" << ident.toString() <<".";
        return s;
    }

    std::unique_ptr<RecordStore> RocksEngine::getRecordStore(OperationContext* opCtx, StringData ns,
                                             StringData ident, const CollectionOptions& options) {
        if (NamespaceString::oplog(ns)) {
            _oplogIdent = ident.toString();
        }

        auto config = _getIdentConfig(ident);
        std::string prefix = _extractPrefix(config);

   
        std::unique_ptr<RocksRecordStore> recordStore = populateRecordStore(ns, ident, prefix, options);    
        if(nullptr == recordStore){
            Status s(ErrorCodes::InternalError, "init record store fail");
            //invariantRocksOK(s);
        }
        recordStore->Init();

        {
            stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
            _identCollectionMap[ident] = recordStore.get();
        }
        return std::move(recordStore);
    }

    std::unique_ptr<RocksRecordStore> RocksEngine::populateRecordStore(StringData ns, StringData ident,
                                        const std::string& prefix, const CollectionOptions& options)
    {
        return options.capped
                ? stdx::make_unique<RocksRecordStore>(
                      ns, ident, _db.get(), prefix,
                      true, options.cappedSize ? options.cappedSize : 4096,  // default size
                      options.cappedMaxDocs ? options.cappedMaxDocs : -1)
                : stdx::make_unique<RocksRecordStore>(ns, ident, _db.get(), prefix);
    }

    Status RocksEngine::createSortedDataInterface(OperationContext* opCtx, StringData ident,
                                                  const IndexDescriptor* desc) {
        BSONObjBuilder configBuilder;
        // let index add its own config things
        MongoRocksIndexBase::generateConfig(&configBuilder, _formatVersion);
        return _createIdent(ident, &configBuilder);
    }

    SortedDataInterface* RocksEngine::getSortedDataInterface(OperationContext* opCtx,
                                                             StringData ident,
                                                             const IndexDescriptor* desc) {

        BSONObj config = _getIdentConfig(ident);
        std::string prefix = _extractPrefix(config);

        RocksIndexBase* index = populateIndex(ident, desc, prefix, std::move(config));
        {
            stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
            _identIndexMap[ident] = index;
        }
        return index;
    }

    RocksIndexBase* RocksEngine::populateIndex(
        StringData ident,
        const IndexDescriptor* desc,
        const std::string& prefix,
        BSONObj&& config
        )
    {
        return populateIndex(_db.get(), ident.toString(), desc, prefix, std::move(config));
    }

    RocksIndexBase* RocksEngine::populateIndex(
        mongo::ChunkRocksDBInstance* db,
        const std::string& ident,
        const IndexDescriptor* desc,
        const std::string& prefix,
        BSONObj&& config
        )
    {
        if (desc->unique())
            return new RocksUniqueIndex(db, prefix, ident, Ordering::make(desc->keyPattern()), std::move(config));

        //
        //  Non unique
        //
        RocksStandardIndex* si = new RocksStandardIndex(db, prefix, ident, Ordering::make(desc->keyPattern()), std::move(config));
        if (rocksGlobalOptions.singleDeleteIndex)
            si->enableSingleDelete();

        return si;
    }

    // cannot be rolled back
    Status RocksEngine::dropIdent(OperationContext* opCtx, StringData ident) {
        rocksdb::WriteBatch wb;
        wb.Delete(kMetadataPrefix + ident.toString());

        // calculate which prefixes we need to drop
        std::vector<std::string> prefixesToDrop;
        prefixesToDrop.push_back(_extractPrefix(_getIdentConfig(ident)));
        if (_oplogIdent == ident.toString()) {
            // if we're dropping oplog, we also need to drop keys from RocksOplogKeyTracker (they
            // are stored at prefix+1)
            prefixesToDrop.push_back(rocksGetNextPrefix(prefixesToDrop[0]));
        }

        // We record the fact that we're deleting this prefix. That way we ensure that the prefix is
        // always deleted
        for (const auto& prefix : prefixesToDrop) {
            wb.Put(kDroppedPrefix + prefix, "");
        }

        // we need to make sure this is on disk before starting to delete data in compactions
        rocksdb::WriteOptions syncOptions;
        syncOptions.sync = true;
        auto s = getMetadataDB()->Write(syncOptions, &wb);
        if (!s.ok()) {
            return rocksToMongoStatus(s);
        }

        // remove from map
        {
            stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
            _identMap.erase(ident);
            log() << "drop ident from identmap,ident:" << ident.toString() << ".";
        }

        // instruct compaction filter to start deleting
        {
            stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
            for (const auto& prefix : prefixesToDrop) {
                uint32_t int_prefix;
                bool ok = extractPrefix(prefix, &int_prefix);
                invariant(ok);
                _droppedPrefixes.insert(int_prefix);
            }
        }

        // Suggest compaction for the prefixes that we need to drop, So that
        // we free space as fast as possible.
        for (auto& prefix : prefixesToDrop) {
            std::string end_prefix_str = rocksGetNextPrefix(prefix);

            rocksdb::Slice start_prefix = prefix;
            rocksdb::Slice end_prefix = end_prefix_str;
            s = rocksdb::experimental::SuggestCompactRange(getMetadataDB(), &start_prefix, &end_prefix);
            if (!s.ok()) {
                log() << "failed to suggest compaction for prefix " << prefix;
            }
        }

        return Status::OK();
    }

    bool RocksEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
        stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
        return _identMap.find(ident) != _identMap.end();
    }

    std::vector<std::string> RocksEngine::getAllIdents(OperationContext* opCtx) const {
        std::vector<std::string> indents;
        for (auto& entry : _identMap) {
            indents.push_back(entry.first);
        }
        return indents;
    }

    void RocksEngine::cleanShutdown() {
        //_db.reset();
        _snapshotManager.dropAllSnapshots();  
        _db->GetCounterManager()->sync();  
        //if (_db->GetJournalFlusher()) {
        //    _journalFlusher->shutdown();
        //}
    }

    void RocksEngine::setJournalListener(JournalListener* jl) {
        //
        //  TODO: do for all DBs
        //
        _db->GetDurabilityManager()->setJournalListener(jl);
    }

    int64_t RocksEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);

        auto indexIter = _identIndexMap.find(ident);
        if (indexIter != _identIndexMap.end()) {
            return static_cast<int64_t>(indexIter->second->getSpaceUsedBytes(opCtx));
        }
        auto collectionIter = _identCollectionMap.find(ident);
        if (collectionIter != _identCollectionMap.end()) {
            return collectionIter->second->storageSize(opCtx);
        }

        // this can only happen if collection or index exists, but it's not opened (i.e.
        // getRecordStore or getSortedDataInterface are not called)
        return 1;
    }

    int RocksEngine::flushAllFiles(bool sync) {
        LOG(1) << "RocksEngine::flushAllFiles";

        //
        //  TODO: Need to do it for each chunk db
        //
        _db->GetCounterManager()->sync();
        _db->GetDurabilityManager()->waitUntilDurable(true);

        return 1;
    }

    void  RocksEngine::resetEngineStats(){

        auto stats = getStatistics();
        stats->Reset();//wait rocksdb interface
        return ;
    }

    void  RocksEngine::getEngineStats( std::vector<std::string> & vs) {
        auto stats = getStatistics();
        vs = stats->ToFormatString();//wait rocksdb interface
        return;
    } 

    Status RocksEngine::beginBackup(OperationContext* txn) {
        return rocksToMongoStatus(getDB()->PauseBackgroundWork());
    }

    void RocksEngine::endBackup(OperationContext* txn) { getDB()->ContinueBackgroundWork(); }

    void RocksEngine::setMaxWriteMBPerSec(int maxWriteMBPerSec) {
        _maxWriteMBPerSec = maxWriteMBPerSec;
        _rateLimiter->SetBytesPerSecond(static_cast<int64_t>(_maxWriteMBPerSec) * 1024 * 1024);
    }

    Status RocksEngine::backup(const std::string& path) {
        rocksdb::Checkpoint* checkpoint;
        auto s = rocksdb::Checkpoint::Create(getDB(), &checkpoint);
        if (s.ok()) {
            s = checkpoint->CreateCheckpoint(path);
        }
        delete checkpoint;
        return rocksToMongoStatus(s);
    }

    std::unordered_set<uint32_t> RocksEngine::getDroppedPrefixes() const {
        stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
        // this will copy the set. that way compaction filter has its own copy and doesn't need to
        // worry about thread safety
        return _droppedPrefixes;
    }

    // non public api
    Status RocksEngine::_createIdent(StringData ident, BSONObjBuilder* configBuilder) {
        BSONObj config;
        uint32_t prefix = 0;
        {
            stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
            if (_identMap.find(ident) != _identMap.end()) {
                // already exists
                return Status::OK();
            }

            prefix = ++_maxPrefix;
            configBuilder->append("prefix", static_cast<int32_t>(prefix));

            config = std::move(configBuilder->obj());
            _identMap[ident] = config.copy();
        }

        BSONObjBuilder builder;

        auto s = getMetadataDB()->Put(rocksdb::WriteOptions(), kMetadataPrefix + ident.toString(),
                          rocksdb::Slice(config.objdata(), config.objsize()));

        if (s.ok()) {
            // As an optimization, add a key <prefix> to the DB
            std::string encodedPrefix(encodePrefix(prefix));
            s = getMetadataDB()->Put(rocksdb::WriteOptions(), encodedPrefix, rocksdb::Slice());
        }

        return rocksToMongoStatus(s);
    }

    BSONObj RocksEngine::_getIdentConfig(StringData ident) {
        stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
        auto identIter = _identMap.find(ident);
        invariant(identIter != _identMap.end());
        return identIter->second.copy();
    }

    std::string RocksEngine::_extractPrefix(const BSONObj& config) {
        return encodePrefix(config.getField("prefix").numberInt());
    }

    rocksdb::Options RocksEngine::_options(const std::string& dbPath) const {
        // default options
        rocksdb::Options options;
        options.rate_limiter = _rateLimiter;
        rocksdb::BlockBasedTableOptions table_options;
        table_options.block_cache = _block_cache;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        table_options.block_size = 16 * 1024; // 16KB
        table_options.format_version = 2;
        //options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

        options.write_buffer_size = /*64*/32 * 1024 * 1024;  // 64MB
        options.level0_slowdown_writes_trigger = 8;
        options.max_write_buffer_number = 4;
        options.max_background_compactions = 128;
        options.max_background_flushes = 128;
        options.base_background_compactions = 64;
        options.target_file_size_base = 64 * 1024 * 1024;  // 64MB
        options.soft_rate_limit = 2.5;
        options.hard_rate_limit = 3;
        options.level_compaction_dynamic_level_bytes = true;
        options.max_bytes_for_level_base = 512 * 1024 * 1024;  // 512 MB
        // This means there is no limit on open files. Make sure to always set ulimit so that it can
        // keep all RocksDB files opened.
        options.max_open_files = -1;
        options.optimize_filters_for_hits = true;
        options.compaction_filter_factory.reset(new PrefixDeletingCompactionFilterFactory(this));
        options.enable_thread_tracking = true;
        // Enable concurrent memtable
        options.allow_concurrent_memtable_write = true;
        options.enable_write_thread_adaptive_yield = true;

        options.compression_per_level.resize(3);
        options.compression_per_level[0] = rocksdb::kNoCompression;
        options.compression_per_level[1] = rocksdb::kNoCompression;

        if (rocksGlobalOptions.compression == "snappy") {
            options.compression_per_level[2] = rocksdb::kSnappyCompression;
        } else if (rocksGlobalOptions.compression == "zlib") {
            options.compression_per_level[2] = rocksdb::kZlibCompression;
        } else if (rocksGlobalOptions.compression == "none") {
            options.compression_per_level[2] = rocksdb::kNoCompression;
        } else if (rocksGlobalOptions.compression == "lz4") {
            options.compression_per_level[2] = rocksdb::kLZ4Compression;
        } else if (rocksGlobalOptions.compression == "lz4hc") {
            options.compression_per_level[2] = rocksdb::kLZ4HCCompression;
        } else {
            log() << "Unknown compression, will use default (snappy)";
            options.compression_per_level[2] = rocksdb::kSnappyCompression;
        }

        options.statistics = _statistics;

        // create the DB if it's not already present
        options.create_if_missing = true;
        options.wal_dir = dbPath + ROCKSDB_PATH + "/journal";
        log() << "rocksdb wal path: " << options.wal_dir;
        
        // allow override
        if (!rocksGlobalOptions.configString.empty()) {
            rocksdb::Options base_options(options);
            auto s = rocksdb::GetOptionsFromString(base_options, rocksGlobalOptions.configString,
                                                   &options);
            if (!s.ok()) {
                log() << "Invalid rocksdbConfigString \"" << rocksGlobalOptions.configString
                      << "\"";
                invariantRocksOK(s);
            }
        }

        //  Set the logger for database
        options.info_log = std::shared_ptr<rocksdb::Logger>(new ResultHandling::MongoRocksLoggerForChunk(dbPath));
        //options.env = rocksdb::Env::Default();       
        rocksdb::NewHdfsEnv(&options.env, GLOBAL_CONFIG_GET(hdfsUri),false);
        options.table_factory.reset(rocksdb::NewFilterBlockTableFactory(table_options));

        return options;
    }

    bool RocksEngine::IsSystemCollection(const StringData& ns)
    {        
        if(ns.compare("_mdb_catalog") == 0)
            return true;

        mongo::NamespaceString nameSpace(ns);
        return nameSpace.isSystemCollection();
        //return nameSpace.isOnInternalDb();            
    }

    bool RocksEngine::IsPLogUsed()
    {
        return GLOBAL_CONFIG_GET(UsePLogEnv);
    }

    bool RocksEngine::IsStreamUsed()
    {
        return GLOBAL_CONFIG_GET(UseStreamEnv);
    }

    std::string RocksEngine::encodePrefix(uint32_t prefix)
    {
        uint32_t bigEndianPrefix = endian::nativeToBig(prefix);
        return std::string(reinterpret_cast<const char*>(&bigEndianPrefix), sizeof(uint32_t));
    }

    rocksdb::Status createDir(rocksdb::Env* fileSystem, const std::string& path)
    {
        log() << "ChunkRocksDBInstance::createDir path: " << path;
        return fileSystem->CreateDirIfMissing(path);
    }

    rocksdb::Status deleteDir( const std::string& path)
    {
        log() << "ChunkRocksDBInstance::deleteDir path: " << path;
        rocksdb::Env* env = nullptr;

        //auto env = rocksdb::Env::Default();
        rocksdb::NewHdfsEnv(&env, GLOBAL_CONFIG_GET(hdfsUri),false);
        rocksdb::Status status = env->DeleteDir(path);
        return status;
    }
}

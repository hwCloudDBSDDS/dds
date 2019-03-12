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

#include <sys/stat.h>
#include <algorithm>
#include <mutex>

#include <boost/filesystem/operations.hpp>

#include <rocksdb/cache.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/comparator.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/experimental.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/persistent_cache.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/utilities/write_batch_with_index.h>


#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/client.h"
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
#include "rocks_index.h"
#include "rocks_record_store.h"
#include "rocks_recovery_unit.h"
#include "rocks_util.h"

#include <stdio.h>
#include <iostream>
#include "mongo/util/util_extend/GlobalConfig.h"
#include "mongo/util/util_extend/config_reader.h"
#include "mongo/db/server_options.h"
#include "mongo/db/repl/replication_coordinator_global.h"
using namespace std;

#define ROCKS_TRACE log()

// Memory Config for shard and config svr
// TODO We will get this config from deploy yml soon
#define M_MIN_MEMORY_SIZEMB (1024ULL)
#define M_SHARDSVR_MEMORY_CONFMB (128 * 1024ULL)
#define M_CONFIGSVR_MEMORY_CONFMB (16 * 1024ULL)
#define M_SHARDSVR_PLOGCLIENT_CONFMB (10 * 1024ULL)

// Memory Model for shard svr and config svr
// TODO We will make following Macro into Config file
#define M_SHARDSVR_SYSTEM_OVERHEAD_MAXSIZEMB (16 * 1024ULL)
#define M_SHARDSVR_SYSTEM_OVERHEAD_PERMILLAGE (130ULL)
#define M_SHARDSVR_SYSTEM_DYNAMIC_MAXSIZEMB (32 * 1024ULL)
#define M_SHARDSVR_SYSTEM_DYNAMIC_PERMILLAGE (260ULL)
#define M_SHARDSVR_READAHEADBUFF_MAXSIZEMB (1536ULL)  //(1.5*1024)
#define M_SHARDSVR_READAHEADBUFF_PERMILLAGE (15ULL)
#define M_SHARDSVR_TABLECACHE_PERMILLAGE (220ULL)
#define M_SHARDSVR_BLOCKCACHE_PERMILLAGE (330ULL)
#define M_SHARDSVR_ZBLOCKCACHE_PERMILLAGE (45ULL)

#define M_CONFIGSVR_SYSTEM_OVERHEAD_MAXSIZEMB (4 * 1024ULL)
#define M_CONFIGSVR_SYSTEM_OVERHEAD_PERMILLAGE (250ULL)
#define M_CONFIGSVR_SYSTEM_DYNAMIC_MAXSIZEMB (2 * 1024ULL)
#define M_CONFIGSVR_SYSTEM_DYNAMIC_PERMILLAGE (130ULL)
#define M_CONFIGSVR_READAHEADBUFF_MAXSIZEMB (0ULL)
#define M_CONFIGSVR_READAHEADBUFF_PERMILLAGE (0ULL)
#define M_CONFIGSVR_TABLECACHE_PERMILLAGE (130ULL)
#define M_CONFIGSVR_BLOCKCACHE_PERMILLAGE (430ULL)
#define M_CONFIGSVR_ZBLOCKCACHE_PERMILLAGE (60ULL)

namespace mongo {
    const uint32_t RET_OK = 0;
    const uint32_t RET_ERROR = 1;

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
            explicit PrefixDeletingCompactionFilterFactory(const RocksEngine* engine)
                : _engine(engine) {}

            virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
                const rocksdb::CompactionFilter::Context& context) override {
                //  Make sure we do it only for our own column family
                if (context.column_family_id > 0) return nullptr;

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
                    double cacheMB = (memSizeMB - 1024) * 0.3;
                    cacheSizeGB = static_cast<uint64_t>(cacheMB / 1024);
                }
                if (cacheSizeGB < 1) {
                    cacheSizeGB = 1;
                }
            }
            size_t block_cache_size = ConfigReader::getInstance()->getDecimalNumber<size_t>(
                "TableOptions", "block_cache");
            size_t db_write_buffer_size = ConfigReader::getInstance()->getDecimalNumber<size_t>(
                "TableOptions", "db_write_buffer_size");
            index_log() << "block_cache_size: " << block_cache_size
                        << "; db_write_buffer_size: " << db_write_buffer_size << "; paht: " << path;
            //_block_cache = rocksdb::NewLRUCache(cacheSizeGB * 1024 * 1024 * 1024LL, 6);
            _block_cache = rocksdb::NewLRUCache(block_cache_size, 6);
            _write_buf.reset(new rocksdb::WriteBufferManager(db_write_buffer_size));
            int enable_row_cache =
                ConfigReader::getInstance()->getDecimalNumber<int>("CFOptions", "enable_row_cache");
            if (enable_row_cache) {
                _row_cache =
                    rocksdb::NewLRUCache(ConfigReader::getInstance()->getDecimalNumber<size_t>(
                        "CFOptions", "row_cache_size"));
            }
            rocksdb::Status ret;
            std::shared_ptr<rocksdb::Logger> info_log;
            std::string file_path =
                ConfigReader::getInstance()->getString("TableOptions", "persistent_cache_path");
            size_t persistent_cache_size = ConfigReader::getInstance()->getDecimalNumber<size_t>(
                "TableOptions", "persistent_cache_size");
            info_log.reset(new ResultHandling::MongoRocksLoggerForChunk(path));
            int enable_persisentce_cache = ConfigReader::getInstance()->getDecimalNumber<int>(
                "TableOptions", "enable_persistent_cache");
            if (enable_persisentce_cache) {
                index_log() << "[MongoRocks] NewPersistentCache path(" << file_path << ") size("
                            << persistent_cache_size << ") log ptr(" << (int64_t)(info_log.get())
                            << ")";
                ret = rocksdb::NewPersistentCache(rocksdb::Env::Default(), file_path,
                                                  persistent_cache_size, info_log, true,
                                                  &_persistent_cache);
                if (ret != rocksdb::Status::OK()) {
                    index_LOG(3) << "[MongoRocks]NewPersistentCache failed. may be ERROR"
                                 << ret.ToString();
                }
            }
        }
        _maxWriteMBPerSec = rocksGlobalOptions.maxWriteMBPerSec;
        _rateLimiter.reset(
            rocksdb::NewGenericRateLimiter(static_cast<int64_t>(_maxWriteMBPerSec) * 1024 * 1024));
        if (rocksGlobalOptions.counters) {
            _statistics = rocksdb::CreateDBStatistics();
        } else {
            index_warning() << "[Init] not init rocksGlobalOptions.counters!";
        }

        Init();
    }

    RocksEngine::~RocksEngine() {
        cleanShutdown();
        _db.reset();
    }

    void RocksEngine::Init() {
        index_log() << "configInfo: " << getConfigInfo();
        index_log() << "[MongoRocks] Start initialization of RocksEngine....";
        if (ClusterRole::ShardServer == serverGlobalParams.clusterRole) {
            std::string dbpath = _path + ROCKSDB_PATH;
            deleteDir(dbpath);
        }
        //  Always should be able to open system DB to start
        _db = stdx::make_unique<ChunkRocksDBInstance>(_path, nullptr);
        invariantOK(OpenDB(_path, _db.get()));
        LoadMetadata();

        index_log() << "[MongoRocks] RocksEngine initialized";
    }

    Status RocksEngine::OpenDB(const std::string& path, ChunkRocksDBInstance* dbInstance,
                                      const CollectionOptions* options) const {
        index_log()<<"this is sys db.";
        // TODO remove after the dbpath is no longer used by RocksDB
        std::string pathWithoutPlog = path;
        if (pathWithoutPlog.find("plogcnt") != std::string::npos) {
            pathWithoutPlog.assign(pathWithoutPlog, 0, pathWithoutPlog.find_last_of('/'));
        }
        auto db_options = _options(path);
        db_options.compaction_filter_factory.reset(new PrefixDeletingCompactionFilterFactory(this));
        MDB_RIF(dbInstance->Open(db_options, pathWithoutPlog, _durable, options));

        return mongo::Status::OK();
    }

    void RocksEngine::LoadMetadata() {
        auto mdb = getMetadataDB();
        if (!mdb) return;

        // open iterator
        std::unique_ptr<rocksdb::Iterator> iter(mdb->NewIterator(rocksdb::ReadOptions()));

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

                index_log() << "add one collection to _identmap,ident:" << ident.data()
                            << ",value:" << identConfig << ".";
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
            std::unique_ptr<rocksdb::Iterator> prefixIter(mdb->NewIterator(rocksdb::ReadOptions()));
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
                auto s = mdb->Write(rocksdb::WriteOptions(), &wb);
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

        index_LOG(5) << "create Record return " << s.codeString() << ",reason:" << s.reason()
                     << ",ns: " << ns.toString() << ", ident:" << ident.toString() << ".";
        return s;
    }

    std::unique_ptr<RecordStore> RocksEngine::getRecordStore(OperationContext* opCtx, StringData ns,
                                                             StringData ident,
                                                             const CollectionOptions& options) {
        if (NamespaceString::oplog(ns)) {
            _oplogIdent = ident.toString();
        }

        auto config = _getIdentConfig(ident);
        std::string prefix = _extractPrefix(config);

        std::unique_ptr<RocksRecordStore> recordStore =
            populateRecordStore(ns, ident, prefix, options);
        if (nullptr == recordStore) {
            Status s(ErrorCodes::InternalError, "init record store fail");
            // invariantRocksOK(s);
        }
        recordStore->Init();

        if (NamespaceString(ns).isSystemCollection()) {
            stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
            _identCollectionMap[ident] = recordStore.get();
        }

        return std::move(recordStore);
    }

    std::unique_ptr<RocksRecordStore> RocksEngine::populateRecordStore(
        StringData ns, StringData ident, const std::string& prefix,
        const CollectionOptions& options) {
        return options.capped ? stdx::make_unique<RocksRecordStore>(
                                    ns, ident, _db.get(), prefix, true,
                                    options.cappedSize ? options.cappedSize : 4096,  // default size
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

    RocksIndexBase* RocksEngine::populateIndex(StringData ident, const IndexDescriptor* desc,
                                               const std::string& prefix, BSONObj&& config) {
        return populateIndex(_db.get(), ident.toString(), desc, prefix, std::move(config));
    }

    RocksIndexBase* RocksEngine::populateIndex(mongo::ChunkRocksDBInstance* db,
                                               const std::string& ident,
                                               const IndexDescriptor* desc,
                                               const std::string& prefix, BSONObj&& config) {
        if (desc->unique())
            return new RocksUniqueIndex(db, prefix, ident, Ordering::make(desc->keyPattern()),
                                        std::move(config));

        //
        //  Non unique
        //
        RocksStandardIndex* si = new RocksStandardIndex(
            db, prefix, ident, Ordering::make(desc->keyPattern()), std::move(config));
        if (rocksGlobalOptions.singleDeleteIndex) si->enableSingleDelete();

        return si;
    }

    // cannot be rolled back
    Status RocksEngine::dropIdent(OperationContext* opCtx, StringData ident, StringData ns) {
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
            index_log() << "drop ident from identmap,ident:" << ident.toString() << ".";
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
            s = rocksdb::experimental::SuggestCompactRange(getMetadataDB(), &start_prefix,
                                                           &end_prefix);
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
        // if (_db->GetJournalFlusher()) {
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

    void RocksEngine::resetEngineStats() {
        auto stats = getStatistics();
        if (stats) {
            stats->Reset();  // wait rocksdb interface
        }
    }

    void RocksEngine::getEngineStats(std::vector<std::string>& vs) {
        auto stats = getStatistics();
        if (stats) {
            vs = stats->ToFormatString();  // wait rocksdb interface
        }
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
        auto mdb = getMetadataDB();
        if (mdb == nullptr) {
            index_err() << "getMetadataDB failed.";
            return Status(ErrorCodes::InternalError, "getMetadataDB failed.");
        }

        auto s = mdb->Put(rocksdb::WriteOptions(), kMetadataPrefix + ident.toString(),
                          rocksdb::Slice(config.objdata(), config.objsize()));

        if (s.ok()) {
            // As an optimization, add a key <prefix> to the DB
            std::string encodedPrefix(encodePrefix(prefix));
            s = mdb->Put(rocksdb::WriteOptions(), encodedPrefix, rocksdb::Slice());
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

    void RocksEngine::_setFixRocksTableOptions(
        rocksdb::BlockBasedTableOptions& table_options) const {
        table_options.cache_index_and_filter_blocks = true;                     //?????false
        table_options.cache_index_and_filter_blocks_with_high_priority = true;  //?????false
        table_options.pin_l0_filter_and_index_blocks_in_cache = true;           //?????false
        table_options.skip_table_builder_flush = true;
        table_options.no_block_cache = false;
        table_options.block_cache = _block_cache;
        if (nullptr != _persistent_cache) {
            table_options.persistent_cache = _persistent_cache;
        } else {
            index_LOG(3) << "[MongoRocks]RocksEngine not init the persistent cache  may be a ERROR";
        }

        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        table_options.block_size = 16 * 1024;  // 16KB
        table_options.format_version = 2;
    }

    void RocksEngine::_setRocksTableOptionWithFlavor(
        rocksdb::BlockBasedTableOptions& table_options) const {}

    void RocksEngine::_setRocksOptionsWithFlavor(rocksdb::Options& options) const {
        options.write_buffer_manager = _write_buf;
        options.max_background_compactions = ConfigReader::getInstance()->getDecimalNumber<int>(
            "DBOptions", "max_background_compactions");
        options.base_background_compactions = ConfigReader::getInstance()->getDecimalNumber<int>(
            "DBOptions", "base_background_compactions");
        options.max_background_flushes = ConfigReader::getInstance()->getDecimalNumber<int>(
            "DBOptions", "max_background_flushes");
        if (ConfigReader::getInstance()->getDecimalNumber<int>("CFOptions", "enable_row_cache")) {
            options.row_cache = _row_cache;
        }
    }

    void RocksEngine::_setFixRocksOptions(rocksdb::Options& options) const {
        // DBOptions
        options.avoid_flush_during_recovery = false;
        options.max_file_opening_threads = 16;
        options.use_direct_writes = false;
        // options.access_hint_on_compaction_start = rocksdb::AccessHint::NORMAL;
        options.create_if_missing = true;
        options.allow_fallocate = false;
        options.disableDataSync = false;
        options.paranoid_checks = true;
        options.delayed_write_rate = 2097152;
        options.create_missing_column_families = true;
        options.use_direct_reads = false;
        // options.max_log_file_size = 0; rocksdb????o1??????o|??DEBUG????????????o???2??oy?T???
        options.is_fd_close_on_exec = true;
        options.random_access_max_buffer_size = 1048576;
        options.delete_obsolete_files_period_micros = 21600000000;
        options.enable_thread_tracking = true;
        options.error_if_exists = false;
        options.new_table_reader_for_compaction_inputs = true;
        options.skip_log_error_on_recovery = false;
        options.avoid_flush_during_shutdown = false;
        options.skip_stats_update_on_db_open = false;
        options.compaction_readahead_size = 256 * 1024;
        // options.disable_data_sync = false;
        options.writable_file_max_buffer_size = 1048576;
        options.allow_mmap_reads = false;
        options.allow_mmap_writes = false;
        options.use_adaptive_mutex = true;
        options.use_fsync = false;
        // options.db_log_dir   //????o1???rocksdb???|?aDEBUG????????????o?2??|????
        options.max_open_files = -1;
        options.table_cache_numshardbits = 6;
        options.db_write_buffer_size = 0;  //??2??oy???DB??12??a2??oy?o?|??????????????T???a?????o?
        options.allow_concurrent_memtable_write = true;
        // options.recycle_log_file_num=0; //????o1???rocksdb???|?aDEBUG????????????o?2??|????
        options.manifest_preallocation_size = 4194304;
        options.enable_write_thread_adaptive_yield = true;
        // options.WAL_ttl_seconds = 0; //??2??oy????Archived
        // Log?o?D?????a?????Y????Y??2????DD?????o?????3?|???????Y??1??23?
        // options.WAL_size_limit_MB = 0; //??2??oy????Archived
        // Log?o?D?????a?????Y????Y??2????DD????
        options.max_subcompactions = 2;
        options.dump_malloc_stats = false;
        options.bytes_per_sync = 1048576;
        options.max_manifest_file_size = 128 * 1024 * 1024;  // 128MB
        options.wal_bytes_per_sync = 0;
        options.wal_recovery_mode = rocksdb::WALRecoveryMode::kPointInTimeRecovery;
        // options.keep_log_file_num = 100; //????o1???rocksdb???|?aDEBUG????????????o?2??|????
        options.max_total_wal_size = 0;  // 0??a?o?memtable??1????D?|??4???o?
        options.stats_dump_period_sec = 600;
        options.fail_if_options_file_error = false;
        options.write_thread_slow_yield_usec = 3;
        options.write_thread_max_yield_usec = 100;
        options.advise_random_on_open = true;
        options.target_file_size_multiplier = 1;
        //options.max_bytes_for_level_base = 640 * 1024 * 1024;
        options.level_compaction_dynamic_level_bytes = false;
        // CF options
        options.write_buffer_size = ConfigReader::getInstance()->getDecimalNumber<int>(
            "CFOptions", "data_write_buffer_size");
        options.max_write_buffer_number = ConfigReader::getInstance()->getDecimalNumber<int>(
            "CFOptions", "data_max_write_buffer_number");
        options.min_write_buffer_number_to_merge = 1;
        options.max_write_buffer_number_to_maintain = 0;
        //options.num_levels = 3;
        options.level0_file_num_compaction_trigger = 4;
        options.level0_slowdown_writes_trigger = 20;
        options.level0_stop_writes_trigger = 128;
        options.target_file_size_base = 64 * 1024 * 1024;
        options.max_bytes_for_level_multiplier = 10;
        options.soft_pending_compaction_bytes_limit = 8 * 1024 * 1024 * 1024ull;
        options.hard_pending_compaction_bytes_limit = 12 * 1024 * 1024 * 1024ull;
        // options.arena_block_size =  //??????|??o?write_buffer_size*1/8
        options.disable_auto_compactions = false;
        options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleLevel;
        options.verify_checksums_in_compaction = true;
        options.inplace_update_support = false;
        options.optimize_filters_for_hits = true;
        options.report_bg_io_stats = true;
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
    }

    rocksdb::Options RocksEngine::_options(const std::string& dbPath) const {
        rocksdb::Options options;
        rocksdb::BlockBasedTableOptions table_options;
        _setFixRocksOptions(options);
        _setRocksOptionsWithFlavor(options);
        _setFixRocksTableOptions(table_options);
        _setRocksTableOptionWithFlavor(table_options);
        options.table_factory.reset(rocksdb::NewFilterBlockTableFactory(table_options));
        options.statistics = _statistics;

        options.wal_dir = dbPath + ROCKSDB_PATH + "/journal";
        index_log() << "rocksdb wal path: " << options.wal_dir;

        // allow override
        if (!rocksGlobalOptions.configString.empty()) {
            rocksdb::Options base_options(options);
            auto s = rocksdb::GetOptionsFromString(base_options, rocksGlobalOptions.configString,
                                                   &options);
            if (!s.ok()) {
                index_err() << "Invalid rocksdbConfigString ";
                invariantRocksOK(s);
            }
        }

        //  Set the logger for database
        options.info_log =
            std::shared_ptr<rocksdb::Logger>(new ResultHandling::MongoRocksLoggerForChunk(dbPath));

        options.env = initEnv();
        return options;
    }

    bool RocksEngine::IsSystemCollection(const StringData& ns) {
        if (ns.compare("_mdb_catalog") == 0) return true;

        mongo::NamespaceString nameSpace(ns);
        return nameSpace.isSystemCollection();
        // return nameSpace.isOnInternalDb();
    }

    Status RocksEngine::endSnapshot() {
        index_log() << "entry endSnapshot ";
        stdx::unique_lock<stdx::mutex> pauseLock(pauseMutex);
        if (pauseState == kRunning) {
            index_log() << "repeate endSnapshot!";
            // return Status::OK();
        }

        bool isSystemDone = false;
        {
            stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
            for (auto recordStoreEntry : _identCollectionMap) {
                // start compaction
                bool isSystem = recordStoreEntry.second->GetChunkDBInstance().isSystemChunk();
                std::string ns = isSystem ? "System" : recordStoreEntry.second->ns();
                if (isSystem && isSystemDone) {
                    continue;
                }

                // start compaction
                rocksdb::Status status = recordStoreEntry.second->GetChunkDBInstance()
                                             .GetDB()
                                             ->ContinueBackgroundCompation();
                if (!status.ok()) {
                    index_err() << "ContinueBackgroundCompation Failed with " << status.ToString();
                    // return rocksToMongoStatus(status);
                }
                index_log() << "ContinueBackgroundCompation Success";

                // start file deletion
                status =
                    recordStoreEntry.second->GetChunkDBInstance().GetDB()->EnableFileDeletions();
                if (!status.ok()) {
                    index_err() << "EnableFileDeletions Failed with " << status.ToString();
                    // return rocksToMongoStatus(status);
                }
                index_log() << "EnableFileDeletions Success";

                if (isSystem && !isSystemDone) {
                    isSystemDone = true;
                }
            }
        }

        // start gc
        if (!continueGC()) {
            index_warning() << "[prepareSnapshot] pauseGC faild!";
        }

        pauseState = kRunning;
        return Status::OK();
    }

    Status RocksEngine::prepareSnapshot(BSONObj& cmdObj, BSONObjBuilder& result) {
        index_log() << "entry prepareSnapshot ";

        stdx::unique_lock<stdx::mutex> pauseLock(pauseMutex);

        if (pauseState == kPause) {
            return Status::OK();
        }

        // stop gc
        if (!pauseGC()) {
            index_err() << "[prepareSnapshot] pauseGC faild!";
            continueGC();
            return Status(ErrorCodes::OperationFailed, str::stream() << "pause GC faild! ");
        }

        BSONObjBuilder temp(result.subobjStart("walPaths"));
        bool isSystemDone = false;
        bool needRollBack = false;
        BSONElement elementRepairDBWAL = cmdObj["repairDBWAL"];
        bool needRepairWal = elementRepairDBWAL.eoo() ? false : true;
        rocksdb::Status status;
        StringMap<RocksRecordStore*> _disableFileDeletionsRollBackMap;
        StringMap<RocksRecordStore*> _pauseBackgroundCompationRollBackMap;
        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
        for (auto recordStoreEntry : _identCollectionMap) {
            bool isSystem = recordStoreEntry.second->GetChunkDBInstance().isSystemChunk();
            if (isSystem && isSystemDone) {
                continue;
            }

            // stop file deletion
            status = recordStoreEntry.second->GetChunkDBInstance().GetDB()->DisableFileDeletions();
            if (!status.ok()) {
                index_err() << "DisableFileDeletions Failed with " << status.ToString();
                needRollBack = true;
                break;
            }
            _disableFileDeletionsRollBackMap[recordStoreEntry.first] = recordStoreEntry.second;
            index_log() << "DisableFileDeletions Success";
            // stop compaction
            status =
                recordStoreEntry.second->GetChunkDBInstance().GetDB()->PauseBackgroundCompation();
            if (!status.ok()) {
                index_err() << "PauseBackgroundCompation Failed with " << status.ToString();
                needRollBack = true;
                break;
            }
            _pauseBackgroundCompationRollBackMap[recordStoreEntry.first] = recordStoreEntry.second;
            index_log() << "PauseBackgroundCompation Success";

            // repair wal
            if (needRepairWal) {
                status = recordStoreEntry.second->GetChunkDBInstance().GetDB()->RecoverSyncOldWal();
                if (!status.ok()) {
                    index_err() << "RepairWal Failed with " << status.ToString();
                    needRollBack = true;
                    break;
                }
                index_log() << "RepairWal Success";
            }
            // sync wal
            uint64_t lastWalNumber = 0;
            status = recordStoreEntry.second->GetChunkDBInstance().GetDB()->SwitchAndSyncWal(
                lastWalNumber);
            if (!status.ok()) {
                index_err() << "SwitchAndSyncWal Failed with " << status.ToString();
                needRollBack = true;
                break;
            }
            index_log() << "SwitchAndSyncWal Success " << lastWalNumber;

            if (isSystem && !isSystemDone) {
                isSystemDone = true;
            }

            temp.append(recordStoreEntry.second->GetChunkDBInstance().getPath(),
                        std::to_string(lastWalNumber) + ".log");
        }

        if (needRollBack) {
            index_err() << "entry prepareSnapshot rollback";
            for (auto recordStoreEntry : _disableFileDeletionsRollBackMap) {
                recordStoreEntry.second->GetChunkDBInstance().GetDB()->EnableFileDeletions();
            }
            for (auto recordStoreEntry : _pauseBackgroundCompationRollBackMap) {
                recordStoreEntry.second->GetChunkDBInstance()
                    .GetDB()
                    ->ContinueBackgroundCompation();
            }

            continueGC();

            return rocksToMongoStatus(status);
        }

        pauseState = kPause;
        return Status::OK();
    }

    Status RocksEngine::compact() {
        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
        for (auto recordStoreEntry : _identCollectionMap) {
            bool isSystem = recordStoreEntry.second->GetChunkDBInstance().isSystemChunk();
            if (isSystem) {
                continue;
            }

            recordStoreEntry.second->GetChunkDBInstance().compact();
        }

        return Status::OK();
    }

    Status RocksEngine::rmCollectionDir(OperationContext* txn, const std::string& path) {
        return rocksToMongoStatus(deleteDir(path));
    }

    Status RocksEngine::dropFromIdentCollectionMap(const mongo::StringData& ident) {
        rocksdb::WriteBatch wb;
        wb.Delete(kMetadataPrefix + ident.toString());
        rocksdb::WriteOptions syncOptions;
        syncOptions.sync = true;

        auto db = getMetadataDB();
        if (db == nullptr) {
            index_err() << "getMetadataDB failed.";
            return Status(ErrorCodes::InternalError, "getMetadataDB failed.");
        }
        MDB_RIF(db->Write(syncOptions, &wb));
        {
            stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
            _identCollectionMap.erase(ident);
        }

        return Status::OK();
    }

    void RocksEngine::setStorageEngineLogLevel(int level) {
        rocksdb::InfoLogLevel rlevel;
        switch (level) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
                rlevel = rocksdb::InfoLogLevel::DEBUG_LEVEL;
                break;
            case -1:
                rlevel = rocksdb::InfoLogLevel::INFO_LEVEL;
                break;
            case -2:
                rlevel = rocksdb::InfoLogLevel::WARN_LEVEL;
                break;
            case -3:
                rlevel = rocksdb::InfoLogLevel::ERROR_LEVEL;
                break;
            case -4:
                rlevel = rocksdb::InfoLogLevel::FATAL_LEVEL;
                break;
            default:
                rlevel = rocksdb::InfoLogLevel::HEADER_LEVEL;
        }

        index_LOG(0) << "[setLog] PartitionedRocksEngine::setStorageEngineLogLevel newLevel: "
                     << level;

        ResultHandling::MongoRocksLoggerForChunk::setLogLevel(rlevel);

        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
        for (auto it = _identCollectionMap.begin(); it != _identCollectionMap.end(); ++it) {
            if (it->second->GetChunkDBInstance().getLogger()) {
                it->second->GetChunkDBInstance().getLogger()->SetInfoLogLevel(rlevel);
            }
        }
    }

    void  RocksEngine::getTransactionEngineStat(long long& numKey, long long& numActiveSnapshot) {
        numKey = static_cast<long long>(getTransactionEngine()->numKeysTracked());
        numActiveSnapshot = static_cast<long long>(getTransactionEngine()->numActiveSnapshots());
    }

    std::string RocksEngine::getConfigInfo() const {
        mongo::BSONObjBuilder builder;
        for(auto it=ConfigReader::getInstance()->getConfigMap().begin(); 
            it!=ConfigReader::getInstance()->getConfigMap().end(); it++) {
            builder.append(it->first.key, it->second);
        }
        return builder.obj().toString();
    }
}

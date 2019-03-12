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

#pragma once

#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>

#include <boost/optional.hpp>

#include <rocksdb/cache.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/statistics.h>
#include <rocksdb/status.h>
#include "rocksdb/options.h"
#include "rocksdb/table.h"

#include "mongo/base/disallow_copying.h"
#include "mongo/bson/ordering.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/util/string_map.h"

#include "rocks_compaction_scheduler.h"
#include "rocks_counter_manager.h"
#include "rocks_durability_manager.h"
#include "rocks_snapshot_manager.h"
#include "rocks_transaction.h"

#include "Chunk/ChunkRocksDBInstance.h"

namespace rocksdb {
    class ColumnFamilyHandle;
    struct ColumnFamilyDescriptor;
    struct ColumnFamilyOptions;
    class DB;
    class Comparator;
    class Iterator;
    struct Options;
    struct ReadOptions;
    struct RootPlogInfo;
}

namespace mongo {

    const std::string ROCKSDB_PATH = "/db";
    const std::string TRANS_LOG_PATH = "/translog";
    const std::string CHUNK_META_PATH = "/chunkmeta";

    struct CollectionOptions;
    class RocksIndexBase;
    class RocksRecordStore;
    class JournalListener;
    class ChunkJounalFlush;

    inline rocksdb::Env* initEnv() {
        rocksdb::Status status;
        rocksdb::Env* env = nullptr;
        status = rocksdb::NewHdfsEnv(&env, GLOBAL_CONFIG_GET(hdfsUri));

        invariant(status.ok());
        invariant(env != nullptr);
        return env;
    }

    inline rocksdb::Env* getGlobalEnv() {
        static rocksdb::Env* env = nullptr;
        if (!env) {
            env = initEnv();
        }
        return env;
    }

    inline rocksdb::Status createDir(const std::string& path) {
        return getGlobalEnv()->CreateDirIfMissing(path);
        

        std::string filepath = path;
        size_t pos = 1;

        while (true) {
            if (pos == std::string::npos) {
                filepath = path;
                pos = path.size();
            } else {
                filepath.assign(path, 0, path.find("/", pos));
            }

            auto s = getGlobalEnv()->FileExists(filepath);
            if (s == rocksdb::Status::NotFound()) {
                s = getGlobalEnv()->CreateDir(filepath);
                if (!s.ok()) {
                    s = getGlobalEnv()->FileExists(filepath);
                    if (!s.ok()) {
                        return s;
                    }
                }
            } else if (!s.ok()) {
                return s;
            }

            pos += 1;
            if (pos >= path.length()) {
                break;
            }

            pos = path.find("/", pos);
        }

        return rocksdb::Status::OK();
    }

    inline rocksdb::Status deleteDir(const std::string& path) {
        return getGlobalEnv()->DeleteDir(path);
    }

    inline rocksdb::Status deleteFile(const std::string& file) {
        auto status = getGlobalEnv()->DeleteFile(file);
        if (!status.ok()) {
            if (status.ToString().find("No such file or directory") != std::string::npos ||
                status.ToString().find("Success") != std::string::npos) {
                    return rocksdb::Status::OK();
            } else {
                return status;
            }
        }

        return rocksdb::Status::OK();
    }

    inline uint32_t decodePrefix(const rocksdb::Slice& key) {
        return endian::bigToNative(*(uint32_t*)key.data());
    }

    inline std::string encodePrefix(uint32_t prefix) {
        uint32_t bigEndianPrefix = endian::nativeToBig(prefix);
        return std::string(reinterpret_cast<const char*>(&bigEndianPrefix), sizeof(uint32_t));
    }
    inline bool extractPrefix(const rocksdb::Slice& slice, uint32_t* prefix) {
        if (slice.size() < sizeof(uint32_t)) {
            return false;
        }
        *prefix = endian::bigToNative(*reinterpret_cast<const uint32_t*>(slice.data()));
        return true;
    }

    class RocksEngine : public KVEngine {
        MONGO_DISALLOW_COPYING(RocksEngine);

    public:
        RocksEngine(const std::string& path, bool durable, int formatVersion);

        virtual ~RocksEngine();

        virtual RecoveryUnit* newRecoveryUnit() override;

        virtual Status createRecordStore(OperationContext* opCtx, StringData ns, StringData ident,
                                         const CollectionOptions& options) override;

        virtual std::unique_ptr<RecordStore> getRecordStore(
            OperationContext* opCtx, StringData ns, StringData ident,
            const CollectionOptions& options) override;

        virtual Status createSortedDataInterface(OperationContext* opCtx, StringData ident,
                                                 const IndexDescriptor* desc) override;

        virtual SortedDataInterface* getSortedDataInterface(OperationContext* opCtx,
                                                            StringData ident,
                                                            const IndexDescriptor* desc) override;

        virtual Status dropIdent(OperationContext* opCtx, StringData ident, StringData ns=StringData()) override;

        virtual bool hasIdent(OperationContext* opCtx, StringData ident) const override;

        virtual std::vector<std::string> getAllIdents(OperationContext* opCtx) const override;

        virtual bool supportsDocLocking() const override { return true; }

        virtual bool supportsDirectoryPerDB() const override { return false; }

        virtual int flushAllFiles(bool sync) override;

        virtual Status beginBackup(OperationContext* txn) override;

        virtual void endBackup(OperationContext* txn) override;

        virtual bool isDurable() const override { return _durable; }

        virtual bool isEphemeral() const override { return false; }

        virtual int64_t getIdentSize(OperationContext* opCtx, StringData ident);

        virtual Status repairIdent(OperationContext* opCtx, StringData ident) {
            return Status::OK();
        }

        virtual void cleanShutdown();

        virtual SnapshotManager* getSnapshotManager() const final {
            return (SnapshotManager*)&_snapshotManager;
        }

        virtual void resetEngineStats() override;

        virtual void getEngineStats(std::vector<std::string>& vs) override;
        /**
         * Initializes a background job to remove excess documents in the oplog collections.
         * This applies to the capped collections in the local.oplog.* namespaces (specifically
         * local.oplog.rs for replica sets and local.oplog.$main for master/slave replication).
         * Returns true if a background job is running for the namespace.
         */
        static bool initRsOplogBackgroundThread(StringData ns);

        virtual void setJournalListener(JournalListener* jl);

        // rocks specific api

        rocksdb::DB* getDB() { return _db.get()->GetDB(); }
        const rocksdb::DB* getDB() const { return _db.get()->GetDB(); }

        virtual rocksdb::DB* getMetadataDB() { return getDB(); }
        virtual const rocksdb::DB* getMetadataDB() const { return getDB(); }

        size_t getBlockCacheUsage() const { return _block_cache->GetUsage(); }
        std::shared_ptr<rocksdb::Cache> getBlockCache() { return _block_cache; }
        std::unordered_set<uint32_t> getDroppedPrefixes() const;

        RocksTransactionEngine* getTransactionEngine() { return _db->GetTransactionEngine(); }

        int getMaxWriteMBPerSec() const { return _maxWriteMBPerSec; }
        void setMaxWriteMBPerSec(int maxWriteMBPerSec);

        Status backup(const std::string& path);

        rocksdb::Statistics* getStatistics() const { return _statistics.get(); }

        virtual Status prepareSnapshot(BSONObj& cmdObj, BSONObjBuilder& result);

        virtual Status compact() override;

        virtual Status endSnapshot();

        virtual Status rmCollectionDir(OperationContext* txn, const std::string& path);

        virtual Status dropFromIdentCollectionMap(const mongo::StringData& ident) override;

        virtual void setStorageEngineLogLevel(int level) override;

        static bool IsSystemCollection(const StringData& ns);

        virtual Status destroyRocksDB(const std::string& nss) { return Status::OK(); }

        virtual Status reNameNss(const std::string& srcName, const std::string& destName) {
            return Status::OK();
        }

        virtual void getTransactionEngineStat(long long& numKey, long long& numActiveSnapshot);

        std::string getConfigInfo() const;

    private:
        void _setFixRocksOptions(rocksdb::Options& options) const;
        void _setFixRocksTableOptions(rocksdb::BlockBasedTableOptions& table_options) const;
        void _setRocksOptionsWithFlavor(rocksdb::Options& options) const;
        void _setRocksTableOptionWithFlavor(rocksdb::BlockBasedTableOptions& table_options) const;

    protected:
        virtual std::unique_ptr<RocksRecordStore> populateRecordStore(
            StringData ns, StringData ident, const std::string& prefix,
            const CollectionOptions& options);

        virtual RocksIndexBase* populateIndex(StringData ident, const IndexDescriptor* desc,
                                              const std::string& prefix, BSONObj&& config);

        RocksIndexBase* populateIndex(mongo::ChunkRocksDBInstance* db, const std::string& ident,
                                      const IndexDescriptor* desc, const std::string& prefix,
                                      BSONObj&& config);

        void Init();

        virtual bool pauseGC(void) { return true; }
        virtual void stopGC(void) {}
        virtual bool continueGC(void) { return true; }
        virtual void setGCInfo(const std::string& ns, const std::string& dataPath) {}
        virtual Status openChunkDbInstance(OperationContext* txn, StringData ns,
                                           const mongo::CollectionOptions& options) {
            return Status::OK();
        }

        virtual Status OpenDB(const std::string& path, ChunkRocksDBInstance* db,
                             const CollectionOptions* options = nullptr) const;
        void LoadMetadata();

        Status _createIdent(StringData ident, BSONObjBuilder* configBuilder);
        BSONObj _getIdentConfig(StringData ident);
        std::string _extractPrefix(const BSONObj& config);

        rocksdb::Options _options(const std::string& dbPath) const;

        std::string _path;
        std::unique_ptr<mongo::ChunkRocksDBInstance> _db;

        // shard or config svr memory plan
        std::shared_ptr<rocksdb::Cache> _block_cache = nullptr;
        std::shared_ptr<rocksdb::Cache> _row_cache = nullptr;
        std::shared_ptr<rocksdb::WriteBufferManager> _write_buf = nullptr;
        std::shared_ptr<rocksdb::Cache> _block_cache_compressed = nullptr;
        std::shared_ptr<rocksdb::Cache> _table_cache = nullptr;
        // std::shared_ptr<BufferSizeCtrl> _bufferSizeCtrl = nullptr;
        std::shared_ptr<rocksdb::PersistentCache> _persistent_cache = nullptr;

        int _maxWriteMBPerSec;
        std::shared_ptr<rocksdb::RateLimiter> _rateLimiter;
        // can be nullptr
        std::shared_ptr<rocksdb::Statistics> _statistics;

        const bool _durable;
        const int _formatVersion;

        // ident map stores mapping from ident to a BSON config
        mutable stdx::mutex _identMapMutex;
        typedef StringMap<BSONObj> IdentMap;
        IdentMap _identMap;
        std::string _oplogIdent;

        // protected by _identMapMutex
        uint32_t _maxPrefix;

        // _identObjectMapMutex protects both _identIndexMap and _identCollectionMap. It should
        // never be locked together with _identMapMutex
        mutable stdx::mutex _identObjectMapMutex;
        // mapping from ident --> index object. we don't own the object
        StringMap<RocksIndexBase*> _identIndexMap;
        // mapping from ident --> collection object
        StringMap<RocksRecordStore*> _identCollectionMap;

        // set of all prefixes that are deleted. we delete them in the background thread
        mutable stdx::mutex _droppedPrefixesMutex;
        std::unordered_set<uint32_t> _droppedPrefixes;

        RocksSnapshotManager _snapshotManager;

        static const std::string kMetadataPrefix;
        static const std::string kDroppedPrefix;

        enum State {
            kRunning,  // kRunning
            kPause,
        };

        std::atomic<State> pauseState{kRunning};
        stdx::mutex pauseMutex;

    };
}

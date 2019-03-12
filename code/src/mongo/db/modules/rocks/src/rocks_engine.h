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
#include <string>
#include <memory>
#include <unordered_set>

#include <boost/optional.hpp>

#include <rocksdb/cache.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/status.h>
#include <rocksdb/statistics.h>
#include "rocksdb/options.h"
#include "rocksdb/table.h"

#include "mongo/base/disallow_copying.h"
#include "mongo/bson/ordering.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/util/string_map.h"

#include "rocks_compaction_scheduler.h"
#include "rocks_counter_manager.h"
#include "rocks_transaction.h"
#include "rocks_snapshot_manager.h"
#include "rocks_durability_manager.h"

#include "Chunk/ChunkRocksDBInstance.h"
#include "config/config_reader.h"

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

    const std::string ROCKSDB_PATH = "/dfvrocks";
    const std::string TRANS_LOG_PATH = "/translog";
    const std::string CHUNK_META_PATH = "/chunkmeta";

    struct CollectionOptions;
    class RocksIndexBase;
    class RocksRecordStore;
    class JournalListener;

    extern rocksdb::Status createDir(rocksdb::Env* fileSystem, const std::string& path);
    extern rocksdb::Status deleteDir(const std::string& path);
    extern bool extractPrefix(const rocksdb::Slice& slice, uint32_t* prefix);

    class RocksEngine : public KVEngine {
        MONGO_DISALLOW_COPYING( RocksEngine );
    public:
        RocksEngine(const std::string& path, bool durable, int formatVersion);

        virtual ~RocksEngine();

        virtual RecoveryUnit* newRecoveryUnit() override;

        virtual Status createRecordStore(OperationContext* opCtx,
                                         StringData ns,
                                         StringData ident,
                                         const CollectionOptions& options) override;

        virtual std::unique_ptr<RecordStore> getRecordStore(OperationContext* opCtx, StringData ns,
                                            StringData ident,
                                            const CollectionOptions& options) override;

        virtual Status createSortedDataInterface(OperationContext* opCtx, StringData ident,
                                                 const IndexDescriptor* desc) override;

        virtual SortedDataInterface* getSortedDataInterface(OperationContext* opCtx,
                                                            StringData ident,
                                                            const IndexDescriptor* desc) override;

        virtual Status dropIdent(OperationContext* opCtx, StringData ident) override;

        virtual bool hasIdent(OperationContext* opCtx, StringData ident) const override;

        virtual std::vector<std::string> getAllIdents( OperationContext* opCtx ) const override;

        virtual bool supportsDocLocking() const override {
            return true;
        }

        virtual bool supportsDirectoryPerDB() const override {
            return false;
        }

        virtual int flushAllFiles(bool sync) override;

        virtual Status beginBackup(OperationContext* txn) override;

        virtual void endBackup(OperationContext* txn) override;

        virtual bool isDurable() const override { return _durable; }

        virtual bool isEphemeral() const override { return false; }

        virtual int64_t getIdentSize(OperationContext* opCtx, StringData ident);

        virtual Status repairIdent(OperationContext* opCtx,
                                    StringData ident) {
            return Status::OK();
        }

        virtual void cleanShutdown();

        virtual SnapshotManager* getSnapshotManager() const final {
            return (SnapshotManager*) &_snapshotManager;
        }

        virtual void resetEngineStats() override;

        virtual void getEngineStats( std::vector<std::string> & vs) override;

        static rocksdb::CompressionType getCompressLevelByName(const std::string cmprsLvl);
        static uint32_t getAllConfig(rocksdb::Options &option, rocksdb::BlockBasedTableOptions & blkBasedTblOption);
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

        rocksdb::Statistics* getStatistics() const {
          return _statistics.get();
        }

        static bool IsSystemCollection(const StringData& ns);

        static bool IsPLogUsed(); 

        static bool IsStreamUsed();

        static std::string encodePrefix(uint32_t prefix);
    private:
        void _initMemPlan();
        void _initMemPlanForShardConfigSvr(unsigned long long systemSizeMB);


    protected:
        virtual std::unique_ptr<RocksRecordStore> populateRecordStore(StringData ns, StringData ident,
                const std::string& prefix, const CollectionOptions& options);

        virtual RocksIndexBase* populateIndex(
                    StringData ident,
                    const IndexDescriptor* desc,
                    const std::string& prefix,
                    BSONObj&& config
                    );

        RocksIndexBase* populateIndex(
                    mongo::ChunkRocksDBInstance* db,
                    const std::string& ident,
                    const IndexDescriptor* desc,
                    const std::string& prefix,
                    BSONObj&& config
                    );

        virtual void Init();

        mongo::Status OpenDB(const std::string& path, std::unique_ptr<mongo::ChunkRocksDBInstance>& db) const;
        void LoadMetadata();

        Status _createIdent(StringData ident, BSONObjBuilder* configBuilder);
        BSONObj _getIdentConfig(StringData ident);
        std::string _extractPrefix(const BSONObj& config);

        rocksdb::Options _options(const std::string& dbPath) const;

        std::string _path;
        std::unique_ptr<mongo::ChunkRocksDBInstance> _db;

        // shard or config svr memory plan
        std::shared_ptr<rocksdb::Cache> _block_cache = nullptr;
        std::shared_ptr<rocksdb::Cache> _block_cache_compressed = nullptr;
        std::shared_ptr<rocksdb::Cache> _table_cache = nullptr;

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

    };
}

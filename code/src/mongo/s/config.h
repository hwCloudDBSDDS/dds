/**
 *    Copyright (C) 2008-2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
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

#include <set>

#include "mongo/db/jsobj.h"
#include "mongo/db/repl/optime.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/client/shard.h"
#include "mongo/util/concurrency/mutex.h"
#include "mongo/util/concurrency/notification.h"

namespace mongo {

class ChunkManager;
class DatabaseType;
class DBConfig;
class OperationContext;

struct CollectionInfo {
    CollectionInfo() {
        _dirty = false;
        _dropped = false;
    }

    CollectionInfo(OperationContext* txn, const CollectionType& in, repl::OpTime);
    ~CollectionInfo();

    bool isSharded() const {
        return _cm.get();
    }

    std::shared_ptr<ChunkManager> getCM() const {
        return _cm;
    }
    CollectionType::TableType getCollTabType() const {
        return _tableType;
    }

    void resetCM(ChunkManager* cm);

    void unshard();

    bool isDirty() const {
        return _dirty;
    }

    bool wasDropped() const {
        return _dropped;
    }

    void save(OperationContext* txn, const std::string& ns);

    void useChunkManager(std::shared_ptr<ChunkManager> manager);

    bool unique() const {
        return _unique;
    }

    BSONObj key() const {
        return _key;
    }

    repl::OpTime getConfigOpTime() const {
        return _configOpTime;
    }

    void updateTrigrefreshCount() {
        _trigrefreshCount++;
        return;
    }

    void updateCmrefreshCount() {
        _cmrefreshCount++;
        return;
    }

    void getStatisticsofLoadingChunks(BSONObjBuilder& result);

    std::shared_ptr<Notification<Status>> refreshCompletionNotification;

private:
    BSONObj _key;
    bool _unique;
    std::shared_ptr<ChunkManager> _cm;
    bool _dirty;
    bool _dropped;
    repl::OpTime _configOpTime;
    CollectionType::TableType _tableType = CollectionType::TableType::kNonShard;
    // for refresh statistics
    uint64_t _trigrefreshCount;
    uint64_t _cmrefreshCount;
};

/**
 * top level configuration for a database
 */
class DBConfig {
public:
    DBConfig(std::string name, const DatabaseType& dbt, repl::OpTime configOpTime);
    ~DBConfig();

    /**
     * The name of the database which this entry caches.
     */
    const std::string& name() const {
        return _name;
    }

    /**
     * Whether sharding is enabled for this database.
     */
    bool isShardingEnabled();

    ShardId getPrimaryId();

    /**
     * Removes all cached metadata for the specified namespace so that subsequent attempts to
     * retrieve it will cause a full reload.
     */
    void invalidateNs(const std::string& ns);

    void enableSharding(OperationContext* txn);

    /**
       @return true if there was sharding info to remove
     */
    bool removeSharding(OperationContext* txn, const std::string& ns);

    /**
     * @return whether or not the 'ns' collection is partitioned
     */
    bool isSharded(const std::string& ns);
    int  getCollNums(){return _collections.size();}

    StatusWith<bool> isCollectionExist(OperationContext* txn,
                                       const std::string& ns,
                                       bool full = false);

    CollectionType::TableType getCollTabType(const std::string& ns);


    // Atomically returns *either* the chunk manager *or* the primary shard for the collection,
    // neither if the collection doesn't exist.
    Status getChunkManagerOrPrimary(OperationContext* txn,
                                    const std::string& ns,
                                    std::shared_ptr<ChunkManager>& manager,
                                    std::shared_ptr<Shard>& primary);

    std::shared_ptr<ChunkManager> getChunkManager(OperationContext* txn,
                                                  const std::string& ns,
                                                  bool reload = false);
    std::shared_ptr<ChunkManager> getChunkManagerIfExists(OperationContext* txn,
                                                          const std::string& ns,
                                                          bool reload = false);

    void setPrimary(OperationContext* txn, const ShardId& newPrimaryId);

    /**
     * Returns true if it is successful at loading the DBConfig, false if the database is not found,
     * and throws on all other errors.
     */
    bool load(OperationContext* txn);
    bool reload(OperationContext* txn);

    bool dropDatabase(OperationContext*, std::string& errmsg);

    void getAllShardIds(std::set<ShardId>* shardIds);
    void getAllShardedCollections(std::set<std::string>& namespaces);
    // get all non sharded collections
    void getAllNonShardedCollections(std::set<std::string>& namespaces);
    // we have to re-check after reload in case that other mongos already create a new sharded
    // collection
    bool isShardedAfterReload(OperationContext* txn, const std::string& ns);

    void getStat(BSONObjBuilder& result);

protected:
    typedef std::map<std::string, CollectionInfo> CollectionInfoMap;
    typedef AtomicUInt64::WordType Counter;

    bool _dropShardedCollections(OperationContext* txn,
                                 int& num,
                                 std::set<ShardId>& shardIds,
                                 std::string& errmsg);

    /**
     * Returns true if it is successful at loading the DBConfig, false if the database is not found,
     * and throws on all other errors.
     * Also returns true without reloading if reloadIteration is not equal to the _reloadCount.
     * This is to avoid multiple threads attempting to reload do duplicate work.
     */
    bool _loadIfNeeded(OperationContext* txn, Counter reloadIteration);

    void _chunkManagerRefresh(OperationContext* txn,
                              const std::string& ns,
                              const ChunkManager* oldManager);

    void _save(OperationContext* txn, bool db = true, bool coll = true);

    // All member variables are labeled with one of the following codes indicating the
    // synchronization rules for accessing them.
    //
    // (L) Must hold _lock for access.
    // (I) Immutable, can access freely.
    // (S) Self synchronizing, no explicit locking needed.
    //
    // Mutex lock order:
    // _hitConfigServerLock -> _lock
    //

    static const uint64_t SkipBackSteps;

    // Name of the database which this entry caches
    const std::string _name;  // (I)

    // Primary shard id
    ShardId _primaryId;  // (L)

    // Whether sharding has been enabled for this database
    bool _shardingEnabled;  // (L)

    // Set of collections and lock to protect access
    stdx::mutex _lock;
    CollectionInfoMap _collections;  // (L)

    // OpTime of config server when the database definition was loaded.
    repl::OpTime _configOpTime;  // (L)

    // Increments every time this performs a full reload. Since a full reload can take a very
    // long time for very large clusters, this can be used to minimize duplicate work when multiple
    // threads tries to perform full rerload at roughly the same time.
    AtomicUInt64 _reloadCount;  // (S)

    // for statistics
    uint64_t _collloadCount;
    uint64_t _collcachehitCount;
    uint64_t _colleraseCount;
};


class ConfigServer {
public:
    /**
     * For use in mongos and mongod which needs notifications about changes to shard and config
     * server replset membership to update the ShardRegistry.
     *
     * This is expected to be run in an existing thread.
     */
    static void replicaSetChangeShardRegistryUpdateHook(const std::string& setName,
                                                        const std::string& newConnectionString);

    /**
     * For use in mongos which needs notifications about changes to shard replset membership to
     * update the config.shards collection.
     *
     * This is expected to be run in a brand new thread.
     */
    static void replicaSetChangeConfigServerUpdateHook(const std::string& setName,
                                                       const std::string& newConnectionString);
};

}  // namespace mongo

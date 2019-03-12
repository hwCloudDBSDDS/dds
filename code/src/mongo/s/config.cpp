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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/s/config.h"

#include "mongo/client/connpool.h"
#include "mongo/db/client.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/write_concern.h"
#include "mongo/s/balancer_configuration.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/chunk_manager.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"

namespace mongo {

using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

const uint64_t DBConfig::SkipBackSteps = 100;

CollectionInfo::CollectionInfo(OperationContext* txn,
                               const CollectionType& coll,
                               repl::OpTime opTime)
    : _configOpTime(std::move(opTime)), _trigrefreshCount(0), _cmrefreshCount(0) {
    _dropped = coll.getDropped();

    // Do this *first* so we're invisible to everyone else
    std::unique_ptr<ChunkManager> manager(stdx::make_unique<ChunkManager>(txn, coll));
    manager->loadExistingRanges(txn, nullptr);

    // Collections with no chunks are unsharded, no matter what the collections entry says. This
    // helps prevent errors when dropping in a different process.
    if (manager->numChunks() != 0) {
        useChunkManager(std::move(manager));
    } else {
        // all collections of maas is sharded, this case isn't reached.
        warning() << "no chunks found for collection " << manager->getns()
                  << ", assuming unsharded";
        // unshard();
    }

    _dirty = false;
    _tableType = coll.getTabType();
}

CollectionInfo::~CollectionInfo() = default;

void CollectionInfo::resetCM(ChunkManager* cm) {
    invariant(cm);
    invariant(_cm);

    updateCmrefreshCount();
    _cm.reset(cm);
}

void CollectionInfo::unshard() {
    _cm.reset();
    _dropped = true;
    _dirty = true;
    _key = BSONObj();
}

void CollectionInfo::useChunkManager(std::shared_ptr<ChunkManager> manager) {
    _cm = manager;
    _key = manager->getShardKeyPattern().toBSON().getOwned();
    _unique = manager->isUnique();
    _dirty = true;
    _dropped = false;
}

void CollectionInfo::save(OperationContext* txn, const string& ns) {
    CollectionType coll;
    auto collStatus = grid.catalogClient(txn)->getCollection(txn, ns);
    if (collStatus.isOK()) {
        coll = collStatus.getValue().value;
    } else {
        index_err() << "[dropDatabase.save] getCollection error due to:" << collStatus.getStatus();
        // return collStatus.getStatus();
    }
    BSONArrayBuilder bb;
    coll.setIndex(bb.arr());
    coll.setNs(NamespaceString{ns});
    coll.setPrefix(1);
    if (_cm) {
        invariant(!_dropped);
        coll.setEpoch(_cm->getVersion().epoch());
        // TODO(schwerin): The following isn't really a date, but is stored as one in-memory and
        // in config.collections, as a historical oddity.
        coll.setUpdatedAt(Date_t::fromMillisSinceEpoch(_cm->getVersion().toLong()));
        coll.setKeyPattern(_cm->getShardKeyPattern().toBSON());
        coll.setDefaultCollation(
            _cm->getDefaultCollator() ? _cm->getDefaultCollator()->getSpec().toBSON() : BSONObj());
        coll.setUnique(_cm->isUnique());
    } else {
        invariant(_dropped);
        coll.setDropped(true);
        coll.setEpoch(ChunkVersion::DROPPED().epoch());
        coll.setUpdatedAt(Date_t::now());
    }

    uassertStatusOK(grid.catalogClient(txn)->updateCollection(txn, ns, coll));
    _dirty = false;
}

void CollectionInfo::getStatisticsofLoadingChunks(BSONObjBuilder& result) {

    result.append("sharded", isSharded());
    result.append("optime", getConfigOpTime().getTimestamp());
    result.append("trigrefresh", (long long)_trigrefreshCount);
    result.append("refresh", (long long)_cmrefreshCount);

    return;
}

DBConfig::DBConfig(std::string name, const DatabaseType& dbt, repl::OpTime configOpTime)
    : _name(name),
      _configOpTime(std::move(configOpTime)),
      _reloadCount(0),
      _collloadCount(0),
      _collcachehitCount(0),
      _colleraseCount(0) {
    invariant(_name == dbt.getName());
    _primaryId = dbt.getPrimary();
    _shardingEnabled = dbt.getSharded();
}

DBConfig::~DBConfig() = default;

bool DBConfig::isSharded(const string& ns) {
    stdx::lock_guard<stdx::mutex> lk(_lock);

    if (!_shardingEnabled) {
        return false;
    }

    CollectionInfoMap::iterator i = _collections.find(ns);
    if (i == _collections.end()) {
        return false;
    }
    return i->second.isSharded();
}

StatusWith<bool> DBConfig::isCollectionExist(OperationContext* txn, const string& ns, bool full) {
    stdx::lock_guard<stdx::mutex> lk(_lock);

    CollectionInfoMap::iterator it = _collections.find(ns);
    if (it != _collections.end()) {
        return true;
    }

    if (full) {
        auto re = grid.catalogClient(txn)->isCollectionExist(txn, ns);
        if (re.isOK()) {
            return re.getValue();
        } else {
            return re;
        }
    }
    return false;
}

CollectionType::TableType DBConfig::getCollTabType(const string& ns) {
    stdx::lock_guard<stdx::mutex> lk(_lock);

    CollectionInfoMap::iterator it = _collections.find(ns);
    if (it != _collections.end()) {
        return it->second.getCollTabType();
    }

    return CollectionType::TableType::kNonShard;
}


void DBConfig::invalidateNs(const std::string& ns) {
    stdx::lock_guard<stdx::mutex> lk(_lock);

    CollectionInfoMap::iterator it = _collections.find(ns);
    if (it != _collections.end()) {
        _collections.erase(it);
    }
}

void DBConfig::enableSharding(OperationContext* txn) {
    invariant(_name != "config");

    stdx::lock_guard<stdx::mutex> lk(_lock);

    if (_shardingEnabled) {
        return;
    }

    _shardingEnabled = true;
    _save(txn);
}

bool DBConfig::removeSharding(OperationContext* txn, const string& ns) {
    stdx::lock_guard<stdx::mutex> lk(_lock);

    if (!_shardingEnabled) {
        warning() << "could not remove sharding for collection " << ns
                  << ", sharding not enabled for db";
        return false;
    }

    CollectionInfoMap::iterator i = _collections.find(ns);

    if (i == _collections.end())
        return false;

    CollectionInfo& ci = _collections[ns];
    if (!ci.isSharded()) {
        warning() << "could not remove sharding for collection " << ns
                  << ", no sharding information found";
        return false;
    }

    ci.unshard();
    _save(txn, false, true);
    return true;
}

// Handles weird logic related to getting *either* a chunk manager *or* the collection primary
// shard
Status DBConfig::getChunkManagerOrPrimary(OperationContext* txn,
                                          const string& ns,
                                          std::shared_ptr<ChunkManager>& manager,
                                          std::shared_ptr<Shard>& primary) {
    // The logic here is basically that at any time, our collection can become sharded or
    // unsharded
    // via a command.  If we're not sharded, we want to send data to the primary, if sharded, we
    // want to send data to the correct chunks, and we can't check both w/o the lock.

    manager.reset();
    primary.reset();
    bool ns_not_exist = false;
    //{
    stdx::lock_guard<stdx::mutex> lk(_lock);
    CollectionInfoMap::iterator i = _collections.find(ns);
    // No namespace
    if (i == _collections.end()) {
        ns_not_exist = true;
    }
    //}

    // only for internal DB(config.local.admin) or view  we should send data to the primary
    // for other DB collection, the primary is meaningless
    NamespaceString nameSpace(ns);
    if (nameSpace.isOnInternalDb() || true == ns_not_exist) {
        index_LOG(2) << "DBConfig::getChunkManagerOrPrimary ns: " << ns
                    << ", ns_not_exist:" << ns_not_exist;
        auto primaryStatus = grid.shardRegistry()->getShard(txn, _primaryId);
        if (!primaryStatus.isOK()) {
            return primaryStatus.getStatus();
        }

        primary = primaryStatus.getValue();
    } else {
        /*if (!isShardedAfterReload(txn, ns)) {
            return Status(ErrorCodes::NamespaceNotSharded,
                          str::stream() << "ns " << ns << " is not sharded yet");
        }*/

        CollectionInfo& cInfo = i->second;
        if (_shardingEnabled && cInfo.isSharded()) {
            manager = cInfo.getCM();
        } else {
            auto primaryStatus = grid.shardRegistry()->getShard(txn, _primaryId);
            if (!primaryStatus.isOK()) {
                return primaryStatus.getStatus();
            }
            primary = primaryStatus.getValue();
        }
    }

    invariant(manager || primary);
    invariant(!manager || !primary);

    return Status::OK();
}


std::shared_ptr<ChunkManager> DBConfig::getChunkManagerIfExists(OperationContext* txn,
                                                                const string& ns,
                                                                bool shouldReload) {
    // Don't report exceptions here as errors in GetLastError
    LastError::Disabled ignoreForGLE(&LastError::get(cc()));

    try {
        return getChunkManager(txn, ns, shouldReload);
    } catch (AssertionException& e) {
        warning() << "chunk manager not found for " << ns << causedBy(e);
        return nullptr;
    }
}

std::shared_ptr<ChunkManager> DBConfig::getChunkManager(OperationContext* txn,
                                                        const string& ns,
                                                        bool shouldReload) {

    index_LOG(1) << "DBConfig::getChunkManager ns:" << ns << " shouldReload: " << shouldReload;

    std::shared_ptr<ChunkManager> oldManager;
    {
        stdx::lock_guard<stdx::mutex> lk(_lock);

        // CollectionInfoMap::iterator i = _collections.find(ns);
        // uassert(26, str::stream() << "not found: " << ns, i != _collections.end());

        bool earlyReload = !_collections[ns].isSharded() && (shouldReload);
        if (earlyReload) {
            // This is to catch cases where there this is a new sharded collection.
            // Note: read the _reloadCount inside the _lock mutex, so _loadIfNeeded will always
            // be forced to perform a reload.
            const auto currentReloadIteration = _reloadCount.load();
            _loadIfNeeded(txn, currentReloadIteration);
        }

        CollectionInfo& ci = _collections[ns];
        index_LOG(1) << "DBConfig::getChunkManager ns:" << ns << ", earlyReload: " << earlyReload
                     << ", _reloadCount:" << _reloadCount.load();
        uassert(10181, str::stream() << "not sharded:" << ns, ci.isSharded());
        invariant(!ci.key().isEmpty());

        if (!(shouldReload) || earlyReload) {
            return ci.getCM();
        }

        oldManager = ci.getCM();
        ci.updateTrigrefreshCount();
    }

    stdx::unique_lock<stdx::mutex> ul(_lock);

    CollectionInfo& ci = _collections[ns];

    bool doRefreshJob = false;
    auto refreshNotification = ci.refreshCompletionNotification;
    if (!refreshNotification) {
        refreshNotification =
            (ci.refreshCompletionNotification = std::make_shared<Notification<Status>>());
        doRefreshJob = true;
    }

    // Wait on the notification outside of the mutex
    ul.unlock();

    if (doRefreshJob) {
        _chunkManagerRefresh(txn, ns, oldManager.get());
    }

    stdx::unique_lock<stdx::mutex> cl(_lock);
    CollectionInfo& c_info = _collections[ns];
    auto ref_notify = c_info.refreshCompletionNotification;
    cl.unlock();

    if (ref_notify) {
        // if refresh fail, we just still use the old cm for now
        auto refreshStatus = ref_notify->get();
        if (!refreshStatus.isOK()) {
            index_err() << "refresh fail " << ns << "," << refreshStatus;
        }
    }


    return getChunkManager(txn, ns, false);
}

void DBConfig::setPrimary(OperationContext* txn, const ShardId& newPrimaryId) {
    stdx::lock_guard<stdx::mutex> lk(_lock);
    _primaryId = newPrimaryId;
    _save(txn);
}

bool DBConfig::load(OperationContext* txn) {
    const auto currentReloadIteration = _reloadCount.load();
    stdx::lock_guard<stdx::mutex> lk(_lock);
    return _loadIfNeeded(txn, currentReloadIteration);
}

bool DBConfig::_loadIfNeeded(OperationContext* txn, Counter reloadIteration) {
    if (reloadIteration != _reloadCount.load()) {
        return true;
    }

    auto status = grid.catalogClient(txn)->getDatabase(txn, _name);
    if (status == ErrorCodes::NamespaceNotFound) {
        return false;
    }

    // All other errors are connectivity, etc so throw an exception.
    uassertStatusOK(status.getStatus());

    const auto& dbOpTimePair = status.getValue();
    const auto& dbt = dbOpTimePair.value;
    invariant(_name == dbt.getName());
    _primaryId = dbt.getPrimary();
    _shardingEnabled = dbt.getSharded();

    invariant(dbOpTimePair.opTime >= _configOpTime);
    _configOpTime = dbOpTimePair.opTime;

    if (NamespaceString::internalDb(StringData(_name))) {
        _reloadCount.fetchAndAdd(1);
        index_LOG(1) << "load for internal db " << _name;
        return true;
    }

    // Load all collections
    vector<CollectionType> collections;
    repl::OpTime configOpTimeWhenLoadingColl;
    uassertStatusOK(grid.catalogClient(txn)->getCollections(
        txn, &_name, &collections, &configOpTimeWhenLoadingColl));
    index_LOG(1) << "DBConfig::_loadIfNeeded find " << collections.size() << " collections";
    int numCollsErased = 0;
    int numCollsSharded = 0;
    int numCollsCacheHit = 0;

    invariant(configOpTimeWhenLoadingColl >= _configOpTime);

    for (const auto& coll : collections) {
        CollectionInfo& ci = _collections[coll.getNs().ns()];
        if (coll.getDropped()) {
            index_LOG(1) << "DBConfig::_loadIfNeeded coll: " << coll.getNs().ns() << " is dropped.";
            if (ci.refreshCompletionNotification) {
                ci.refreshCompletionNotification->set(Status::OK());
                ci.refreshCompletionNotification = nullptr;
            }
            _collections.erase(coll.getNs().ns());
            numCollsErased++;
            _colleraseCount++;
        } else if (coll.getCreated()) {
            // keep refreshCompletionNotification
            index_LOG(1) << "DBConfig::_loadIfNeeded find coll: " << coll.getNs().ns();
            auto refreshNotification = ci.refreshCompletionNotification;
            ci = CollectionInfo(txn, coll, configOpTimeWhenLoadingColl);
            ci.refreshCompletionNotification = refreshNotification;

            numCollsSharded++;
            _collloadCount++;
        } else {
            _collections.erase(coll.getNs().ns());
            numCollsErased++;
        }
    }

    index_warning() << "DBConfig::_loadIfNeeded load statistics for " << _name << ": load "
                    << numCollsSharded << ", erase " << numCollsErased << ", hits "
                    << numCollsCacheHit;

    _reloadCount.fetchAndAdd(1);

    return true;
}

void DBConfig::_chunkManagerRefresh(OperationContext* txn,
                                    const string& ns,
                                    const ChunkManager* oldManager) {

    const auto refreshFailed_inlock = [ this, ns ](const Status& status) noexcept {
        index_err() << "DBConfig::_chunkManagerRefresh refresh fail for collection " << ns
                    << causedBy(redact(status));

        stdx::lock_guard<stdx::mutex> lk(_lock);
        CollectionInfo& ci = _collections[ns];
        if (ci.refreshCompletionNotification) {
            ci.refreshCompletionNotification->set(status);
            ci.refreshCompletionNotification = nullptr;
        }
    };

    unique_ptr<ChunkManager> tempChunkManager;

    try {
        tempChunkManager.reset(new ChunkManager(
            oldManager->getns(),
            oldManager->getShardKeyPattern(),
            oldManager->getDefaultCollator() ? oldManager->getDefaultCollator()->clone() : nullptr,
            oldManager->isUnique()));

        if (oldManager->getVersion().isSet()) {
            // we skip back by some version steps
            ChunkVersion oldStartVersion = oldManager->getVersion();
            uint64_t newStartCombined = (oldStartVersion.toLong() < SkipBackSteps)
                ? 0
                : (oldStartVersion.toLong() - SkipBackSteps);
            tempChunkManager->setStartingVersion(
                ChunkVersion(newStartCombined, oldStartVersion.epoch()));
        }

        tempChunkManager->loadExistingRanges(txn, oldManager);
        if (tempChunkManager->numChunks() == 0) {
            // maybe epoch changed, should full reload.
            reload(txn);
        }
    } catch (const DBException& ex) {
        refreshFailed_inlock(ex.toStatus());
        return;
    }

    if (tempChunkManager->numChunks() == 0) {
        refreshFailed_inlock(Status(ErrorCodes::NamespaceNotSharded, "load 0 chunk"));
        return;
    }

    stdx::lock_guard<stdx::mutex> lk(_lock);
    CollectionInfo& ci = _collections[ns];
    if (ci.refreshCompletionNotification) {
        ci.refreshCompletionNotification->set(Status::OK());
        ci.refreshCompletionNotification = nullptr;
    }
    uassert(14822, (string) "state changed in the middle: " + ns, ci.isSharded());

    ci.resetCM(tempChunkManager.release());
    return;
}

void DBConfig::_save(OperationContext* txn, bool db, bool coll) {
    if (db) {
        auto o_db = grid.catalogClient(txn)->getDatabase(txn, _name);
        auto o_dbt = o_db.getValue().value;
        DatabaseType dbt;
        dbt.setName(_name);
        dbt.setPrimary(_primaryId);
        dbt.setSharded(_shardingEnabled);
        dbt.setEnableSharding(o_dbt.getEnableSharding());

        uassertStatusOK(grid.catalogClient(txn)->updateDatabase(txn, _name, dbt));
    }

    if (coll) {
        for (CollectionInfoMap::iterator i = _collections.begin(); i != _collections.end(); ++i) {
            if (!i->second.isDirty()) {
                continue;
            }

            i->second.save(txn, i->first);
        }
    }
}

bool DBConfig::reload(OperationContext* txn) {
    bool successful = false;
    const auto currentReloadIteration = _reloadCount.load();

    {
        stdx::lock_guard<stdx::mutex> lk(_lock);
        successful = _loadIfNeeded(txn, currentReloadIteration);
    }

    // If we aren't successful loading the database entry, we don't want to keep the stale
    // object around which has invalid data.
    if (!successful) {
        grid.catalogCache()->invalidate(_name);
    }

    return successful;
}

bool DBConfig::dropDatabase(OperationContext* txn, string& errmsg) {
    /**
     * 1) update config server
     * 2) drop and reset sharded collections
     * 3) drop and reset primary
     * 4) drop everywhere to clean up loose ends
     */

    log() << "DBConfig::dropDatabase: " << _name;
    grid.catalogClient(txn)->logChange(
        txn, "dropDatabase.start", _name, BSONObj(), ShardingCatalogClient::kMajorityWriteConcern);

    // 1
    grid.catalogCache()->invalidate(_name);


    LOG(1) << "\t DBConfig::dropDatabase removed entry from config server for: " << _name;

    set<ShardId> shardIds;

    // 2
    while (true) {
        int num = 0;
        if (!_dropShardedCollections(txn, num, shardIds, errmsg)) {
            return 0;
        }

        log() << "DBConfig::dropDatabase: " << _name << " dropped sharded collections: " << num;

        if (num == 0) {
            break;
        }
    }

    // 3
    {
        const auto shard = uassertStatusOK(grid.shardRegistry()->getShard(txn, _primaryId));

        ScopedDbConnection conn(shard->getConnString(), 30.0);
        BSONObj res;
        if (!conn->dropDatabase(_name, txn->getWriteConcern(), &res)) {
            errmsg = res.toString() + " at " + _primaryId.toString();
            return 0;
        }
        conn.done();
        if (auto wcErrorElem = res["writeConcernError"]) {
            auto wcError = wcErrorElem.Obj();
            if (auto errMsgElem = wcError["errmsg"]) {
                errmsg = errMsgElem.str() + " at " + _primaryId.toString();
                return false;
            }
        }
    }

    // 4
    for (const ShardId& shardId : shardIds) {
        const auto shardStatus = grid.shardRegistry()->getShard(txn, shardId);
        if (!shardStatus.isOK()) {
            continue;
        }

        ScopedDbConnection conn(shardStatus.getValue()->getConnString(), 30.0);
        BSONObj res;
        if (!conn->dropDatabase(_name, txn->getWriteConcern(), &res)) {
            errmsg = res.toString() + " at " + shardId.toString();
            return 0;
        }
        conn.done();
        if (auto wcErrorElem = res["writeConcernError"]) {
            auto wcError = wcErrorElem.Obj();
            if (auto errMsgElem = wcError["errmsg"]) {
                errmsg = errMsgElem.str() + " at " + shardId.toString();
                return false;
            }
        }
    }

    LOG(1) << "\t DBConfig::dropDatabase: dropped primary db for: " << _name;
    Status result = grid.catalogClient(txn)->removeConfigDocuments(
        txn,
        DatabaseType::ConfigNS,
        BSON(DatabaseType::name(_name)),
        ShardingCatalogClient::kMajorityWriteConcern);
    if (!result.isOK()) {
        errmsg = result.reason();
        log() << "could not drop '" << _name << "': " << errmsg;
        return false;
    }

    grid.catalogClient(txn)->logChange(
        txn, "dropDatabase", _name, BSONObj(), ShardingCatalogClient::kMajorityWriteConcern);

    return true;
}

bool DBConfig::_dropShardedCollections(OperationContext* txn,
                                       int& num,
                                       set<ShardId>& shardIds,
                                       string& errmsg) {
    num = 0;
    set<std::string> seen;
    while (true) {
        std::string aCollection;
        {
            stdx::lock_guard<stdx::mutex> lk(_lock);

            CollectionInfoMap::iterator i = _collections.begin();
            for (; i != _collections.end(); ++i) {
                if (i->second.isSharded()) {
                    break;
                }
            }

            if (i == _collections.end()) {
                break;
            }

            aCollection = i->first;
            if (seen.count(aCollection)) {
                errmsg = "seen a collection twice!";
                return false;
            }

            seen.insert(aCollection);
            LOG(1) << "\t DBConfig::_dropShardedCollections dropping sharded collection: "
                   << aCollection;

            i->second.getCM()->getAllShardIds(&shardIds);
        }
        // drop lock before network activity

        uassertStatusOK(grid.catalogClient(txn)->dropCollection(txn, NamespaceString(aCollection)));

        // We should warn, but it's not a fatal error if someone else reloaded the db/coll as
        // unsharded in the meantime
        if (!removeSharding(txn, aCollection)) {
            warning() << "collection " << aCollection
                      << " was reloaded as unsharded before drop completed"
                      << " during drop of all collections";
        }

        num++;
        uassert(10184, "_dropShardedCollections too many collections - bailing", num < 100000);
        LOG(2) << "\t\t DBConfig::_dropShardedCollections dropped " << num << " so far";
    }

    return true;
}

void DBConfig::getAllShardIds(set<ShardId>* shardIds) {
    dassert(shardIds);

    stdx::lock_guard<stdx::mutex> lk(_lock);
    shardIds->insert(_primaryId);
    for (CollectionInfoMap::const_iterator it(_collections.begin()), end(_collections.end());
         it != end;
         ++it) {
        if (it->second.isSharded()) {
            it->second.getCM()->getAllShardIds(shardIds);
        }  // TODO: handle collections on non-primary shard
    }
}

void DBConfig::getAllShardedCollections(set<string>& namespaces) {
    stdx::lock_guard<stdx::mutex> lk(_lock);

    for (CollectionInfoMap::const_iterator i = _collections.begin(); i != _collections.end(); i++) {
        log() << "Coll : " << i->first << " sharded? " << i->second.isSharded();
        if (i->second.getCollTabType() != CollectionType::TableType::kNonShard) {
            namespaces.insert(i->first);
        }
    }
}

void DBConfig::getAllNonShardedCollections(set<string>& namespaces) {
    stdx::lock_guard<stdx::mutex> lk(_lock);

    for (CollectionInfoMap::const_iterator i = _collections.begin(); i != _collections.end(); i++) {
        // log() << "Coll : " << i->first << " sharded? " << i->second.isSharded();
        if (i->second.getCollTabType() == CollectionType::TableType::kNonShard) {
            namespaces.insert(i->first);
        }
    }
}
bool DBConfig::isShardingEnabled() {
    stdx::lock_guard<stdx::mutex> lk(_lock);
    return _shardingEnabled;
}

ShardId DBConfig::getPrimaryId() {
    stdx::lock_guard<stdx::mutex> lk(_lock);
    return _primaryId;
}

bool DBConfig::isShardedAfterReload(OperationContext* txn, const std::string& ns) {
    if (!isSharded(ns)) {
        reload(txn);
        if (!isSharded(ns)) {
            return false;
        }
    }

    return true;
}

void DBConfig::getStat(BSONObjBuilder& result) {
    BSONObjBuilder subObjBuilder(result.subobjStart(_name));

    subObjBuilder.append("dbloadCount", (long long)_reloadCount.load());
    subObjBuilder.append("collloadCount", (long long)_collloadCount);
    subObjBuilder.append("collcachehitCount", (long long)_collcachehitCount);
    subObjBuilder.append("colleraseCount", (long long)_colleraseCount);

    {
        stdx::lock_guard<stdx::mutex> lk(_lock);
        for (auto coll : _collections) {
            BSONObjBuilder collsubObjBuilder(subObjBuilder.subobjStart(coll.first));
            coll.second.getStatisticsofLoadingChunks(collsubObjBuilder);
            collsubObjBuilder.doneFast();
        }
    }

    subObjBuilder.doneFast();
    return;
}

/* --- ConfigServer ---- */

void ConfigServer::replicaSetChangeShardRegistryUpdateHook(const string& setName,
                                                           const string& newConnectionString) {
    // Inform the ShardRegsitry of the new connection string for the shard.
    auto connString = fassertStatusOK(28805, ConnectionString::parse(newConnectionString));
    invariant(setName == connString.getSetName());
    grid.shardRegistry()->updateReplSetHosts(connString);
}

void ConfigServer::replicaSetChangeConfigServerUpdateHook(const string& setName,
                                                          const string& newConnectionString) {
    // This is run in it's own thread. Exceptions escaping would result in a call to terminate.
    Client::initThread("replSetChange");
    auto txn = cc().makeOperationContext();

    try {
        std::shared_ptr<Shard> s = grid.shardRegistry()->lookupRSName(setName);
        if (!s) {
            LOG(1) << "shard not found for set: " << newConnectionString
                   << " when attempting to inform config servers of updated set membership";
            return;
        }

        if (s->isConfig()) {
            // No need to tell the config servers their own connection string.
            return;
        }

        auto status = grid.catalogClient(txn.get())->updateConfigDocument(
            txn.get(),
            ShardType::ConfigNS,
            BSON(ShardType::name(s->getId().toString())),
            BSON("$set" << BSON(ShardType::host(newConnectionString))),
            false,
            ShardingCatalogClient::kMajorityWriteConcern);
        if (!status.isOK()) {
            error() << "RSChangeWatcher: could not update config db for set: " << setName
                    << " to: " << newConnectionString << causedBy(status.getStatus());
        }
    } catch (const std::exception& e) {
        warning() << "caught exception while updating config servers: " << e.what();
    } catch (...) {
        warning() << "caught unknown exception while updating config servers";
    }
}

}  // namespace mongo

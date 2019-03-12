/**
 *    Copyright (C) 2015 MongoDB Inc.
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

#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/base/status_with.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/config.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"

namespace mongo {

using std::shared_ptr;
using std::string;


CatalogCache::CatalogCache() {}

void CatalogCache::getStat(BSONObjBuilder& result) {
    stdx::lock_guard<stdx::mutex> guard(_mutex);

    for (auto db : _databases) {
        db.second->getStat(result);
    }

    return;
}

StatusWith<shared_ptr<DBConfig>> CatalogCache::getDatabase(OperationContext* txn,
                                                           const string& dbName) {
    stdx::lock_guard<stdx::mutex> guard(_mutex);

    ShardedDatabasesMap::iterator it = _databases.find(dbName);
    if (it != _databases.end()) {
        return it->second;
    }

    // Need to load from the store
    auto status = grid.catalogClient(txn)->getDatabase(txn, dbName);
    if (!status.isOK()) {
        return status.getStatus();
    }

    const auto dbOpTimePair = status.getValue();
    shared_ptr<DBConfig> db =
        std::make_shared<DBConfig>(dbName, dbOpTimePair.value, dbOpTimePair.opTime);
    try {
        db->load(txn);
    } catch (const DBException& excep) {
        return excep.toStatus();
    }

    invariant(_databases.insert(std::make_pair(dbName, db)).second);

    return db;
}

void CatalogCache::invalidate(const string& dbName) {
    stdx::lock_guard<stdx::mutex> guard(_mutex);

    ShardedDatabasesMap::iterator it = _databases.find(dbName);
    if (it != _databases.end()) {
        _databases.erase(it);
    }
}

void CatalogCache::invalidateAll() {
    stdx::lock_guard<stdx::mutex> guard(_mutex);

    _databases.clear();
}

void CatalogCache::periodFlushDbIfNeeded(OperationContext* txn) {
        std::vector<DbCacheInfo> _dbCacheInfo;
        {
            stdx::lock_guard<stdx::mutex> guard(_mutex);
            for(auto db: _databases){
                _dbCacheInfo.push_back(DbCacheInfo(db.second->name(),db.second->getPrimaryId(),db.second->getCollNums()));
            }
        }
        for(auto info: _dbCacheInfo){
            index_LOG(3)<<"dbname : "<< info.GetName() <<", primaryId: "<<info.GetPrimaryId()<<", collNums: "<<info.GetCollNums();
            auto status = grid.catalogClient(txn)->getDatabase(txn, info.GetName());
            if (!status.isOK()) {
                invalidate(info.GetName());
                index_err()<<"dbnamme "<< info.GetName()<<" is not exist on config.";
                continue;
            }

            const auto dbOpTimePair = status.getValue();
            const DatabaseType& dbt = dbOpTimePair.value;
            if(dbt.getPrimary() != info.GetPrimaryId()){
                invalidate(info.GetName());
                index_err()<<"dbnamme "<< info.GetName() <<"primaryId not equal,"
                            <<", local primaryId: "<< info.GetPrimaryId()<<", config primaryId:"<< dbt.getPrimary();
                continue;
            }

            std::vector<CollectionType> collections;
            repl::OpTime configOpTimeWhenLoadingColl;
            auto coll_status = grid.catalogClient(txn)->getCollections(
                                txn, &info.GetName(), &collections, &configOpTimeWhenLoadingColl);

            if (!coll_status.isOK()) {
                index_err()<<"dbnamme "<< info.GetName()<<", getCollections failed, status: "<< coll_status;
                break;
            }

            int cfg_coll_nums = 0;
            for (const auto& coll : collections) {
                if (coll.getCreated() && !coll.getDropped()) {
                    cfg_coll_nums++;
                }
            }
            if(info.GetCollNums() != cfg_coll_nums){
                invalidate(info.GetName());
                index_err()<<"dbnamme: "<< info.GetName() <<", collection number not equal,"
                           <<", local number: "<< info.GetCollNums()<<", config number:"<< cfg_coll_nums;
                continue;
            }
        }

        return;
    }

}  // namespace mongo

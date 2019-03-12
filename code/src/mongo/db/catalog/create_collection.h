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
#pragma once
#include <string>

#include "mongo/base/status.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/namespace_string.h"


namespace mongo {
class BSONObj;
class OperationContext;
/*
class nsToLock{
private:
    const std::string _ns;
    std::mutex _mtx;
public:
    nsToLock(std::string ns):_ns(ns){}
    void lock(){
         _mtx.lock();
    }
    void unlock(){
         _mtx.unlock();
    }
};
class nsToLockMap{
private:
    class nsToLock{
    private:
        const std::string _ns;
        std::mutex _mtx;
    public:
        nsToLock(std::string ns):_ns(ns){}
        void lock(){
             _mtx.lock();
        }
        void unlock(){
             _mtx.unlock();
        }
    };
    std::map<std::string,nsToLock *> nsMap;
    std::mutex mtx;
    Status insert(std::string ns,nsToLock **lock){
         nsToLock *nl= new nsToLock(ns);
         *lock = nl;
         nsMap[ns]=nl;
         return Status::OK();
    }
    bool find(std::string ns ,nsToLock **lock){
        mtx.lock();
        auto it = nsMap.find(ns);
        if(it != nsMap.end()){
            *lock = it->second;
        }else{
            insert(ns,lock);
        }
        mtx.unlock();
        return true;
    }
public:
    ~nsToLockMap(){
        for(auto it=nsMap.begin();it != nsMap.end();it++){
            delete it->second;
        }
    }
    void lockNs(std::string ns){
        // if ns not exist ,then insert,and lock
        // if ns exist, then get ns's lock,if locked,waiting ...
        nsToLock *lock = NULL;
        find(ns,&lock);
        if( lock != NULL )
            lock->lock();
    }
    void unlockNs(std::string ns){
        nsToLock *lock = NULL;
        find(ns,&lock);
        if( lock != NULL )
            lock->unlock();
    }
};
*/

/**
 * Creates a collection as described in "cmdObj" on the database "dbName". Creates the collection's
 * _id index according to 'idIndex', if it is non-empty. When 'idIndex' is empty, creates the
 * default _id index.
 */
Status createCollection(OperationContext* txn,
                        const std::string& dbName,
                        const BSONObj& cmdObj,
                        const BSONObj& idIndex = BSONObj());


/**
 * Creates a collection metadata as described in "cmdObj" on the database "dbName". Creates the
 * collection's
 * _id index metadata according to 'idIndex', if it is non-empty. When 'idIndex' is empty, creates
 * the metadata for
 * default _id index.
 */
Status createCollectionMetadata(OperationContext* txn,
                                const NamespaceString& ns,
                                const BSONObj& cmdObj,
                                BSONObj& indexSpec);

}  // namespace mongo

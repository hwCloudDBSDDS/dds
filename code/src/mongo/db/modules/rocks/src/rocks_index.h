/**
 *    Copyright (C) 2014 MongoDB Inc.
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

#include "mongo/db/storage/sorted_data_interface.h"

#include <atomic>
#include <boost/shared_ptr.hpp>
#include <string>

#include <rocksdb/db.h>

#include "mongo/bson/ordering.h"
#include "mongo/db/storage/key_string.h"

namespace rocksdb {
    class DB;
}

namespace mongo {

    class ChunkRocksDBInstance;

    BSONObj stripFieldNames(const BSONObj& obj);

    std::string dupKeyError(const BSONObj& key);

    // constexpr int kTempKeyMaxSize = 2516;  // Index Layer support key's size larger than 1024

    constexpr int kTempKeyMaxSize = 1024;
    Status checkKeySize(const BSONObj& key);

    class RocksRecoveryUnit;

    class RocksIndexBase : public SortedDataInterface {
        MONGO_DISALLOW_COPYING(RocksIndexBase);

    public:
        RocksIndexBase(mongo::ChunkRocksDBInstance* db);

        virtual void fullValidate(OperationContext* txn, long long* numKeysOut,
                                  ValidateResults* fullResults) const;

        virtual bool appendCustomStats(OperationContext* txn, BSONObjBuilder* output,
                                       double scale) const {
            return false;
        }

        virtual Status initAsEmpty(OperationContext* txn) { return Status::OK(); }

        virtual long long getSpaceUsedBytes(OperationContext* txn) const;

        const mongo::ChunkRocksDBInstance* getDBInstance() const { return _db; }

    protected:
        mongo::ChunkRocksDBInstance* _db;  // not owned
        rocksdb::DB* getDB();
        const rocksdb::DB* getDB() const;

        // very approximate index storage size
        std::atomic<long long> _indexStorageSize;

        void SetCorrectDBForRecoveryUnit(OperationContext* txn) const;
    };

    class MongoRocksIndexBase : public RocksIndexBase {
        MONGO_DISALLOW_COPYING(MongoRocksIndexBase);

    public:
        MongoRocksIndexBase(mongo::ChunkRocksDBInstance* db, std::string prefix, std::string ident,
                            Ordering order, const BSONObj& config);

        virtual bool isEmpty(OperationContext* txn);

        static void generateConfig(BSONObjBuilder* configBuilder, int formatVersion);

        static std::string _makePrefixedKey(const std::string& prefix, const KeyString& encodedKey);

    protected:
        // Each key in the index is prefixed with _prefix
        std::string _prefix;
        std::string _ident;

        // used to construct RocksCursors
        const Ordering _order;
        KeyString::Version _keyStringVersion;

        class StandardBulkBuilder;
        class UniqueBulkBuilder;
        friend class UniqueBulkBuilder;
    };

    class RocksUniqueIndex : public MongoRocksIndexBase {
    public:
        RocksUniqueIndex(mongo::ChunkRocksDBInstance* db, std::string prefix, std::string ident,
                         Ordering order, const BSONObj& config);

        virtual Status insert(OperationContext* txn, const BSONObj& key, const RecordId& loc,
                              bool dupsAllowed);
        virtual void unindex(OperationContext* txn, const BSONObj& key, const RecordId& loc,
                             bool dupsAllowed);
        virtual std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* txn,
                                                                       bool forward) const;

        virtual Status dupKeyCheck(OperationContext* txn, const BSONObj& key, const RecordId& loc);

        virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* txn,
                                                           bool dupsAllowed) override;
    };

    class RocksStandardIndex : public MongoRocksIndexBase {
    public:
        RocksStandardIndex(mongo::ChunkRocksDBInstance* db, std::string prefix, std::string ident,
                           Ordering order, const BSONObj& config);

        virtual Status insert(OperationContext* txn, const BSONObj& key, const RecordId& loc,
                              bool dupsAllowed);
        virtual void unindex(OperationContext* txn, const BSONObj& key, const RecordId& loc,
                             bool dupsAllowed);
        virtual std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* txn,
                                                                       bool forward) const;
        virtual Status dupKeyCheck(OperationContext* txn, const BSONObj& key, const RecordId& loc) {
            // dupKeyCheck shouldn't be called for non-unique indexes
            invariant(false);
        }

        virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* txn,
                                                           bool dupsAllowed) override;

        void enableSingleDelete() { useSingleDelete = true; }

    private:
        bool useSingleDelete;
    };

}  // namespace mongo

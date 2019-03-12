/**
 *    Copyright (C) 2012 10gen Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#pragma once

#include <boost/optional.hpp>
#include <string>

#include "mongo/db/jsobj.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/namespace_string.h"

namespace mongo {

class Status;
template <typename T>
class StatusWith;


/**
 * This class represents the layout and contents of documents contained in the
 * config.collections collection. All manipulation of documents coming from that collection
 * should be done with this class.
 */
class CollectionType {
public:
    // Event type, used in the recovery process
    enum class TableType : int {
        kNonShard = 0,
        kCapped = 1,
        kSharded = 2,
        kView = 3,
    };

    // Name of the collections collection in the config server.
    static const std::string ConfigNS;

    static const BSONField<std::string> fullNs;
    static const BSONField<OID> epoch;
    static const BSONField<Date_t> updatedAt;
    static const BSONField<BSONObj> keyPattern;
    static const BSONField<BSONObj> defaultCollation;
    static const BSONField<bool> unique;
    static const BSONField<TableType> tabType;
    static const BSONField<long long> prefix;
    static const BSONField<BSONObj> index;
    static const BSONField<BSONObj> options;
    static const BSONField<std::string> ident;

    static const BSONField<bool> kNoBalance;
    static const BSONField<bool> kDropped;
    static const BSONField<bool> kCreated;
    static const BSONField<std::unordered_set<uint32_t>> kDroppedPrefixes;

    /**
     * Constructs a new DatabaseType object from BSON. Also does validation of the contents.
     */
    static StatusWith<CollectionType> fromBSON(const BSONObj& source);

    // Sanity check the status
    static bool isValidTabType(TableType tab) {
        return (tab >= TableType::kNonShard && tab <= TableType::kView);
    }


    /**
     * Returns OK if all fields have been set. Otherwise returns NoSuchKey and information
     * about what is the first field which is missing.
     */
    Status validate() const;

    /**
     * Returns the BSON representation of the entry.
     */
    BSONObj toBSON() const;

    /**
     * Returns a std::string representation of the current internal state.
     */
    std::string toString() const;

    const NamespaceString& getNs() const {
        return _fullNs.get();
    }
    void setNs(const NamespaceString& fullNs);

    OID getEpoch() const {
        return _epoch.get();
    }
    void setEpoch(OID epoch);

    Date_t getUpdatedAt() const {
        return _updatedAt.get();
    }
    void setUpdatedAt(Date_t updatedAt);

    bool getDropped() const {
        return _dropped.get_value_or(false);
    }
    void setDropped(bool dropped) {
        _dropped = dropped;
    }

    bool getCreated() const {
        return _created.get_value_or(false);
    }
    void setCreated(bool created) {
        _created = created;
    }

    const KeyPattern& getKeyPattern() const {
        return _keyPattern.get();
    }
    void setKeyPattern(const KeyPattern& keyPattern);

    const BSONObj& getDefaultCollation() const {
        return _defaultCollation;
    }
    void setDefaultCollation(const BSONObj& collation) {
        _defaultCollation = collation.getOwned();
    }

    bool getUnique() const {
        return _unique.get_value_or(false);
    }
    void setUnique(bool unique) {
        _unique = unique;
    }

    TableType getTabType() const {
        return _tableType;
    }
    void setTabType(TableType tabType);


    bool getAllowBalance() const {
        return _allowBalance.get_value_or(true);
    }

    void setAllowBalance(bool allow) {
        _allowBalance = allow;
    }

    long long getPrefix() const {
        return _prefix.get_value_or(0);
    }

    void setPrefix(long long prefix);

    const BSONArray& getIndex() const {
        return _index;
    }
    void setIndex(const BSONArray& index);
    void setOptions(const BSONObj& options) {
        _options = options;
    }
    const BSONObj& getOptions() const{
        return _options;
    }

    const std::string getIdent() const {
        return _ident.get();
    }

    void setIdent(const std::string& ident) {
        _ident = ident;
    }
    const std::unordered_set<uint32_t>&getDroppedPrefixes()const{
        return _droppedPrefixes;
    }
    
    void setDroppedPrefixes(const std::unordered_set<uint32_t>& prefixes) {
        _droppedPrefixes = prefixes;
    }

private:
    // Required full namespace (with the database prefix).
    boost::optional<NamespaceString> _fullNs;

    // Required to disambiguate collection namespace incarnations.
    boost::optional<OID> _epoch;

    // Required last updated time.
    boost::optional<Date_t> _updatedAt;

    // Optional, whether the collection has been dropped. If missing, implies false.
    boost::optional<bool> _dropped;
    // collection create status 0 creating, 1 created
    boost::optional<bool> _created;

    // Sharding key. Required, if collection is not dropped.
    boost::optional<KeyPattern> _keyPattern;

    // Optional collection default collation. If empty, implies simple collation.
    BSONObj _defaultCollation;

    // Optional uniqueness of the sharding key. If missing, implies false.
    boost::optional<bool> _unique;

    // Optional whether balancing is allowed for this collection. If missing, implies true.
    boost::optional<bool> _allowBalance;

    // prefix for collection and index
    boost::optional<long long> _prefix;

    // metadata for indexes of collection
    BSONArray _index;

    TableType _tableType = TableType::kNonShard;
    // Optional collection options,include capped,autoIndexId,size,max,storageEngine,validator and
    // so on
    BSONObj _options;

    boost::optional<std::string> _ident;
    //index dropped prefixes set
    std::unordered_set<uint32_t> _droppedPrefixes; 
};

}  // namespace mongo

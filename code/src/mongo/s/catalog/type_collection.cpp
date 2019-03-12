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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/s/catalog/type_collection.h"

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

namespace mongo {

const BSONField<bool> CollectionType::kNoBalance("noBalance");
const BSONField<bool> CollectionType::kDropped("dropped");
const BSONField<bool> CollectionType::kCreated("created");

const std::string CollectionType::ConfigNS = "config.collections";

const BSONField<std::string> CollectionType::fullNs("_id");
const BSONField<OID> CollectionType::epoch("lastmodEpoch");
const BSONField<Date_t> CollectionType::updatedAt("lastmod");
const BSONField<BSONObj> CollectionType::keyPattern("key");
const BSONField<BSONObj> CollectionType::defaultCollation("defaultCollation");
const BSONField<bool> CollectionType::unique("unique");
const BSONField<CollectionType::TableType> CollectionType::tabType("tableType");
const BSONField<long long> CollectionType::prefix("prefix");
const BSONField<BSONObj> CollectionType::index("indexes");
const BSONField<BSONObj> CollectionType::options("options");
const BSONField<std::string> CollectionType::ident("ident");
const BSONField<std::unordered_set<uint32_t>> CollectionType::kDroppedPrefixes("droppedPrefixes");

StatusWith<CollectionType> CollectionType::fromBSON(const BSONObj& source) {
    CollectionType coll;
    {
        BSONElement colloptions;
        Status status = bsonExtractTypedField(source, options.name(), Object, &colloptions);
        if (status.isOK()) {
            BSONObj obj = colloptions.Obj();
            if (obj.isEmpty()) {
                return Status(ErrorCodes::BadValue, "empty options");
            }

            coll._options = obj.getOwned();
        } else if (status != ErrorCodes::NoSuchKey) {
            // TODO // return status;
        }
    }
    {
        std::string collFullNs;
        Status status = bsonExtractStringField(source, fullNs.name(), &collFullNs);
        if (!status.isOK())
            return status;

        coll._fullNs = NamespaceString{collFullNs};
    }

    {
        OID collEpoch;
        Status status = bsonExtractOIDFieldWithDefault(source, epoch.name(), OID(), &collEpoch);
        if (!status.isOK())
            return status;

        coll._epoch = collEpoch;
    }

    {
        BSONElement collUpdatedAt;
        Status status = bsonExtractTypedField(source, updatedAt.name(), Date, &collUpdatedAt);
        if (!status.isOK())
            return status;

        coll._updatedAt = collUpdatedAt.Date();
    }

    {
        bool collDropped;
        Status status = bsonExtractBooleanField(source, kDropped.name(), &collDropped);
        if (status.isOK()) {
            coll._dropped = collDropped;
        } else if (status == ErrorCodes::NoSuchKey) {
            // Dropped can be missing in which case it is presumed false
        } else {
            return status;
        }
    }

    {
        bool collCreated;
        Status status = bsonExtractBooleanField(source, kCreated.name(), &collCreated);
        if (status.isOK()) {
            coll._created = collCreated;
        } else if (status == ErrorCodes::NoSuchKey) {
            // Dropped can be missing in which case it is presumed false
        } else {
            return status;
        }
    }

    {
        BSONElement collKeyPattern;
        Status status = bsonExtractTypedField(source, keyPattern.name(), Object, &collKeyPattern);
        if (status.isOK()) {
            BSONObj obj = collKeyPattern.Obj();
            if (obj.isEmpty()) {
                return Status(ErrorCodes::ShardKeyNotFound, "empty shard key");
            }

            coll._keyPattern = KeyPattern(obj.getOwned());
        } else if (status == ErrorCodes::NoSuchKey) {
            // Sharding key can only be missing if the collection is dropped
            if (!coll.getDropped()) {
                return {status.code(),
                        str::stream() << "Shard key for collection " << coll._fullNs->ns()
                                      << " is missing, but the collection is not marked as "
                                         "dropped. This is an indication of corrupted sharding "
                                         "metadata."};
            }
        } else {
            return status;
        }
    }

    {
        BSONElement collDefaultCollation;
        Status status =
            bsonExtractTypedField(source, defaultCollation.name(), Object, &collDefaultCollation);
        if (status.isOK()) {
            BSONObj obj = collDefaultCollation.Obj();
            if (obj.isEmpty()) {
                return Status(ErrorCodes::BadValue, "empty defaultCollation");
            }

            coll._defaultCollation = obj.getOwned();
        } else if (status != ErrorCodes::NoSuchKey) {
            return status;
        }
    }

    {
        bool collUnique;
        Status status = bsonExtractBooleanField(source, unique.name(), &collUnique);
        if (status.isOK()) {
            coll._unique = collUnique;
        } else if (status == ErrorCodes::NoSuchKey) {
            // Key uniqueness can be missing in which case it is presumed false
        } else {
            return status;
        }
    }

    {
        bool collNoBalance;
        Status status = bsonExtractBooleanField(source, kNoBalance.name(), &collNoBalance);
        if (status.isOK()) {
            coll._allowBalance = !collNoBalance;
        } else if (status == ErrorCodes::NoSuchKey) {
            // No balance can be missing in which case it is presumed as false
        } else {
            return status;
        }
    }

    {
        long long ll_prefix;
        Status status = bsonExtractIntegerField(source, prefix.name(), &ll_prefix);
        if (status.isOK()) {
            coll._prefix = ll_prefix;
        } else if (status == ErrorCodes::NoSuchKey) {
            return status;
        } else {
            return status;
        }
    }

    {
        BSONElement collIndex;
        Status status = bsonExtractTypedField(source, index.name(), Array, &collIndex);
        if (status.isOK()) {
            BSONArrayBuilder b;
            BSONObjIterator it(collIndex.Obj());
            while (it.more()) {
                b << it.next().Obj().getOwned();
            }
            coll._index = b.arr();
        } else if (status != ErrorCodes::NoSuchKey) {
            return status;
        }
    }

    {

        long long tab;
        Status t_status = bsonExtractIntegerField(source, tabType.name(), &tab);
        if (t_status.isOK()) {
            // Make sure the state field falls within the valid range of ChunkState values.
            if (!isValidTabType(static_cast<TableType>(tab))) {
                return Status(ErrorCodes::BadValue,
                              str::stream() << "Invalid tabType value: " << tab);
            } else {
                coll._tableType = static_cast<TableType>(tab);
            }
        } else {
            return t_status;
        }
    }

    {
        std::string collIdent;
        Status status = bsonExtractStringField(source, ident.name(), &collIdent);
        if (!status.isOK()) {
            index_err() << "no ident: " << source;
        } else {
            coll._ident = collIdent;
        }
    }

    {
        BSONElement droppedPrefixes;
        Status status = bsonExtractTypedField(source, kDroppedPrefixes.name(), Array, &droppedPrefixes);
        if (!status.isOK()) {
            index_err() << "no droppedPrefixes: " << source <<", status: "<<status;
        }else{
            BSONObjIterator it(droppedPrefixes.Obj());
            while (it.more()) {
                BSONElement e = it.next();
                if (e.type() != NumberInt) {
                    return Status(ErrorCodes::TypeMismatch,
                                  str::stream() << "Elements in \"" << kDroppedPrefixes.name()
                                                << "\" array must be NumberInt but found "
                                                << typeName(e.type()));
                }
                coll._droppedPrefixes.insert(e.numberInt());
            }
        }
    }

    return StatusWith<CollectionType>(coll);
}

Status CollectionType::validate() const {
    // These fields must always be set
    if (!_fullNs.is_initialized()) {
        return Status(ErrorCodes::NoSuchKey, "missing ns");
    }

    if (!_fullNs->isValid()) {
        return Status(ErrorCodes::BadValue, "invalid namespace " + _fullNs->toString());
    }

    if (!_epoch.is_initialized()) {
        return Status(ErrorCodes::NoSuchKey, "missing epoch");
    }

    if (!_updatedAt.is_initialized()) {
        return Status(ErrorCodes::NoSuchKey, "missing updated at timestamp");
    }

    if (!_dropped.get_value_or(false)) {
        if (!_epoch->isSet()) {
            return Status(ErrorCodes::BadValue, "invalid epoch");
        }

        if (Date_t() == _updatedAt.get()) {
            return Status(ErrorCodes::BadValue, "invalid updated at timestamp");
        }

        if (!_keyPattern.is_initialized()) {
            return Status(ErrorCodes::NoSuchKey, "missing key pattern");
        } else {
            invariant(!_keyPattern->toBSON().isEmpty());
        }
    }

    if (_prefix.get_value_or(0) <= 0) {
        return Status(ErrorCodes::NoSuchKey, "missing prefix");
    }
    if (!isValidTabType(_tableType)) {
        return {ErrorCodes::BadValue,
                str::stream() << "tabType(" << static_cast<int>(_tableType) << ") is invalid"};
    }

    return Status::OK();
}

BSONObj CollectionType::toBSON() const {
    BSONObjBuilder builder;

    if (_fullNs) {
        builder.append(fullNs.name(), _fullNs->toString());
    }
    builder.append(epoch.name(), _epoch.get_value_or(OID()));
    builder.append(updatedAt.name(), _updatedAt.get_value_or(Date_t()));
    builder.append(kDropped.name(), _dropped.get_value_or(false));
    builder.append(kCreated.name(), _created.get_value_or(false));

    builder.append(prefix.name(), _prefix.get_value_or(0));


    // These fields are optional, so do not include them in the metadata for the purposes of
    // consuming less space on the config servers.

    if (_keyPattern.is_initialized()) {
        builder.append(keyPattern.name(), _keyPattern->toBSON());
    }

    if (!_defaultCollation.isEmpty()) {
        builder.append(defaultCollation.name(), _defaultCollation);
    }

    if (_unique.is_initialized()) {
        builder.append(unique.name(), _unique.get());
    }

    if (_allowBalance.is_initialized()) {
        builder.append(kNoBalance.name(), !_allowBalance.get());
    }
    if (_index.isValid()) {
        builder.append(index.name(), _index);
    }

    if (isValidTabType(_tableType)) {
        builder.append(tabType.name(),
                       static_cast<std::underlying_type<TableType>::type>(getTabType()));
    }
    if (!_options.isEmpty()) {
        builder.append(options.name(), _options);
    }

    if (_ident.is_initialized()) {
        builder.append(ident.name(), _ident.get());
    } else {
        // nothing
    }
    
    builder.append(kDroppedPrefixes.name(), _droppedPrefixes);
    return builder.obj();
}

std::string CollectionType::toString() const {
    return toBSON().toString();
}

void CollectionType::setNs(const NamespaceString& fullNs) {
    invariant(fullNs.isValid());
    _fullNs = fullNs;
}

void CollectionType::setEpoch(OID epoch) {
    _epoch = epoch;
}

void CollectionType::setUpdatedAt(Date_t updatedAt) {
    _updatedAt = updatedAt;
}

void CollectionType::setKeyPattern(const KeyPattern& keyPattern) {
    invariant(!keyPattern.toBSON().isEmpty());
    _keyPattern = keyPattern;
}

void CollectionType::setPrefix(long long prefix) {
    _prefix = prefix;
}

void CollectionType::setIndex(const BSONArray& index) {
    // todo
    _index = index;
}

void CollectionType::setTabType(TableType tab) {
    _tableType = tab;
}

}  // namespace mongo

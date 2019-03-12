/**
 *    Copyright (C) 2012-2015 MongoDB Inc.
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

#include "mongo/s/catalog/type_chunk.h"

#include <cstring>

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"


#include "mongo/db/operation_context.h"
#include "mongo/db/server_options.h"

#include "mongo/s/catalog/sharding_catalog_client_impl.h"
#include "mongo/s/grid.h"
#include <iomanip>


namespace mongo {

const std::string ChunkType::ConfigNS = "config.chunks";

const BSONField<std::string> ChunkType::name("_id");
const BSONField<std::string> ChunkType::ns("ns");
const BSONField<BSONObj> ChunkType::min("min");
const BSONField<BSONObj> ChunkType::max("max");
const BSONField<std::string> ChunkType::shard("shard");
const BSONField<std::string> ChunkType::processIdentity("processIdentity");
const BSONField<bool> ChunkType::jumbo("jumbo");
const BSONField<Date_t> ChunkType::DEPRECATED_lastmod("lastmod");
const BSONField<OID> ChunkType::DEPRECATED_epoch("lastmodEpoch");
const BSONField<std::string> ChunkType::rootFolder("rootFolder");
const BSONField<ChunkType::ChunkStatus> ChunkType::status("status");

const int ChunkType::kChunkIDDigitWidth(16);  // support billions of chunks

namespace {

const char kMinKey[] = "min";
const char kMaxKey[] = "max";

}  // namespace

ChunkRange::ChunkRange(BSONObj minKey, BSONObj maxKey)
    : _minKey(std::move(minKey)), _maxKey(std::move(maxKey)) {
    dassert(SimpleBSONObjComparator::kInstance.evaluate(_minKey < _maxKey));
}

StatusWith<ChunkRange> ChunkRange::fromBSON(const BSONObj& obj) {
    BSONElement minKey;
    {
        Status minKeyStatus = bsonExtractTypedField(obj, kMinKey, Object, &minKey);
        if (!minKeyStatus.isOK()) {
            return {minKeyStatus.code(),
                    str::stream() << "Invalid min key due to " << minKeyStatus.reason()};
        }

        if (minKey.Obj().isEmpty()) {
            return {ErrorCodes::BadValue, "The min key cannot be empty"};
        }
    }

    BSONElement maxKey;
    {
        Status maxKeyStatus = bsonExtractTypedField(obj, kMaxKey, Object, &maxKey);
        if (!maxKeyStatus.isOK()) {
            return {maxKeyStatus.code(),
                    str::stream() << "Invalid max key due to " << maxKeyStatus.reason()};
        }

        if (maxKey.Obj().isEmpty()) {
            return {ErrorCodes::BadValue, "The max key cannot be empty"};
        }
    }

    if (SimpleBSONObjComparator::kInstance.evaluate(minKey.Obj() >= maxKey.Obj())) {
        return {ErrorCodes::FailedToParse,
                str::stream() << "min: " << minKey.Obj() << " should be less than max: "
                              << maxKey.Obj()};
    }

    return ChunkRange(minKey.Obj().getOwned(), maxKey.Obj().getOwned());
}

bool ChunkRange::containsKey(const BSONObj& key) const {
    return _minKey.woCompare(key) <= 0 && key.woCompare(_maxKey) < 0;
}

void ChunkRange::append(BSONObjBuilder* builder) const {
    builder->append(kMinKey, _minKey);
    builder->append(kMaxKey, _maxKey);
}

std::string ChunkRange::toString() const {
    return str::stream() << "[" << _minKey << ", " << _maxKey << ")";
}

bool ChunkRange::operator==(const ChunkRange& other) const {
    return _minKey.woCompare(other._minKey) == 0 && _maxKey.woCompare(other._maxKey) == 0;
}

bool ChunkRange::operator!=(const ChunkRange& other) const {
    return !(*this == other);
}

StatusWith<ChunkType> ChunkType::fromBSON(const BSONObj& source) {
    ChunkType chunk;

    {
        std::string chunkNS;
        Status status = bsonExtractStringField(source, ns.name(), &chunkNS);
        if (!status.isOK())
            return status;
        chunk._ns = chunkNS;
    }

    {
        auto chunkRangeStatus = ChunkRange::fromBSON(source);
        if (!chunkRangeStatus.isOK())
            return chunkRangeStatus.getStatus();

        const auto chunkRange = std::move(chunkRangeStatus.getValue());
        chunk._min = chunkRange.getMin().getOwned();
        chunk._max = chunkRange.getMax().getOwned();
    }

    {
        std::string chunkShard;
        Status status = bsonExtractStringField(source, shard.name(), &chunkShard);
        if (!status.isOK())
            return status;
        chunk._shard = chunkShard;
    }

    {
        std::string processIdentityStr;
        Status status = bsonExtractStringField(source, processIdentity.name(), &processIdentityStr);
        if (!status.isOK()) {
            return status;
        }
        chunk._processIdentity = processIdentityStr;
    }

    {
        bool chunkJumbo;
        Status status = bsonExtractBooleanField(source, jumbo.name(), &chunkJumbo);
        if (status.isOK()) {
            chunk._jumbo = chunkJumbo;
        } else if (status == ErrorCodes::NoSuchKey) {
            // Jumbo status is missing, so it will be presumed false
        } else {
            return status;
        }
    }

    {
        auto versionStatus = ChunkVersion::parseFromBSONForChunk(source);
        if (!versionStatus.isOK()) {
            return versionStatus.getStatus();
        }
        chunk._version = std::move(versionStatus.getValue());
    }

    {
        long long chunkStatus;
        Status chunkstatus = bsonExtractIntegerField(source, status.name(), &chunkStatus);
        if (chunkstatus.isOK()) {
            // Make sure the state field falls within the valid range of ChunkState values.
            if (!isStatusValid(static_cast<ChunkStatus>(chunkStatus))) {
                return Status(ErrorCodes::BadValue,
                              str::stream() << "Invalid chunk status value: " << chunkStatus);
            } else {
                chunk._status = static_cast<ChunkStatus>(chunkStatus);
            }
        } else {
            return chunkstatus;
        }
    }

    {
        std::string rootFolderData;
        Status status = bsonExtractStringField(source, rootFolder.name(), &rootFolderData);

        if (status.isOK()) {
            chunk._rootFolder = rootFolderData;
        } else if (status == ErrorCodes::NoSuchKey) {
            // root folder is missing, so it will be presumed false
        } else {
            return status;
        }
    }

    {
        std::string chunkID;
        Status status = bsonExtractStringField(source, name.name(), &chunkID);

        if (!status.isOK())
            return status;

        chunk._id = std::move(chunkID);
    }

    return StatusWith<ChunkType>(chunk);
}

std::string ChunkType::genID(StringData ns, const BSONObj& o) {
    StringBuilder buf;
    buf << ns << "-";

    BSONObjIterator i(o);
    while (i.more()) {
        BSONElement e = i.next();
        buf << e.fieldName() << "_" << e.toString(false, true);
    }

    return buf.str();
}

std::string ChunkType::toID(const std::string& id) {
    invariant(!id.empty());
    std::string newId;
    if (id.size() < kChunkIDDigitWidth) {
        std::stringstream ss;
        ss << std::setfill('0') << std::setw(kChunkIDDigitWidth) << id;
        newId = ss.str();
    } else {
        newId = id;
    }

    return newId;
}

Status ChunkType::validate() const {
    if (!_ns.is_initialized() || _ns->empty()) {
        return Status(ErrorCodes::NoSuchKey, str::stream() << "missing " << ns.name() << " field");
    }

    if (!_min.is_initialized() || _min->isEmpty()) {
        return Status(ErrorCodes::NoSuchKey, str::stream() << "missing " << min.name() << " field");
    }

    if (!_max.is_initialized() || _max->isEmpty()) {
        return Status(ErrorCodes::NoSuchKey, str::stream() << "missing " << max.name() << " field");
    }

    if (!_version.is_initialized() || !_version->isSet()) {
        return Status(ErrorCodes::NoSuchKey, str::stream() << "missing version field");
    }

    if (!_shard.is_initialized() || !_shard->isValid()) {
        return Status(ErrorCodes::NoSuchKey,
                      str::stream() << "missing " << shard.name() << " field");
    }

    // 'min' and 'max' must share the same fields.
    if (_min->nFields() != _max->nFields()) {
        return {ErrorCodes::BadValue,
                str::stream() << "min and max don't have the same number of keys: " << *_min << ", "
                              << *_max};
    }

    BSONObjIterator minIt(getMin());
    BSONObjIterator maxIt(getMax());
    while (minIt.more() && maxIt.more()) {
        BSONElement minElem = minIt.next();
        BSONElement maxElem = maxIt.next();
        if (strcmp(minElem.fieldName(), maxElem.fieldName())) {
            return {ErrorCodes::BadValue,
                    str::stream() << "min and max don't have matching keys: " << *_min << ", "
                                  << *_max};
        }
    }

    // 'max' should be greater than 'min'.
    if (_min->woCompare(getMax()) >= 0) {
        return {ErrorCodes::BadValue,
                str::stream() << "max is not greater than min: " << *_min << ", " << *_max};
    }

    if (!isStatusValid(_status)) {
        return {ErrorCodes::BadValue,
                str::stream() << "chunk status(" << static_cast<int>(_status) << ") is invalid"};
    }

    return Status::OK();
}

BSONObj ChunkType::toBSON() const {
    BSONObjBuilder builder;

    builder.append(name.name(), getID());

    if (_ns)
        builder.append(ns.name(), getNS());
    if (_min)
        builder.append(min.name(), getMin());
    if (_max)
        builder.append(max.name(), getMax());
    if (_shard)
        builder.append(shard.name(), getShard().toString());
    if (!_processIdentity.empty())
        builder.append(processIdentity(), getProcessIdentity());
    if (_version)
        _version->appendForChunk(&builder);
    if (_jumbo)
        builder.append(jumbo.name(), getJumbo());

    if (_rootFolder)
        builder.append(rootFolder.name(), getRootFolder());

    if (isStatusValid(_status))
        builder.append(status.name(),
                       static_cast<std::underlying_type<ChunkStatus>::type>(getStatus()));

    return builder.obj();
}

std::string ChunkType::toString() const {
    return toBSON().toString();
}

std::string ChunkType::getName() const {
    // remove the leading 0
    std::string id_to_print = _id;
    id_to_print =
        id_to_print.erase(0, std::min(id_to_print.find_first_not_of('0'), id_to_print.size() - 1));

    return id_to_print;
}

void ChunkType::setName(const std::string& id) {
    invariant(!id.empty());
    _id = widthChunkID(id);
}


void ChunkType::setNS(const std::string& ns) {
    invariant(!ns.empty());
    _ns = ns;
}

void ChunkType::setMin(const BSONObj& min) {
    invariant(!min.isEmpty());
    _min = min;
}

void ChunkType::setMax(const BSONObj& max) {
    invariant(!max.isEmpty());
    _max = max;
}

void ChunkType::setVersion(const ChunkVersion& version) {
    invariant(version.isSet());
    _version = version;
}

void ChunkType::setShard(const ShardId& shard) {
    invariant(shard.isValid());
    _shard = shard;
}

void ChunkType::setProcessIdentity(const std::string& processIdentity) {
    invariant(!processIdentity.empty());
    _processIdentity = processIdentity;
}

void ChunkType::setJumbo(bool jumbo) {
    _jumbo = jumbo;
}

void ChunkType::setRootFolder(const std::string& rootFolder) {
    invariant(!rootFolder.empty());
    _rootFolder = rootFolder;
}

void ChunkType::clearRootFolder() {
    _rootFolder = "";
}

void ChunkType::setStatus(const ChunkStatus chunkstatus) {
    _status = chunkstatus;
}

std::string ChunkType::getCollectionIdent() const {
    std::string ident = _rootFolder.get();
    ident.assign(ident, 0, ident.rfind("/"));
    ident.assign(ident, ident.rfind("/") + 1, ident.size());
    index_LOG(1) << "getCollectionIdent() rootFolder: " << _rootFolder.get() << " ident: " << ident;
    return ident;
}

std::string ChunkType::getRelativeRootFolder() const {
    std::string relativeFolder = _rootFolder.get();
    relativeFolder.assign(relativeFolder,
                          getDataPath().length() + 1,
                          relativeFolder.length() - getDataPath().length() - 1);
    return relativeFolder;
}

std::string ChunkType::toSex(uint64_t id) {
    std::stringstream stringStream;
    stringStream << std::hex << id;
    return stringStream.str();
}

std::string ChunkType::widthChunkID(const std::string& chunkId) {
    if (chunkId.size() < kChunkIDDigitWidth) {
        std::stringstream ss;
        ss << std::setfill('0') << std::setw(kChunkIDDigitWidth) << chunkId;
        return ss.str();
    }

    return chunkId;
}

std::string ChunkType::getChunkDataPath(void) const {
    if (!_rootFolder.is_initialized()) {
        index_err() << "ChunkType::getChunkDataPath : _rootFolder not initialized ";
        return "";
    }

    std::string datapath = _rootFolder.get();
    for (auto i = 0; i < 2; i++) {
        if (datapath.rfind("/") == std::string::npos) {
            index_err() << "ChunkType::getChunkDataPath : _rootFolder invalied: " << datapath;
            return "";
        }
        datapath.assign(datapath, 0, datapath.rfind("/"));
    }
    return datapath;
}


}  // namespace mongo

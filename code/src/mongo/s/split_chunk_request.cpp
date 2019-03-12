#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/base/status_with.h"
#include "mongo/bson/bson_comparator_interface_base.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/logger/redaction.h"
#include "mongo/s/split_chunk_request.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {

const char ksplitChunk[] = "splitChunk";
const char kCollInfo[] = "collection";
const char kChunkInfo[] = "chunk";
const char kSplitPoint[] = "splitPoint";
const char kRightDBPath[] = "rightDBPath";
const char kRightChunkId[] = "rightChunkID";


}  // namespace

SplitChunkReq::SplitChunkReq(const CollectionType& coll,
                             const ChunkType& chunk,
                             const std::string& rightDBPath,
                             const std::string& rightChunkId,
                             const BSONObj& splitPoint)
    : _coll(coll),
      _chunk(chunk),
      _rightDBPath(rightDBPath),
      _rightChunkId(rightChunkId),
      _splitPoint(splitPoint) {}

StatusWith<SplitChunkReq> SplitChunkReq::createFromCommand(const BSONObj& cmdobj) {

    CollectionType coll;
    {
        BSONElement source;
        Status status = bsonExtractTypedField(cmdobj, kCollInfo, BSONType::Object, &source);
        if (!status.isOK()) {
            return status;
        }

        auto statuscoll = CollectionType::fromBSON(source.Obj());
        if (!statuscoll.isOK()) {
            return statuscoll.getStatus();
        }

        coll = std::move(statuscoll.getValue());
    }

    ChunkType chunk;
    {
        BSONElement source;
        Status status = bsonExtractTypedField(cmdobj, kChunkInfo, BSONType::Object, &source);
        if (!status.isOK()) {
            return status;
        }

        auto statuschunk = ChunkType::fromBSON(source.Obj());
        if (!statuschunk.isOK()) {
            return statuschunk.getStatus();
        }

        chunk = std::move(statuschunk.getValue());
    }

    std::string rightDBPath;
    {
        Status status = bsonExtractStringField(cmdobj, kRightDBPath, &rightDBPath);
        if (!status.isOK()) {
            return status;
        }
    }

    std::string rightChunkId;
    {
        Status status = bsonExtractStringField(cmdobj, kRightChunkId, &rightChunkId);
        if (!status.isOK()) {
            return status;
        }
    }

    BSONObj splitPoint;
    {
        if (cmdobj.hasField(kSplitPoint)) {
            BSONElement source;
            Status status = bsonExtractTypedField(cmdobj, kSplitPoint, BSONType::Object, &source);
            if (status.isOK()) {
                splitPoint = source.Obj().getOwned();
            }
        }
    }

    SplitChunkReq request(coll, chunk, rightDBPath, rightChunkId, splitPoint);
    return request;
}

void SplitChunkReq::appendAsCommand(BSONObjBuilder* builder,
                                    const ChunkType& chunk,
                                    const CollectionType& coll,
                                    const std::string& rightDBPath,
                                    const std::string& rightChunkId,
                                    const BSONObj& splitPoint) {
    invariant(builder->asTempObj().isEmpty());

    appendAsCommand(builder, chunk, coll, rightDBPath, rightChunkId);
    builder->append(kSplitPoint, splitPoint);
}

void SplitChunkReq::appendAsCommand(BSONObjBuilder* builder,
                                    const ChunkType& chunk,
                                    const CollectionType& coll,
                                    const std::string& rightDBPath,
                                    const std::string& rightChunkId) {

    builder->append(ksplitChunk, chunk.getNS());
    builder->append(kCollInfo, coll.toBSON());
    builder->append(kChunkInfo, chunk.toBSON());
    builder->append(kRightDBPath, rightDBPath);
    builder->append(kRightChunkId, rightChunkId);
}

std::string SplitChunkReq::toString() const {
    std::stringstream ss;
    ss << "split: " << _coll.toBSON() << "; " << _chunk.toBSON()
       << ";  rightDbPath: " << _rightDBPath << "; splitPoint: " << _splitPoint;
    return ss.str();
}

void SplitChunkReq::setNs(const NamespaceString& ns) {
    _coll.setNs(ns);
}

bool SplitChunkReq::validSplitPoint() const {
    if (getSplitPoint().isEmpty()) {
        return true;
    }

    if (_splitPoint.woCompare(getMinKey()) <= 0) {
        return false;
    }

    if (_splitPoint.woCompare(getMaxKey()) >= 0) {
        return false;
    }

    // todo  key pattern and other field
    return true;
}

}  // namespace mongo

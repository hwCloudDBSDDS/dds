#include "mongo/platform/basic.h"

#include "mongo/base/status_with.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/logger/redaction.h"
#include "mongo/s/assign_chunk_request.h"

namespace mongo {
namespace {

const char kAssignChunk[] = "assignChunk";
const char kCollInfo[] = "collection";
const char kChunkInfo[] = "chunk";
const char kNewChunkFlag[] = "newChunkFlag";
const char kTargetShardName[] = "shardName";
const char kTargetProcessIdentity[] = "processIdentity";


}  // namespace

AssignChunkRequest::AssignChunkRequest(CollectionType& coll,
                                       ChunkType& chunk,
                                       bool newChunkFlag,
                                       std::string shardName,
                                       std::string processIdentity)
    : _coll(coll),
      _chunk(chunk),
      _newChunkFlag(newChunkFlag),
      _shardName(std::move(shardName)),
      _processIdentity(std::move(processIdentity)) {}

StatusWith<AssignChunkRequest> AssignChunkRequest::createFromCommand(const BSONObj& cmdobj) {

    ChunkType chunk;
    CollectionType coll;
    bool newflag;
    std::string shardName;
    std::string processIdentity;
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

    {
        Status status = bsonExtractBooleanField(cmdobj, kNewChunkFlag, &newflag);
        if (!status.isOK()) {
            return status;
        }
    }

    {
        Status status = bsonExtractStringField(cmdobj, kTargetShardName, &shardName);
        if (!status.isOK()) {
            return status;
        }
    }

    {
        Status status = bsonExtractStringField(cmdobj, kTargetProcessIdentity, &processIdentity);
        if (!status.isOK()) {
            return status;
        }
    }

    AssignChunkRequest request(coll, chunk, newflag, shardName, processIdentity);
    return request;
}

void AssignChunkRequest::appendAsCommand(BSONObjBuilder* builder,
                                         const ChunkType& chunk,
                                         const CollectionType& coll,
                                         bool newflag,
                                         const std::string& shardName,
                                         const std::string& processIdentity) {
    invariant(builder->asTempObj().isEmpty());

    builder->append(kAssignChunk, chunk.getNS());
    builder->append(kCollInfo, coll.toBSON());
    builder->append(kChunkInfo, chunk.toBSON());
    builder->append(kNewChunkFlag, newflag);
    builder->append(kTargetShardName, shardName);
    builder->append(kTargetProcessIdentity, processIdentity);
}

std::string AssignChunkRequest::toString() const {
    std::stringstream ss;
    ss << "assign: " << _coll.toBSON() << ", " << _chunk.toBSON() << ", "
       << "newchunkflag=" << _newChunkFlag << ", "
       << "shardName=" << _shardName << ", "
       << "processIdentity=" << _processIdentity;
    return ss.str();
}

void AssignChunkRequest::setNs(const NamespaceString& ns) {
    _coll.setNs(ns);
}

void AssignChunkRequest::setNewChunkFlag(bool newChunkFlag) {
    _newChunkFlag = newChunkFlag;
}

}  // namespace mongo

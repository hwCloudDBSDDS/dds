#include "mongo/platform/basic.h"

#include "mongo/base/status_with.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/logger/redaction.h"
#include "mongo/s/offload_chunk_request.h"

namespace mongo {
namespace {

const char kOffloadChunk[] = "offloadChunk";
const char kChunkInfo[] = "chunk";


}  // namespace

OffloadChunkRequest::OffloadChunkRequest(ChunkType &chunk)
        :_chunk(chunk){}

StatusWith<OffloadChunkRequest> OffloadChunkRequest::createFromCommand(const BSONObj& cmdobj) {

    ChunkType      chunk;

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

    OffloadChunkRequest request(chunk);
    return request;
}

void OffloadChunkRequest::appendAsCommand(BSONObjBuilder* builder,
                                                    const ChunkType &chunk) {
    invariant(builder->asTempObj().isEmpty());

    builder->append(kOffloadChunk, chunk.getNS());
    builder->append(kChunkInfo, chunk.toBSON());
}

std::string OffloadChunkRequest::toString() const {
    std::stringstream ss;
    ss << "offload: "<< _chunk.toBSON();
    return ss.str();
}

}  // namespace mongo



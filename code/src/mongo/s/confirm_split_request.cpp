#include "mongo/platform/basic.h"

#include "mongo/base/status_with.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/logger/redaction.h"
#include "mongo/s/confirm_split_request.h"

namespace mongo {
    namespace {

        const char kConfirmsplit[] = "confirmSplit";
        const char kCollInfo[] = "collection";
        const char kChunkInfo[] = "chunk";
        const char kSplitSuccess[] = "splitSuccess";

    }  // namespace

    ConfirmSplitRequest::ConfirmSplitRequest(const CollectionType &coll, const ChunkType &chunk, bool suc)
        : _coll(coll), _chunk(chunk), _splitSuccess(suc) {}

    StatusWith<ConfirmSplitRequest> ConfirmSplitRequest::createFromCommand(const BSONObj& cmdobj) {

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

        bool splitSuc;
        {
            Status status = bsonExtractBooleanField(cmdobj, kSplitSuccess, &splitSuc);
            if (!status.isOK()) {
                return status;
            }
        }

        return ConfirmSplitRequest(coll, chunk, splitSuc);

    }

    void ConfirmSplitRequest::appendAsCommand(BSONObjBuilder* builder,
                                              const ChunkType &chunk,
                                              const CollectionType &coll,
                                              bool splitSuc) {

        builder->append(kConfirmsplit, chunk.getNS());
        builder->append(kCollInfo, coll.toBSON());
        builder->append(kChunkInfo, chunk.toBSON());
        builder->append(kSplitSuccess, splitSuc);
    }

    std::string ConfirmSplitRequest::toString() const {
        std::stringstream ss;
        ss << "confirmsplit: " << _coll.toBSON() << "; " << _chunk.toBSON() << ";  splitSucess: " << _splitSuccess;

        return ss.str();
    }

    void ConfirmSplitRequest::setNs(const NamespaceString & ns){
        _coll.setNs(ns);
    }

}  // namespace mongo

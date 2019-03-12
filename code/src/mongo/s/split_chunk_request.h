#pragma once

#include <string>

#include "mongo/client/connection_string.h"
#include "mongo/db/namespace_string.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/chunk_version.h"

namespace mongo {

class BSONObjBuilder;
template <typename T>
class StatusWith;


class SplitChunkReq {
public:
    static StatusWith<SplitChunkReq> createFromCommand(const BSONObj& cmdobj);

    static void appendAsCommand(BSONObjBuilder* builder,
                                const ChunkType& chunk,
                                const CollectionType& coll,
                                const std::string& rightDBPath,
                                const std::string& rightChunkId,
                                const BSONObj& splitPoint);

    static void appendAsCommand(BSONObjBuilder* builder,
                                const ChunkType& chunk,
                                const CollectionType& coll,
                                const std::string& rightDBPath,
                                const std::string& rightChunkId);

    const NamespaceString& getNss() const {
        return _coll.getNs();
    }

    const ChunkType& getChunk() const {
        return _chunk;
    }

    const CollectionType& getCollection() const {
        return _coll;
    }

    void setNs(const NamespaceString& ns);

    std::string getName() const {
        return _chunk.getName();
    }

    const BSONObj& getMinKey() const {
        return _chunk.getMin();
    }

    const BSONObj& getMaxKey() const {
        return _chunk.getMax();
    }

    const std::string& getRightChunkId() const {
        return _rightChunkId;
    }

    std::string getRightChunkName() const {
        // remove the leading 0
        std::string name = _rightChunkId;
        name = name.erase(0, std::min(name.find_first_not_of('0'), name.size() - 1));
        return name;
    }

    const BSONObj& getSplitPoint() const {
        return _splitPoint;
    }

    const std::string& getFullRightDBPath() const {
        return _fullRightDBPath;
    }

    void setFullRightDBPath(const std::string& fullRightDBPath) {
        _fullRightDBPath = fullRightDBPath;
    }

    const std::string& getRightDBPath() const {
        return _rightDBPath;
    }

    bool validSplitPoint() const;

    std::string toString() const;

private:
    SplitChunkReq(const CollectionType& coll,
                  const ChunkType& chunk,
                  const std::string& rightDBPath,
                  const std::string& rightChunkId,
                  const BSONObj& splitPoint);

    CollectionType _coll;
    ChunkType _chunk;
    std::string _rightDBPath;
    std::string _rightChunkId;
    BSONObj _splitPoint;
    std::string _fullRightDBPath;
};

}  // namespace mongo

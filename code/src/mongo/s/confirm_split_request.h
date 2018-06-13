#pragma once

#include <string>

#include "mongo/client/connection_string.h"
#include "mongo/db/namespace_string.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/chunk_version.h"

namespace mongo {

    class BSONObjBuilder;
    template <typename T>
    class StatusWith;

    class ConfirmSplitRequest {
    public:

        static StatusWith<ConfirmSplitRequest> createFromCommand(const BSONObj& cmdobj);

        static void appendAsCommand(BSONObjBuilder* builder,
                                   const ChunkType &chunk,
                                   const CollectionType &coll,
                                   bool splitSuc);

        const NamespaceString& getNss() const {
            return _coll.getNs();
        }

        const ChunkType&  getChunk() const{
            return _chunk;
        }

        const CollectionType&  getCollection() const{
            return _coll;
        }

        void setNs(const NamespaceString & ns);

        std::string getName() const {
            return _chunk.getName();
        }

        const ChunkVersion& getChunkVersion() const {
            return _chunk.getVersion();
        }

        bool splitSuccess()const {
            return _splitSuccess;
        }

        std::string toString() const;

    private:
        ConfirmSplitRequest(const CollectionType &coll, const ChunkType &chunk, bool suc);

        CollectionType _coll;
        ChunkType      _chunk;
        bool _splitSuccess;
    };

}  // namespace mongo


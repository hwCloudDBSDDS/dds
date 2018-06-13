/******************************************************************************
                Copyright 1999 - 2017, Huawei Tech. Co., Ltd.
                           ALL RIGHTS RESERVED
  File Name     : assign_chunk_request.h
  Version       : Initial Draft
  Author        : 
  Created       : 2017/6/21
  Description   : assign chunk request
  History       :
  1.Date        : 2017/6/21
    Author      : 
    Modification: Created file

******************************************************************************/

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


class AssignChunkRequest {
public:

    static StatusWith<AssignChunkRequest> createFromCommand(const BSONObj& cmdobj);

    static void appendAsCommand(BSONObjBuilder* builder,
                                const ChunkType &chunk,
                                const CollectionType &coll,
                                bool newflag, 
                                const std::string& shardName,
                                const std::string& processIdentity);

    const NamespaceString& getNss() const {
        return _coll.getNs();
    }

    ChunkType  getChunk() const{

        return _chunk;
    }

    CollectionType  getCollection() const{

        return _coll;
    }
    
    void setNs(const NamespaceString & ns);
    
  
    std::string getName() const {
        return _chunk.getName();
    }

    const BSONObj& getMinKey() const {
        return _chunk.getMin();
    }

    const BSONObj& getMaxKey() const {
        return _chunk.getMax();
    }

    std::string toString() const;

    bool getNewChunkFlag() const {
        return _newChunkFlag;
    }
    void setNewChunkFlag(bool newChunkFlag);

    const std::string& getShardName() const {
        return _shardName;
    }

    const std::string& getProcessIdentity() const {
        return _processIdentity;
    }

private:
    AssignChunkRequest(CollectionType &coll, ChunkType &chunk, bool newChunkFlag, 
                       std::string shardName, std::string processIdentity);

    CollectionType _coll;
    ChunkType      _chunk;
    bool           _newChunkFlag;
    std::string    _shardName;
    std::string    _processIdentity;
    //chunkfolder;
};

}  // namespace mongo


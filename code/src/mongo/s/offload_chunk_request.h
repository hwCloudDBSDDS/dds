/******************************************************************************
                Copyright 1999 - 2017, Huawei Tech. Co., Ltd.
                           ALL RIGHTS RESERVED
  File Name     : offload_chunk_request.h
  Version       : Initial Draft
  Author        : 
  Created       : 2017/6/21
  Description   : offloadchunkreques.h
  History       :
  1.Date        : 2017/6/21
    Author      : 
    Modification: Created file

******************************************************************************/

#pragma once

#include <string>

#include "mongo/client/connection_string.h"
#include "mongo/db/namespace_string.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/chunk_version.h"

namespace mongo {

class BSONObjBuilder;
template <typename T>
class StatusWith;


class OffloadChunkRequest {
public:

    static StatusWith<OffloadChunkRequest> createFromCommand(const BSONObj& cmdobj);

    static void appendAsCommand(BSONObjBuilder* builder,
                                       const ChunkType &chunk);

    std::string getName() const {
        return _chunk.getName();
    }

    const BSONObj& getMinKey() const {
        return _chunk.getMin();
    }

    const BSONObj& getMaxKey() const {
        return _chunk.getMax();
    }
    
    const std::string & getNss() const {

        return _chunk.getNS();
    }

    std::string toString() const;

private:
    OffloadChunkRequest(ChunkType &chunk);

    ChunkType      _chunk;
};

}  // namespace mongo



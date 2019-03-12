#pragma once

#include <string>
#include "mongo/platform/basic.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/base/status.h"

namespace mongo {

using std::string;

class OperationContext;

namespace operationutil{
bool getChunk(OperationContext* txn, 
              const string& ns, 
	      ChunkType& chunk, 
	      string& errmsg);

bool copyData(OperationContext* txn,
              const string& source,
	      const string& target,
	      bool capped,
	      double size,
	      string& errmsg,
	      BSONObjBuilder& result);

bool copyIndexes(OperationContext* txn,
                 const std::string& source,
		 const std::string& target,
		 std::string& errmsg,
		 BSONObjBuilder& result);

bool renameCollection(OperationContext* txn,
                      const string& source,
		      const string& target,
		      string& errmsg,
		      BSONObjBuilder& result);

Status removeCollAndIndexMetadata(OperationContext* txn,
                                   const NamespaceString& nss,
				   BSONObjBuilder& result);
}
}

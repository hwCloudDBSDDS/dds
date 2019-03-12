#include "mongo/base/status.h"

namespace mongo {
class BSONObjBuilder;
class NamespaceString;
class OperationContext;

Status copyData(OperationContext* txn,
                const NamespaceString& sourceNS,
                const NamespaceString& targetNS,
                std::string& errmsg,
                BSONObjBuilder& result);
}

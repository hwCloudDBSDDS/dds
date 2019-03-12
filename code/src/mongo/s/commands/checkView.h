#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/platform/basic.h"
#pragma once
namespace mongo {
using std::string;

StatusWith<bool> isView(OperationContext* txn, const string& dbName, const NamespaceString& nss);
}

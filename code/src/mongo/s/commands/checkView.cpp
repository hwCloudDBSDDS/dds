#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include "mongo/s/commands/checkView.h"

#include "mongo/bson/bsonobj_comparator.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/connpool.h"
#include "mongo/db/commands.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/matcher/extensions_callback_disallow_extensions.h"
#include "mongo/db/matcher/extensions_callback_noop.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/query/cursor_request.h"
#include "mongo/db/query/cursor_response.h"
#include "mongo/db/query/parsed_distinct.h"
#include "mongo/db/query/view_response_formatter.h"
#include "mongo/db/views/resolved_view.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/client/shard_connection.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/cluster_last_error_info.h"
#include "mongo/s/commands/cluster_commands_common.h"
#include "mongo/s/commands/cluster_explain.h"
#include "mongo/s/commands/sharded_command_processing.h"
#include "mongo/s/config.h"
#include "mongo/s/grid.h"
#include "mongo/s/query/store_possible_cursor.h"
#include "mongo/s/sharding_raii.h"
#include "mongo/s/stale_exception.h"
#include "mongo/scripting/engine.h"
#include "mongo/util/log.h"
#include "mongo/util/timer.h"

namespace mongo {
using std::string;
using std::list;

StatusWith<bool> isView(OperationContext* txn, const string& dbName, const NamespaceString& nss) {

    auto config = uassertStatusOK(grid.catalogCache()->getDatabase(txn, nss.db().toString()));
    auto success = config->reload(txn);
    if (!success) {
        return Status(ErrorCodes::NamespaceNotFound, dbName + " not found");
    }

    ConnectionString shardConnString;
    {
        const auto shard =
            uassertStatusOK(grid.shardRegistry()->getShard(txn, config->getPrimaryId()));
        shardConnString = shard->getConnString();
    }
    ScopedDbConnection conn(shardConnString);
    try {
        BSONObj res;
        {
            list<BSONObj> all =
                conn->getCollectionInfos(config->name(), BSON("name" << nss.coll()));
            if (!all.empty()) {
                res = all.front().getOwned();
            }
        }
        if (!res.isEmpty()) {
            std::string namespaceType;
            auto status = bsonExtractStringField(res, "type", &namespaceType);
            if (!status.isOK()) {
                conn.done();
                return status;
            }
            conn.done();
            return (namespaceType == "view");
        }
        conn.done();
    } catch (const DBException& e) {
        config->reload(txn);
        conn.done();
        int code = e.getCode();
        if (code == 13328) {
            index_err() << "[isView] code 13328 , reason :" << e.what();
            return Status(ErrorCodes::HostUnreachable, e.what());
        }
        if (code == 18630) {
            index_err() << "[isView] code 18630 , reason :" << e.what();
            return Status(ErrorCodes::NotYetInitialized, "shard not being initialized");
        } else {
            return e.toStatus();
        }
    }
    return false;
}
}  // namespace

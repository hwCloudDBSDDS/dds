#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include "mongo/db/repl/repl_set_command.h"

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/server_status_metric.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/op_observer.h"
#include "mongo/db/repl/initial_sync.h"
#include "mongo/db/repl/old_update_position_args.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/repl_extend/shard_server_heartbeat_args.h"
#include "mongo/db/repl/replication_coordinator_external_state_impl.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/repl/replication_executor.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/update_position_args.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/executor/network_interface.h"
#include "mongo/transport/session.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/time_support.h"
#include "mongo/s/grid.h"
#include "mongo/s/catalog/sharding_catalog_manager.h"
#include "mongo/s/catalog/sharding_catalog_manager_impl.h"
#include "mongo/s/catalog/catalog_extend/sharding_catalog_shard_server_manager.h"

namespace mongo {
namespace repl {

using std::string;
using std::stringstream;
using str::stream;

/* { shardServerHeartbeat : <hostAndPort> } */
class CmdShardServerHeartbeat : public ReplSetCommand {
public:
    CmdShardServerHeartbeat() : ReplSetCommand("shardServerHeartbeat") {}
    virtual bool run(OperationContext* txn,
                     const string&,
                     BSONObj& cmdObj,
                     int,
                     string& errmsg,
                     BSONObjBuilder& result) {        
        LOG(3) << "Receive heartbeat cmd (" << cmdObj.toString() << ")";                

        Status primaryStatus = getGlobalReplicationCoordinator()->checkIfIAmPrimary(); 
        if (!primaryStatus.isOK()) {
            log() << "Heartbeat cmd (" << cmdObj.toString() << ") fails" << causedBy(primaryStatus);
            return appendCommandStatus(result, primaryStatus);
        }
        
        ShardServerHeartbeatArgs args;
        Status argsStatus = args.initialize(cmdObj);
        if (!argsStatus.isOK()) {
            log() << "Heartbeat cmd (" << cmdObj.toString() << ") fails" << causedBy(argsStatus);
            return appendCommandStatus(result, argsStatus);
        }    
        if (!args.hasSender()) {
            Status status = Status(ErrorCodes::InternalError, 
                "Heartbeat msg does not contain sender info");
            log() << "Heartbeat cmd (" << cmdObj.toString() << ") fails" << causedBy(status);
            return appendCommandStatus(result, status);
        }
        
        HostAndPort senderHost = args.getSenderHost();
        Date_t now = Date_t::now();
        Status setLastHeartbeatTimeStatus = 
            Grid::get(txn)->catalogManager()->getShardServerManager()->setLastHeartbeatTime(
                txn, senderHost, now);    
        if ((!setLastHeartbeatTimeStatus.isOK()) && 
            (setLastHeartbeatTimeStatus.code() != ErrorCodes::NotYetInitialized)) {
            Status status = Status(ErrorCodes::InternalError, 
                stream() << "Failed to set last heartbeat time for shard server: " 
                    << senderHost.toString() << causedBy(setLastHeartbeatTimeStatus));
            log() << "Heartbeat cmd (" << cmdObj.toString() << ") fails" << causedBy(status);
            return appendCommandStatus(result, status);
        }
        
        LOG(3) << "Heartbeat cmd (" << cmdObj.toString() << ") OK";        
        return appendCommandStatus(result, Status::OK());
    }
} cmdShardServerHeartbeat;

}  // namespace repl
}  // namespace mongo

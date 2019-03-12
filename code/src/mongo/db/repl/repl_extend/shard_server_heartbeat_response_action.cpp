#include "mongo/platform/basic.h"

#include "mongo/db/repl/repl_extend/shard_server_heartbeat_response_action.h"

namespace mongo {
namespace repl {

ShardServerHeartbeatResponseAction::ShardServerHeartbeatResponseAction()
    : _action(NoAction), _primaryConfigServer("") {}

ShardServerHeartbeatResponseAction ShardServerHeartbeatResponseAction::makeNoAction(
    const std::string& primaryConfigServer) {
    ShardServerHeartbeatResponseAction result;
    result._action = NoAction;
    result._primaryConfigServer = primaryConfigServer;
    return result;
}

ShardServerHeartbeatResponseAction ShardServerHeartbeatResponseAction::makeKillSelfAction(
    const std::string& primaryConfigServer) {
    ShardServerHeartbeatResponseAction result;
    result._action = KillSelf;
    result._primaryConfigServer = primaryConfigServer;
    return result;
}

ShardServerHeartbeatResponseAction ShardServerHeartbeatResponseAction::makeSwitchTargetAction(
    const std::string& primaryConfigServer) {
    ShardServerHeartbeatResponseAction result;
    result._action = SwitchTarget;
    result._primaryConfigServer = primaryConfigServer;
    return result;
}

void ShardServerHeartbeatResponseAction::setNextHeartbeatStartDate(Date_t when) {
    _nextHeartbeatStartDate = when;
}

}  // namespace repl
}  // namespace mongo

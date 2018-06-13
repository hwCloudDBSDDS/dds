#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kReplication

#include "mongo/platform/basic.h"

#include <algorithm>
#include <vector>
#include <stdlib.h>

#include "mongo/base/status.h"
#include "mongo/db/repl/repl_extend/shard_server_heartbeat_coordinator.h"
#include "mongo/db/server_options.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/stdx/functional.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/exit.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/time_support.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/util_extend/default_parameters.h"
#include "mongo/db/modules/rocks/src/GlobalConfig.h"

namespace mongo {

using str::stream;

namespace {

ShardServerHeartbeatCoordinator* globalShardServerHeartbeatCoordinator = NULL;

}  // namespace

ShardServerHeartbeatCoordinator* getGlobalShardServerHeartbeatCoordinator() {
    fassert(50000, globalShardServerHeartbeatCoordinator);

    return globalShardServerHeartbeatCoordinator;
}

void setGlobalShardServerHeartbeatCoordinator(
    std::unique_ptr<ShardServerHeartbeatCoordinator>&& shardServerHeartbeatCoordinator) {

    fassert(50001, shardServerHeartbeatCoordinator.get());

    delete globalShardServerHeartbeatCoordinator;

    globalShardServerHeartbeatCoordinator = shardServerHeartbeatCoordinator.release();
}

namespace repl {

namespace {

using LockGuard = stdx::lock_guard<stdx::mutex>;

const int kMaxHeartbeatRetries = 2;

}  // namespace

using executor::RemoteCommandRequest;

ShardServerHeartbeatCoordinator::ShardServerHeartbeatCoordinator(
    std::vector<HostAndPort> configServers) : _configServers(configServers){
    executor::NetworkInterface* network =
        executor::makeNetworkInterface("NetworkInterfaceASIO-Heartbeat").release();
    int64_t prngSeed = static_cast<int64_t>(curTimeMillis64());
    _replExecutor = new ReplicationExecutor(network, prngSeed);
}

ShardServerHeartbeatCoordinator::~ShardServerHeartbeatCoordinator() {
    delete(_replExecutor);
}

void ShardServerHeartbeatCoordinator::startup(const HostAndPort& primaryConfigServer) {
    _replExecutor->startup();
    _startHeartbeats(primaryConfigServer);
}

void ShardServerHeartbeatCoordinator::shutdown() {
    _cancelHeartbeats();
    _replExecutor->shutdown();
    _replExecutor->join();
}

void ShardServerHeartbeatCoordinator::_startHeartbeats(const HostAndPort& primaryConfigServer) {   
    const Date_t now = _replExecutor->now();
    _scheduleHeartbeatToTarget(primaryConfigServer, now);
}

void ShardServerHeartbeatCoordinator::_cancelHeartbeats() {
    LockGuard heartbeatHandlesLock(_heartbeatHandlesMutex);
    std::for_each(_heartbeatHandles.begin(),
                  _heartbeatHandles.end(),
                  stdx::bind(&ReplicationExecutor::cancel, _replExecutor, stdx::placeholders::_1));
}

void ShardServerHeartbeatCoordinator::_restartHeartbeats(const HostAndPort& primaryConfigServer) {
    _cancelHeartbeats();
    _startHeartbeats(primaryConfigServer);
}

void ShardServerHeartbeatCoordinator::_scheduleHeartbeatToTarget(const HostAndPort& target,
                                                            Date_t when) {  
    LOG(2) << HostAndPort(serverGlobalParams.bind_ip, serverGlobalParams.port).toString()
        << " schedules heartbeat to " << target 
        << " after " << (Milliseconds)(when - _replExecutor->now());

    _trackHeartbeatHandle(_replExecutor->scheduleWorkAt(when,
                            stdx::bind(&ShardServerHeartbeatCoordinator::_doHeartbeat,
                                        this,
                                        stdx::placeholders::_1,
                                        target)));
}

void ShardServerHeartbeatCoordinator::_doHeartbeat(ReplicationExecutor::CallbackArgs cbData,
                                                    const HostAndPort& target) {
    _untrackHeartbeatHandle(cbData.myHandle);
    if (cbData.status.code() == ErrorCodes::CallbackCanceled) {
        log() << "Heartbeat canceled" << causedBy(cbData.status);
        return;
    }

    const Date_t now = _replExecutor->now();
    BSONObj heartbeatObj;
    Milliseconds timeout(0);
    const std::pair<ShardServerHeartbeatArgs, Milliseconds> hbRequest =
        _prepareShardServerHeartbeatRequest(now, target);
    heartbeatObj = hbRequest.first.toBSON();
    timeout = hbRequest.second;

    const RemoteCommandRequest request(
        target, "admin", heartbeatObj, nullptr, timeout);
    const ReplicationExecutor::RemoteCommandCallbackFn callback =
        stdx::bind(&ShardServerHeartbeatCoordinator::_handleHeartbeatResponse,
                   this,
                   stdx::placeholders::_1);

    _trackHeartbeatHandle(_replExecutor->scheduleRemoteCommand(request, callback));
}

std::pair<ShardServerHeartbeatArgs, Milliseconds>
ShardServerHeartbeatCoordinator::_prepareShardServerHeartbeatRequest(
    Date_t now, const HostAndPort& target) {
    if (_heartbeatRetryCount > kMaxHeartbeatRetries) {
        // This is the first heartbeat with _heartbeatRetryCount = std::numeric_limits<int>::max()
        _lastHeartbeatHitDate = now;
        _heartbeatRetryCount = 0;
    }
    ShardServerHeartbeatArgs hbArgs;
    hbArgs.setSenderHost(HostAndPort(serverGlobalParams.bind_ip, serverGlobalParams.port));

    Milliseconds alreadyElapsed(now.asInt64() - _lastHeartbeatHitDate.asInt64());
    const Milliseconds timeout(std::min(kDefaultShardHeartbeatTimeoutPeriod - alreadyElapsed, kDefaultShardHeartbeatInterval));

    return std::make_pair(hbArgs, timeout);
}

void ShardServerHeartbeatCoordinator::_handleHeartbeatResponse(
    const ReplicationExecutor::RemoteCommandCallbackArgs& cbData) {
    // remove handle from queued heartbeats
    _untrackHeartbeatHandle(cbData.myHandle);
    if (cbData.response.status.code() == ErrorCodes::CallbackCanceled) {
        log() << "Heartbeat canceled" << causedBy(cbData.response.status);
        return;
    }

    const HostAndPort& target = cbData.request.target;
    Status cmdStatus = cbData.response.status;

    bool heartbeatMiss;

    if (!cmdStatus.isOK()) {
        heartbeatMiss = true;
        log() << "Heartbeat misses for target: " << target.toString()
            << causedBy(cmdStatus);
    }
    else {
        Status heartbeatResultStatus = getStatusFromCommandResult(cbData.response.data);
        if (!heartbeatResultStatus.isOK()) {
            heartbeatMiss = true;
            log() << "Heartbeat misses for target: " << target.toString()
                << causedBy(heartbeatResultStatus);
        }
        else {
            heartbeatMiss = false;
            LOG(2) << "Heartbeat hits for target: " << target.toString();
        }
    }

    const Date_t now = _replExecutor->now();
    Milliseconds alreadyElapsed(now.asInt64() - _lastHeartbeatHitDate.asInt64());
    ShardServerHeartbeatResponseAction nextAction;

    if (heartbeatMiss) {
        if (alreadyElapsed >= kDefaultShardHeartbeatTimeoutPeriod) {
            // Kill self when heartbeat timeout
            nextAction = ShardServerHeartbeatResponseAction::makeKillSelfAction(target.toString());
            nextAction.setNextHeartbeatStartDate(now);
        } else if (_heartbeatRetryCount >= kMaxHeartbeatRetries) {
            // Try another target after kMaxHeartbeatRetries tries now
            int configServerIndex;
            for (configServerIndex = 0; configServerIndex < (int)_configServers.size(); configServerIndex++) {
                if (_configServers[configServerIndex] == target) {
                    break;
                }
            }
            HostAndPort& nextConfigServer = _configServers[(configServerIndex + 1)%((int)_configServers.size())];
            nextAction = ShardServerHeartbeatResponseAction::makeSwitchTargetAction(nextConfigServer.toString());
            nextAction.setNextHeartbeatStartDate(now + kDefaultShardHeartbeatRetryInterval);

            _heartbeatRetryCount = 0;
        } else {
            // Try again now
            nextAction = ShardServerHeartbeatResponseAction::makeNoAction(target.toString());
            nextAction.setNextHeartbeatStartDate(now + kDefaultShardHeartbeatRetryInterval);

            _heartbeatRetryCount++;
        }
    }
    else {
        // Still send heartbeat to the previous target primary config server after a heartbeat interval
        nextAction = ShardServerHeartbeatResponseAction::makeNoAction(target.toString());
        nextAction.setNextHeartbeatStartDate(now + kDefaultShardHeartbeatInterval);

        _lastHeartbeatHitDate = now;
        _heartbeatRetryCount = 0;
    }

    // Check if need to kill self (in the case of network partition between shard server and config server)
    if (nextAction.getAction() == ShardServerHeartbeatResponseAction::KillSelf) {
        log() << "Exit due to heartbeat timeout";
       // exitCleanly(EXIT_CLEAN);
       // quickExit(0);
        ::_Exit(EXIT_FAILURE);
    }

    _scheduleHeartbeatToTarget(
        HostAndPort(nextAction.getPrimaryConfigServer()), std::max(now, nextAction.getNextHeartbeatStartDate()));
}

void ShardServerHeartbeatCoordinator::_trackHeartbeatHandle(
    const StatusWith<ReplicationExecutor::CallbackHandle>& handle) {
    LockGuard heartbeatHandlesLock(_heartbeatHandlesMutex);
    if (handle.getStatus() == ErrorCodes::ShutdownInProgress) {
        return;
    }
    fassert(20000, handle.getStatus());
    _heartbeatHandles.push_back(handle.getValue());
}

void ShardServerHeartbeatCoordinator::_untrackHeartbeatHandle(
    const ReplicationExecutor::CallbackHandle& handle) {
    LockGuard heartbeatHandlesLock(_heartbeatHandlesMutex);
    const HeartbeatHandles::iterator newEnd =
        std::remove(_heartbeatHandles.begin(), _heartbeatHandles.end(), handle);
    invariant(newEnd != _heartbeatHandles.end());
    _heartbeatHandles.erase(newEnd, _heartbeatHandles.end());
}

}  // namespace repl
}  // namespace mongo

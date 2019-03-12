#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "mongo/db/repl/repl_extend/shard_server_heartbeat_args.h"
#include "mongo/db/repl/repl_extend/shard_server_heartbeat_response_action.h"
#include "mongo/db/repl/replication_executor.h"
#include "mongo/executor/network_interface.h"
#include "mongo/executor/network_interface_factory.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/net/hostandport.h"

namespace mongo {

class Timer;
template <typename T>
class StatusWith;

using executor::NetworkInterface;

namespace repl {

class ReplicationExecutor;
class ReplicationCoordinator;
class ReplicationCoordinatorImpl;

class ShardServerHeartbeatCoordinator {
    MONGO_DISALLOW_COPYING(ShardServerHeartbeatCoordinator);

public:
    // Takes ownership of the "externalState", "topCoord" and "network" objects.
    ShardServerHeartbeatCoordinator(std::vector<HostAndPort> configServers);

    virtual ~ShardServerHeartbeatCoordinator();

    void startup(const HostAndPort& primaryConfigServer);

    void shutdown();

    ReplicationExecutor* getExecutor() {
        return _replExecutor;
    }

private:
    typedef std::vector<ReplicationExecutor::CallbackHandle> HeartbeatHandles;

    /**
     * Starts a heartbeat for each member in the current config.  Called while holding _topoMutex
     * and replCoord _mutex.
     */
    void _startHeartbeats(const HostAndPort& primaryConfigServer);

    /**
     * Cancels all heartbeats.  Called while holding _topoMutex and replCoord _mutex.
     */
    void _cancelHeartbeats();

    /**
     * Cancels all heartbeats, then starts a heartbeat for each member in the current config.
     * Called while holding _topoMutex and replCoord _mutex.
     */
    void _restartHeartbeats(const HostAndPort& primaryConfigServer);

    /**
     * Schedules a heartbeat to be sent to "target" at "when".
     */
    void _scheduleHeartbeatToTarget(const HostAndPort& target, Date_t when);

    /**
     * Asynchronously sends a heartbeat to "target".
     *
     * Scheduled by _scheduleHeartbeatToTarget.
     */
    void _doHeartbeat(ReplicationExecutor::CallbackArgs cbData, const HostAndPort& target);

    /**
     * Prepare a heartbeat request before scheduling a heartbeat to "target" at "now".
     */
    std::pair<ShardServerHeartbeatArgs, Milliseconds> _prepareShardServerHeartbeatRequest(
        Date_t now, const HostAndPort& target);

    /**
     * Handle each heartbeat response.
     */
    void _handleHeartbeatResponse(const ReplicationExecutor::RemoteCommandCallbackArgs& cbData);

    void _trackHeartbeatHandle(const StatusWith<ReplicationExecutor::CallbackHandle>& handle);

    void _untrackHeartbeatHandle(const ReplicationExecutor::CallbackHandle& handle);

    // Protects member data of the HeartbeatHandles.
    mutable stdx::mutex _heartbeatHandlesMutex;

    // Handles to actively queued heartbeats.
    HeartbeatHandles _heartbeatHandles;

    // Executor that drives the heartbeat coordinator.
    ReplicationExecutor* _replExecutor;

    Date_t _lastHeartbeatHitDate;
    int _heartbeatRetryCount = std::numeric_limits<int>::max();
    Date_t _doHeartbeatTime;
    Date_t _doHeartbeatTimeExpect;
    std::vector<HostAndPort> _configServers;
};

}  // namespace repl

using repl::ShardServerHeartbeatCoordinator;

ShardServerHeartbeatCoordinator* getGlobalShardServerHeartbeatCoordinator();

void setGlobalShardServerHeartbeatCoordinator(
    std::unique_ptr<ShardServerHeartbeatCoordinator>&& shardServerHeartbeatCoordinator);

}  // namespace mongo

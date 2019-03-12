#pragma once

#include "mongo/util/time_support.h"

namespace mongo {
namespace repl {

/**
 * Description of actions taken in response to a shard server heartbeat.
 *
 * This includes when to schedule the next heartbeat to a target, and any other actions to
 * take, such as scheduling killing self.
 */
class ShardServerHeartbeatResponseAction {
public:
    /**
     * Actions taken based on heartbeat responses
     */
    enum Action { NoAction, KillSelf, SwitchTarget };

    /**
     * Construct an action with unspecified action and a next heartbeat start date in the
     * past.
     */
    ShardServerHeartbeatResponseAction();

    /**
     * Makes a new action representing doing nothing but sending a heartbeat.
     */
    static ShardServerHeartbeatResponseAction makeNoAction(const std::string& primaryConfigServer);

    /**
     * Makes a new action telling the current node to kill self.
     */
    static ShardServerHeartbeatResponseAction makeKillSelfAction(
        const std::string& primaryConfigServer);

    /**
     * Makes a new action telling the current node to send heartbeat to
     * a switched target.
     */
    static ShardServerHeartbeatResponseAction makeSwitchTargetAction(
        const std::string& primaryConfigServer);

    /**
     * Gets the action type of this action.
     */
    Action getAction() const {
        return _action;
    }

    /**
     * Gets the primary config server for this heartbeat action.
     */
    const std::string& getPrimaryConfigServer() const {
        return _primaryConfigServer;
    }

    /**
     * Sets the date at which the next heartbeat should be scheduled.
     */
    void setNextHeartbeatStartDate(Date_t when);


    /**
     * Gets the time at which the next heartbeat should be scheduled.  If the
     * time is not in the future, the next heartbeat should be scheduled immediately.
     */
    Date_t getNextHeartbeatStartDate() const {
        return _nextHeartbeatStartDate;
    }

private:
    Action _action;
    std::string _primaryConfigServer;
    Date_t _nextHeartbeatStartDate;
};

}  // namespace repl
}  // namespace mongo

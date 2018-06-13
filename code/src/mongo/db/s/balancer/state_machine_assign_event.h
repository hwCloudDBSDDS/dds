/**
 *    Copyright (C) 2016 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */
#include "mongo/db/s/balancer/state_machine.h"
    
namespace mongo {

// AssignEvent definition for rebalance state machine
class AssignEvent final : public IRebalanceEvent {
    
    MONGO_DISALLOW_COPYING(AssignEvent);

public:
    AssignEvent(const ChunkType& chunk,
                const ShardIdent& target,
                IRebalanceEvent::EventResultInfo* result,
                bool newFlag = false,
                bool userCommand = false);

    AssignEvent(const std::string& eventId,
                IRebalanceEvent::EventResultInfo* result);

    virtual ~AssignEvent();

    struct AssignEventInfo {
        StateMachine::RebalanceEventState prevState;
        StateMachine::RebalanceEventState curState;
        ChunkType chunk;
        ShardIdent target;
    };

    // encode internal data into a BSONObj
    virtual BSONObj dataToBSON();

    // decode internal data from a BSONObj
    virtual Status dataFromBSON(const BSONObj& eventData);

    // set current state to nextState
    virtual void transitState(StateMachine::RebalanceEventState nextState);

    virtual void rollbackLastTransition();

    virtual const std::string& getEventId() const;

    // get chunk of this event
    virtual const ChunkType& getChunk() const;

    // get current state of this event
    virtual StateMachine::RebalanceEventState getState();

    // get previous state of this event
    virtual StateMachine::RebalanceEventState getPrevState();

    virtual bool getNewChunkFlag();

    virtual void refreshChunk(const ChunkType& chunk);
    virtual void refreshChunk(const ChunkType& pChunk,
                              const ChunkType& cChunk);

    virtual bool isInInitialState();

    // get target shard server of this event
    const ShardIdent& getTarget() const;

private:
    bool _newChunkFlag = false;
    std::string _eventId;
    AssignEventInfo _assignEventInfo;
    StateMachine::RebalanceEventState _rollbackState = StateMachine::RebalanceEventState::kStartAssign;
    const StateMachine::RebalanceEventState _initState = StateMachine::RebalanceEventState::kStartAssign;
    const StateMachine::RebalanceEventState _finalState = StateMachine::RebalanceEventState::kAssigned;
};

} // namespace mongo

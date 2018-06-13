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

#pragma once

#include "mongo/base/disallow_copying.h"
#include "mongo/db/s/balancer/state_machine.h"
    
namespace mongo {

// MoveEvent definition for rebalance state machine
class MoveEvent final : public IRebalanceEvent {
    
    MONGO_DISALLOW_COPYING(MoveEvent);

public:
    MoveEvent(const ChunkType& chunk,
              const ShardIdent& target,
              IRebalanceEvent::EventResultInfo* result,
              bool userCommand = false);

    MoveEvent(const std::string& eventId,
              IRebalanceEvent::EventResultInfo* result);

    virtual ~MoveEvent();

    struct MoveEventInfo {
        StateMachine::RebalanceEventState curState;
        StateMachine::RebalanceEventState prevState;
        ChunkType chunk;
        ShardIdent target;
    };

    // encode internal data into a BSONObj
    virtual BSONObj dataToBSON() override;

    // decode internal data from a BSONObj
    virtual Status dataFromBSON(const BSONObj& eventData) override;

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
    virtual bool isInInitialState();

    virtual void refreshChunk(const ChunkType& chunk);
    virtual void refreshChunk(const ChunkType& pChunk,
                              const ChunkType& cChunk);

    // get source shard server of this event
    const ShardIdent getSource() const;

    // get target shard server of this event
    const ShardIdent& getTarget() const;

private:
    std::string _eventId;
    MoveEventInfo _moveEventInfo;
    StateMachine::RebalanceEventState _rollbackState
            = StateMachine::RebalanceEventState::kStartOffload;
    const StateMachine::RebalanceEventState _initState
            = StateMachine::RebalanceEventState::kStartOffload;
    const StateMachine::RebalanceEventState _finalState
            = StateMachine::RebalanceEventState::kAssigned;
};

} // namespace mongo

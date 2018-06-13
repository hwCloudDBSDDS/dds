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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding
    
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/s/balancer/state_machine.h"
#include "mongo/db/s/balancer/state_machine_offload_event.h"
#include "mongo/util/log.h"
    
namespace mongo {

OffloadEvent::OffloadEvent(const ChunkType& chunk,
                           IRebalanceEvent::EventResultInfo* result,
                           bool userCommand)
                           : IRebalanceEvent(StateMachine::EventType::kOffloadEvent, result, userCommand) {
    _offloadEventInfo.chunk = chunk;
    _offloadEventInfo.curState = _initState;
    _offloadEventInfo.prevState = _initState;

    std::stringstream strId;
    strId << chunk.getName() << chunk.getVersion().toLong();
    _eventId = strId.str();

    if (result) {
        result->setCurrentState(_initState);
    }
}

OffloadEvent::OffloadEvent(const std::string& eventId,
                           IRebalanceEvent::EventResultInfo* result)
                           : IRebalanceEvent(StateMachine::EventType::kOffloadEvent, result) {
    _eventId = eventId;
}

OffloadEvent::~OffloadEvent() {}
    
BSONObj OffloadEvent::dataToBSON() {
    BSONObjBuilder builder;
    long long curstate = static_cast<long long>(getState());
    builder.append(kCurStateFieldName, curstate);
    long long prevstate = static_cast<long long>(getPrevState());
    builder.append(kPrevStateFieldName, prevstate);
    builder.append(kChunkFieldName, getChunk().toBSON());
    
    return builder.obj();
}

Status OffloadEvent::dataFromBSON(const BSONObj& eventData) {
    {
        long long curstate;
        Status status = bsonExtractIntegerField(eventData, kCurStateFieldName, &curstate);
        if (!status.isOK())
            return status;

        _offloadEventInfo.curState = static_cast<StateMachine::RebalanceEventState>(curstate);
    }

    {
        long long prevstate;
        Status status = bsonExtractIntegerField(eventData, kPrevStateFieldName, &prevstate);
        if (!status.isOK())
            return status;

        _offloadEventInfo.prevState = static_cast<StateMachine::RebalanceEventState>(prevstate);
    }

    {
        BSONElement chunk;
        Status status = bsonExtractTypedField(eventData, kChunkFieldName, Object, &chunk);
        if (!status.isOK()) {
            return {status.code(),
                    str::stream() << "Invalid chunk data " << status.reason()};
        }

        if (chunk.Obj().isEmpty()) {
            return {ErrorCodes::BadValue, "The chunk data cannot be empty"};
        }

        auto chunkStatus = ChunkType::fromBSON(chunk.Obj().getOwned());
        if (!chunkStatus.isOK())
            return chunkStatus.getStatus();

        _offloadEventInfo.chunk = std::move(chunkStatus.getValue());
    }

    getEventResultInfo()->setCurrentState(_offloadEventInfo.curState);

    return Status::OK();
}

void OffloadEvent::transitState(StateMachine::RebalanceEventState nextState) {
    _rollbackState = _offloadEventInfo.prevState;
    _offloadEventInfo.prevState = _offloadEventInfo.curState;
    _offloadEventInfo.curState = nextState;
}

void OffloadEvent::rollbackLastTransition() {
    _offloadEventInfo.curState = _offloadEventInfo.prevState;
    _offloadEventInfo.prevState = _rollbackState;
}

const ChunkType& OffloadEvent::getChunk() const {
    return _offloadEventInfo.chunk;
}

const ShardIdent OffloadEvent::getSource() const {
    auto shardId = _offloadEventInfo.chunk.getShard();
    auto processIdent = _offloadEventInfo.chunk.getProcessIdentity();
    return ShardIdent(shardId, processIdent);
}

StateMachine::RebalanceEventState OffloadEvent::getState() {
    return _offloadEventInfo.curState;
}

StateMachine::RebalanceEventState OffloadEvent::getPrevState() {
    return _offloadEventInfo.prevState;
}

const std::string& OffloadEvent::getEventId() const {
    return _eventId;
}

bool OffloadEvent::getNewChunkFlag() {
    return false;
}

bool OffloadEvent::isInInitialState() {
    return (_offloadEventInfo.curState == _initState);
}

void OffloadEvent::refreshChunk(const ChunkType& chunk) {
    _offloadEventInfo.chunk = chunk;
}

void OffloadEvent::refreshChunk(const ChunkType& pChunk,
                                const ChunkType& cChunk) {}

} // namespace mongo

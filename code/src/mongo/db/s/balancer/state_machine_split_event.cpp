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
#include "mongo/db/s/balancer/state_machine_split_event.h"
#include "mongo/util/log.h"

namespace mongo {

SplitEvent::SplitEvent(const ChunkType& parentChunk,
                       const ChunkType& childChunk,
                       const BSONObj& splitPoint,
                       IRebalanceEvent::EventResultInfo* result,
                       bool userCommand)
                       : IRebalanceEvent(StateMachine::EventType::kSplitEvent,result, userCommand) {
    _splitEventInfo.parentChunk = parentChunk;
    _splitEventInfo.childChunk = childChunk;
    _splitEventInfo.splitPoint = splitPoint;
    _splitEventInfo.curState = _initState;
    _splitEventInfo.prevState = _initState;

    std::stringstream strId;
    strId << parentChunk.getName() << parentChunk.getVersion().toLong();
    _eventId = strId.str();

    if (result) {
        result->setCurrentState(_initState);
    }
}

SplitEvent::SplitEvent(const std::string& eventId,
                       IRebalanceEvent::EventResultInfo* result)
                       : IRebalanceEvent(StateMachine::EventType::kSplitEvent,result) {
    _eventId = eventId;
}

SplitEvent::~SplitEvent() {}

BSONObj SplitEvent::dataToBSON() {
    BSONObjBuilder builder;

    long long curstate = static_cast<long long>(getState());
    builder.append(kCurStateFieldName, curstate);
    long long prevstate = static_cast<long long>(getPrevState());
    builder.append(kPrevStateFieldName, prevstate);
    builder.append(kChunkFieldName, getChunk().toBSON());
    builder.append(kChildChunkFieldName, getChildChunk().toBSON());
    builder.append(kSplitPointFieldName, getSplitPoint());

    return builder.obj();
}

Status SplitEvent::dataFromBSON(const BSONObj& eventData) {
    {
        long long curstate;
        Status status = bsonExtractIntegerField(eventData, kCurStateFieldName, &curstate);
        if (!status.isOK())
            return status;

        _splitEventInfo.curState = static_cast<StateMachine::RebalanceEventState>(curstate);
    }

    {
        long long prevstate;
        Status status = bsonExtractIntegerField(eventData, kPrevStateFieldName, &prevstate);
        if (!status.isOK())
            return status;

        _splitEventInfo.prevState = static_cast<StateMachine::RebalanceEventState>(prevstate);
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

        _splitEventInfo.parentChunk = std::move(chunkStatus.getValue());
    }

    {
        BSONElement chunk;
        Status status = bsonExtractTypedField(eventData, kChildChunkFieldName,
                                              Object, &chunk);
        if (!status.isOK()) {
            return {status.code(),
                    str::stream() << "Invalid childChunk data " << status.reason()};
        }

        if (chunk.Obj().isEmpty()) {
            return {ErrorCodes::BadValue, "The childChunk data cannot be empty"};
        }

        auto chunkStatus = ChunkType::fromBSON(chunk.Obj().getOwned());
        if (!chunkStatus.isOK())
            return chunkStatus.getStatus();

        _splitEventInfo.childChunk = std::move(chunkStatus.getValue());
    }

    {
        if (_splitEventInfo.curState >= StateMachine::RebalanceEventState::kCommitSplit) {
            BSONElement splitPoint;
            Status status = bsonExtractTypedField(eventData, kSplitPointFieldName,
                                                  Object, &splitPoint);
            if (!status.isOK()) {
                return {status.code(),
                        str::stream() << "Invalid splitPoint data " << status.reason()};
            }

            if (splitPoint.Obj().isEmpty()) {
                return {ErrorCodes::BadValue, "The splitPoint data cannot be empty"};
            }

            _splitEventInfo.splitPoint = std::move(splitPoint.Obj().getOwned());
        }
    }

    getEventResultInfo()->setCurrentState(_splitEventInfo.curState);

    return Status::OK();
}

void SplitEvent::transitState(StateMachine::RebalanceEventState nextState) {
    _rollbackState = _splitEventInfo.prevState;
    _splitEventInfo.prevState = _splitEventInfo.curState;
    _splitEventInfo.curState = nextState;
}

void SplitEvent::rollbackLastTransition() {
    _splitEventInfo.curState = _splitEventInfo.prevState;
    _splitEventInfo.prevState = _rollbackState;
}

const ChunkType& SplitEvent::getChunk() const {
    return _splitEventInfo.parentChunk;
}

const ChunkType& SplitEvent::getChildChunk() const {
    return _splitEventInfo.childChunk;
}

StateMachine::RebalanceEventState SplitEvent::getState() {
    return _splitEventInfo.curState;
}

StateMachine::RebalanceEventState SplitEvent::getPrevState() {
    return _splitEventInfo.prevState;
}

const std::string& SplitEvent::getEventId() const {
    return _eventId;
}

StateMachine::EventType SplitEvent::getEventType() {
    return StateMachine::EventType::kSplitEvent;
}

bool SplitEvent::getNewChunkFlag() {
    return false;
}

bool SplitEvent::isInInitialState() {
    return (_splitEventInfo.curState == _initState);
}

void SplitEvent::refreshChunk(const ChunkType& chunk) {
    _splitEventInfo.parentChunk = chunk;
}

void SplitEvent::refreshChunk(const ChunkType& pChunk,
                              const ChunkType& cChunk) {
    _splitEventInfo.parentChunk = pChunk;
    _splitEventInfo.childChunk = cChunk;
}

const BSONObj& SplitEvent::getSplitPoint() const {
    return _splitEventInfo.splitPoint;
}

void SplitEvent::setSplitPoint(BSONObj splitPoint) {
    _splitEventInfo.splitPoint = splitPoint.getOwned();
}

} // namespace mongo

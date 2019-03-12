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

#include "mongo/db/s/balancer/balance_move_event.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/s/balancer/balance_event_engine.h"
#include "mongo/util/log.h"

namespace mongo {

MoveEvent::MoveEvent(const ChunkType& chunk,
                     const ShardIdent& target,
                     IRebalanceEvent::EventResultInfo* result,
                     bool userCommand,
                     bool needWriteEvent,
                     bool bgCommand)
    : IRebalanceEvent(BalanceEventEngine::EventType::kMoveEvent,
                      result,
                      userCommand,
                      needWriteEvent,
                      bgCommand) {
    _moveEventInfo.chunk = chunk;
    _moveEventInfo.target = target;
    _moveEventInfo.curState = _initState;
    _moveEventInfo.prevState = _initState;

    std::stringstream strId;
    strId << chunk.getName() << chunk.getVersion().toLong();
    _eventId = strId.str();

    if (result) {
        result->setCurrentState(_initState);
    }
}

MoveEvent::MoveEvent(const std::string& eventId, IRebalanceEvent::EventResultInfo* result)
    : IRebalanceEvent(BalanceEventEngine::EventType::kMoveEvent, result) {
    _eventId = eventId;
}

MoveEvent::~MoveEvent() {}

BSONObj MoveEvent::dataToBSON() {
    BSONObjBuilder builder;

    long long curstate = static_cast<long long>(getState());
    builder.append(kCurStateFieldName, curstate);
    long long prevstate = static_cast<long long>(getPrevState());
    builder.append(kPrevStateFieldName, prevstate);
    builder.append(kChunkFieldName, getChunk().toBSON());
    builder.append(kTargetShardFieldName, getTarget().toBSON());

    return builder.obj();
}

Status MoveEvent::dataFromBSON(const BSONObj& eventData) {
    {
        long long curstate;
        Status status = bsonExtractIntegerField(eventData, kCurStateFieldName, &curstate);
        if (!status.isOK())
            return status;

        _moveEventInfo.curState = static_cast<BalanceEventEngine::RebalanceEventState>(curstate);
    }

    {
        long long prevstate;
        Status status = bsonExtractIntegerField(eventData, kPrevStateFieldName, &prevstate);
        if (!status.isOK())
            return status;

        _moveEventInfo.prevState = static_cast<BalanceEventEngine::RebalanceEventState>(prevstate);
    }

    {
        BSONElement chunk;
        Status status = bsonExtractTypedField(eventData, kChunkFieldName, Object, &chunk);
        if (!status.isOK()) {
            return {status.code(), str::stream() << "Invalid chunk data " << status.reason()};
        }

        if (chunk.Obj().isEmpty()) {
            return {ErrorCodes::BadValue, "The chunk data cannot be empty"};
        }

        auto chunkStatus = ChunkType::fromBSON(chunk.Obj().getOwned());
        if (!chunkStatus.isOK())
            return chunkStatus.getStatus();

        _moveEventInfo.chunk = std::move(chunkStatus.getValue());
    }

    {
        BSONElement targetShard;
        Status status =
            bsonExtractTypedField(eventData, kTargetShardFieldName, Object, &targetShard);
        if (!status.isOK())
            return status;

        if (targetShard.Obj().isEmpty()) {
            return {ErrorCodes::BadValue, "The targetShard data cannot be empty"};
        }

        auto targetShardIdentStatus = ShardIdent::fromBSON(targetShard.Obj().getOwned());
        if (!targetShardIdentStatus.isOK())
            return targetShardIdentStatus.getStatus();

        _moveEventInfo.target = std::move(targetShardIdentStatus.getValue());
    }

    getEventResultInfo()->setCurrentState(_moveEventInfo.curState);

    return Status::OK();
}

void MoveEvent::transitState(BalanceEventEngine::RebalanceEventState nextState) {
    _rollbackState = _moveEventInfo.prevState;
    _moveEventInfo.prevState = _moveEventInfo.curState;
    _moveEventInfo.curState = nextState;
}

void MoveEvent::rollbackLastTransition() {
    _moveEventInfo.curState = _moveEventInfo.prevState;
    _moveEventInfo.prevState = _rollbackState;
}

const ChunkType& MoveEvent::getChunk() const {
    return _moveEventInfo.chunk;
}

const ShardIdent MoveEvent::getSource() const {
    auto shardId = _moveEventInfo.chunk.getShard();
    auto processIdent = _moveEventInfo.chunk.getProcessIdentity();
    return ShardIdent(shardId, processIdent);
}

const ShardIdent& MoveEvent::getTarget() const {
    return _moveEventInfo.target;
}

BalanceEventEngine::RebalanceEventState MoveEvent::getState() {
    return _moveEventInfo.curState;
}

BalanceEventEngine::RebalanceEventState MoveEvent::getPrevState() {
    return _moveEventInfo.prevState;
}

const std::string& MoveEvent::getEventId() const {
    return _eventId;
}

bool MoveEvent::getNewChunkFlag() {
    return false;
}

bool MoveEvent::isInInitialState() {
    return (_moveEventInfo.curState == _initState);
}

bool MoveEvent::shouldContinue() {
    if (_moveEventInfo.curState == BalanceEventEngine::RebalanceEventState::kOffloaded ||
        _moveEventInfo.curState == BalanceEventEngine::RebalanceEventState::kStartAssign) {
	return true;
    }
    return false;
}

void MoveEvent::refreshChunk(const ChunkType& chunk) {
    _moveEventInfo.chunk = chunk;
}

void MoveEvent::refreshChunk(const ChunkType& pChunk, const ChunkType& cChunk) {}

}  // namespace mongo

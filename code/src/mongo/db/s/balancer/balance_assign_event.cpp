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

#include "mongo/db/s/balancer/balance_assign_event.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/s/balancer/balance_event_engine.h"
#include "mongo/util/log.h"

namespace mongo {

AssignEvent::AssignEvent(const ChunkType& chunk,
                         const ShardIdent& target,
                         IRebalanceEvent::EventResultInfo* result,
                         bool newChunk,
                         bool userCommand,
                         bool needWriteEvent)
    : IRebalanceEvent(
          BalanceEventEngine::EventType::kAssignEvent, result, userCommand, needWriteEvent) {
    _assignEventInfo.chunk = chunk;
    _assignEventInfo.target = target;
    _assignEventInfo.curState = _initState;
    _assignEventInfo.prevState = _initState;

    std::stringstream strId;
    strId << chunk.getName() << chunk.getVersion().toLong();
    _eventId = strId.str();
    _newChunkFlag = newChunk;
    if (result) {
        result->setCurrentState(_initState);
    }
}

AssignEvent::AssignEvent(const std::string& eventId, IRebalanceEvent::EventResultInfo* result)
    : IRebalanceEvent(BalanceEventEngine::EventType::kAssignEvent, result) {
    _eventId = eventId;
}

AssignEvent::~AssignEvent() {}

BSONObj AssignEvent::dataToBSON() {
    BSONObjBuilder builder;
    long long curState = static_cast<long long>(getState());
    builder.append(kCurStateFieldName, curState);
    long long prevState = static_cast<long long>(getPrevState());
    builder.append(kPrevStateFieldName, prevState);
    builder.append(kChunkFieldName, getChunk().toBSON());
    builder.append(kTargetShardFieldName, getTarget().toBSON());

    return builder.obj();
}

Status AssignEvent::dataFromBSON(const BSONObj& eventData) {
    {
        long long curstate;
        Status status = bsonExtractIntegerField(eventData, kCurStateFieldName, &curstate);
        if (!status.isOK())
            return status;

        _assignEventInfo.curState = static_cast<BalanceEventEngine::RebalanceEventState>(curstate);
    }

    {
        long long prevstate;
        Status status = bsonExtractIntegerField(eventData, kPrevStateFieldName, &prevstate);
        if (!status.isOK())
            return status;

        _assignEventInfo.prevState =
            static_cast<BalanceEventEngine::RebalanceEventState>(prevstate);
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

        _assignEventInfo.chunk = std::move(chunkStatus.getValue());
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

        auto shardIdentStatus = ShardIdent::fromBSON(targetShard.Obj().getOwned());
        if (!shardIdentStatus.isOK())
            return shardIdentStatus.getStatus();

        _assignEventInfo.target = std::move(shardIdentStatus.getValue());
    }

    getEventResultInfo()->setCurrentState(_assignEventInfo.curState);
    return Status::OK();
}

void AssignEvent::transitState(BalanceEventEngine::RebalanceEventState nextState) {
    _rollbackState = _assignEventInfo.prevState;
    _assignEventInfo.prevState = _assignEventInfo.curState;
    _assignEventInfo.curState = nextState;
}

void AssignEvent::rollbackLastTransition() {
    _assignEventInfo.curState = _assignEventInfo.prevState;
    _assignEventInfo.prevState = _rollbackState;
}

const ChunkType& AssignEvent::getChunk() const {
    return _assignEventInfo.chunk;
}

const ShardIdent& AssignEvent::getTarget() const {
    return _assignEventInfo.target;
}

BalanceEventEngine::RebalanceEventState AssignEvent::getState() {
    return _assignEventInfo.curState;
}

BalanceEventEngine::RebalanceEventState AssignEvent::getPrevState() {
    return _assignEventInfo.prevState;
}

const std::string& AssignEvent::getEventId() const {
    return _eventId;
}

bool AssignEvent::getNewChunkFlag() {
    return _newChunkFlag;
}

bool AssignEvent::isInInitialState() {
    return (_assignEventInfo.curState == _initState);
}

void AssignEvent::refreshChunk(const ChunkType& chunk) {
    _assignEventInfo.chunk = chunk;
}

void AssignEvent::refreshChunk(const ChunkType& pChunk, const ChunkType& cChunk) {}

}  // namespace mongo

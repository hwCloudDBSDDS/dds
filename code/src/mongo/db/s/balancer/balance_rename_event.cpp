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

#include "mongo/db/s/balancer/balance_rename_event.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/s/balancer/balance_event_engine.h"
#include "mongo/util/log.h"

namespace mongo {

RenameEvent::RenameEvent(const ChunkType& chunk,
                         const std::string& toNS,
                         IRebalanceEvent::EventResultInfo* result,
                         bool dropTarget,
                         bool stayTemp,
                         bool userCommand,
                         bool needWriteEvent)
    : IRebalanceEvent(
          BalanceEventEngine::EventType::kRenameEvent, result, userCommand, needWriteEvent) {
    _renameEventInfo.chunk = chunk;
    _renameEventInfo.curState = _initState;
    _renameEventInfo.prevState = _initState;
    _renameEventInfo.toNS = toNS;
    _renameEventInfo.dropTarget = dropTarget;
    _renameEventInfo.stayTemp = stayTemp;

    std::stringstream strId;
    strId << chunk.getName() << chunk.getVersion().toLong();
    _eventId = strId.str();

    if (result) {
        result->setCurrentState(_initState);
    }
}

RenameEvent::RenameEvent(const std::string& eventId, IRebalanceEvent::EventResultInfo* result)
    : IRebalanceEvent(BalanceEventEngine::EventType::kRenameEvent, result) {
    _eventId = eventId;
}

RenameEvent::~RenameEvent() {}

BSONObj RenameEvent::dataToBSON() {
    BSONObjBuilder builder;

    long long curstate = static_cast<long long>(getState());
    builder.append(kCurStateFieldName, curstate);
    long long prevstate = static_cast<long long>(getPrevState());
    builder.append(kPrevStateFieldName, prevstate);
    builder.append(kChunkFieldName, getChunk().toBSON());
    builder.append(kTargetCollectionName, getTargetCollection());

    return builder.obj();
}

Status RenameEvent::dataFromBSON(const BSONObj& eventData) {
    {
        long long curstate;
        Status status = bsonExtractIntegerField(eventData, kCurStateFieldName, &curstate);
        if (!status.isOK())
            return status;

        _renameEventInfo.curState = static_cast<BalanceEventEngine::RebalanceEventState>(curstate);
    }

    {
        long long prevstate;
        Status status = bsonExtractIntegerField(eventData, kPrevStateFieldName, &prevstate);
        if (!status.isOK())
            return status;

        _renameEventInfo.prevState =
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

        _renameEventInfo.chunk = std::move(chunkStatus.getValue());
    }

    {
        std::string targetCollection;
        Status status = bsonExtractStringField(eventData, kTargetCollectionName, &targetCollection);
        if (!status.isOK())
            return status;
        _renameEventInfo.toNS = std::move(targetCollection);
    }

    getEventResultInfo()->setCurrentState(_renameEventInfo.curState);

    return Status::OK();
}

void RenameEvent::transitState(BalanceEventEngine::RebalanceEventState nextState) {
    _rollbackState = _renameEventInfo.prevState;
    _renameEventInfo.prevState = _renameEventInfo.curState;
    _renameEventInfo.curState = nextState;
}

void RenameEvent::rollbackLastTransition() {
    _renameEventInfo.curState = _renameEventInfo.prevState;
    _renameEventInfo.prevState = _rollbackState;
}

const ChunkType& RenameEvent::getChunk() const {
    return _renameEventInfo.chunk;
}

const ShardIdent RenameEvent::getSource() const {
    auto shardId = _renameEventInfo.chunk.getShard();
    auto processIdent = _renameEventInfo.chunk.getProcessIdentity();
    return ShardIdent(shardId, processIdent);
}

BalanceEventEngine::RebalanceEventState RenameEvent::getState() {
    return _renameEventInfo.curState;
}

BalanceEventEngine::RebalanceEventState RenameEvent::getPrevState() {
    return _renameEventInfo.prevState;
}

const std::string& RenameEvent::getEventId() const {
    return _eventId;
}

bool RenameEvent::getNewChunkFlag() {
    return true;
}

bool RenameEvent::isInInitialState() {
    return (_renameEventInfo.curState == _initState);
}

bool RenameEvent::shouldContinue() {
    if (_renameEventInfo.curState == BalanceEventEngine::RebalanceEventState::kOffloaded ||
        _renameEventInfo.curState == BalanceEventEngine::RebalanceEventState::kStartAssign) {
	return true;
    }
    return false;
}

void RenameEvent::refreshChunk(const ChunkType& chunk) {
    _renameEventInfo.chunk = chunk;
}

const std::string& RenameEvent::getTargetCollection() const {
    return _renameEventInfo.toNS;
}

const bool RenameEvent::getDropTarget() const {
    return _renameEventInfo.dropTarget;
}

const bool RenameEvent::getStayTemp() const {
    return _renameEventInfo.stayTemp;
}

void RenameEvent::refreshChunk(const ChunkType& pChunk, const ChunkType& cChunk) {}

}  // namespace mongo

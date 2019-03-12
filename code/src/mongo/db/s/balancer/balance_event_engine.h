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

#include <list>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/repl/replication_executor.h"
#include "mongo/db/s/balancer/balancer_policy.h"
#include "mongo/db/s/balancer/type_rebalance_events.h"
#include "mongo/executor/task_executor.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/shard_id.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/concurrency/notification.h"
#include "mongo/util/concurrency/notification.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/util_extend/default_parameters.h"

namespace mongo {

class ChunkType;
class ClusterStatistics;
class OperationContext;
class ServiceContext;
class Status;
class IRebalanceEvent;
class RebalanceEventWriter;
class RebalanceEventType;

typedef std::string EventIdentifier;
using RemoteCommandCallbackArgs = executor::TaskExecutor::RemoteCommandCallbackArgs;
const int maxRetryCnt = 200;
// Field name of internal data
const std::string kCurStateFieldName = "curState";
const std::string kPrevStateFieldName = "prevState";
const std::string kChunkFieldName = "chunk";
const std::string kTargetCollectionName = "targetCollection";
const std::string kChildChunkFieldName = "childChunk";
const std::string kSourceShardFieldName = "sourceShard";
const std::string kTargetShardFieldName = "targetShard";
const std::string kSplitPointFieldName = "splitPoint";
const std::string kCurStateInEventData = "eventData.curState";
const std::string kPrevStateInEventData = "eventData.prevState";
const std::string kSplitPointInEventData = "eventData.splitPoint";
const std::string kChunkInEventData = "eventData.chunk";
const std::string kChildChunkInEventData = "eventData.childChunk";

// const varabiles used in response extraction for assign event
const std::string kAssignProcessIdentityData = "processIdentity";
const std::string kAssignShardNameData = "shardName";

// Every load-balancing command will be represented as an rebalance event and
// executed in rebalance state machine.
// Rebalance state machine will be implemented as a singleton instance.
// Every transition of specific event will be logged as a document into
// config.rebalanceevents so that the rebalance events can recover itself on any state
// after primary CS crash in the new primary CS.
class BalanceEventEngine {
    MONGO_DISALLOW_COPYING(BalanceEventEngine);

public:
    // The current state of the state machine
    enum class State {  // Allowed transitions:
        kStopped,       // kRecovering
        kRunning,       // kStopping
        kStopping       // kStopped
    };

    // Event type, used in the recovery process
    enum class EventType : int {
        kAssignEvent = 0,
        kOffloadEvent = 1,
        kMoveEvent = 2,
        kSplitEvent = 3,
        kMergeEvent = 4,
        kRenameEvent = 5
    };

    // The state definition of rebalance event
    enum class RebalanceEventState : int {
        kStateInit = 0,     // initial state
        kStartOffload = 1,  // Offload command will be sent to shard server
        kKillSourceSS = 2,  // Do not receive response of Assign/Split after certain retries
        kOffloaded = 3,     // Chunk has been offloaded
        kOffloadFailed = 4,
        kStartAssign =
            5,  // Apply root plog, chunk version, send assign command to target shard server
        kKillTargetSS = 6,  // Do not receive response of Assign after certain retries
        kAssignFailed =
            7,  // If target shard server returns errors indicating it can not serve the chunk
        kAssigned = 8,      // Chunk has been assigned
        kStartSplit = 9,    // Split command will be sent to target shard server
        kSplitFailed = 10,  // Target responds with a error on split request
        kCommitSplit = 11,  // Update configDB after receive response of split request
        kAcknowledgeSplit =
            12,  // Notify parent chunk to update chunk metadata and unblock the I/O of child
        kSplitted = 13,  // Chunk has been splitted
        kInvalidState    // Max state, in order to easily extend the new state
    };

    enum class NextAction : int {
        kActionContinue = 0,  // continue to transition to next state
        kActionWait = 1,      // wait the response of an async request
        kActionFinish = 2,    // state machine has complete
        kActionError = 3      // there is an error happend within the transition
    };

    // Used as a type in which to store a list of active rebalance event. The reason to choose list
    // is
    // that its iterators do not get invalidated when entries are removed around them. This allows
    // O(1) removal time.
    using RebalanceEventList = std::list<IRebalanceEvent*>;

    struct ChunkBalanceInfo {
        ChunkBalanceInfo(const std::string& chunkid) : chunkId(chunkid), activeEvent(nullptr) {}
        ChunkBalanceInfo() {}

        std::string chunkId;
        IRebalanceEvent* activeEvent;
        RebalanceEventList waitingEvents;
    };

    typedef StatusWith<NextAction> (*RebalanceEventHandler)(IRebalanceEvent* event);
    typedef std::map<BalanceEventEngine::RebalanceEventState, RebalanceEventHandler>
        StateHandlerMapType;

    BalanceEventEngine();

    virtual ~BalanceEventEngine();

    // Reclaim all the resource belong to the event
    static void deleteEvent(OperationContext* txn, IRebalanceEvent* event);

    // Add rebalance info to list and notify threadpool to process the task
    static Status executeEvent(OperationContext* txn,
                               IRebalanceEvent* event,
                               bool recovery = false);

    // Non-blocking method that puts the statemachine in the kRecovering state, in which
    // new rebalance events will be blocked until finishRecovery is called.
    Status startRecovery(OperationContext* txn);

    // Blocking call, which waits for the statemachine to leave the recovering state (if it is
    // currently recovering).
    void waitForRecovery();

    // Blocking method that must only be called after startRecovery has been called. Recovers the
    // state of statemachine  (if necessary and able) and puts it in the kRunning state,
    // where it will accept new rebalance events.
    void finishRecovery(OperationContext* txn);

    void interruptAndDisableRebalance();

    // Blocking method that waits for any currently scheduled migrations to complete. Must be
    // called after interruptAndDisableRebalance has been called in order to be able to re-enable
    // migrations again.
    void drainActiveEvents();

    // Persist the state transition into config.rebalanceevents collection
    Status persistStateTransition(OperationContext* txn, IRebalanceEvent* rbEvent);

    void onStepDownFromPrimary();
    void onTransitionToPrimary(OperationContext* txn);
    void onDrainComplete(OperationContext* txn);

    // Add Event to _balancedChunkMap
    Status addEventToBalancedChunkMap(OperationContext* txn, IRebalanceEvent* event);
    void removeEventFromBalancedChunkMap(IRebalanceEvent* event);

    // Add Event to events list
    Status addEvent(IRebalanceEvent* rbEvent);
    Status pickOneActiveEvent(IRebalanceEvent*& rbEvent);
    bool findEvent(const std::string& chunkId);
    void addRetryingEvent(IRebalanceEvent* rbEvent);
    void addRetryingEvent_inlock(IRebalanceEvent* rbEvent);

    // Execute the rebalance task
    static void pickAndProcessEvent();
    static void processEvent(OperationContext* txn, IRebalanceEvent* rbEvent);

    // Functions used to maintain event document in config.rebalanceevents collection.
    static Status writeEventDocument(OperationContext* txn, IRebalanceEvent* rebalanceEvent);
    static Status updateEventDocument(OperationContext* txn,
                                      const std::string& eventId,
                                      const BSONObj& eventData);
    static Status removeEventDocument(OperationContext* txn, const std::string& eventId);

    // Util functions
    BalanceEventEngine::State getBalanceEventEngineState();
    bool canExecuteEvent();
    void scheduleRebalanceEvent();

private:
    // IRebalanceEvent state transition map
    static StateHandlerMapType stateHandlerMap;

    static StatusWith<NextAction> _sendBalanceCommand(OperationContext* txn,
                                                      IRebalanceEvent* event,
                                                      bool isRetried);

    // Event handlers
    static StatusWith<NextAction> _startAssignHandler(IRebalanceEvent* event);
    static StatusWith<NextAction> _killTargetSSHandler(IRebalanceEvent* event);
    static StatusWith<NextAction> _assignFailedHandler(IRebalanceEvent* event);
    static StatusWith<NextAction> _assignedHandler(IRebalanceEvent* event);
    static StatusWith<NextAction> _startOffloadHandler(IRebalanceEvent* event);
    static StatusWith<NextAction> _killSourceSSHandler(IRebalanceEvent* event);
    static StatusWith<NextAction> _offloadedHandler(IRebalanceEvent* event);
    static StatusWith<NextAction> _offloadFailedHandler(IRebalanceEvent* event);
    static StatusWith<NextAction> _startSplitHandler(IRebalanceEvent* event);
    static StatusWith<NextAction> _splitFailedHandler(IRebalanceEvent* event);
    static StatusWith<NextAction> _commitSplitHandler(IRebalanceEvent* event);
    static StatusWith<NextAction> _acknowledgeSplitHandler(IRebalanceEvent* event);
    static StatusWith<NextAction> _splittedHandler(IRebalanceEvent* event);

    // Transiton state according to the result of previous handler
    static BalanceEventEngine::RebalanceEventState transitionState(IRebalanceEvent* rbEvent,
                                                                   ErrorCodes::Error statusCode);

    // Callback handler for remote request
    static void handleRequestResponse(const RemoteCommandCallbackArgs& cbArgs,
                                      IRebalanceEvent* rbEvent);
    // process error within the response to the remote request, returning true means
    // the request has been retried
    static bool errorHandler(IRebalanceEvent* rbEvent, Status result);
    static Status extractDataFromCommandResponse(IRebalanceEvent* rbEvent,
                                                 const BSONObj& commandResponse);

    // Determine if we have finish processing the event
    static bool isEventDone(IRebalanceEvent* rbEvent);

    // Handle the duplicate event
    bool _canExecuteDuplicateEvent(IRebalanceEvent* existingEvent, IRebalanceEvent* newEvent);

    // Validate event chunk data
    static Status _validateEventChunk(OperationContext* txn,
                                      IRebalanceEvent* existingEvent,
                                      ChunkType& chunkType);

    // Validate fields expect ns and shard of event chunk data
    static Status _isStaleChunk(IRebalanceEvent* event, const ChunkType& chunkType);

    // Send ack command to confirm split or merge, confirm equals true means to notify
    // SS to confirming the split or merge, confirm equals false means to notify SS to
    // rollback the operation
    static Status _sendAckCommand(IRebalanceEvent* event, bool confirm);
    static void _addUpdateChunkObject(OperationContext* txn,
                                      BSONArrayBuilder& updates,
                                      ChunkType& chunk,
                                      bool parent);

    // Checks whether balance event engine has been requested to stop
    bool _stopRequested();
    void _sleepFor(Milliseconds waitTimeout);

    // The main recovery thread
    stdx::thread _thread;
    stdx::thread _retryCmdThread;
    // Condition variable, which is used to retry balance command periodically
    stdx::condition_variable _retryCondVar;


    // Recovery thread used to recover all the rebalance events
    void _recoveryThread();
    void _retryBalanceCmdThread();

    // The operation context of the recoveryThread. This value may only be available in the
    // kRunning state and is used to force interrupt of any blocking calls made by the
    // thread.
    OperationContext* _threadOperationContext = nullptr;
    OperationContext* _retryThreadOperationContext = nullptr;

    // Should only be called from startRecovery or finishRecovery functions when the rebalance
    // manager is in either the kStopped or kRecovering state.
    void _abandonActiveRebalances(OperationContext* txn);

    // Carries rebalance information over from startRecovery to finishRecovery. Should only be set
    // in startRecovery and then accessed in finishRecovery.
    std::list<RebalanceEventType> _rebalanceEventRecoveryList;

    // Protects the class state and rebalances lists below.
    stdx::mutex _mutex;

    // Condition variable, which is waited on when the statemachine's state is changing and
    // signaled when the state change is complete.
    stdx::condition_variable _condVar;

    // Always start the statemachine in a stopped state.
    State _state{State::kStopped};
    bool _eventsRecovered = false;

    // In-memory chunk balance info to serialize the rebalance events
    stdx::mutex _chunkMapMutex;
    typedef stdx::unordered_map<std::string, ChunkBalanceInfo*> BalancedChunkMap;
    BalancedChunkMap _balancedChunkMap;

    // Holds the active rebalance events, worker from _rebalanceHandlingThreadPool will pick and
    // execute one event from _waitingEvents at a time.
    RebalanceEventList _waitingEvents;

    stdx::mutex _retryingEventsMutex;
    RebalanceEventList _retryingEvents;

    // Thread pool for handling the rebalance tasks
    ThreadPool* _eventHandlingThreadPool;

    // flag for corrupted document in rebalanceevent
    // No accumulative time. Once there is corrupted document, report the alarm immediately
    // If there is no alarm sended during loading data, it will recover the alarm
    bool _corrRebalanceeventDocHasSend = false;
};

// Event definition for rebalance state machine
class IRebalanceEvent {

    MONGO_DISALLOW_COPYING(IRebalanceEvent);

public:
    typedef stdx::function<Status(OperationContext*, IRebalanceEvent*, ErrorCodes::Error)>
        RebalanceResultHandler;

    struct EventResultInfo {
        EventResultInfo(std::shared_ptr<Notification<ErrorCodes::Error>> notification,
                        BalanceEventEngine::EventType type) {
            resultNotification = notification;
            eventType = type;
            curState = BalanceEventEngine::RebalanceEventState::kStateInit;
            eventErrorCode = ErrorCodes::OK;
        }

        void setErrorInfo(ErrorCodes::Error errorCode, std::string errorStr) {
            eventErrorCode = errorCode;
            errorMsg = errorStr;
        }

        ErrorCodes::Error getErrorCode() {
            return eventErrorCode;
        }

        std::string getErrorMsg() {
            return errorMsg;
        }

        void setCurrentState(BalanceEventEngine::RebalanceEventState state) {
            curState = state;
        }

        BalanceEventEngine::RebalanceEventState getCurrentState() {
            return curState;
        }

        std::shared_ptr<Notification<ErrorCodes::Error>> resultNotification;
        BalanceEventEngine::EventType eventType;
        BalanceEventEngine::RebalanceEventState curState;
        ErrorCodes::Error eventErrorCode;
        std::string errorMsg;
    };

    struct BalanceCmdInfo {
        BalanceCmdInfo(std::string name, HostAndPort tgt, BSONObj rcmdObj, Milliseconds tmout)
            : dbname(std::move(name)),
              target(std::move(tgt)),
              cmdObj(std::move(rcmdObj)),
              timeout(tmout) {}

        std::string dbname;
        HostAndPort target;
        BSONObj cmdObj;
        Milliseconds timeout;
    };

    IRebalanceEvent(BalanceEventEngine::EventType eventType)
        : _eventType(eventType), _startTime(curTimeMicros64()) {}

    IRebalanceEvent(BalanceEventEngine::EventType eventType,
                    EventResultInfo* result,
                    bool isUserRequest = false,
                    bool needWriteEvent = false,
                    bool isBGCommand = false)
        : _eventType(eventType), _startTime(curTimeMicros64()) {
        _eventResultInfo = result;
        _isUserRequest = isUserRequest;
        _needWriteEvent = needWriteEvent;
        _bgCommand = isBGCommand;
    }

    virtual ~IRebalanceEvent() {
        // uint64_t totalTime = curTimeMicros64() - _startTime;
        switch (_eventType) {
            case BalanceEventEngine::EventType::kAssignEvent:

                break;
            case BalanceEventEngine::EventType::kOffloadEvent:

                break;
            case BalanceEventEngine::EventType::kMoveEvent:

                break;
            case BalanceEventEngine::EventType::kSplitEvent:

                break;
            case BalanceEventEngine::EventType::kMergeEvent:

                break;
            case BalanceEventEngine::EventType::kRenameEvent:
                break;
        }
    }

    virtual void transitState(BalanceEventEngine::RebalanceEventState nextState) = 0;

    virtual void rollbackLastTransition() = 0;

    virtual BSONObj dataToBSON() = 0;

    virtual Status dataFromBSON(const BSONObj& eventData) = 0;

    virtual const std::string& getEventId() const = 0;

    const BalanceEventEngine::EventType getEventType() {
        return _eventType;
    }

    virtual const ChunkType& getChunk() const = 0;

    virtual BalanceEventEngine::RebalanceEventState getState() = 0;

    virtual BalanceEventEngine::RebalanceEventState getPrevState() = 0;

    virtual bool getNewChunkFlag() = 0;

    virtual bool isInInitialState() = 0;

    virtual bool shouldContinue() {return false;}

    virtual void refreshChunk(const ChunkType& chunk) = 0;
    virtual void refreshChunk(const ChunkType& pChunk, const ChunkType& cChunk) = 0;

    void increaseRetryCnt() {
        _curRetryNum++;
    }

    bool exhaustRetries() {
        return (_curRetryNum >= maxRetryCnt);
    }

    EventResultInfo* getEventResultInfo() {
        return _eventResultInfo;
    }

    BalanceCmdInfo* getBalanceCmdInfo() {
        return _balanceCmdInfo;
    }

    void setBalanceCmdInfo(BalanceCmdInfo* info) {
        _balanceCmdInfo = info;
    }

    void deleteBalanceCmdInfo() {
        if (_balanceCmdInfo) {
            delete _balanceCmdInfo;
        }
        _balanceCmdInfo = nullptr;
    }

    // whether need to persist the state
    bool needPersist() {
        return _needPersisted;
    }

    void setNeedPersist(bool persist) {
        _needPersisted = persist;
    }

    // whether the event has been processed
    bool isProcessed() {
        return _processed;
    }

    void setProcessed(bool isProcessed) {
        _processed = isProcessed;
    }

    // the event is created due to a user command
    bool isUserRequest() {
        return _isUserRequest;
    }

    void setInterrupt(bool interrupt) {
        _interrupted = interrupt;
    }

    bool isInterrupted() {
        return _interrupted;
    }

    void setRecoveryFlag(bool recovery) {
        _recovery = recovery;
    }

    bool isInRecovery() {
        return _recovery;
    }

    void setIsolateChunkFlag(bool isolate) {
        //_needIsolateChunk = isolate;
    }

    bool needIsolateChunk() {
        return _needIsolateChunk;
    }

    void setChunkChangedFlag(bool changed) {
        _chunkChanged = changed;
    }

    bool getChunkChangedFlag() {
        return _chunkChanged;
    }

    void setNeedWriteEvent(bool needWriteEvent) {
        _needWriteEvent = needWriteEvent;
    }

    bool getNeedWriteEvent() {
        return _needWriteEvent;
    }

    bool isBGCommand() const {
        return _bgCommand;
    }

private:
    const BalanceEventEngine::EventType _eventType;
    const uint64_t _startTime;

    // uint64_t _startTraceTime;

    int _curRetryNum = 0;
    bool _needPersisted = false;
    bool _processed = false;
    bool _isUserRequest = false;
    bool _interrupted = false;
    bool _recovery = false;
    bool _chunkChanged = false;
    bool _needIsolateChunk = false;
    bool _needWriteEvent = false;
    bool _bgCommand = false;
    EventResultInfo* _eventResultInfo = nullptr;
    BalanceCmdInfo* _balanceCmdInfo = nullptr;
};

/*
 * Deserialize RebalanceEventType BSONObj and create corresponding rebalance event.
 */
StatusWith<IRebalanceEvent*> createRebalanceEventFromBSON(
    OperationContext* txn,
    const std::string& eventId,
    const BalanceEventEngine::EventType eventType,
    const BSONObj& smData);

}  // namespace mongo

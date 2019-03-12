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

#include <list>
#include <map>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/remote_command_targeter.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/s/balancer/balance_assign_event.h"
#include "mongo/db/s/balancer/balance_event_engine.h"
#include "mongo/db/s/balancer/balance_move_event.h"
#include "mongo/db/s/balancer/balance_offload_event.h"
#include "mongo/db/s/balancer/balance_rename_event.h"
#include "mongo/db/s/balancer/balance_split_event.h"
#include "mongo/db/s/balancer/balancer.h"
#include "mongo/db/s/balancer/type_rebalance_events.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/assign_chunk_request.h"
#include "mongo/s/balancer_configuration.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/sharding_catalog_manager.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/confirm_split_request.h"
#include "mongo/s/grid.h"
#include "mongo/s/offload_chunk_request.h"
#include "mongo/s/sharding_raii.h"
#include "mongo/s/split_chunk_request.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/stacktrace.h"
#include "mongo/util/util_extend/GlobalConfig.h"

namespace mongo {

using CallbackHandle = executor::TaskExecutor::CallbackHandle;
using CallbackArgs = executor::TaskExecutor::CallbackArgs;
using RemoteCommandCallbackArgs = executor::TaskExecutor::RemoteCommandCallbackArgs;
using RemoteCommandCallbackFn = executor::TaskExecutor::RemoteCommandCallbackFn;
using RemoteCommandRequest = executor::RemoteCommandRequest;
using RemoteCommandResponse = executor::RemoteCommandResponse;
using std::shared_ptr;
using std::vector;
using str::stream;

MONGO_FP_DECLARE(writeEventDuplicateHook);

namespace {

const WriteConcernOptions kMajorityWriteConcern(WriteConcernOptions::kMajority,
                                                WriteConcernOptions::SyncMode::UNSET,
                                                Seconds(10));

const std::string kErrMsg = "fail to parse";


// Returns whether the specified status is an error caused by stepdown of the primary config node
// currently running the BalanceEventEngine.
bool isErrorDueToConfigStepdown(Status status, bool isStopping) {
    return ((status == ErrorCodes::CallbackCanceled && isStopping) ||
            status == ErrorCodes::ShutdownInProgress ||
            status == ErrorCodes::InterruptedDueToReplStateChange);
}

// util function to print log for debugging
#define logBalanceEvent(event)                                                          \
    {                                                                                   \
        index_log() << "[BEE] eventid(" + event->getEventId() + "), "                   \
                    << "eventtype(" << static_cast<int>(event->getEventType()) << "), " \
                    << "chunk(" + event->getChunk().getID() + "), "                     \
                    << "prevState(" << static_cast<int>(event->getPrevState()) << "), " \
                    << "curState(" << static_cast<int>(event->getState()) << ")";       \
    }


}  // namespace

// Decode RebalanceEventType BSONObj and create corresponding rebalance event.
StatusWith<IRebalanceEvent*> createRebalanceEventFromBSON(OperationContext* txn,
                                                          std::string& eventId,
                                                          BalanceEventEngine::EventType eventType,
                                                          const BSONObj& eventData) {
    IRebalanceEvent* rbEvent = nullptr;
    auto resultNotification = std::make_shared<Notification<ErrorCodes::Error>>();
    IRebalanceEvent::EventResultInfo* eventResultInfo;
    switch (eventType) {
        case BalanceEventEngine::EventType::kAssignEvent: {
            eventResultInfo = new (std::nothrow) IRebalanceEvent::EventResultInfo(
                resultNotification, BalanceEventEngine::EventType::kAssignEvent);
            if (!eventResultInfo) {
                return Status(ErrorCodes::ExceededMemoryLimit,
                              "failed to allocate EventResultInfo.");
            }
            AssignEvent* assignEvent = new (std::nothrow) AssignEvent(eventId, eventResultInfo);
            if (!assignEvent) {
                delete eventResultInfo;
                return Status(ErrorCodes::ExceededMemoryLimit, "failed to allocate AssignEvent.");
            }
            Status status = assignEvent->dataFromBSON(eventData);
            if (!status.isOK()) {
                delete eventResultInfo;
                delete assignEvent;
                return status;
            }

            rbEvent = assignEvent;
            break;
        }

        case BalanceEventEngine::EventType::kOffloadEvent: {
            eventResultInfo = new (std::nothrow) IRebalanceEvent::EventResultInfo(
                resultNotification, BalanceEventEngine::EventType::kOffloadEvent);
            if (!eventResultInfo) {
                return Status(ErrorCodes::ExceededMemoryLimit,
                              "failed to allocate EventResultInfo.");
            }
            OffloadEvent* offloadEvent = new (std::nothrow) OffloadEvent(eventId, eventResultInfo);
            if (!offloadEvent) {
                delete eventResultInfo;
                return Status(ErrorCodes::ExceededMemoryLimit, "failed to allocate OffloadEvent.");
            }
            Status status = offloadEvent->dataFromBSON(eventData);
            if (!status.isOK()) {
                delete eventResultInfo;
                delete offloadEvent;
                return status;
            }

            rbEvent = offloadEvent;
            break;
        }

        case BalanceEventEngine::EventType::kMoveEvent: {
            eventResultInfo = new (std::nothrow) IRebalanceEvent::EventResultInfo(
                resultNotification, BalanceEventEngine::EventType::kMoveEvent);
            if (!eventResultInfo) {
                return Status(ErrorCodes::ExceededMemoryLimit,
                              "failed to allocate EventResultInfo.");
            }
            MoveEvent* moveEvent = new (std::nothrow) MoveEvent(eventId, eventResultInfo);
            if (!moveEvent) {
                delete eventResultInfo;
                return Status(ErrorCodes::ExceededMemoryLimit, "failed to allocate MoveEvent.");
            }

            Status status = moveEvent->dataFromBSON(eventData);
            if (!status.isOK()) {
                delete eventResultInfo;
                delete moveEvent;
                return status;
            }

            rbEvent = moveEvent;
            break;
        }

        case BalanceEventEngine::EventType::kSplitEvent: {
            eventResultInfo = new (std::nothrow) IRebalanceEvent::EventResultInfo(
                resultNotification, BalanceEventEngine::EventType::kSplitEvent);
            if (!eventResultInfo) {
                return Status(ErrorCodes::ExceededMemoryLimit,
                              "failed to allocate EventResultInfo.");
            }

            SplitEvent* splitEvent = new (std::nothrow) SplitEvent(eventId, eventResultInfo);
            if (!splitEvent) {
                delete eventResultInfo;
                return Status(ErrorCodes::ExceededMemoryLimit, "failed to allocate SplitEvent.");
            }

            Status status = splitEvent->dataFromBSON(eventData);
            if (!status.isOK()) {
                delete eventResultInfo;
                delete splitEvent;
                return status;
            }

            rbEvent = splitEvent;
            break;
        }
        case BalanceEventEngine::EventType::kRenameEvent: {
            eventResultInfo = new (std::nothrow) IRebalanceEvent::EventResultInfo(
                resultNotification, BalanceEventEngine::EventType::kRenameEvent);
            if (!eventResultInfo) {
                return Status(ErrorCodes::ExceededMemoryLimit,
                              "failed to allocate EventResultInfo.");
            }

            RenameEvent* renameEvent = new (std::nothrow) RenameEvent(eventId, eventResultInfo);
            if (!renameEvent) {
                delete eventResultInfo;
                return Status(ErrorCodes::ExceededMemoryLimit, "failed to allocate RenameEvent.");
            }

            Status status = renameEvent->dataFromBSON(eventData);
            if (!status.isOK()) {
                delete eventResultInfo;
                delete renameEvent;
                return status;
            }

            rbEvent = renameEvent;
            break;
        }

        default: {
            index_err() << "[BEE] unknown event type";
            invariant(false);
        }
    }

    // refresh chunkType according to config.chunks
    SplitEvent* splitEvent = nullptr;
    auto chunkId = rbEvent->getChunk().getID();
    BSONArrayBuilder orQueryBuilder;
    orQueryBuilder.append(BSON(ChunkType::name() << chunkId));
    if (eventType == BalanceEventEngine::EventType::kSplitEvent) {
        splitEvent = (SplitEvent*)rbEvent;
        auto childChunkId = splitEvent->getChildChunk().getID();
        orQueryBuilder.append(BSON(ChunkType::name() << childChunkId));
    }

    Status chunkStatus(ErrorCodes::BadValue, "ChunkType bad value");
    bool hasError = false;
    do {
        auto findChunkStatus =
            Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                txn,
                ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                repl::ReadConcernLevel::kLocalReadConcern,
                NamespaceString(ChunkType::ConfigNS),
                BSON("$or" << orQueryBuilder.arr()),
                BSONObj(),
                boost::none);
        if (!findChunkStatus.isOK()) {
            chunkStatus = findChunkStatus.getStatus();
            hasError = true;
            break;
        }

        auto chunkDocs = findChunkStatus.getValue().docs;
        if (chunkDocs.size() == 0 || chunkDocs.size() > 2) {
            chunkStatus = {ErrorCodes::BadValue, "number of chunk docs is not correct"};
            hasError = true;
            break;
        }

        auto chunkDocStatus = ChunkType::fromBSON(chunkDocs[0]);
        if (!chunkDocStatus.isOK()) {
            chunkStatus = chunkDocStatus.getStatus();
            hasError = true;
            break;
        }
        auto chunkType = chunkDocStatus.getValue();
        if (chunkDocs.size() == 1) {
            if (eventType != BalanceEventEngine::EventType::kRenameEvent)
                rbEvent->refreshChunk(chunkType);
        } else {
            if (splitEvent) {
                auto childChunkDocStatus = ChunkType::fromBSON(chunkDocs[1]);
                if (childChunkDocStatus.isOK()) {
                    auto childChunkType = childChunkDocStatus.getValue();
                    splitEvent->refreshChunk(chunkType, childChunkType);
                    break;
                }
            }
            chunkStatus = {ErrorCodes::BadValue, "failed to process second chunk type"};
            hasError = true;
            break;
        }
    } while (0);

    if (hasError) {
        delete eventResultInfo;
        delete rbEvent;
        return chunkStatus;
    }

    return rbEvent;
}

BalanceEventEngine::BalanceEventEngine() {
    ThreadPool::Options options;
    options.poolName = "eventHanldingThreadPool";
    options.threadNamePrefix = "eventHanlding";
    options.onCreateThread = [](const std::string& threadName) {
        Client::initThread(threadName.c_str());
    };
    _eventHandlingThreadPool = new ThreadPool(options);
    _eventHandlingThreadPool->startup();
}

BalanceEventEngine::~BalanceEventEngine() {
    _eventHandlingThreadPool->shutdown();
    _eventHandlingThreadPool->join();
    delete (_eventHandlingThreadPool);
    _eventHandlingThreadPool = nullptr;
}

BalanceEventEngine::State BalanceEventEngine::getBalanceEventEngineState() {
    return _state;
}

Status BalanceEventEngine::writeEventDocument(OperationContext* txn, IRebalanceEvent* rbEvent) {
    // If rbEvent does not need persistent return
    if (!rbEvent->getNeedWriteEvent()) {
        return Status::OK();
    }

    if (rbEvent->isBGCommand()) {
        index_log() << "[Balancer] background event, chunk: " << rbEvent->getChunk().getFullNs()
                    << "; type: " << (int)(rbEvent->getEventType());
        if (EventType::kMoveEvent == rbEvent->getEventType() &&
            !Grid::get(txn)->getBalancerConfiguration()->shouldBalance()) {
            index_warning() << "[Balancer] background moveChunk faild, stoped balancer!";
            return Status(ErrorCodes::Interrupted, "stoped balancer!");
        } else if (EventType::kSplitEvent == rbEvent->getEventType() &&
                   !Grid::get(txn)->getBalancerConfiguration()->getShouldAutoSplit()) {
            index_warning() << "[Balancer] background splitChunk faild, stoped balancer!";
            return Status(ErrorCodes::Interrupted, "stoped balancer!");
        }
    }

    // Try to write a unique BalanceEventEngine document to config.BalanceEventEngine.
    const RebalanceEventType rebalanceEventType(rbEvent);
    std::string eventid = rbEvent->getEventId();
    if (MONGO_FAIL_POINT(writeEventDuplicateHook)) {
        return Status(ErrorCodes::DuplicateKey,
                      "failed to read document from config.rebalanceevents");
    }

    // this is a new event, write a document into config.rebalanceevents
    index_LOG(1) << "[BEE] going to write event document: " << rebalanceEventType.toBSON();
    Status result = grid.catalogClient(txn)->insertConfigDocument(
        txn, RebalanceEventType::ConfigNS, rebalanceEventType.toBSON(), kMajorityWriteConcern);

    if (!result.isOK()) {
        if (result == ErrorCodes::DuplicateKey) {
            auto statusWithEventQueryResult =
                grid.shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                    txn,
                    ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                    repl::ReadConcernLevel::kLocalReadConcern,
                    NamespaceString(RebalanceEventType::ConfigNS),
                    BSON(RebalanceEventType::eventId(eventid)),
                    BSONObj(),
                    boost::none);

            if (!statusWithEventQueryResult.isOK()) {
                index_err() << "[BEE] failed to read document due to "
                            << statusWithEventQueryResult.getStatus().reason();
                return Status(ErrorCodes::DuplicateKey,
                              "failed to read document from config.rebalanceevents");
            }

            auto eventDocs = statusWithEventQueryResult.getValue().docs;
            if (eventDocs.size() == 0) {
                index_err() << "[BEE] event(" + eventid + ") "
                            << "document has been deleted after executing exhaustiveFindOnConfig";
                return Status(ErrorCodes::NoSuchKey,
                              "failed to read document from config.rebalanceevents");
            } else if (eventDocs.size() > 1) {
                index_err() << "[BEE] event(" + eventid + ") "
                            << "document has more than one record";
                invariant(eventDocs.size() > 1);
            }

            auto statusWithRebalanceEventType = RebalanceEventType::fromBSON(eventDocs[0]);
            auto eventType = statusWithRebalanceEventType.getValue();
            index_err() << "[BEE] existing event document " << eventType.toString();

            // we believe that it should be considered success if data on disk is completely
            // consistent with what we gonna write,
            // since there are never two events in "_balancedChunkMap" with same chunkid.
            // "_balancedChunkMap" means the online events
            if (eventType == rebalanceEventType) {
                return Status::OK();
            }
        }

        index_err() << "[BEE] failed to write document into config.rebalanceevents";
        return result;
    }

    return Status::OK();
}

Status BalanceEventEngine::updateEventDocument(OperationContext* txn,
                                               const std::string& eventId,
                                               const BSONObj& eventData) {
    index_LOG(1) << "[BEE] going to update event document, eventid: " << eventId 
        << " , eventData: " << eventData;
    auto updateStatus =
        grid.catalogClient(txn)->updateConfigDocument(txn,
                                                      RebalanceEventType::ConfigNS,
                                                      BSON(RebalanceEventType::eventId(eventId)),
                                                      eventData,
                                                      false,
                                                      kMajorityWriteConcern);

    if (!updateStatus.isOK()) {
        index_err() << "[BEE] Failed to update event document: " << eventId
                    << " err: " << updateStatus.getStatus().reason();
        return updateStatus.getStatus();
    }

    return Status::OK();
}

Status BalanceEventEngine::removeEventDocument(OperationContext* txn, const std::string& eventId) {
    BSONObj rebalaneEventDocumentIdentifier = BSON(RebalanceEventType::eventId(eventId));
    index_LOG(1) << "[BEE] going to remove event document: " << rebalaneEventDocumentIdentifier;
    Status status = grid.catalogClient(txn)->removeConfigDocuments(
        txn, RebalanceEventType::ConfigNS, rebalaneEventDocumentIdentifier, kMajorityWriteConcern);
    if (!status.isOK()) {
        index_err() << "[BEE] remove event failed, event: " << rebalaneEventDocumentIdentifier;
    }
    return status;
}

void BalanceEventEngine::scheduleRebalanceEvent() {
    _eventHandlingThreadPool->schedule(BalanceEventEngine::pickAndProcessEvent);
}

bool BalanceEventEngine::canExecuteEvent() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    if (_state != State::kRunning) {
        index_err() << "[BEE] Event cannot be processed because state machine is not running";
        return false;
    }
    return true;
}

void BalanceEventEngine::_recoveryThread() {
    index_log() << "[BEE] start recovery thread";
    Client::initThread("BalanceEventEngine-recovery");
    auto txn = cc().makeOperationContext();

    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _threadOperationContext = txn.get();
    }

    // clearAllPartitionAlarmParameters();
    int count_startRecovery = 0;
    while (1) {
        Status status = startRecovery(txn.get());
        count_startRecovery++;
        if (status.isOK()) {
            break;
        }

        stdx::lock_guard<stdx::mutex> lock(_mutex);
        if (_state != State::kRunning) {
            index_err() << "BalanceEventEngine recovery thread is now stopped, with failed "
                        << "startRecovery" << causedBy(status);
            _threadOperationContext = nullptr;

            return;
        }

        if (count_startRecovery < 60) {
            sleepmillis(500);
            continue;
        }

        fassertFailedWithStatus(
            40400,
            Status(status.code(), stream() << "Failed to start recovery" << causedBy(status)));
    }

    finishRecovery(txn.get());
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _eventsRecovered = true;
        _threadOperationContext = nullptr;
        _condVar.notify_all();
    }

    index_log() << "BalanceEventEngine recovery thread is now stopped";

    return;
}

void BalanceEventEngine::addRetryingEvent(IRebalanceEvent* rbEvent) {
    stdx::lock_guard<stdx::mutex> lock(_retryingEventsMutex);
    _retryingEvents.push_back(rbEvent);
}

void BalanceEventEngine::addRetryingEvent_inlock(IRebalanceEvent* rbEvent) {
    _retryingEvents.push_back(rbEvent);
}

void BalanceEventEngine::_retryBalanceCmdThread() {
    index_log() << "[BEE] start event command retry thread";
    Client::initThread("BalanceEventEngine-retry");
    auto txn = cc().makeOperationContext();

    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _retryThreadOperationContext = txn.get();
    }

    while (!_stopRequested()) {
        {
            stdx::lock_guard<stdx::mutex> lock(_retryingEventsMutex);
            RebalanceEventList::iterator it = _retryingEvents.begin();
            while (it != _retryingEvents.end() && !_stopRequested()) {
                _sendBalanceCommand(_retryThreadOperationContext, *it, true);
                it = _retryingEvents.erase(it);
            }
        }

        _sleepFor(
            Milliseconds(1000 * 3 /*BASE_GetParaS64Val(E_SPA_CONFIG_LB_CMD_RETRY_INTERVAL)*/));
    }

    // handle all events in _retryingEvents again which are added before _stopRequested
    RebalanceEventList handleEvents;
    {
        stdx::lock_guard<stdx::mutex> lock(_retryingEventsMutex);
        handleEvents = _retryingEvents;
        _retryingEvents.clear();
    }
    for (auto event : handleEvents) {
        _sendBalanceCommand(_retryThreadOperationContext, event, false);
    }

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        invariant(_state == State::kStopping);
        _retryThreadOperationContext = nullptr;
    }

    index_log() << "[BEE] event command retry thread is now stopped";

    return;
}

bool BalanceEventEngine::_stopRequested() {
    stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
    return (_state != State::kRunning);
}

void BalanceEventEngine::_sleepFor(Milliseconds waitTimeout) {
    stdx::unique_lock<stdx::mutex> lock(_retryingEventsMutex);
    _retryCondVar.wait_for(
        lock, waitTimeout.toSteadyDuration(), [&] { return _state != State::kRunning; });
}

void BalanceEventEngine::onTransitionToPrimary(OperationContext* txn) {
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        invariant(_state == State::kStopped);
        invariant(_rebalanceEventRecoveryList.empty());
        _state = State::kRunning;
        _eventsRecovered = false;
    }

    invariant(!_thread.joinable());
    invariant(!_threadOperationContext);
    _thread = stdx::thread([this] { _recoveryThread(); });

    invariant(!_retryCmdThread.joinable());
    invariant(!_retryThreadOperationContext);
    _retryCmdThread = stdx::thread([this] { _retryBalanceCmdThread(); });
    index_log() << "[BEE] onTransitionToPrimary complete";
}

void BalanceEventEngine::onStepDownFromPrimary() {
    stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
    if (_state != State::kRunning)
        return;

    _state = State::kStopping;

    if (_threadOperationContext) {
        stdx::lock_guard<Client> scopedClientLock(*_threadOperationContext->getClient());
        _threadOperationContext->markKilled(ErrorCodes::InterruptedDueToReplStateChange);
    }

    if (_retryThreadOperationContext) {
        stdx::lock_guard<Client> scopedClientLock(*_retryThreadOperationContext->getClient());
        _retryThreadOperationContext->markKilled(ErrorCodes::InterruptedDueToReplStateChange);
    }

    _condVar.notify_all();
    // clearAllPartitionAlarmParameters();
    index_log() << "[BEE] onStepDownFromPrimary compelte";
}

void BalanceEventEngine::onDrainComplete(OperationContext* txn) {
    {
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        if (_state == State::kStopped)
            return;
        invariant(_state == State::kStopping);
    }

    drainActiveEvents();

    _thread.join();
    _retryCmdThread.join();

    {
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        _state = State::kStopped;
        _thread = {};
        _retryCmdThread = {};
    }

    {
        stdx::lock_guard<stdx::mutex> lock(_retryingEventsMutex);
        _retryingEvents.clear();
        stdx::lock_guard<stdx::mutex> maplock(_chunkMapMutex);
        _balancedChunkMap.clear();
    }

    index_log() << "[BEE] Balance event engine is terminated";
}

void BalanceEventEngine::drainActiveEvents() {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    if (_state == State::kStopped)
        return;
    invariant(_state == State::kStopping);

    _condVar.wait(lock, [this] { return _waitingEvents.empty(); });

    index_log() << "[BEE] drainActiveEvents complete";
}

Status BalanceEventEngine::_validateEventChunk(OperationContext* txn,
                                               IRebalanceEvent* event,
                                               ChunkType& chunkType) {
    auto chunk = event->getChunk();
    auto chunkId = chunk.getID();
    auto findChunkStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            repl::ReadConcernLevel::kLocalReadConcern,
            NamespaceString(ChunkType::ConfigNS),
            BSON(ChunkType::name(chunkId)),
            BSONObj(),
            boost::none);
    if (!findChunkStatus.isOK()) {
        return findChunkStatus.getStatus();
    }

    auto chunkDocs = findChunkStatus.getValue().docs;
    if (chunkDocs.size() == 0) {
        return Status(ErrorCodes::ChunkNotFound,
                      stream() << "chunk does not exist : " << chunk.getFullNs());
    }
    invariant(chunkDocs.size() == 1);

    auto chunkDocStatus = ChunkType::fromBSON(chunkDocs.front());
    invariant(chunkDocStatus.isOK());
    chunkType = chunkDocStatus.getValue();

    if (chunk.getNS() != chunkType.getNS()) {
        return Status(ErrorCodes::ChunkChanged, "chunk ns has changed");
    }

    auto status = _isStaleChunk(event, chunkType);

    return status;
}

Status BalanceEventEngine::_isStaleChunk(IRebalanceEvent* event, const ChunkType& chunkType) {
    auto chunk = event->getChunk();

    ErrorCodes::Error errorCode = ErrorCodes::OK;
    std::string errorMessage = "";

    do {
        if (chunk.getShard() != chunkType.getShard()) {
            errorCode = ErrorCodes::ChunkChanged;
            errorMessage = "chunk shard has changed";
            break;
        }

        if (!chunk.getVersion().equals(chunkType.getVersion())) {
            errorCode = ErrorCodes::ChunkChanged;
            errorMessage = "chunk version has changed";
            break;
        }

        if (chunk.getMin().woCompare(chunkType.getMin()) != 0 ||
            chunk.getMax().woCompare(chunkType.getMax()) != 0) {
            errorCode = ErrorCodes::ChunkChanged;
            errorMessage = "chunk key range has changed";
            break;
        }
    } while (0);

    if ((errorCode == ErrorCodes::ChunkChanged) &&
        (event->getEventType() == BalanceEventEngine::EventType::kAssignEvent) &&
        !(event->isUserRequest())) {
        ShardIdent shardIdent;
        auto shardId = chunkType.getShard();
        auto processIdent = chunkType.getProcessIdentity();
        shardIdent.setShardId(shardId);
        shardIdent.setProcessIdentity(processIdent);
        bool alive = grid.catalogManager()->getShardServerManager()->isShardServerAlive(shardIdent);
        if (!alive) {
            return Status::OK();
        }
    }

    return Status(errorCode, errorMessage);
}

bool BalanceEventEngine::_canExecuteDuplicateEvent(IRebalanceEvent* existingEvent,
                                                   IRebalanceEvent* newEvent) {
    logBalanceEvent(newEvent);
    if ((newEvent->getEventType() != BalanceEventEngine::EventType::kAssignEvent) ||
        newEvent->isUserRequest()) {
        return false;
    }

    ShardIdent existingTargetIdent;
    switch (existingEvent->getEventType()) {
        case BalanceEventEngine::EventType::kAssignEvent: {
            AssignEvent* existingAssignEvent = (AssignEvent*)existingEvent;
            existingTargetIdent = existingAssignEvent->getTarget();
            break;
        }

        case BalanceEventEngine::EventType::kOffloadEvent: {
            OffloadEvent* existingOffloadEvent = (OffloadEvent*)existingEvent;
            existingTargetIdent = existingOffloadEvent->getSource();
            break;
        }

        case BalanceEventEngine::EventType::kMoveEvent: {
            MoveEvent* existingMoveEvent = (MoveEvent*)existingEvent;
            existingTargetIdent = existingMoveEvent->getTarget();
            break;
        }

        case BalanceEventEngine::EventType::kSplitEvent: {
            auto existingShardId = existingEvent->getChunk().getShard();
            auto existingProcessIdent = existingEvent->getChunk().getProcessIdentity();
            existingTargetIdent.setShardId(existingShardId);
            existingTargetIdent.setProcessIdentity(existingProcessIdent);
            break;
        }

        case BalanceEventEngine::EventType::kRenameEvent: {
            RenameEvent* existingRenameEvent = (RenameEvent*)existingEvent;
            existingTargetIdent = existingRenameEvent->getSource();
            break;
        }

        default: {
            // should never be here
            index_err() << "[BEE] unknow event type";
            invariant(false);
        }
    }

    // determine target is dead or not
    bool alive =
        grid.catalogManager()->getShardServerManager()->isShardServerAlive(existingTargetIdent);
    if (alive) {
        return false;
    }

    index_log() << "[BEE] interrupte duplicate event(eventId:" << existingEvent->getEventId()
                << ", eventType:" << static_cast<int>(existingEvent->getEventType())
                << ", chunk:" << existingEvent->getChunk().getFullNs();
    existingEvent->setInterrupt(true);
    return true;
}

Status BalanceEventEngine::addEventToBalancedChunkMap(OperationContext* txn,
                                                      IRebalanceEvent* rbEvent) {
    auto chunkId = rbEvent->getChunk().getID();
    logBalanceEvent(rbEvent);
    bool scheduleEvent = false;
    ChunkBalanceInfo* balanceInfo = new (std::nothrow) ChunkBalanceInfo(chunkId);
    if (balanceInfo == nullptr) {
        return Status(ErrorCodes::ExceededMemoryLimit, "fail to allocate ChunkBalanceInfo");
    }

    {
        stdx::lock_guard<stdx::mutex> lock(_chunkMapMutex);
        BalancedChunkMap::const_iterator it = _balancedChunkMap.find(chunkId);
        if (it == _balancedChunkMap.end()) {
            index_LOG(2) << "[BEE] addEventToBalancedChunkMap: chunk not found in map " << chunkId;
            _balancedChunkMap[chunkId] = balanceInfo;
        } else {
            index_LOG(2) << "[BEE] addEventToBalancedChunkMap: chunk found in map " << chunkId;
            delete balanceInfo;
            balanceInfo = _balancedChunkMap[chunkId];
        }

        if (balanceInfo->activeEvent == nullptr) {
            index_LOG(2) << "[BEE] addEventToBalancedChunkMap: activeEvent is null " << chunkId;
            balanceInfo->activeEvent = rbEvent;
            scheduleEvent = true;
        } else {
            index_log() << "[BEE] found an existing event, chunk("
                        << balanceInfo->activeEvent->getChunk().getFullNs() << "), eventtype("
                        << static_cast<int>(balanceInfo->activeEvent->getEventType())
                        << "), curstate(" << static_cast<int>(balanceInfo->activeEvent->getState())
                        << "), prevstate("
                        << static_cast<int>(balanceInfo->activeEvent->getPrevState()) << ")";

            auto canExecute = _canExecuteDuplicateEvent(balanceInfo->activeEvent, rbEvent);
            if (canExecute) {
                rbEvent->getEventResultInfo()->setErrorInfo(ErrorCodes::ChunkBusy, "Chunk Busy");
            }
            return Status(ErrorCodes::ChunkBusy, "A rebalance event for this chunk already exists");
        }

        if (scheduleEvent) {
            if ((rbEvent->getEventType() == BalanceEventEngine::EventType::kAssignEvent) &&
                (!rbEvent->isInRecovery())) {
                getGlobalServiceContext()
                    ->getProcessStageTime("assignChunk:" + rbEvent->getChunk().getName())
                    ->noteStageStart("addEventToBalancedChunkMap:addEvent:State" +
                                     std::to_string((int)rbEvent->getState()));
            }
            addEvent(rbEvent);
            if ((rbEvent->getEventType() == BalanceEventEngine::EventType::kAssignEvent) &&
                (!rbEvent->isInRecovery())) {
                getGlobalServiceContext()
                    ->getProcessStageTime("assignChunk:" + rbEvent->getChunk().getName())
                    ->noteStageStart("addEventToBalancedChunkMap:scheduleRebalanceEvent:State" +
                                     std::to_string((int)rbEvent->getState()));
            }
            scheduleRebalanceEvent();
        }
    }

    return Status::OK();
}

void BalanceEventEngine::removeEventFromBalancedChunkMap(IRebalanceEvent* rbEvent) {
    logBalanceEvent(rbEvent);
    auto chunkId = rbEvent->getChunk().getID();
    ChunkBalanceInfo* balanceInfo = nullptr;
    {
        stdx::lock_guard<stdx::mutex> lock(_chunkMapMutex);
        BalancedChunkMap::iterator it = _balancedChunkMap.find(chunkId);
        if (it != _balancedChunkMap.end()) {
            balanceInfo = it->second;
        } else {
            index_log() << "[BEE] can not find balanceInfo for " << chunkId
                        << " in _balancedChunkMap";
            return;
        }

        if (!rbEvent->isProcessed() && (balanceInfo->activeEvent != rbEvent)) {
            index_log() << "[BEE] event is not processed and not equal to balanceInfo->activeEvent";
            return;
        }
        _balancedChunkMap.erase(it);
    }

    delete (balanceInfo);
}

Status BalanceEventEngine::addEvent(IRebalanceEvent* rbEvent) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    // rbEvent->startTraceForAsyncStats(E_TPOINT_CONFIG_STATEMACHINE_INQUEUE);
    _waitingEvents.push_back(rbEvent);

    return Status::OK();
}

Status BalanceEventEngine::pickOneActiveEvent(IRebalanceEvent*& rbEvent) {
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        if (!_waitingEvents.empty()) {
            rbEvent = _waitingEvents.front();
            _waitingEvents.pop_front();
            // rbEvent->endTraceForAsyncStats(true);
        } else {
            if (_state == State::kStopping) {
                index_log() << "[BEE] BalanceEventEngine is interrupted, "
                            << "_waitingEvents is empty notify _condVar";
                _condVar.notify_all();
            }
            index_log() << "[BEE] pickOneActiveEvent error, no rebalance event";
            return Status(ErrorCodes::BadValue, "No rebalance event");
        }
    }

    return Status::OK();
}

void BalanceEventEngine::deleteEvent(OperationContext* txn, IRebalanceEvent* rbEvent) {
    if (!rbEvent) {
        index_log() << "no need to delete rbEvent, it is nullptr";
        return;
    }

    logBalanceEvent(rbEvent);
    index_log() << "[BEE] complete chunk info: " << rbEvent->getChunk().getFullNs();
    auto client = txn->getClient();
    BalanceEventEngine* balanceEventEngine =
        Grid::get(txn)->catalogManager()->getBalanceEventEngine();
    if (rbEvent->isProcessed()) {
        int removeEventRound = 0;
        {
            stdx::lock_guard<Client> lk(*client);
            client->resetOperationContext();
        }

        {
            auto opTxn = client->makeOperationContext();
            while (true) {
                if (!balanceEventEngine->canExecuteEvent()) {
                    index_err() << "[BEE] can delete event " << rbEvent->getEventId()
                                << "due to balance event engine is not running";
                    break;
                }

		if (rbEvent->shouldContinue()) {
		    index_log() << "[BEE] should not delete event: " << rbEvent->getEventId() <<
		        " ,current state " << static_cast<int>(rbEvent->getState()) << " should continue.";
		    break;
		}

                Status status = removeEventDocument(opTxn.get(), rbEvent->getEventId());
                if (status.isOK()) {
                    index_log() << "[BEE] Finish to delete event " << rbEvent->getEventId();
                    break;
                }

                index_err() << "[BEE] Failed to delete event from configDB";
                stdx::this_thread::sleep_for(kDefaultUpdateDbRetryInterval.toSteadyDuration());
                removeEventRound++;
            }
        }

        {
            stdx::lock_guard<Client> lk(*client);
            client->setOperationContext(txn);
        }
    }

    // delete chunkRebalanceInfo if needed
    balanceEventEngine->removeEventFromBalancedChunkMap(rbEvent);

    if ((rbEvent->getEventType() == BalanceEventEngine::EventType::kAssignEvent) &&
        (!rbEvent->isInRecovery())) {
        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + rbEvent->getChunk().getName())
            ->noteProcessEnd();
        // ignore unprocessed event to avoid huge amount of log
        if (rbEvent->isProcessed()) {
            index_log() << "Time of "
                        << "assignChunk:" + rbEvent->getChunk().getName() << ": "
                        << getGlobalServiceContext()
                               ->getProcessStageTime("assignChunk:" + rbEvent->getChunk().getName())
                               ->toString();
        }
        getGlobalServiceContext()->cancelProcessStageTime("assignChunk:" +
                                                          rbEvent->getChunk().getName());
    }
    // put chunk into failed chunk list if needed
    auto eventResultInfo = rbEvent->getEventResultInfo();
    if (eventResultInfo) {
        auto curState = eventResultInfo->getCurrentState();
        auto errorCode = eventResultInfo->getErrorCode();
        // sendOrRecoverAlarmsWhenDeleteEvent(rbEvent, errorCode, balanceEventEngine, txn);
        if (BalanceEventEngine::EventType::kAssignEvent <= rbEvent->getEventType() &&
            BalanceEventEngine::EventType::kRenameEvent >= rbEvent->getEventType()) {
            index_log()
                << "[BEE] determine whether this event needs to be put into failed chunk list,"
                << "curState(" << static_cast<int>(curState) << "),"
                << "errorCode(" << static_cast<int>(errorCode) << "),"
                << "eventType(" << static_cast<int>(rbEvent->getEventType()) << "),"
                << "interrupted(" << static_cast<bool>(rbEvent->isInterrupted()) << "),"
                << "chunkChanged(" << static_cast<int>(rbEvent->getChunkChangedFlag()) << ")";
        }
        if (errorCode != ErrorCodes::OK &&
            curState > BalanceEventEngine::RebalanceEventState::kOffloaded &&
            curState <= BalanceEventEngine::RebalanceEventState::kAssignFailed) {
            if ((!(rbEvent->getEventType() == BalanceEventEngine::EventType::kAssignEvent &&
                   rbEvent->isUserRequest())) &&
                (!rbEvent->isInterrupted()) && (!rbEvent->getChunkChangedFlag()) &&
                (!rbEvent->needIsolateChunk()) && (balanceEventEngine->canExecuteEvent())) {
                auto chunk = rbEvent->getChunk();
                sleepmillis(500);
                Grid::get(txn)->catalogManager()->getShardServerManager()->processFailedChunk(
                    chunk);
            }
        }
        delete eventResultInfo;
    }

    auto balanceCmdInfo = rbEvent->getBalanceCmdInfo();
    if (balanceCmdInfo) {
        delete balanceCmdInfo;
    }

    delete rbEvent;
}

void BalanceEventEngine::processEvent(OperationContext* txn, IRebalanceEvent* rbEvent) {
    BalanceEventEngine* balanceEventEngine =
        Grid::get(txn)->catalogManager()->getBalanceEventEngine();

    if (!balanceEventEngine) {
        index_err() << "[BEE] getBalanceEventEngine failed.";
        return;
    }
    // notify upper layer to complete the task
    auto resultPtr = rbEvent->getEventResultInfo();
    bool internalError = false;
    // Execute the task according to the BalanceEventEngine map
    StatusWith<BalanceEventEngine::NextAction> actionState(
        BalanceEventEngine::NextAction::kActionContinue);
    do {
        if (!balanceEventEngine->canExecuteEvent()) {
            rbEvent->getEventResultInfo()->setErrorInfo(ErrorCodes::ShutdownInProgress,
                                                        "state machine is not running");
            internalError = true;
            break;
        }

        if (rbEvent->isInterrupted()) {
            rbEvent->getEventResultInfo()->setErrorInfo(ErrorCodes::Interrupted,
                                                        "event is interrupted");
            internalError = true;
            break;
        }

        BalanceEventEngine::RebalanceEventState curState = rbEvent->getState();

        if (rbEvent->needPersist()) {
            if ((rbEvent->getEventType() == BalanceEventEngine::EventType::kAssignEvent) &&
                (!rbEvent->isInRecovery())) {
                getGlobalServiceContext()
                    ->getProcessStageTime("assignChunk:" + rbEvent->getChunk().getName())
                    ->noteStageStart("persistStateTransition:State" +
                                     std::to_string(static_cast<int>(rbEvent->getState())));
            }
            Status status = balanceEventEngine->persistStateTransition(txn, rbEvent);
            if (!status.isOK()) {
                index_log() << "[BEE] processEvent persistStateTransition status: " << status.reason();
                //rbEvent->rollbackLastTransition();
                rbEvent->getEventResultInfo()->setErrorInfo(status.code(), status.reason());
                internalError = true;
                break;
            }
        }

        // Get the event handler
        if (curState <= BalanceEventEngine::RebalanceEventState::kStateInit ||
            curState >= BalanceEventEngine::RebalanceEventState::kInvalidState) {
            index_err() << "[BEE] processEvent curState: " << static_cast<int>(curState)
                        << " is invalid";
            rbEvent->getEventResultInfo()->setErrorInfo(ErrorCodes::InternalError,
                                                        "curState is invalid");
            internalError = true;
            break;
        }

        RebalanceEventHandler eventHandler = BalanceEventEngine::stateHandlerMap[curState];
        if ((rbEvent->getEventType() == BalanceEventEngine::EventType::kAssignEvent) &&
            (!rbEvent->isInRecovery())) {
            getGlobalServiceContext()
                ->getProcessStageTime("assignChunk:" + rbEvent->getChunk().getName())
                ->noteStageStart("eventHandler:State" +
                                 std::to_string(static_cast<int>(rbEvent->getState())));
        }
        actionState = eventHandler(rbEvent);
    } while (actionState.getStatus().isOK() &&
             BalanceEventEngine::NextAction::kActionContinue == actionState.getValue());

    resultPtr->setCurrentState(rbEvent->getState());
    if (!actionState.getStatus().isOK()) {
        resultPtr->setErrorInfo(actionState.getStatus().code(), actionState.getStatus().reason());
    }

    if (!actionState.getStatus().isOK() || internalError) {
        index_err() << "Event[" + rbEvent->getEventId() + "] " + resultPtr->getErrorMsg();

        if (!rbEvent->isUserRequest()) {
            deleteEvent(txn, rbEvent);
        } else {
            resultPtr->resultNotification->set(resultPtr->getErrorCode());
        }
    } else if (BalanceEventEngine::NextAction::kActionWait != actionState.getValue()) {
        ErrorCodes::Error statusCode = ErrorCodes::OK;
        if (BalanceEventEngine::NextAction::kActionError == actionState.getValue()) {
            statusCode = resultPtr->getErrorCode();
            // statusCode = ErrorCodes::RebalanceEventFailed;
            // resultPtr->setErrorInfo(ErrorCodes::RebalanceEventFailed, "Rebalance event execution
            // failed");
        } else if (BalanceEventEngine::NextAction::kActionFinish == actionState.getValue()) {
            statusCode = ErrorCodes::OK;
        }

        if (!rbEvent->isUserRequest()) {
            deleteEvent(txn, rbEvent);
        } else {
            if (rbEvent->getEventType() == BalanceEventEngine::EventType::kSplitEvent) {
                SplitEvent* splitEvent = (SplitEvent*)rbEvent;
                if (splitEvent->getSplitSuccessFlag() &&
                    (ErrorCodes::Interrupted == resultPtr->getErrorCode())) {
                    // return OK if split success and split envent is Interrupted.
                    statusCode = ErrorCodes::OK;
                }
            }

            resultPtr->resultNotification->set(statusCode);
        }
    }

    // for BalanceEventEngine::NextAction::kActionWait, just return
}

void BalanceEventEngine::pickAndProcessEvent() {
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    IRebalanceEvent* rbEvent = nullptr;
    Status getActiveEventStatus =
        Grid::get(txn)->catalogManager()->getBalanceEventEngine()->pickOneActiveEvent(rbEvent);

    if (getActiveEventStatus.isOK()) {
        // Execute the task according to the balance event engine map
        if ((rbEvent->getEventType() == BalanceEventEngine::EventType::kAssignEvent) &&
            (!rbEvent->isInRecovery())) {
            getGlobalServiceContext()
                ->getProcessStageTime("assignChunk:" + rbEvent->getChunk().getName())
                ->noteStageStart("processEvent:State" + std::to_string((int)rbEvent->getState()));
        }
        // Register a job for processEvent in case that it is hanging inside
        std::stringstream jobName;
        jobName << rbEvent->getEventId() << "-" << static_cast<int>(rbEvent->getEventType()) << "-"
                << txn->getOpID();

        processEvent(txn, rbEvent);

    } else {
        index_err() << "[BEE] pickAndProcessEvent failed, due to " << getActiveEventStatus.reason();
    }
}

Status BalanceEventEngine::executeEvent(OperationContext* txn,
                                        IRebalanceEvent* event,
                                        bool recovery) {
    BalanceEventEngine* balanceEventEngine =
        Grid::get(txn)->catalogManager()->getBalanceEventEngine();

    if (!balanceEventEngine) {
        index_err() << "[BEE] getBalanceEventEngine failed.";
        return Status(ErrorCodes::InternalError, "getBalanceEventEngine failed.");
    }

    if (!balanceEventEngine->canExecuteEvent()) {
        return Status(ErrorCodes::ShutdownInProgress,
                      "Event cannot be executed because state machine is not running");
    }

    if (recovery) {
        event->setRecoveryFlag(true);
    }

    // Add the event into the _balancedChunkMap list
    if ((event->getEventType() == BalanceEventEngine::EventType::kAssignEvent) &&
        (!event->isInRecovery())) {
        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + event->getChunk().getName())
            ->noteStageStart("addEventToBalancedChunkMap:State" +
                             std::to_string((int)event->getState()));
    }
    auto status = balanceEventEngine->addEventToBalancedChunkMap(txn, event);
    if (!status.isOK()) {
        index_err() << "[BEE] failed to add event " << causedBy(status);
        return status;
    }
    return Status::OK();
}

Status BalanceEventEngine::startRecovery(OperationContext* txn) {
    _corrRebalanceeventDocHasSend = false;
    index_log() << "[BEE] Begin to load rebalance events";
    // Load all the active events from config.rebalanceevents collection
    auto statusWithEventsQueryResponse =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            repl::ReadConcernLevel::kMajorityReadConcern,
            NamespaceString(RebalanceEventType::ConfigNS),
            BSONObj(),
            BSONObj(),
            boost::none);

    if (!statusWithEventsQueryResponse.isOK()) {
        return Status(statusWithEventsQueryResponse.getStatus().code(),
                      stream() << "Failed to read " << RebalanceEventType::ConfigNS
                               << " collection documents for rebalance events recovery"
                               << causedBy(statusWithEventsQueryResponse.getStatus()));
    }

    for (const BSONObj& event : statusWithEventsQueryResponse.getValue().docs) {
        index_log() << "[BEE] start Recovery find event: " << event;
        auto statusWithRebalanceEventType = RebalanceEventType::fromBSON(event);
        if (!statusWithRebalanceEventType.isOK()) {
            // The format of this statemachine document is incorrect.
            // Grid::get(txn)->catalogManager()->getShardServerManager()->sendCorrClstMgmtIndexAlarm(
            // RebalanceEventType::ConfigNS, _corrRebalanceeventDocHasSend);
	    index_err() << "Failed to parse Event: " << event;
            continue;
        }
        _rebalanceEventRecoveryList.push_back(std::move(statusWithRebalanceEventType.getValue()));
    }

    // Grid::get(txn)->catalogManager()->getShardServerManager()->recoverCorrClstMgmtIndexAlarm(
    // RebalanceEventType::ConfigNS, _corrRebalanceeventDocHasSend);
    index_log() << "[BEE] Finish loading rebalance events";
    return Status::OK();
}

void BalanceEventEngine::finishRecovery(OperationContext* txn) {
    index_log() << "[BEE] Start finishRecovery";

    // Schedule recovered rebalance events.
    std::list<RebalanceEventType>::iterator it = _rebalanceEventRecoveryList.begin();
    while (it != _rebalanceEventRecoveryList.end()) {
        // create BalanceEventEngine based on the BalanceEventEngineType
	index_log() << "[BEE] finishRecovery, event " << it->toString();
        auto eventId = it->getEventId();
        auto eventType = static_cast<BalanceEventEngine::EventType>(it->getEventType());
        auto eventData = it->getEventData();
        auto createStatus = createRebalanceEventFromBSON(txn, eventId, eventType, eventData);
        if (!createStatus.isOK()) {
            index_err() << "failed to recover event " << it->toString() << ", due to "
                        << createStatus.getStatus().reason();
            it = _rebalanceEventRecoveryList.erase(it);
            int removeEventRound = 0;
            while (true) {
                if (!canExecuteEvent()) {
                    index_err() << "[BEE] cannot delete event " << eventId
                                << "due to state machine is not running";
                    break;
                }

                Status status = removeEventDocument(txn, eventId);
                if (status.isOK()) {
                    index_log() << "[BEE] Finish to delete event " << eventId;
                    break;
                }

                index_err() << "[BEE] Failed to delete event " << eventId;
                stdx::this_thread::sleep_for(kDefaultUpdateDbRetryInterval.toSteadyDuration());
                removeEventRound++;
            }
            continue;
        }

        IRebalanceEvent* rbEvent = createStatus.getValue();
        // add chunk into alarm map
        // addChunksToAlarmMapsInRecovery(txn, rbEvent);

        rbEvent->setProcessed(true);
        auto executeEventStatus = executeEvent(txn, rbEvent, true);
        if (!executeEventStatus.isOK()) {
            index_err() << "failed to execute recover event " << it->toString()
                        << "due to " + executeEventStatus.reason();

            BalanceEventEngine::deleteEvent(txn, rbEvent);
        }

        it = _rebalanceEventRecoveryList.erase(it);
    }

    if (it != _rebalanceEventRecoveryList.end()) {
        index_log() << "finishRecovery is interrupted";
        _rebalanceEventRecoveryList.clear();

        return;
    }

    index_log() << "Finish finishRecovery";

    return;
}

void BalanceEventEngine::waitForRecovery() {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    _condVar.wait(lock,
                  [this] { return ((_eventsRecovered == true) || (_state != State::kRunning)); });
}

// TODO, will implement this later
void BalanceEventEngine::interruptAndDisableRebalance() {}

// TODO, will implement this later
void _abandonActiveRebalances(OperationContext* txn) {}

//////////////////////////////////////////////////////////////////////////
// Start to define state transition of the state machine
BalanceEventEngine::StateHandlerMapType BalanceEventEngine::stateHandlerMap = {
    {BalanceEventEngine::RebalanceEventState::kStartOffload, _startOffloadHandler},
    {BalanceEventEngine::RebalanceEventState::kKillSourceSS, _killSourceSSHandler},
    {BalanceEventEngine::RebalanceEventState::kOffloaded, _offloadedHandler},
    {BalanceEventEngine::RebalanceEventState::kOffloadFailed, _offloadFailedHandler},
    {BalanceEventEngine::RebalanceEventState::kStartAssign, _startAssignHandler},
    {BalanceEventEngine::RebalanceEventState::kKillTargetSS, _killTargetSSHandler},
    {BalanceEventEngine::RebalanceEventState::kAssignFailed, _assignFailedHandler},
    {BalanceEventEngine::RebalanceEventState::kAssigned, _assignedHandler},
    {BalanceEventEngine::RebalanceEventState::kStartSplit, _startSplitHandler},
    {BalanceEventEngine::RebalanceEventState::kSplitFailed, _splitFailedHandler},
    {BalanceEventEngine::RebalanceEventState::kCommitSplit, _commitSplitHandler},
    {BalanceEventEngine::RebalanceEventState::kAcknowledgeSplit, _acknowledgeSplitHandler},
    {BalanceEventEngine::RebalanceEventState::kSplitted, _splittedHandler}};

Status BalanceEventEngine::persistStateTransition(OperationContext* txn, IRebalanceEvent* rbEvent) {
    if (!rbEvent->getNeedWriteEvent()) {
        return Status::OK();
    }
    logBalanceEvent(rbEvent);

    BSONObjBuilder updateBuilder{};
    updateBuilder.append(kCurStateInEventData, static_cast<long long>(rbEvent->getState()));
    updateBuilder.append(kPrevStateInEventData, static_cast<long long>(rbEvent->getPrevState()));
    updateBuilder.append(kChunkInEventData, rbEvent->getChunk().toBSON());
    // TODO need to retry a few times once we got certain failure from updateEventDocument()
    if (rbEvent->getEventType() == BalanceEventEngine::EventType::kSplitEvent) {
        SplitEvent* splitEvent = (SplitEvent*)rbEvent;
        if ((rbEvent->getState() == BalanceEventEngine::RebalanceEventState::kCommitSplit) &&
            (rbEvent->getPrevState() == BalanceEventEngine::RebalanceEventState::kStartSplit)) {
            updateBuilder.append(kSplitPointInEventData, splitEvent->getSplitPoint());
        }
        updateBuilder.append(kChildChunkInEventData, splitEvent->getChildChunk().toBSON());
    }

    auto status =
        updateEventDocument(txn, rbEvent->getEventId(), BSON("$set" << updateBuilder.obj()));
    if (status.isOK()) {
        rbEvent->setNeedPersist(false);
    }

    return status;
}

bool BalanceEventEngine::isEventDone(IRebalanceEvent* rbEvent) {
    BalanceEventEngine::RebalanceEventState curState = rbEvent->getState();
    if ((curState == BalanceEventEngine::RebalanceEventState::kAssignFailed) ||
        (curState == BalanceEventEngine::RebalanceEventState::kAssigned)) {
        return true;
    }

    if ((curState == BalanceEventEngine::RebalanceEventState::kOffloaded) &&
        (rbEvent->getEventType() == BalanceEventEngine::EventType::kOffloadEvent)) {
        return true;
    }

    return false;
}

BalanceEventEngine::RebalanceEventState BalanceEventEngine::transitionState(
    IRebalanceEvent* rbEvent, ErrorCodes::Error statusCode) {
    BalanceEventEngine::RebalanceEventState curState = rbEvent->getState();
    BalanceEventEngine::RebalanceEventState nextState =
        BalanceEventEngine::RebalanceEventState::kStateInit;

    // State transition logic
    switch (curState) {
        case BalanceEventEngine::RebalanceEventState::kStartAssign: {
            if (statusCode == ErrorCodes::OK) {
                nextState = BalanceEventEngine::RebalanceEventState::kAssigned;
            } else {
                nextState = BalanceEventEngine::RebalanceEventState::kAssignFailed;
            }
            break;
        }

        case BalanceEventEngine::RebalanceEventState::kKillTargetSS: {
            nextState = BalanceEventEngine::RebalanceEventState::kAssignFailed;
            break;
        }

        case BalanceEventEngine::RebalanceEventState::kAssignFailed: {
            nextState = BalanceEventEngine::RebalanceEventState::kAssignFailed;
            break;
        }

        case BalanceEventEngine::RebalanceEventState::kAssigned: {
            nextState = BalanceEventEngine::RebalanceEventState::kAssigned;
            break;
        }

        case BalanceEventEngine::RebalanceEventState::kStartOffload: {
            if (statusCode == ErrorCodes::OK) {
                nextState = BalanceEventEngine::RebalanceEventState::kOffloaded;
            } else {
                nextState = BalanceEventEngine::RebalanceEventState::kOffloadFailed;
            }
            break;
        }

        case BalanceEventEngine::RebalanceEventState::kKillSourceSS: {
            nextState = BalanceEventEngine::RebalanceEventState::kOffloaded;
            break;
        }

        case BalanceEventEngine::RebalanceEventState::kOffloaded: {
            if (BalanceEventEngine::EventType::kMoveEvent == rbEvent->getEventType()) {
                nextState = BalanceEventEngine::RebalanceEventState::kStartAssign;
                if (!rbEvent->isInRecovery()) {
                    getGlobalServiceContext()->registerProcessStageTime(
                        "assignChunk:" + rbEvent->getChunk().getName());
                    getGlobalServiceContext()
                        ->getProcessStageTime("assignChunk:" + rbEvent->getChunk().getName())
                        ->noteStageStart("assignChunkByMove");
                }
            } else if (BalanceEventEngine::EventType::kOffloadEvent == rbEvent->getEventType()) {
                nextState = BalanceEventEngine::RebalanceEventState::kOffloaded;
            } else if (BalanceEventEngine::EventType::kRenameEvent == rbEvent->getEventType()) {
                nextState = BalanceEventEngine::RebalanceEventState::kStartAssign;

                if (!rbEvent->isInRecovery()) {
                    getGlobalServiceContext()->registerProcessStageTime(
                        "assignChunk:" + rbEvent->getChunk().getName());
                    getGlobalServiceContext()
                        ->getProcessStageTime("assignChunk:" + rbEvent->getChunk().getName())
                        ->noteStageStart("assignChunkByRename");
                }
            } else {
                index_err() << "[BEE] not support event type";
                invariant(false);
            }
            break;
        }

        case BalanceEventEngine::RebalanceEventState::kStartSplit: {
            if (statusCode == ErrorCodes::OK) {
                nextState = BalanceEventEngine::RebalanceEventState::kCommitSplit;
            } else {
                nextState = BalanceEventEngine::RebalanceEventState::kSplitFailed;
            }
            break;
        }

        case BalanceEventEngine::RebalanceEventState::kCommitSplit: {
            if (statusCode == ErrorCodes::OK) {
                nextState = BalanceEventEngine::RebalanceEventState::kAcknowledgeSplit;
            } else {
                nextState = BalanceEventEngine::RebalanceEventState::kSplitFailed;
            }
            break;
        }

        case BalanceEventEngine::RebalanceEventState::kAcknowledgeSplit: {
            nextState = BalanceEventEngine::RebalanceEventState::kSplitted;
            break;
        }

        case BalanceEventEngine::RebalanceEventState::kSplitted: {
            nextState = BalanceEventEngine::RebalanceEventState::kSplitted;
            break;
        }

        case BalanceEventEngine::RebalanceEventState::kSplitFailed: {
            nextState = BalanceEventEngine::RebalanceEventState::kSplitFailed;
            break;
        }

        default: {
            // should never happen, still add a log here
            index_err() << "[BEE] Failed to transition the state, because state("
                        << static_cast<int>(curState) << ") is invalid";
            break;
        }
    }

    rbEvent->transitState(nextState);
    if (nextState != curState) {
        rbEvent->setNeedPersist(true);
    }
    logBalanceEvent(rbEvent);

    return nextState;
}

Status BalanceEventEngine::extractDataFromCommandResponse(IRebalanceEvent* rbEvent,
                                                          const BSONObj& commandResponse) {

    if ((rbEvent->getEventType() == BalanceEventEngine::EventType::kSplitEvent) &&
        (rbEvent->getState() == BalanceEventEngine::RebalanceEventState::kStartSplit)) {
        // need to extract the split point from the command Response
        BSONElement splitPoint;
        auto status =
            bsonExtractTypedField(commandResponse, kSplitPointFieldName, Object, &splitPoint);
        if (!status.isOK()) {
            return {status.code(), str::stream() << "Invalid splitPoint data " << status.reason()};
        }

        if (splitPoint.Obj().isEmpty()) {
            return {ErrorCodes::BadValue, "The splitPoint data cannot be empty"};
        }
        SplitEvent* splitEvent = (SplitEvent*)rbEvent;
        splitEvent->setSplitPoint(splitPoint.Obj().getOwned());
    }

    return Status::OK();
}

bool BalanceEventEngine::errorHandler(IRebalanceEvent* rbEvent, Status result) {
    auto eventType = rbEvent->getEventType();
    auto curState = rbEvent->getState();
    bool needRetry = false;
    auto statusCode = result.code();

    auto resultPtr = rbEvent->getEventResultInfo();
    resultPtr->setErrorInfo(result.code(), result.reason());

    if (curState == BalanceEventEngine::RebalanceEventState::kStartAssign) {
        ShardIdent targetIdent;
        if (eventType == BalanceEventEngine::EventType::kAssignEvent) {
            AssignEvent* assignEvent = (AssignEvent*)rbEvent;
            targetIdent = assignEvent->getTarget();
        } else if (eventType == BalanceEventEngine::EventType::kMoveEvent) {
            MoveEvent* moveEvent = (MoveEvent*)rbEvent;
            targetIdent = moveEvent->getTarget();
        } else if (eventType == BalanceEventEngine::EventType::kRenameEvent) {
            RenameEvent* renameEvent = (RenameEvent*)rbEvent;
            targetIdent = renameEvent->getSource();
        } else {
            // should not come here
            invariant(false);
        }

        auto targetAlive =
            grid.catalogManager()->getShardServerManager()->isShardServerAlive(targetIdent);
        if (!targetAlive || statusCode == ErrorCodes::NeedChangeShard) {
            transitionState(rbEvent, statusCode);
            return false;
        }

        if (statusCode == ErrorCodes::NeedIsolateChunk) {
            // TODO add alert info about the isolation
            rbEvent->setIsolateChunkFlag(true);
            transitionState(rbEvent, ErrorCodes::OK);

            return false;
        }
        if( ErrorCodes::BadValue == statusCode){
            transitionState(rbEvent, statusCode);
            return false;
        }    

        // just retry assign command
        // TODO alert after tried some certain times
        if (rbEvent->exhaustRetries()) {
            rbEvent->setIsolateChunkFlag(true);
            transitionState(rbEvent, statusCode);
            return false;
        } else {
            needRetry = true;
        }
    } else if (curState == BalanceEventEngine::RebalanceEventState::kStartOffload) {
        ShardIdent sourceIdent;
        if (eventType == BalanceEventEngine::EventType::kOffloadEvent) {
            OffloadEvent* offloadEvent = (OffloadEvent*)rbEvent;
            sourceIdent = offloadEvent->getSource();
        } else if (eventType == BalanceEventEngine::EventType::kMoveEvent) {
            MoveEvent* moveEvent = (MoveEvent*)rbEvent;
            sourceIdent = moveEvent->getSource();
        } else if (eventType == BalanceEventEngine::EventType::kRenameEvent) {
            RenameEvent* renameEvent = (RenameEvent*)rbEvent;
            sourceIdent = renameEvent->getSource();
            // transitionState(rbEvent, statusCode);
            // return false;
        } else {
            // should not come here
            invariant(false);
        }

        auto sourceAlive =
            grid.catalogManager()->getShardServerManager()->isShardServerAlive(sourceIdent);
        if (!sourceAlive || statusCode == ErrorCodes::NamespaceNotFound) {
            transitionState(rbEvent, ErrorCodes::OK);
            return false;
        }

        // just retry offload command, if Cursor Exist on chunk, return error
        // TODO alert after tried some certain times
        if (rbEvent->exhaustRetries() || statusCode == ErrorCodes::CursorInUse ||
            statusCode == ErrorCodes::InvalidLength) {
            rbEvent->setIsolateChunkFlag(true);
            transitionState(rbEvent, statusCode);
            return false;
        } else {
            needRetry = true;
        }
    } else if (curState == BalanceEventEngine::RebalanceEventState::kStartSplit) {
        SplitEvent* splitEvent = (SplitEvent*)rbEvent;
        auto sourceIdent = splitEvent->getSource();

        auto sourceAlive =
            grid.catalogManager()->getShardServerManager()->isShardServerAlive(sourceIdent);
        if (!sourceAlive || statusCode == ErrorCodes::CannotSplit ||
            statusCode == ErrorCodes::NeedRollBackSplit || statusCode == ErrorCodes::BadValue
            || statusCode == ErrorCodes::InternalError) {
            transitionState(rbEvent, statusCode);
            if (statusCode == ErrorCodes::NeedRollBackSplit) {
                splitEvent->setRollbackFlag(true);
            }
            return false;
        }

        // just retry split command
        // TODO alert after tried some certain times and stop the retry
        if (rbEvent->exhaustRetries()) {
            transitionState(rbEvent, statusCode);
            return false;
        } else {
            needRetry = true;
        }
    } else if (curState == BalanceEventEngine::RebalanceEventState::kAcknowledgeSplit ||
               curState == BalanceEventEngine::RebalanceEventState::kSplitFailed) {
        transitionState(rbEvent, statusCode);
        return false;
    } else {
        invariant(false);
    }

    if (needRetry) {
        BalanceEventEngine* balanceEventEngine = grid.catalogManager()->getBalanceEventEngine();
        if (!balanceEventEngine->canExecuteEvent()) {
            transitionState(rbEvent, statusCode);
            balanceEventEngine->addEvent(rbEvent);
            balanceEventEngine->scheduleRebalanceEvent();
        } else {
            rbEvent->increaseRetryCnt();
            balanceEventEngine->addRetryingEvent(rbEvent);
        }
    }

    return true;
}

void BalanceEventEngine::handleRequestResponse(const RemoteCommandCallbackArgs& cbArgs,
                                               IRebalanceEvent* rbEvent) {
    if ((rbEvent->getEventType() == BalanceEventEngine::EventType::kAssignEvent) &&
        (!rbEvent->isInRecovery())) {
        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + rbEvent->getChunk().getName())
            ->noteStageStart("handleRequestResponse:State" +
                             std::to_string((int)rbEvent->getState()));
    }

    BalanceEventEngine* balanceEventEngine = grid.catalogManager()->getBalanceEventEngine();
    auto response = cbArgs.response;
    if (isErrorDueToConfigStepdown(response.status, !balanceEventEngine->canExecuteEvent())) {
        index_err() << "[BEE] event interrupted because event engine is stopping";
        transitionState(rbEvent, response.status.code());
        balanceEventEngine->addEvent(rbEvent);
        balanceEventEngine->scheduleRebalanceEvent();
        return;
    }

    // rbEvent->endTraceForAsyncStats(response.isOK());
    bool retried = false;
    if (!response.isOK()) {
        index_err() << "[BEE] response error from response status, errorcode("
                    << static_cast<int>(response.status.code()) << "), reason("
                    << response.status.reason() << "), chunk("
                    << rbEvent->getChunk().toString() + ")";
        retried = errorHandler(rbEvent, response.status);
        if (retried) {
            return;
        }
    } else {
        auto status = getStatusFromCommandResult(response.data);
        if (!status.isOK()) {
            index_err() << "[BEE] response error from response data, errorcode("
                        << static_cast<int>(status.code()) << "), reason(" << status.reason()
                        << "), chunk(" << rbEvent->getChunk().toString() + ")";
            retried = errorHandler(rbEvent, status);
            if (retried) {
                return;
            }
        } else {
            index_log() << "[BEE] response.isOK(), chunk " << rbEvent->getChunk().getID();
            auto status = extractDataFromCommandResponse(rbEvent, response.data);
            if (!status.isOK()) {
                index_err() << "[BEE] extractDataFromCommandResponse failed due to "
                            << status.reason() << ", chunk(" << rbEvent->getChunk().toString()
                            << ")";
            }
            transitionState(rbEvent, status.code());
        }
    }
    if ((rbEvent->getEventType() == BalanceEventEngine::EventType::kAssignEvent) &&
        (!rbEvent->isInRecovery())) {
        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + rbEvent->getChunk().getName())
            ->noteStageStart("handleRequestResponse:addEvent:State" +
                             std::to_string((int)rbEvent->getState()));
    }

    balanceEventEngine->addEvent(rbEvent);
    if ((rbEvent->getEventType() == BalanceEventEngine::EventType::kAssignEvent) &&
        (!rbEvent->isInRecovery())) {
        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + rbEvent->getChunk().getName())
            ->noteStageStart("handleRequestResponse:scheduleRebalanceEvent:State" +
                             std::to_string((int)rbEvent->getState()));
    }
    balanceEventEngine->scheduleRebalanceEvent();
}

StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_sendBalanceCommand(
    OperationContext* txn, IRebalanceEvent* event, bool isRetried) {

    IRebalanceEvent::BalanceCmdInfo* balanceCmdInfo = event->getBalanceCmdInfo();
    executor::TaskExecutor* const executor = Grid::get(txn)->getExecutorPool()->getFixedExecutor();
    if (!executor) {
        index_err() << "[BEE] getFixedExecutor return null";
        return BalanceEventEngine::NextAction::kActionError;
    }

    const RemoteCommandRequest request(balanceCmdInfo->target,
                                       balanceCmdInfo->dbname,
                                       balanceCmdInfo->cmdObj,
                                       txn,
                                       balanceCmdInfo->timeout);

    const RemoteCommandCallbackFn callback =
        stdx::bind(&BalanceEventEngine::handleRequestResponse, stdx::placeholders::_1, event);
    StatusWith<CallbackHandle> callbackHandleWithStatus =
        executor->scheduleRemoteCommand(request, callback);

    if (!callbackHandleWithStatus.isOK()) {
        // event->endTraceForAsyncStats(false);
        index_err() << "[BEE] failed to schedule command to " << balanceCmdInfo->target.toString()
                    << ": " << redact(callbackHandleWithStatus.getStatus());
        BalanceEventEngine* balanceEventEngine =
            Grid::get(txn)->catalogManager()->getBalanceEventEngine();
        if (event->exhaustRetries()) {
            transitionState(event, callbackHandleWithStatus.getStatus().code());
            balanceEventEngine->addEvent(event);
            balanceEventEngine->scheduleRebalanceEvent();
        } else {
            // add into retry list
            event->increaseRetryCnt();
            if (isRetried) {
                balanceEventEngine->addRetryingEvent_inlock(event);
            } else {
                balanceEventEngine->addRetryingEvent(event);
            }
        }
    } else {
        index_log() << "[BEE] succeed to send out async command to "
                    << event->getChunk().getShard().toString() << "("
                    << balanceCmdInfo->target.toString() << "), chunk(" << event->getChunk().getID()
                    << ")";
    }

    return BalanceEventEngine::NextAction::kActionWait;
}

StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_startAssignHandler(
    IRebalanceEvent* event) {
    if (!event->isInRecovery()) {
        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + event->getChunk().getName())
            ->noteStageStart("startAssignHandler");
    }

    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    ChunkType chunkType;
    if (!event->getNewChunkFlag()) {
        auto validateStatus = _validateEventChunk(txn, event, chunkType);
        if (!validateStatus.isOK()) {
            index_err() << "[BEE] failed to validate event chunk "
                        << event->getChunk().getID() + ", due to " << validateStatus.reason();
            event->setChunkChangedFlag(true);
            return validateStatus;
        }

        if (chunkType.getStatus() == ChunkType::ChunkStatus::kDisabled) {
            index_err() << "[BEE] failed to assign chunk "
                        << event->getChunk().getID() + ", due to it is disabled";
            return Status(ErrorCodes::ChunkDisbaled, "fail to assign because chunk is disabled");
        }

        /*if (chunkType.getStatus() == ChunkType::ChunkStatus::kAssigned) {
            auto sourceShardName = chunkType.getShard();
            auto sourceIdentity = chunkType.getProcessIdentity();
            ShardIdent sourceShardIdent(sourceShardName, sourceIdentity);

            bool alive = grid.catalogManager()->getShardServerManager()->isShardServerAlive(
                sourceShardIdent);
            if (alive) {
                index_log() << "[BEE] chunk " << event->getChunk().getID()
                            << " is already in Assigned state";
                return BalanceEventEngine::NextAction::kActionFinish;
            }
        }*/

        event->refreshChunk(chunkType);
    } else {
        chunkType = event->getChunk();
    }

    if (event->getEventType() == BalanceEventEngine::EventType::kRenameEvent) {
        RenameEvent* renameEvent = (RenameEvent*)event;
        chunkType.setNS(renameEvent->getTargetCollection());
    }

    if (!event->isInRecovery() &&
        event->getEventType() != BalanceEventEngine::EventType::kMoveEvent &&
        event->getEventType() != BalanceEventEngine::EventType::kRenameEvent) {
        auto status = writeEventDocument(txn, event);
        if (!status.isOK()) {
            event->getEventResultInfo()->setErrorInfo(status.code(), status.reason());
            index_err() << "[BEE] failed to write event document for chunk "
                        << event->getChunk().getID() << ", due to " << status.reason();
            return status;
        }
        event->setProcessed(true);
    }

    // get the collection info
    if (!event->isInRecovery()) {
        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + event->getChunk().getName())
            ->noteStageStart("getCollection");
    }
    auto collstatus = grid.catalogClient(txn)->getCollection(txn, chunkType.getNS());
    if (!collstatus.isOK()) {
        index_err() << "[BEE] failed to get collection: " << chunkType.getNS() << ", for chunk "
                    << event->getChunk().getID();
        return collstatus.getStatus();
    }
    const auto collOpTimePair = collstatus.getValue();
    CollectionType coll = collOpTimePair.value;

    ShardId newShardId;
    std::string newShardIdent;
    if (BalanceEventEngine::EventType::kMoveEvent == event->getEventType()) {
        MoveEvent* moveEvent = (MoveEvent*)event;
        newShardId = moveEvent->getTarget().getShardId();
        newShardIdent = moveEvent->getTarget().getProcessIdentity();
        auto balanceCmdInfo = event->getBalanceCmdInfo();
        if (balanceCmdInfo) {
            event->deleteBalanceCmdInfo();
        }
    } else if (BalanceEventEngine::EventType::kAssignEvent == event->getEventType()) {
        AssignEvent* assignEvent = (AssignEvent*)event;
        newShardId = assignEvent->getTarget().getShardId();
        newShardIdent = assignEvent->getTarget().getProcessIdentity();
    } else if (BalanceEventEngine::EventType::kRenameEvent == event->getEventType()) {
        RenameEvent* renameEvent = (RenameEvent*)event;
        newShardId = renameEvent->getSource().getShardId();
        newShardIdent = renameEvent->getSource().getProcessIdentity();
        auto balanceCmdInfo = event->getBalanceCmdInfo();
        if (balanceCmdInfo) {
            event->deleteBalanceCmdInfo();
        }
    }
    chunkType.setShard(newShardId);
    chunkType.setProcessIdentity(newShardIdent);

    // get shard host
    const auto ShardStatus = grid.shardRegistry()->getShard(txn, newShardId);
    if (!ShardStatus.isOK()) {
        index_err() << "[BEE] failed to get shard: " << newShardId
                    << " , for chunk: " << chunkType.getID();
        return ShardStatus.getStatus();
    }

    const auto shard = ShardStatus.getValue();
    const auto hostCS = shard->getConnString();
    invariant(hostCS.getServers().size() == 1);
    auto host = hostCS.getServers().front();

    // create root folder: ploglist for OBSindex and file for Maas
    if (!event->getNewChunkFlag()) {
        index_log() << "processEvent [BEE] _startAssignHandler for chunk(" << chunkType.getID()
                    << ") before create, root folder is " << chunkType.getRootFolder();
    }

    if (!event->isInRecovery()) {
        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + event->getChunk().getName())
            ->noteStageStart("createRootFolder");
    }
    // update chunk status if this is a new chunk
    if (event->getNewChunkFlag()) {
        std::string chunkRootFolder;
        Status status = grid.catalogManager()->createRootFolder(
            txn, coll.getIdent(), chunkType.getID(), chunkRootFolder);
        if (!status.isOK()) {
            index_err() << "[BEE] failed to create root folder for chunkId: " << chunkType.getID();
            return status;
        }
        chunkType.setRootFolder(chunkRootFolder);
        index_LOG(0) << "[BEE] _startAssignHandler for chunk(" << chunkType.getID()
                     << ") after create, root folder is " << chunkRootFolder;
        chunkType.setStatus(ChunkType::ChunkStatus::kOffloaded);
    } else {
        chunkType.setStatus(ChunkType::ChunkStatus::kOffloaded);
    }

    if (!event->isInRecovery()) {
        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + event->getChunk().getName())
            ->noteStageStart("newMaxChunkVersion");
    }
    auto maxVersionStatus = grid.catalogManager()->newMaxChunkVersion(txn, chunkType.getNS());
    if (!maxVersionStatus.isOK()) {
        index_err() << "[BEE] _startAssignHandler failed to newMaxChunkVersion";
        return maxVersionStatus.getStatus();
    }
    ChunkVersion currentMaxVersion(maxVersionStatus.getValue(), chunkType.getVersion().epoch());
    chunkType.setVersion(currentMaxVersion);

    auto updateChunkStatus =
        grid.catalogClient(txn)->updateConfigDocument(txn,
                                                      ChunkType::ConfigNS,
                                                      BSON(ChunkType::name(chunkType.getID())),
                                                      chunkType.toBSON(),
                                                      true,
                                                      ShardingCatalogClient::kMajorityWriteConcern);
    if (!updateChunkStatus.isOK()) {
        index_err() << "[BEE] failed to update chunk(" << chunkType.getID() << ")";
        return updateChunkStatus.getStatus();
    }

    event->refreshChunk(chunkType);
    BSONObjBuilder builder;
    AssignChunkRequest::appendAsCommand(
        &builder, chunkType, coll, event->getNewChunkFlag(), newShardId.toString(), newShardIdent);

    std::string dbName = "admin";
    IRebalanceEvent::BalanceCmdInfo* balanceCmdInfo = event->getBalanceCmdInfo();
    if (balanceCmdInfo == nullptr) {
        balanceCmdInfo = new (std::nothrow) IRebalanceEvent::BalanceCmdInfo(
            dbName,
            host,
            builder.obj(),
            Milliseconds(1000 * 300 /*BASE_GetParaS64Val(E_SPA_CONFIG_LB_CMD_TIMEOUT)*/));
        if (balanceCmdInfo == nullptr) {
            return Status(ErrorCodes::ExceededMemoryLimit, "failed to allocate BalanceCmdInfo.");
        }
        event->setBalanceCmdInfo(balanceCmdInfo);
    }

    if (!event->isInRecovery()) {
        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + event->getChunk().getName())
            ->noteStageStart("startAssignHandler:sendBalanceCommand:newShard:" +
                             event->getChunk().getShard().toString());
    }
    return _sendBalanceCommand(txn, event, false);
}


// TODO, will implement in the near future
StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_killTargetSSHandler(
    IRebalanceEvent* event) {
    logBalanceEvent(event);
    transitionState(event, ErrorCodes::OK);
    return BalanceEventEngine::NextAction::kActionContinue;
}

StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_assignFailedHandler(
    IRebalanceEvent* event) {
    if ((event->getEventType() == BalanceEventEngine::EventType::kAssignEvent) &&
        (!event->isInRecovery())) {
        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + event->getChunk().getName())
            ->noteStageStart("assignFailedHandler");
    }
    logBalanceEvent(event);
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    if (event->needIsolateChunk() && !event->isInterrupted()) {
        auto chunk = event->getChunk();
        BSONObjBuilder chunkstatusafterbuilder;
        chunkstatusafterbuilder.append(
            ChunkType::status(),
            static_cast<std::underlying_type<ChunkType::ChunkStatus>::type>(
                ChunkType::ChunkStatus::kDisabled));

        auto updatestatus = grid.catalogClient(txn)->updateConfigDocument(
            txn,
            ChunkType::ConfigNS,
            BSON(ChunkType::name(chunk.getID())),
            BSON("$set" << chunkstatusafterbuilder.obj()),
            false,
            ShardingCatalogClient::kMajorityWriteConcern);
        // TODO: if fail, retry
        if (!updatestatus.isOK()) {
            index_err() << "[BEE] update chunk status fail for chunk " << event->getChunk().getID();
            return updatestatus.getStatus();
        }

        auto chunkType = event->getChunk();
        chunkType.setStatus(ChunkType::ChunkStatus::kDisabled);

        event->refreshChunk(chunkType);
    }


    Grid::get(txn)->catalogClient(txn)->logChange(
        txn,
        "assignFailed",
        "",
        BSON(RebalanceEventType::eventData() << event->dataToBSON()),
        kMajorityWriteConcern);

    index_err() << "[BEE] failed to assign chunk(" << event->getChunk() << ")";

    return BalanceEventEngine::NextAction::kActionError;
}

StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_assignedHandler(
    IRebalanceEvent* event) {
    if (!event->isInRecovery()) {
        getGlobalServiceContext()
            ->getProcessStageTime("assignChunk:" + event->getChunk().getName())
            ->noteStageStart("assignFailedHandler");
    }
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    auto chunk = event->getChunk();

    // mark the chunk state as kAssigned, and modify the "shard"
    BSONObjBuilder chunkstatusafterbuilder;
    auto shardName = event->getChunk().getShard();
    auto processIdentity = event->getChunk().getProcessIdentity();
    chunkstatusafterbuilder.append(ChunkType::shard(), shardName);
    chunkstatusafterbuilder.append(ChunkType::processIdentity(), processIdentity);
    if (event->needIsolateChunk()) {
        chunkstatusafterbuilder.append(
            ChunkType::status(),
            static_cast<std::underlying_type<ChunkType::ChunkStatus>::type>(
                ChunkType::ChunkStatus::kDisabled));
    } else {
        chunkstatusafterbuilder.append(
            ChunkType::status(),
            static_cast<std::underlying_type<ChunkType::ChunkStatus>::type>(
                ChunkType::ChunkStatus::kAssigned));
    }

    bool hasError = false;
    do {
    if (!event->isInterrupted()) {
        auto updatestatus = grid.catalogClient(txn)->updateConfigDocument(
            txn,
            ChunkType::ConfigNS,
            BSON(ChunkType::name(chunk.getID())),
            BSON("$set" << chunkstatusafterbuilder.obj()),
            false,
            ShardingCatalogClient::kMajorityWriteConcern);
        // TODO: if fail, retry
        if (!updatestatus.isOK()) {
            index_err() << "[BEE] fail to update chunk status for chunk "
                        << event->getChunk().getID();
            //return updatestatus.getStatus();
	    hasError = true;
	    break;
        }

        ChunkType chunkType;
        chunkType.setNS(chunk.getNS());
        chunkType.setMin(chunk.getMin());
        chunkType.setMax(chunk.getMax());
        chunkType.setShard(chunk.getShard());
        chunkType.setProcessIdentity(chunk.getProcessIdentity());
        chunkType.setVersion(chunk.getVersion());
        chunkType.setRootFolder(chunk.getRootFolder());
        chunkType.setName(chunk.getID());
        chunkType.setStatus(ChunkType::ChunkStatus::kAssigned);

        event->refreshChunk(chunkType);
        index_log() << "[BEE] finish assign chunk: " << event->getChunk().getID();
    }
    } while(0);

    if (hasError) {
        return BalanceEventEngine::NextAction::kActionContinue;
    }

    if (event->needIsolateChunk()) {
        index_warning() << "[BEE] chunk " << event->getChunk().getID() << " is isolated!";
    } else {
        index_log() << "[BEE] finish assigning chunk: " << event->getChunk().getID();
    }
    return BalanceEventEngine::NextAction::kActionFinish;
}

StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_startOffloadHandler(
    IRebalanceEvent* event) {
    OperationContext* txn = cc().getOperationContext();

    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    ChunkType chunkType;
    auto validateStatus = _validateEventChunk(txn, event, chunkType);
    if (!validateStatus.isOK()) {
        index_err() << "[BEE] failed to validate event chunk for " << event->getChunk().getID()
                    << ", due to " << validateStatus.reason();
        event->setChunkChangedFlag(true);
        return validateStatus;
    }
    /*
        if (chunkType.getStatus() == ChunkType::ChunkStatus::kDisabled) {
            index_err() << "[BEE] failed to offload chunk for " << event->getChunk().getID()
                        << ", due to it is disabled";
            return Status(ErrorCodes::ChunkDisbaled, "fail to offload because chunk is disabled");
        }
    */
    if (chunkType.getStatus() == ChunkType::ChunkStatus::kOffloaded) {
        if (event->getEventType() == BalanceEventEngine::EventType::kOffloadEvent) {
            // return BalanceEventEngine::NextAction::kActionFinish;
            // DO Nothing ,will also send command to shard
        } else if (event->getEventType() == BalanceEventEngine::EventType::kMoveEvent ||
                   event->getEventType() == BalanceEventEngine::EventType::kRenameEvent) {
            transitionState(event, ErrorCodes::OK);
            return BalanceEventEngine::NextAction::kActionContinue;
        } else {
            index_err() << "[BEE] invalid event type";
            return Status(ErrorCodes::BadValue, "invalid event type");
        }
    }
    event->refreshChunk(chunkType);

    if (!event->isInRecovery()) {
        auto status = writeEventDocument(txn, event);
        if (!status.isOK()) {
            event->getEventResultInfo()->setErrorInfo(status.code(), status.reason());
            index_err() << "[BEE] failed to write event document for chunk "
                        << event->getChunk().getID() << ", due to " << status.reason();
            return status;
        }
        event->setProcessed(true);
    }

    const auto ShardStatus = grid.shardRegistry()->getShard(txn, chunkType.getShard());
    if (!ShardStatus.isOK()) {
        index_err() << "[BEE] fail to get shard for chunk " << event->getChunk().getID();
        return ShardStatus.getStatus();
    }
    const auto shard = ShardStatus.getValue();
    const auto hostCS = shard->getConnString();
    invariant(hostCS.getServers().size() == 1);
    auto host = hostCS.getServers().front();

    // Send out Offload command
    BSONObjBuilder builder;
    OffloadChunkRequest::appendAsCommand(&builder, chunkType);
    if (event->getEventType() == BalanceEventEngine::EventType::kMoveEvent) {
        builder.append("moveChunk", true);
    } else if (event->getEventType() == BalanceEventEngine::EventType::kRenameEvent) {
        builder.append("renameCollection", true);
        RenameEvent* renameEvent = (RenameEvent*)event;
        builder.append("targetCollection", renameEvent->getTargetCollection());
        builder.append("dropTarget", renameEvent->getDropTarget());
        builder.append("stayTemp", renameEvent->getStayTemp());
    }
    std::string dbName = "admin";
    IRebalanceEvent::BalanceCmdInfo* balanceCmdInfo = event->getBalanceCmdInfo();
    if (balanceCmdInfo == nullptr) {
        balanceCmdInfo = new (std::nothrow) IRebalanceEvent::BalanceCmdInfo(
            dbName,
            host,
            builder.obj(),
            Milliseconds(1000 * 300 /*BASE_GetParaS64Val(E_SPA_CONFIG_LB_CMD_TIMEOUT)*/));
        if (balanceCmdInfo == nullptr) {
            return Status(ErrorCodes::ExceededMemoryLimit, "failed to allocate BalanceCmdInfo.");
        }
        event->setBalanceCmdInfo(balanceCmdInfo);
    }

    return _sendBalanceCommand(txn, event, false);
}

// TODO, will implement in the near future
StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_killSourceSSHandler(
    IRebalanceEvent* event) {
    logBalanceEvent(event);
    (void)transitionState(event, ErrorCodes::OK);
    return BalanceEventEngine::NextAction::kActionContinue;
}

StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_offloadFailedHandler(
    IRebalanceEvent* event) {
    return BalanceEventEngine::NextAction::kActionError;
}

StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_offloadedHandler(
    IRebalanceEvent* event) {
    logBalanceEvent(event);
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    bool hasError = false;
    auto chunk = event->getChunk();
    ChunkType chunkType;

    do{
    BSONObjBuilder chunkstatusafterbuilder;
    if (event->needIsolateChunk()) {
        chunkstatusafterbuilder.append(
            ChunkType::status(),
            static_cast<std::underlying_type<ChunkType::ChunkStatus>::type>(
                ChunkType::ChunkStatus::kDisabled));
    } else {
        chunkstatusafterbuilder.append(
            ChunkType::status(),
            static_cast<std::underlying_type<ChunkType::ChunkStatus>::type>(
                ChunkType::ChunkStatus::kOffloaded));
    }
    auto status =
        grid.catalogClient(txn)->updateConfigDocument(txn,
                                                      ChunkType::ConfigNS,
                                                      BSON(ChunkType::name(chunk.getID())),
                                                      BSON("$set" << chunkstatusafterbuilder.obj()),
                                                      false,
                                                      ShardingCatalogClient::kMajorityWriteConcern);
    if (!status.isOK()) {
        index_err() << "[BEE] fail to update state of chunk(" << chunk.getID() << ")";
        //return status.getStatus();
	hasError = true;
	break;
    }

    //ChunkType chunkType;
    if (BalanceEventEngine::EventType::kRenameEvent == event->getEventType()) {
        // update config.collection and config.chunks
        RenameEvent* renameEvent = (RenameEvent*)event;
        BSONObjBuilder chunkUpdateBuilder;
        chunkUpdateBuilder.append(ChunkType::ns(), renameEvent->getTargetCollection());
        BSONObjBuilder collectionUpdateBuilder;
        collectionUpdateBuilder.append(CollectionType::fullNs(),
                                       renameEvent->getTargetCollection());
        auto status = grid.catalogClient(txn)->updateConfigDocument(
            txn,
            ChunkType::ConfigNS,
            BSON(ChunkType::name(chunk.getID())),
            BSON("$set" << chunkUpdateBuilder.obj()),
            false,
            ShardingCatalogClient::kMajorityWriteConcern);
        if (!status.isOK()) {
            index_err() << "[BEE] fail to update chunk(" << chunk.getID() << ") ns from "
                        << chunk.getNS() << " to " << renameEvent->getTargetCollection();
            //return status.getStatus();
	    hasError = true;
	    break;
        }

        CollectionType coll;
        auto collStatus = Grid::get(txn)->catalogClient(txn)->getCollection(txn, chunk.getNS());
        if (!collStatus.isOK()) {
            index_err() << "[BEE] failed to get collection: " << chunk.getNS() << " for rename cmd";
            //return collStatus.getStatus();
	    hasError = true;
	    break;
        }
        coll = collStatus.getValue().value;
        BSONArrayBuilder newIndexesBuilder;
        for (const mongo::BSONElement& elm : coll.getIndex()) {
            BSONObj index = elm.Obj();
            BSONObjBuilder indexBuilder;
            for (const BSONElement& item : index) {
                if (item.fieldNameStringData() == "ns") {
                    indexBuilder.append("ns", renameEvent->getTargetCollection());
                } else {
                    indexBuilder.append(item);
                }
            }

            newIndexesBuilder.append(indexBuilder.done());
        }

        coll.setIndex(newIndexesBuilder.arr());
        NamespaceString ns(renameEvent->getTargetCollection());
        coll.setNs(ns);
        if (!renameEvent->getStayTemp()) {
            BSONObj options = coll.getOptions();
            coll.setOptions(options.removeField("temp"));
        }
        // remove source collection metadata
        status = grid.catalogClient(txn)->removeConfigDocuments(
            txn,
            CollectionType::ConfigNS,
            BSON(CollectionType::fullNs(chunk.getNS())),
            ShardingCatalogClient::kMajorityWriteConcern);
        if (!status.isOK()) {
            index_err() << "[BEE] fail to remove collection(" << chunk.getNS() << " for rename cmd";
            //return status.getStatus();
	    hasError = true;
	    break;
        }
        // insert target collection metadata
        status = grid.catalogClient(txn)->updateConfigDocument(
            txn,
            CollectionType::ConfigNS,
            BSON(CollectionType::fullNs(coll.getNs().ns())),
            coll.toBSON(),
            true,
            kMajorityWriteConcern);
        if (!status.isOK()) {
            index_err() << "[BEE] fail to create collection(" << renameEvent->getTargetCollection()
                        << " for rename cmd";
            //return status.getStatus();
	    hasError = true;
	    break;
        }
    }
    } while(0);

    if (hasError) {
        return BalanceEventEngine::NextAction::kActionContinue;
    }

    chunkType.setNS(chunk.getNS());
    chunkType.setMin(chunk.getMin());
    chunkType.setMax(chunk.getMax());
    chunkType.setShard(chunk.getShard());
    chunkType.setProcessIdentity(chunk.getProcessIdentity());
    chunkType.setVersion(chunk.getVersion());
    chunkType.setRootFolder(chunk.getRootFolder());
    chunkType.setName(chunk.getID());
    if (event->needIsolateChunk()) {
        chunkType.setStatus(ChunkType::ChunkStatus::kDisabled);
        event->refreshChunk(chunkType);
        index_err() << "[BEE] chunk " << chunkType.getID() << " has been disabled";
        return BalanceEventEngine::NextAction::kActionError;
    } else {
        chunkType.setStatus(ChunkType::ChunkStatus::kOffloaded);
        event->refreshChunk(chunkType);
    }

    index_log() << "[BEE] finish offload chunk " << chunkType.getID();
    if (BalanceEventEngine::EventType::kMoveEvent == event->getEventType() ||
        BalanceEventEngine::EventType::kRenameEvent == event->getEventType()) {
        (void)transitionState(event, ErrorCodes::OK);
        return BalanceEventEngine::NextAction::kActionContinue;
    }

    return BalanceEventEngine::NextAction::kActionFinish;
}

StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_startSplitHandler(
    IRebalanceEvent* event) {
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    ChunkType chunkType;
    auto validateStatus = _validateEventChunk(txn, event, chunkType);
    if (!validateStatus.isOK()) {
        index_err() << "[BEE] failed to validate event chunk for " << event->getChunk().getID()
                    << ", due to " << validateStatus.reason();
        event->setChunkChangedFlag(true);
        return validateStatus;
    }

    if (!chunkType.isAssigned()) {
        index_err() << "[BEE] cannot split chunk " << event->getChunk().getID()
                    << ", because its status is not assigned";
        return Status(ErrorCodes::ChunkNotAssigned, "chunk is not in assigned status");
    }
    event->refreshChunk(chunkType);

    SplitEvent* splitEvent = (SplitEvent*)event;
    auto splitPoint = splitEvent->getSplitPoint();
    if (!splitPoint.isEmpty()) {
        // there is no need to check shardkey here,
        // otherwise shardkey like "a.b" will report error
        /*const NamespaceString nss(chunkType.getNS());
        auto scopedChunkManager = uassertStatusOK(ScopedChunkManager::getExisting(txn, nss));
        ChunkManager* const chunkManager = scopedChunkManager.cm();

        BSONObj shardkey = chunkManager->getShardKeyPattern().extractShardKeyFromDoc(splitPoint);
        if (shardkey.isEmpty()) {
            log() << "[BalanceEventEngine] splitPoint is not a shard key";
            return Status(ErrorCodes::BadValue, "splitPoint is not a shard key");
        }*/

        if (SimpleBSONObjComparator::kInstance.evaluate(splitPoint < chunkType.getMin()) ||
            SimpleBSONObjComparator::kInstance.evaluate(splitPoint >= chunkType.getMax())) {
            index_err() << "[BEE] chunk " << event->getChunk().getID()
                        << " split failed, splitPoint is invalid";
            return Status(ErrorCodes::BadValue, "splitPoint is invalid");
        }

        if (SimpleBSONObjComparator::kInstance.evaluate(splitPoint == chunkType.getMin())) {
            return BalanceEventEngine::NextAction::kActionFinish;
        }
    }

    if (!event->isInRecovery()) {
        auto status = writeEventDocument(txn, event);
        if (!status.isOK()) {
            event->getEventResultInfo()->setErrorInfo(status.code(), status.reason());
            index_err() << "[BEE] failed to write event document for chunk "
                        << event->getChunk().getID() << ", due to " << status.reason();
            return status;
        }
        event->setProcessed(true);
    }

    // get the collection info
    auto collstatus = grid.catalogClient(txn)->getCollection(txn, chunkType.getNS());
    if (!collstatus.isOK()) {
        index_err() << "[BEE] failed to get collection: " << chunkType.getNS() << " for chunk "
                    << event->getChunk().getID();
        return collstatus.getStatus();
    }
    const auto collOpTimePair = collstatus.getValue();
    CollectionType coll = collOpTimePair.value;

    ShardId shardId = chunkType.getShard();
    // get shard host
    const auto ShardStatus = grid.shardRegistry()->getShard(txn, shardId);
    if (!ShardStatus.isOK()) {
        index_err() << "[BEE] failed to get shard:" << shardId << " for chunk "
                    << event->getChunk().getID();
        return ShardStatus.getStatus();
    }
    const auto shard = ShardStatus.getValue();
    const auto hostCS = shard->getConnString();
    invariant(hostCS.getServers().size() == 1);
    auto host = hostCS.getServers().front();

    auto childChunk = splitEvent->getChildChunk();

    BSONObjBuilder builder;
    SplitChunkReq::appendAsCommand(&builder,
                                   chunkType,
                                   coll,
                                   childChunk.getRootFolder(),
                                   childChunk.getID(),
                                   splitEvent->getSplitPoint());
    std::string dbName = "admin";
    IRebalanceEvent::BalanceCmdInfo* balanceCmdInfo = event->getBalanceCmdInfo();
    if (balanceCmdInfo == nullptr) {
        balanceCmdInfo = new (std::nothrow) IRebalanceEvent::BalanceCmdInfo(
            dbName,
            host,
            builder.obj(),
            Milliseconds(1000 * 300 /* BASE_GetParaS64Val(E_SPA_CONFIG_LB_CMD_TIMEOUT)*/));
        if (balanceCmdInfo == nullptr) {
            return Status(ErrorCodes::ExceededMemoryLimit, "failed to allocate BalanceCmdInfo.");
        }
        event->setBalanceCmdInfo(balanceCmdInfo);
    }
    return _sendBalanceCommand(txn, event, false);
}

Status BalanceEventEngine::_sendAckCommand(IRebalanceEvent* event, bool confirm) {
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    executor::TaskExecutor* const executor = Grid::get(txn)->getExecutorPool()->getFixedExecutor();
    if (!executor) {
        index_err() << "[BEE] getFixedExecutor failed.";
        return Status(ErrorCodes::InternalError, "getFixedExecutor failed.");
    }

    auto chunk = event->getChunk();
    // get the collection info
    auto collstatus = grid.catalogClient(txn)->getCollection(txn, chunk.getNS());
    if (!collstatus.isOK()) {
        index_err() << "[BEE] failed to get collection: " << chunk.getNS() << " for chunk "
                    << event->getChunk().getID();
        return collstatus.getStatus();
    }
    const auto collOpTimePair = collstatus.getValue();
    CollectionType coll = collOpTimePair.value;

    ShardId shardId = chunk.getShard();
    // get shard host
    const auto ShardStatus = grid.shardRegistry()->getShard(txn, shardId);
    if (!ShardStatus.isOK()) {
        index_err() << "[BEE] failed to get shard:" << shardId << " for chunk "
                    << event->getChunk().getID();
        return ShardStatus.getStatus();
    }
    const auto shard = ShardStatus.getValue();
    const auto hostCS = shard->getConnString();
    invariant(hostCS.getServers().size() == 1);
    auto host = hostCS.getServers().front();

    BSONObjBuilder builder;
    ConfirmSplitRequest::appendAsCommand(&builder, chunk, coll, confirm);
    const RemoteCommandRequest request(
        host,
        "admin",
        builder.obj(),
        txn,
        Milliseconds(1000 * 300 /*BASE_GetParaS64Val(E_SPA_CONFIG_LB_CMD_TIMEOUT)*/));

    const RemoteCommandCallbackFn callback =
        stdx::bind(&BalanceEventEngine::handleRequestResponse, stdx::placeholders::_1, event);

    StatusWith<CallbackHandle> callbackHandleWithStatus =
        executor->scheduleRemoteCommand(request, callback);

    if (callbackHandleWithStatus.isOK()) {
        logBalanceEvent(event);
        return Status::OK();
    }

    return callbackHandleWithStatus.getStatus();
}

StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_splitFailedHandler(
    IRebalanceEvent* event) {
    logBalanceEvent(event);
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    if (event->getPrevState() != BalanceEventEngine::RebalanceEventState::kSplitFailed) {
        Grid::get(txn)->catalogClient(txn)->logChange(
            txn,
            "splitFailed",
            "",
            BSON(RebalanceEventType::eventData() << event->dataToBSON()),
            kMajorityWriteConcern);

        index_err() << "[BEE] failed to split chunk(" << event->getChunk().getID() << ")";
    }

    SplitEvent* splitEvent = (SplitEvent*)event;
    if (splitEvent->getRollbackFlag()) {
        splitEvent->setRollbackFlag(false);
        auto sendAckStatus = _sendAckCommand(event, false);
        if (!sendAckStatus.isOK()) {
            // TODO need to retry ack command
            index_err() << "[BEE] failed to send rollback command of split for "
                        << event->getChunk().getID();
        } else {
            return BalanceEventEngine::NextAction::kActionWait;
        }
    }

    return BalanceEventEngine::NextAction::kActionError;
}

void BalanceEventEngine::_addUpdateChunkObject(OperationContext* txn,
                                               BSONArrayBuilder& updates,
                                               ChunkType& chunk,
                                               bool parent) {

    // build an update operation against the chunks collection of the config database
    // with upsert true
    BSONObjBuilder op;
    op.append("op", "u");
    op.appendBool("b", true);
    op.append("ns", ChunkType::ConfigNS);

    // add the modified (new) chunk information as the update object
    BSONObjBuilder n(op.subobjStart("o"));
    n.append(ChunkType::name(), chunk.getID());
    n.append(ChunkType::ns(), chunk.getNS());
    n.append(ChunkType::min(), chunk.getMin());
    n.append(ChunkType::max(), chunk.getMax());
    n.append(ChunkType::shard(), chunk.getShard().toString());
    n.append(ChunkType::processIdentity(), chunk.getProcessIdentity());
    ChunkVersion currentMaxVersion(chunk.getVersion());
    currentMaxVersion.appendForChunk(&n);
    n.append(ChunkType::rootFolder(), chunk.getRelativeRootFolder());

    n.append(ChunkType::status(), (int)(chunk.getStatus()));
    n.done();

    // add the chunk's _id as the query part of the update statement
    BSONObjBuilder q(op.subobjStart("o2"));
    q.append(ChunkType::name(), chunk.getID());
    q.done();

    auto obj = op.obj();
    updates.append(obj);
}

StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_commitSplitHandler(
    IRebalanceEvent* event) {
    logBalanceEvent(event);
    OperationContext* txn = cc().getOperationContext();
    ServiceContext::UniqueOperationContext txnPtr;
    if (!txn) {
        txnPtr = cc().makeOperationContext();
        txn = txnPtr.get();
    }

    SplitEvent* splitEvent = (SplitEvent*)event;
    auto childChunk = splitEvent->getChildChunk();
    auto chunk = event->getChunk();
    auto maxKey = chunk.getMax();
    auto childMinKey = childChunk.getMin();
    if (event->isInRecovery() &&
        SimpleBSONObjComparator::kInstance.evaluate(maxKey == childMinKey)) {

        if (childChunk.getShard() == chunk.getShard() &&
            childChunk.getProcessIdentity() == chunk.getProcessIdentity()) {
            index_LOG(2) << "[BEE] recovered splitevent for chunk("
                         << chunk.getID() + "), does not assign child chunk("
                         << childChunk.getID() + ")";
            auto childShardId = childChunk.getShard();
            Balancer::get(txn)->assignChunk(txn, childChunk, false, false, childShardId,true);
        }

        transitionState(event, ErrorCodes::OK);
        return BalanceEventEngine::NextAction::kActionContinue;
    }

    auto maxVersionStatus = grid.catalogManager()->newMaxChunkVersion(txn, chunk.getNS());
    if (!maxVersionStatus.isOK()) {
        return maxVersionStatus.getStatus();
    }
    ChunkVersion currentMaxVersion(maxVersionStatus.getValue(), chunk.getVersion().epoch());

    auto splitPoint = splitEvent->getSplitPoint();
    // TODO move this to handleRequestProcess verify the split point is within the chunk
    ChunkRange range = ChunkRange(chunk.getMin(), chunk.getMax());
    if (splitPoint.woCompare(chunk.getMax()) != 0 && !range.containsKey(splitPoint)) {
        return {ErrorCodes::InvalidOptions,
                str::stream() << "Split key " << splitPoint << " not contained within chunk "
                              << range.toString()};
    }

    BSONArrayBuilder updates;
    // update max key of parent chunk and add it as update object
    chunk.setMax(splitPoint);
    chunk.setVersion(currentMaxVersion);
    _addUpdateChunkObject(txn, updates, chunk, true);
    // update child chunk and add it as update object
    childChunk.setMin(splitPoint);
    childChunk.setMax(maxKey);
    _addUpdateChunkObject(txn, updates, childChunk, false);

    BSONArrayBuilder preCond;
    {
        BSONObjBuilder b;
        b.append("ns", ChunkType::ConfigNS);
        b.append(
            "q",
            BSON("query" << BSON(ChunkType::ns(chunk.getNS()) << ChunkType::min() << range.getMin()
                                                              << ChunkType::max()
                                                              << range.getMax())
                         << "orderby"
                         << BSON(ChunkType::DEPRECATED_lastmod() << -1)));
        {
            BSONObjBuilder bb(b.subobjStart("res"));
            bb.append(ChunkType::shard(), chunk.getShard().toString());
        }
        preCond.append(b.obj());
    }

    // apply the batch of updates to remote and local metadata

    Status applyOpsStatus =
        grid.catalogClient(txn)->applyChunkOpsDeprecated(txn,
                                                         updates.arr(),
                                                         preCond.arr(),
                                                         chunk.getNS(),
                                                         currentMaxVersion,
                                                         WriteConcernOptions(),
                                                         repl::ReadConcernLevel::kLocalReadConcern);
    if (!applyOpsStatus.isOK()) {
        index_err() << "[BEE] failed to update chunkmap for chunk " << event->getChunk().getID()
                    << " due to " << applyOpsStatus;
        splitEvent->setRollbackFlag(true);
        transitionState(event, applyOpsStatus.code());
    } else {
        splitEvent->refreshChunk(chunk, childChunk);

        // up to now, split not rollback, split success.
        splitEvent->setSplitSuccessFlag(true);

        // assign the child chunk
        auto assignStatus = Balancer::get(txn)->assignChunk(
            txn, childChunk, false, event->isUserRequest(), ShardId(), false, false,true);
        if (!assignStatus.isOK()) {
            Balancer::get(txn)->assignChunk(txn, childChunk, false, false);
        }
        transitionState(event, ErrorCodes::OK);
    }

    index_log() << "[BEE] _commitSplitHandler, succeed to update chunkmap, p_chunk:"
                << chunk.getID() << ", c_chunk: " << childChunk.getID();
    return BalanceEventEngine::NextAction::kActionContinue;
}

StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_acknowledgeSplitHandler(
    IRebalanceEvent* event) {
    logBalanceEvent(event);
    auto sendAckStatus = _sendAckCommand(event, true);
    if (!sendAckStatus.isOK()) {
        index_err() << "[BEE] failed to send ack command of split for chunk "
                    << event->getChunk().getID();
        // TODO retry a few times, now we just complete the split as success
        transitionState(event, ErrorCodes::OK);
        return BalanceEventEngine::NextAction::kActionContinue;
    }

    return BalanceEventEngine::NextAction::kActionWait;
}

StatusWith<BalanceEventEngine::NextAction> BalanceEventEngine::_splittedHandler(
    IRebalanceEvent* event) {
    logBalanceEvent(event);
    return BalanceEventEngine::NextAction::kActionFinish;
}

}  // namespace mongo

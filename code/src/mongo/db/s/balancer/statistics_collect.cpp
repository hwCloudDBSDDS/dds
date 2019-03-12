#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include <algorithm>
#include <string>
#include "mongo/platform/basic.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/util/log.h"
#include "mongo/db/stats/top.h"
#include "mongo/db/s/balancer/statistics_collect.h"
#include "mongo/db/stats/counters.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/procparser.h"
#include "mongo/base/status.h"

namespace mongo {

static const std::vector<StringData> kCpuKeys{
        "cpu"_sd,
};

const std::string kCollectorThreadName = "StatisticsCollector";

const Seconds kCollectorRoundDefaultInterval(10);
 StatisticsCollector::StatisticsCollector() {
    // The StatisticsCollector thread must have been stopped
    invariant(_state == kStopped);
}

bool StatisticsCollector::_stopRequested() { return (_state == kStopped); }

void StatisticsCollector::start() {
    index_log() << "[StatisticsCollector] is starting";
    stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
    invariant(_state == kStopped);
    _state = kRunning;

    invariant(!_thread.joinable());
    invariant(!_threadOperationContext);
    _thread = stdx::thread([this] { _mainThread(); });
}

void StatisticsCollector::stop() {
    index_log() << "[StatisticsCollector] is stopping";
    if (_state != kRunning) {
        return;
    }
    _state = kStopped;

    // Wake up it if the main thread is sleeping now
    {
        stdx::lock_guard<stdx::mutex> scopedLock(_mutex);
        _condVar.notify_all();

        // Interrupt the StatisticsCollector thread if it has been started. We are guaranteed that the
        // operation
        // context of that thread is still alive, because we hold the Collector mutex.
        if (_threadOperationContext) {
            stdx::lock_guard<Client> scopedClientLock(*_threadOperationContext->getClient());
            _threadOperationContext->markKilled(ErrorCodes::Interrupted);
        }
    }
        invariant(_thread.joinable());

    _thread.join();
    _thread = {};
}


void StatisticsCollector::_beginRound(OperationContext *txn) {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    _condVar.notify_all();
}

void StatisticsCollector::_endRound(OperationContext *txn, Seconds waitTimeout) {
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _condVar.notify_all();
    }
    _sleepFor(txn, waitTimeout);
}

void StatisticsCollector::_sleepFor(OperationContext *txn, Seconds waitTimeout) {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    _condVar.wait_for(lock, waitTimeout.toSteadyDuration(), [&] { return _state != kRunning; });
}
/*
 *  collect last diff count between of top::_usage  top:_old,save to global tpsInterval
 */
void StatisticsCollector::collectChunkTps(int roundIntervalSeconds) {
    std::map<std::string, long long> chunkTpsCount;
    Top::get(_threadOperationContext->getClient()->getServiceContext()).computeChunkTpsInterval(&chunkTpsCount);
    OpCounters::setTps(chunkTpsCount, roundIntervalSeconds);
}

int StatisticsCollector::calCpuOccupy (StatisticsCollector::CpuOccupancy &o, StatisticsCollector::CpuOccupancy &n)
{
    double od, nd;
    double id, sd;
    double cpu_use ;
    int cpu_result;
    od =  (o.user + o.nice + o.system + o.idle+o.softirq+o.iowait+o.irq);//
    nd =  (n.user + n.nice + n.system + n.idle+n.softirq+n.iowait+n.irq);//

    id =  n.idle;
    sd =  o.idle ;
    if((nd-od) > 0)
    {
        cpu_use =100.0- ((id-sd))/(nd-od)*100.0;
        index_LOG(1) << "get cpu usge" << cpu_use;
    }
    else
    {
        cpu_use = -1;
        index_LOG(1) << "get invalid cpu time";
    }

    cpu_result =  cpu_use * 100;
    return cpu_result;
}

Status StatisticsCollector::getCpuProcTime(BSONObjBuilder &builder)
{
    // cpu infomation for the whold system, by readding file /proc/stat
    BSONObjBuilder builderPre;
    auto status = procparser::parseProcStatFile("/proc/stat"_sd, kCpuKeys, &builder);
    if (!status.isOK()) {
        index_warning() << "parse /proc/stat fail " << status.toString();
        return status;
    }
    return Status::OK();
}

StatusWith<StatisticsCollector::CpuOccupancy> StatisticsCollector::parseCpu(const BSONObj& source)
{
    StatisticsCollector::CpuOccupancy cpu;
    long long userMs;
    Status userTimeStatus = bsonExtractIntegerField(source, "user_ms", &userMs);
    if (!userTimeStatus.isOK())
    {
        index_LOG(0) << "parse user cpu used time error";
        return {ErrorCodes::BadValue, "parse user cpu used time error"};;
    }
    cpu.user = userMs;

    long long niceMs;
    Status niceTimeStatus = bsonExtractIntegerField(source, "nice_ms", &niceMs);
    if (!niceTimeStatus.isOK())
    {
        index_LOG(0) << "parse nice cpu used time error";
        return {ErrorCodes::BadValue, "parse nice cpu used time error"};;
    }
    cpu.nice = niceMs;

    long long sysMs;
    Status sysTimeStatus = bsonExtractIntegerField(source, "system_ms", &sysMs);
    if (!sysTimeStatus.isOK())
    {
        index_LOG(0) << "parse system cpu used time error";
        return {ErrorCodes::BadValue, "parse system cpu used time error"};
    }
    cpu.system = sysMs;


    long long idleMs;
    Status idleTimeStatus = bsonExtractIntegerField(source, "idle_ms", &idleMs);
    if (!idleTimeStatus.isOK())
    {
        index_LOG(0) << "parse idle cpu used time error";
        return {ErrorCodes::BadValue, "parse idle cpu used time error"};
    }
    cpu.idle = idleMs;

    long long waitMs;
    Status waitMsTimeStatus = bsonExtractIntegerField(source, "iowait_ms", &waitMs);
    if (!waitMsTimeStatus.isOK())
    {
        index_LOG(0) << "parse iowait cpu used time error";
        return {ErrorCodes::BadValue, "parse iowait cpu used time error"};
    }
    cpu.iowait = waitMs;

    long long irqMs;
    Status irqMsTimeStatus = bsonExtractIntegerField(source, "irq_ms", &irqMs);
    if (!irqMsTimeStatus.isOK())
    {
        index_LOG(0) << "parse irq cpu used time error";
        return {ErrorCodes::BadValue, "parse irq cpu used time error"};
    }
    cpu.irq = irqMs;


    long long softIrqMs;
    Status softIrqTimeStatus = bsonExtractIntegerField(source, "softirq_ms", &softIrqMs);
    if (!softIrqTimeStatus.isOK())
    {
        index_LOG(0) << "parse softirq cpu used time error";
        return {ErrorCodes::BadValue, "parse softirq cpu used time error"};
    }
    cpu.softirq = softIrqMs;
    return cpu;
}

int StatisticsCollector::getCpuRate()
{
    BSONObjBuilder  preBuilder;
    BSONObjBuilder  nowBuilder;
    const BSONObj  nowObs;
    Status pre = getCpuProcTime(preBuilder);
    if (!pre.isOK())

        return -1;

    const BSONObj preObj = preBuilder.obj();
    auto preStatus = parseCpu(preObj);
    if (!preStatus.isOK())
    {
        index_LOG(0) << "parse pre cpu used time error";
        return -1;
    }

    sleep(1);
    Status now = getCpuProcTime(nowBuilder);
    if (!now.isOK())
    {
        index_LOG(0) << "parse now cpu used time error";
        return -1;
    }
    const BSONObj  nowObj = nowBuilder.obj();
    auto nowStatus = parseCpu(nowObj);
    if (!nowStatus.isOK())
    {
        index_LOG(0) << "parse now cpu used time error";
        return -1;
    }
    int cpu = calCpuOccupy(preStatus.getValue(), nowStatus.getValue());
    return cpu;
}

void StatisticsCollector::resetThreadOperationContext(ServiceContext::UniqueOperationContext& txn,
                                           stdx::mutex& gcMutex,
                                           OperationContext** threadOperationContext) {
    txn.reset();
    txn = cc().makeOperationContext();
    {
        stdx::lock_guard<stdx::mutex> scopedLock(gcMutex);
        *threadOperationContext = txn.get();
    }
}


void StatisticsCollector::waitForStorageEngineToBeCreated() {
    StorageEngine* storageEngine = nullptr;
    do {
        storageEngine = getGlobalServiceContext()->getGlobalStorageEngine();

        if (storageEngine) {
            break;
        }
        sleep(2);
    } while (true);
}


void StatisticsCollector::_mainThread() {
    try {
    index_log() << "[StatisticsCollector] main thread in.";
    Client::initThread(kCollectorThreadName.c_str());
    ServiceContext::UniqueOperationContext txn;
    waitForStorageEngineToBeCreated();
    while (!_stopRequested()) {
        _beginRound(_threadOperationContext);
        if (kPause == _state) {
            _endRound(_threadOperationContext, kCollectorRoundDefaultInterval);
            continue;
        }
        resetThreadOperationContext(txn, _mutex, &_threadOperationContext);
        try {
            int cpuUsageTemp = getCpuRate();
            index_LOG(1) << "[StatisticsCollector] get current cpu:" << cpuUsageTemp;
            index_LOG(1) << "[StatisticsCollector] get last  cpu:" << mongo::oldCpuUsage;
            mongo::cpuUsage = (cpuUsageTemp + mongo::oldCpuUsage ) / 2;
            mongo::oldCpuUsage = cpuUsageTemp;
            // Just to match the opening statement if in log level 1
            index_LOG(1) << "*** [StatisticsCollector] client round " << "cpu: " <<  mongo::cpuUsage;
            collectChunkTps(10);
            _endRound(_threadOperationContext, kCollectorRoundDefaultInterval);
        }
        catch (const std::exception& e) {

            index_err() << "[StatisticsCollector] caught exception while collect statistics : " << e.what() ;
            // Just to match the opening statement if in log level 1
            index_LOG(1) << "*** End of StatisticsCollector  round";
            _endRound(_threadOperationContext, kCollectorRoundDefaultInterval);
        }
    }// end while
    }// end try
    catch (const std::exception& e)
    {
    index_err() << "[StatisticsCollector] caught exception while collect statistics : " << e.what();
    // Just to match the opening statement if in log level 1
    index_LOG(1) << "*** End of StatisticsCollector  round";
    }
}
}// namespace mongo


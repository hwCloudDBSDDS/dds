#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/util/util_extend/process_stage_time.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

ProcessStageTime::ProcessStageTime(std::string processName) : _processName(processName) {}

ProcessStageTime::~ProcessStageTime() {}


void ProcessStageTime::noteStageStart(const std::string& stageName) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _stages.emplace_back(stageName, Date_t::now());
}

void ProcessStageTime::noteProcessEnd() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _stages.emplace_back("end", Date_t::now());
}

Milliseconds ProcessStageTime::getProcessDuration() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    if (_stages.empty()) {
        return Milliseconds(0);
    } else {
        return Milliseconds(_stages.back().startTime - _stages.front().startTime);
    }
}

BSONObj ProcessStageTime::toBSON() {
    BSONObjBuilder processBSON;

    processBSON.append("processName", _processName);

    processBSON.append("processDuration", durationCount<Milliseconds>(getProcessDuration()));

    stdx::lock_guard<stdx::mutex> lk(_mutex);

    std::vector<StageDetails>::size_type i;

    BSONObjBuilder stageStartTimeBSON;
    for (i = 0; i < _stages.size(); i++) {
        stageStartTimeBSON.append(_stages[i].name, _stages[i].startTime);
    }
    processBSON.append("stageStartTime", stageStartTimeBSON.obj());

    BSONObjBuilder stageDurationBSON;
    for (i = 0; i < _stages.size(); i++) {
        if (i < _stages.size() - 1) {
            stageDurationBSON.append(
                _stages[i].name,
                durationCount<Milliseconds>(_stages[i + 1].startTime - _stages[i].startTime));
        }
    }
    processBSON.append("stageDuration", stageDurationBSON.obj());

    return processBSON.obj();
}

std::string ProcessStageTime::toString() {
    return toBSON().toString();
}
}

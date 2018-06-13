#include "mongo/util/mongoutils/str.h"
#include "mongo/util/util_extend/job_registry.h"

namespace mongo {

using str::stream;

JobRegistry::JobRegistry() {
}

JobRegistry::~JobRegistry() {
}

void JobRegistry::registerJob(std::string jobName, Milliseconds jobTimeout) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    JobMap::iterator it = _jobMap.find(jobName);
    
    if(it == _jobMap.end()) {
        _jobMap.emplace(jobName, JobDetails(jobTimeout, Date_t::now()));
    }
    else {
        //if exists, just renew timeout and lastUpdateTime values
        it->second.timeout = jobTimeout;
        it->second.lastUpdateTime = Date_t::now();
    }
}

Status JobRegistry::renewJob(std::string jobName) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    JobMap::iterator it = _jobMap.find(jobName);
    
    if(it == _jobMap.end()) {
        return Status(ErrorCodes::NoSuchKey, 
            stream() << "cannot find job: " << jobName);
    }
    
    it->second.lastUpdateTime = Date_t::now();

    return Status::OK();
}

void JobRegistry::cancelJob(std::string jobName) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    
    _jobMap.erase(jobName);
}

std::list<std::string> JobRegistry::checkSuspendedJobs() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    std::list<std::string> suspendedJobs;
    Date_t now = Date_t::now();
    for (JobMap::iterator it = _jobMap.begin(); it != _jobMap.end(); it++) {
        if (now - it->second.lastUpdateTime > it->second.timeout) {
            suspendedJobs.push_back(it->first);
        }
    }

    return suspendedJobs;
}

}

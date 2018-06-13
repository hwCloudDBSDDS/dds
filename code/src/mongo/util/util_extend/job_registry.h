#pragma once

#include <list>
#include <map>
#include <string>
#include <time.h>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/duration.h"
#include "mongo/util/time_support.h"

namespace mongo {

class Status;
template <typename T> // used for StatusWith
class StatusWith;

class JobRegistry  {
    MONGO_DISALLOW_COPYING(JobRegistry);

public:
    struct JobDetails{
        Milliseconds timeout;   
        Date_t lastUpdateTime; 

        JobDetails():
            timeout(0), lastUpdateTime(Date_t::max()) {
        }

        JobDetails(Milliseconds jobTimeout, Date_t now):
            timeout(jobTimeout), lastUpdateTime(now) {
        }
    };

    typedef std::map<std::string, JobDetails> JobMap;
    
    JobRegistry();
    
    virtual ~JobRegistry();

    void registerJob(std::string jobName, Milliseconds jobTimeout);
    
    Status renewJob(std::string jobName);
    
    void cancelJob(std::string jobName);

    std::list<std::string> checkSuspendedJobs();

private:
    JobMap _jobMap;
    stdx::mutex _mutex;            
};

}
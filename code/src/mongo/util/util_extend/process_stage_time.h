#pragma once

#include <vector>
#include <string>
#include <time.h>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/duration.h"
#include "mongo/util/time_support.h"

namespace mongo {

class ProcessStageTime  {
    MONGO_DISALLOW_COPYING(ProcessStageTime);

public:
    struct StageDetails{
        std::string name;
        Date_t startTime;  

        StageDetails():
            name(std::string()), startTime(Date_t::max()) {
        }

        StageDetails(const std::string &stageName, Date_t now):
            name(stageName), startTime(now) {
        }
    };
        
    ProcessStageTime(std::string processName);

    virtual ~ProcessStageTime();

    void clear();

    void noteStageStart(const std::string &stageName);

    void noteProcessEnd();

    Milliseconds getProcessDuration();

    BSONObj toBSON();

    std::string toString();

private:
    std::string _processName;
    
    std::vector<StageDetails> _stages;
    stdx::mutex _mutex;    
};

}

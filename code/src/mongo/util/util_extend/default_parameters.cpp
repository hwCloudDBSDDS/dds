
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"
#include "mongo/util/log.h"
#include "mongo/util/util_extend/default_parameters.h"
#include <unistd.h>

namespace mongo {

//Seconds      kDefaultConfigHeartbeatTimeoutPeriod       = Seconds(5);
//Milliseconds kDefaultConfigElectionTimeoutPeriod        = Milliseconds(5000);
//Milliseconds kDefaultShardHeartbeatTimeoutPeriod        = Milliseconds(10000);
//Milliseconds kDefaultStatusRenewalTimeout               = Milliseconds(5000);
//Milliseconds kDefaultConfigHeartbeatSchedulerTimeout    = Milliseconds(5000);
//Milliseconds kDefaultShardHeartbeatSchedulerTimeout     = Milliseconds(10000);
//Milliseconds kDefaultShardServerFailureDetectionTimeout = Milliseconds(10000);
//Milliseconds kDefaultBalancerTimeout                    = Milliseconds(60000);

// Detects whether or not /etc/enableMongoGdb.ini exists, and if it exists, opens debuge-GDB mode.
// It is not a real-time detection, only when deploying mongo detection once.
void checkGdb()
{
    int32_t        result = 0;
    std::string    gdbFile("/etc/enableMongoGdb.ini");

    result = access((const char*)(gdbFile.c_str()), F_OK);  
    if (-1 != result)
    {  
        //kDefaultConfigHeartbeatTimeoutPeriod        = Seconds(5000000);
        //kDefaultConfigElectionTimeoutPeriod         = Milliseconds(500000000);
        //kDefaultShardHeartbeatTimeoutPeriod         = Milliseconds(500000000);
        //kDefaultStatusRenewalTimeout                = Milliseconds(500000000);
        //kDefaultConfigHeartbeatSchedulerTimeout     = Milliseconds(500000000);
        //kDefaultShardHeartbeatSchedulerTimeout      = Milliseconds(500000000);
        //kDefaultShardServerFailureDetectionTimeout  = Milliseconds(500000000);
        //kDefaultBalancerTimeout                     = Milliseconds(500000000);
        index_log() << "Mongo is in debuge-GDB mode now.";
    }
}

}

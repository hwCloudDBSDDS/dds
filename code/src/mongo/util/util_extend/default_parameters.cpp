
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/util/util_extend/default_parameters.h"
#include "mongo/platform/basic.h"
#include "mongo/util/log.h"
#include <unistd.h>

namespace mongo {

#ifndef BUILD_MOCK
Seconds kDefaultConfigHeartbeatTimeoutPeriod = Seconds(3);
Milliseconds kDefaultConfigElectionTimeoutPeriod = Milliseconds(3000);

Milliseconds kDefaultShardHeartbeatTimeoutPeriod = Milliseconds(12000);
Milliseconds kDefaultShardHeartbeatSchedulerTimeout = Milliseconds(9000);
Milliseconds kDefaultShardFailureDetectionTimeout = Milliseconds(15000);
#else
Seconds kDefaultConfigHeartbeatTimeoutPeriod = Seconds(5);
Milliseconds kDefaultConfigElectionTimeoutPeriod = Milliseconds(5000);

Milliseconds kDefaultShardHeartbeatTimeoutPeriod = Milliseconds(12000);
Milliseconds kDefaultShardHeartbeatSchedulerTimeout = Milliseconds(10000);
Milliseconds kDefaultShardFailureDetectionTimeout = Milliseconds(15000);
#endif


Milliseconds kDefaultConfigHeartbeatSchedulerTimeout = Milliseconds(10000);
Milliseconds kDefaultBalancerTimeout = Milliseconds(7200000);

}

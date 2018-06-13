
#pragma once

#include <string>

#include "mongo/util/duration.h"

namespace mongo {

// 1. Parameters for config and shard takeover

// time for shard takeover = 30s
// time for chunk takeover (100 chunks per shard) = 20s
// time for shard heartbeat timeout = 10s
//
// time for config takeover = 9.5s
// time for config init = 4.5s
// time for config heartbeat = 5s

const Milliseconds kDefaultConfigHeartbeatInterval(1000);
const Seconds kDefaultConfigHeartbeatTimeoutPeriod(5);

const Milliseconds kDefaultConfigElectionTimeoutPeriod(5000);

const Milliseconds kDefaultConfigCatchUpTimeoutPeriod(2000);
const Milliseconds kDefaultConfigOplogBufferGetBatchTimeout(1000);

const Milliseconds kDefaultShardHeartbeatInterval(1000);
const Milliseconds kDefaultShardHeartbeatRetryInterval(10);
const Milliseconds kDefaultShardHeartbeatTimeoutPeriod(10000);

const Milliseconds kDefaultFailureDetectionInterval(500);
const Milliseconds kDefaultRegisterReservedTime(10000);

const Milliseconds kDefaultClientExecCommandMaxRetryTimeout(26000);
const Milliseconds kDefaultClientExecCommandRetryInterval(3000);
// 2. Parameters for suspended jobs detection

const std::string kConfigHeartbeatSchedulerJobName = "ConfigServerHeartbeatScheduler";
const Milliseconds kDefaultConfigHeartbeatSchedulerTimeout(5000);


const std::string kShardHeartbeatSchedulerJobName = "ShardServerHeartbeatScheduler";
const Milliseconds kDefaultShardHeartbeatSchedulerTimeout(10000);


const std::string kShardServerFailureDetectionJobName = "ShardServerFailureDetection";
const Milliseconds kDefaultShardServerFailureDetectionTimeout(10000);

const Milliseconds kShardServerFailureDetectionInterval(500);

const std::string kBalancerJobName = "Balancer";
const Milliseconds kDefaultBalancerTimeout(60000);

const Milliseconds kBalanceRoundDefaultInterval(10000);
// Sleep between balancer rounds in the case where the last round found some chunks which needed to
// be balanced. This value should be set sufficiently low so that imbalanced clusters will quickly
// reach balanced state, but setting it too low may cause CRUD operations to start failing due to
// not being able to establish a stable shard version.
const Milliseconds kShortBalanceRoundInterval(1000);

// Time out value for hang task
const Milliseconds kHangTaskTimeout(120000);
const Milliseconds kMaxReleaseRootFolderTaskInterval(1000);
const Milliseconds kMaxReleaseRootFolderForOneChunkInterval(500);

// Check whether to enter debuge-GDB mode.
void checkGdb();

}



#pragma once

#include <string>

#include "mongo/util/duration.h"

namespace mongo {

// These default parameters should follow the following rules:
// a. kDefaultConfigElectionTimeoutPeriod = kDefaultConfigHeartbeatTimeoutPeriod;
// b. kDefaultShardHeartbeatTimeoutPeriod >= kDefaultConfigElectionTimeoutPeriod +
//        timeReservedForTransitionToPrimary;
// c. timeReservedForTransitionToPrimary > kDefaultConfigElectionTimeoutOffsetSkew *
// numberOfConfigMembers +
//        kDefaultConfigCatchUpTimeoutPeriod + kDefaultConfigOplogBufferGetBatchTimeout * 2;
// d. kDefaultShardHeartbeatSchedulerTimeout= kDefaultShardHeartbeatTimeoutPeriod;
// e. kDefaultShardFailureDetectionTimeout = kDefaultShardHeartbeatTimeoutPeriod;
// f. kDefaultShardFailureDetectionInterval = kDefaultShardHeartbeatInterval * 0.5;
// g. kDefaultClientExecCommandMaxRetryTimeout >= kDefaultShardHeartbeatTimeoutPeriod +
//        timeReservedForShardTakeover.

// 1. Parameters for config and shard takeover

extern Seconds kDefaultConfigHeartbeatTimeoutPeriod;
extern Milliseconds kDefaultConfigElectionTimeoutPeriod;
extern Milliseconds kDefaultShardHeartbeatTimeoutPeriod;

#ifndef BUILD_MOCK
const Milliseconds kDefaultConfigHeartbeatInterval(500);

const Milliseconds kDefaultShardHeartbeatInterval(1000);
const Milliseconds kDefaultShardFailureDetectionInterval(500);
const Milliseconds kDefaultShardHeartBeatOpTimeout(3000);

const Milliseconds kDefaultConfigCatchUpTimeoutPeriod(1000);
const Milliseconds kDefaultConfigOplogBufferGetBatchTimeout(500);

const Milliseconds kDefaultUpdateDbRetryInterval(500);
#else
const Milliseconds kDefaultConfigHeartbeatInterval(1000);

const Milliseconds kDefaultShardHeartbeatInterval(1000);
const Milliseconds kDefaultShardFailureDetectionInterval(500);

const Milliseconds kDefaultConfigCatchUpTimeoutPeriod(2000);
const Milliseconds kDefaultConfigOplogBufferGetBatchTimeout(1000);

const Milliseconds kDefaultUpdateDbRetryInterval(1000);
#endif

// Time reserved for a config server primary candidate to win an election
const Milliseconds kDefaultConfigElectionTimeoutOffsetSkew(100);

const Milliseconds kDefaultShardHeartbeatRetryInterval(10);

const Milliseconds kDefaultReserveForShardHeartbeatStartup(100);


const Milliseconds kDefaultRegisterReservedTime(10000);

const Milliseconds kDefaultClientExecCommandMaxRetryTimeout(30000);
const Milliseconds kDefaultClientExecCommandRetryInterval(1000);
const Milliseconds kDefaultClientExecCommandRetryMinInterval(500);

const unsigned int kDefaultClientExecCommandSleepAfterTimes(3);

const Milliseconds kDefaultBindRetryInterval(500);

// Time reserved for a config server to trans secondary to primary
const std::string kConfigSecondaryToPrimaryJobName = "ConfigSecondaryToPrimary";
const Milliseconds kDefaultConfigSecondaryToPrimaryTimeout(120000);

// 2. Parameters for suspended jobs detection

const std::string kConfigHeartbeatSchedulerJobName = "ConfigServerHeartbeatScheduler";
extern Milliseconds kDefaultConfigHeartbeatSchedulerTimeout;

const std::string kShardHeartbeatSchedulerJobName = "ShardServerHeartbeatScheduler";
extern Milliseconds kDefaultShardHeartbeatSchedulerTimeout;

const std::string kShardFailureDetectionJobName = "ShardServerFailureDetection";
extern Milliseconds kDefaultShardFailureDetectionTimeout;

const std::string kBalancerJobName = "Balancer";
extern Milliseconds kDefaultBalancerTimeout;

const Milliseconds kBalanceRoundDefaultInterval(10000);
// Sleep between balancer rounds in the case where the last round found some chunks which needed to
// be balanced. This value should be set sufficiently low so that imbalanced clusters will quickly
// reach balanced state, but setting it too low may cause CRUD operations to start failing due to
// not being able to establish a stable shard version.
const Milliseconds kShortBalanceRoundInterval(1000);

// Time out value for hang task
const Milliseconds kMaxReleaseRootFolderTaskInterval(1000);
const Milliseconds kMaxReleaseRootFolderForOneChunkInterval(500);

//time for status renew
const Milliseconds kDefaultStatusRenewalInterval(500);
const Milliseconds kDefaultShardExitTimeoutPeriod(15000);

}

#pragma once

#include <cstdint>

namespace mongo {

const uint32_t kPlogCmdTimeoutMS(1000);
const uint32_t kCreateRootFolderTimeoutMS = kPlogCmdTimeoutMS * 3;

const uint32_t kShardRegisterTimeoutMS(10000);
const uint32_t kIsMasterTimeoutMS(2000);
const uint32_t kShardIdentityUpsertTimeoutMS(2000);
const uint32_t kShardVersionTimeoutMS(2000);
const uint32_t kShardActiveReadyTimeoutMS =
    1000 + kIsMasterTimeoutMS + kShardIdentityUpsertTimeoutMS + kShardVersionTimeoutMS;

const uint32_t kListCollectionsTimeoutMS(10000);
const uint32_t kListDatabasesTimeoutMS(10000);
const uint32_t kCollectionCountTimeoutMS(10000);
const uint32_t kCollectionListIndexesTimeoutMS(2000);
const uint32_t kCollectionFindTimeoutMS(2000);

const uint32_t kFindOrGetMoreTimeoutMS(25000);
const uint32_t kKillCursorsTimeoutMS(2000);

const uint32_t kReplSetStepDownTimeoutMS(2000);
const uint32_t kReplSetFreshTimeoutMS(2000);
const uint32_t kReplSetElectTimeoutMS(2000);
const uint32_t kReplSetGetRBIDTimeoutMS(2000);
const uint32_t kSyncSourceFeedbackReporterTimeoutMS(2000);
const uint32_t kLastOplogEntryFetcherTimeoutMS(2000);

const double kDBConnectionSocketTimeoutS(10);
}

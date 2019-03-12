/**
 *    Copyright (C) 2016 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/db/s/balancer/balancer_chunk_selection_policy_impl.h"
#include "mongo/s/client/shard_registry.h"

#include <map>
#include <set>
#include <vector>

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj_comparator_interface.h"
#include "mongo/db/modules/rocks/src/gc_common.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/catalog/type_tags.h"
#include "mongo/s/grid.h"
#include "mongo/s/shard_util.h"
#include "mongo/s/sharding_raii.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include "mongo/s/balancer_configuration.h"




namespace mongo {

using MigrateInfoVector = BalancerChunkSelectionPolicy::MigrateInfoVector;
using SplitInfoVector = BalancerChunkSelectionPolicy::SplitInfoVector;
using IndexSplitInfoVector = BalancerChunkSelectionPolicy::IndexSplitInfoVector;
using std::shared_ptr;
using std::unique_ptr;
using std::vector;
using std::map;
using std::string;
using std::set;

namespace {

/**
 * Does a linear pass over the information cached in the specified chunk manager and extracts chunk
 * distrubution and chunk placement information which is needed by the balancer policy.
 */
StatusWith<DistributionStatus> createCollectionDistributionStatus(
    OperationContext* txn, const ShardStatisticsVector& allShards, ChunkManager* chunkMgr) {
    ShardToChunksMap shardToChunksMap;

    // Makes sure there is an entry in shardToChunksMap for every shard, so empty shards will also
    // be accounted for
    for (const auto& stat : allShards) {
        shardToChunksMap[stat.shardId];
    }

    for (const auto& entry : chunkMgr->getChunkMap()) {
        const auto& chunkEntry = entry.second;

        ChunkType chunk;
        chunk.setNS(chunkMgr->getns());
        chunkEntry->constructChunkType(&chunk);

        shardToChunksMap[chunkEntry->getShardId()].push_back(chunk);
    }

    vector<TagsType> collectionTags;
    Status tagsStatus = Grid::get(txn)->catalogClient(txn)->getTagsForCollection(
        txn, chunkMgr->getns(), &collectionTags);
    if (!tagsStatus.isOK()) {
        return {tagsStatus.code(),
                str::stream() << "Unable to load tags for collection " << chunkMgr->getns()
                              << " due to "
                              << tagsStatus.toString()};
    }

    DistributionStatus distribution(NamespaceString(chunkMgr->getns()),
                                    std::move(shardToChunksMap));

    // Cache the collection tags
    const auto& keyPattern = chunkMgr->getShardKeyPattern().getKeyPattern();

    for (const auto& tag : collectionTags) {
        auto status = distribution.addRangeToZone(
            ZoneRange(keyPattern.extendRangeBound(tag.getMinKey(), false),
                      keyPattern.extendRangeBound(tag.getMaxKey(), false),
                      tag.getTag()));

        if (!status.isOK()) {
            return status;
        }
    }

    return {std::move(distribution)};
}

/**
 * Helper class used to accumulate the split points for the same chunk together so they can be
 * submitted to the shard as a single call versus multiple. This is necessary in order to avoid
 * refreshing the chunk metadata after every single split point (if done one by one), because
 * splitting a chunk does not yield the same chunk anymore.
 */
class SplitCandidatesBuffer {
    MONGO_DISALLOW_COPYING(SplitCandidatesBuffer);

public:
    SplitCandidatesBuffer(NamespaceString nss, ChunkVersion collectionVersion)
        : _nss(std::move(nss)),
          _collectionVersion(collectionVersion),
          _chunkSplitPoints(SimpleBSONObjComparator::kInstance
                                .makeBSONObjIndexedMap<BalancerChunkSelectionPolicy::SplitInfo>()) {
    }

    /**
     * Adds the specified split point to the chunk. The split points must always be within the
     * boundaries of the chunk and must come in increasing order.
     */
    void addSplitPoint(shared_ptr<Chunk> chunk, const BSONObj& splitPoint) {
        auto it = _chunkSplitPoints.find(chunk->getMin());
        if (it == _chunkSplitPoints.end()) {
            _chunkSplitPoints.emplace(chunk->getMin(),
                                      BalancerChunkSelectionPolicy::SplitInfo(chunk->getShardId(),
                                                                              _nss,
                                                                              _collectionVersion,
                                                                              chunk->getLastmod(),
                                                                              chunk->getMin(),
                                                                              chunk->getMax(),
                                                                              {splitPoint}));
        } else if (splitPoint.woCompare(it->second.splitKeys.back()) > 0) {
            it->second.splitKeys.push_back(splitPoint);
        } else {
            // Split points must come in order
            invariant(splitPoint.woCompare(it->second.splitKeys.back()) == 0);
        }
    }

    /**
     * May be called only once for the lifetime of the buffer. Moves the contents of the buffer into
     * a vector of split infos to be passed to the split call.
     */
    SplitInfoVector done() {
        BalancerChunkSelectionPolicy::SplitInfoVector splitPoints;
        for (const auto& entry : _chunkSplitPoints) {
            splitPoints.push_back(std::move(entry.second));
        }

        return splitPoints;
    }

private:
    // Namespace and expected collection version
    const NamespaceString _nss;
    const ChunkVersion _collectionVersion;

    // Chunk min key and split vector associated with that chunk
    BSONObjIndexedMap<BalancerChunkSelectionPolicy::SplitInfo> _chunkSplitPoints;
};

}  // namespace

BalancerChunkSelectionPolicyImpl::BalancerChunkSelectionPolicyImpl(ClusterStatistics* clusterStats)
    : _clusterStats(clusterStats) {}

BalancerChunkSelectionPolicyImpl::~BalancerChunkSelectionPolicyImpl() = default;

StatusWith<IndexSplitInfoVector> BalancerChunkSelectionPolicyImpl::selectChunksToSplit(
    OperationContext* txn) {
    auto shardStatsStatus = _clusterStats->getStats(txn);
    if (!shardStatsStatus.isOK()) {
        return shardStatsStatus.getStatus();
    }

    const auto shardStats = std::move(shardStatsStatus.getValue());

    vector<CollectionType> collections;

    Status collsStatus =
        Grid::get(txn)->catalogClient(txn)->getCollections(txn, nullptr, &collections, nullptr);
    if (!collsStatus.isOK()) {
        return collsStatus;
    }

    if (collections.empty()) {
        return IndexSplitInfoVector{};
    }

    IndexSplitInfoVector splitCandidates;

    for (const auto& coll : collections) {
        if (coll.getTabType() != CollectionType::TableType::kSharded ||
            coll.getAllowBalance() == false) {
            continue;
        }
        const NamespaceString nss(coll.getNs());

        auto candidatesStatus = _getSplitCandidatesForCollection(txn, nss, shardStats);
        if (candidatesStatus == ErrorCodes::NamespaceNotFound) {
            // Namespace got dropped before we managed to get to it, so just skip it
            continue;
        } else if (!candidatesStatus.isOK()) {
            warning() << "Unable to enforce tag range policy for collection " << nss.ns()
                      << causedBy(candidatesStatus.getStatus());
            continue;
        }

        splitCandidates.insert(splitCandidates.end(),
                               std::make_move_iterator(candidatesStatus.getValue().begin()),
                               std::make_move_iterator(candidatesStatus.getValue().end()));
    }

    return splitCandidates;
}

StatusWith<IndexSplitInfoVector> BalancerChunkSelectionPolicyImpl::indexSelectChunksToSplit(
    OperationContext* txn) {
    auto shardStatsStatus = _clusterStats->getStats(txn);
    if (!shardStatsStatus.isOK()) {
        return shardStatsStatus.getStatus();
    }

    const auto shardStats = std::move(shardStatsStatus.getValue());

    vector<CollectionType> collections;

    Status collsStatus =
        Grid::get(txn)->catalogClient(txn)->getCollections(txn, nullptr, &collections, nullptr);
    if (!collsStatus.isOK()) {
        return collsStatus;
    }

    if (collections.empty()) {
        return IndexSplitInfoVector{};
    }

    IndexSplitInfoVector splitCandidates;

    for (const auto& coll : collections) {
        const NamespaceString nss(coll.getNs());
        if (coll.getTabType() != CollectionType::TableType::kSharded || GcRefNs == nss.toString() ||
            coll.getAllowBalance() == false) {
            continue;
        }

        auto candidatesStatus = _indexGetSplitCandidatesForCollection(txn, nss, shardStats);
        if (candidatesStatus == ErrorCodes::NamespaceNotFound) {
            // Namespace got dropped before we managed to get to it, so just skip it
            continue;
        } else if (!candidatesStatus.isOK()) {
            index_warning() << "Unable to enforce tag range policy for collection " << nss.ns()
                            << causedBy(candidatesStatus.getStatus());
            continue;
        }

        splitCandidates.insert(splitCandidates.end(),
                               std::make_move_iterator(candidatesStatus.getValue().begin()),
                               std::make_move_iterator(candidatesStatus.getValue().end()));
    }

    return splitCandidates;
}
string  BalancerChunkSelectionPolicyImpl::canChunkMove(OperationContext* txn, const ClusterStatistics::ChunkStatistics *destChunkStatics,std::vector<ClusterStatistics::ShardStatistics> shardStats,const ClusterStatistics::ShardStatistics *maxShard,const ClusterStatistics::ShardStatistics *minShard )
{
        NamespaceString nss(StringData(destChunkStatics->ns));
        index_LOG(1) << "begin get cm";
        auto scopedCMStatus = ScopedChunkManager::getExisting(txn, nss);
        if (!scopedCMStatus.isOK()) {
            index_LOG(0) << "get chunkmanager error.";
            return std::string();
        }
        auto scopedCM = std::move(scopedCMStatus.getValue());
        ChunkManager *const cm = scopedCM.cm();
        const auto distributionStatus = createCollectionDistributionStatus(txn, shardStats, cm);
        if (!distributionStatus.isOK()) {
            index_LOG(0) << "get distributionStatus error.";
            return std::string();
        }
        const DistributionStatus &distribution = distributionStatus.getValue();
        index_LOG(1) << "create distribution success.";
        const ChunkType *destChunk = getChunkTypeByChunkId(distribution.getChunks(maxShard->shardId), destChunkStatics->chunkId);
        index_LOG(1) << "getChunkTypeByChunkId end.";
        if (nullptr != destChunk) {
            index_LOG(1) << "get chunktype success:" << destChunk->getID() << destChunk->getID();
            Status result = BalancerPolicy::isShardSuitableReceiver(*minShard, distribution.getTagForChunk(
                                                                            *destChunk));
            if (!result.isOK()) {
                index_LOG(0) << "min shard  is not suitable for chunk." << destChunk->getID();
                return std::string();
            }
            index_LOG(1) << "get chunktype success:" << destChunk->getID() ;
            string temp = destChunk->getID();
            return temp;
        }
        return std::string();
}

bool BalancerChunkSelectionPolicyImpl::isChunkNumBalance(const ShardStatisticsVector& shardStats,const std::string &ns,const ShardId& shardId , bool isSource)
{
    size_t totalChunkNum =0;
    size_t chunkNumOnshard =0;

    for (const auto& stat : shardStats) {
        if (shardId == stat.shardId)
        {
            for (const auto& chunk : stat.chunkStatistics) {
                if (chunk.ns == ns) {
                    chunkNumOnshard++;
                    totalChunkNum++;
                }
            }
        } else
        {
            for (const auto& chunk : stat.chunkStatistics) {
                if (chunk.ns == ns) {
                    totalChunkNum++;
                }
            }
        }
    }
    size_t shardSize = shardStats.size();
    size_t averChunkSize = totalChunkNum / shardSize;
    index_LOG(1) << "all chunk num for ns:" << ns << totalChunkNum << ",shard size:" <<shardSize << ",aver size:" << averChunkSize  ;
    // TODO: source chunk num must < averChunkSize/2
    if(isSource)
    {
        if(chunkNumOnshard > averChunkSize / 2)
        {
            index_LOG(1) << "chunk size on source shard is suitable, can still offload chunk from this shard.";
            return true;
        }
        return false;
    }
    else
    {
        if(chunkNumOnshard < 2 * averChunkSize)
        {
            index_LOG(1) << "chunk size on shard is suitable, can still move chunk to this shard.";
            return true;
        }
        return false;
    }

}

string
BalancerChunkSelectionPolicyImpl::findChunk(OperationContext* txn, size_t maxChunkForShard,std::vector<CollectionType>& collections,std::vector<ClusterStatistics::ShardStatistics> shardStats,const ClusterStatistics::ShardStatistics *maxShard,const ClusterStatistics::ShardStatistics *minShard ) {

    uint64_t min = 50000000;;
    uint64_t max = 0;
    ClusterStatistics::ChunkStatistics minTemp ;
    const  std::vector<ClusterStatistics::ChunkStatistics> &chunkStatistics = maxShard->chunkStatistics;
    bool isMaxFind = false;
    bool isMinFind = false;
    size_t chunkSize = maxShard->chunkStatistics.size();
    ClusterStatistics::ChunkStatistics maxTemp ;
    index_LOG(1) << "max cpu shard get " << chunkSize << " chunks.";


    // 1 get max tps chunk
    for (const ClusterStatistics::ChunkStatistics &chunk : chunkStatistics) {
        index_LOG(1) << "one chunk is " << chunk.toBSON().toString();

        if (chunk.tps == 0) {
            continue;
        }

        if (chunk.tps > max) {
            NamespaceString nss(StringData(chunk.ns));
            if(!isShardCollection(nss, collections))
            {
                index_LOG(1) << "chunk of collection is not shard colleciton," <<  chunk.toBSON().toString();
                continue;
            }
            max = chunk.tps;
            maxTemp = chunk;
            isMaxFind = true;
            index_LOG(1) << "now max tps chunk  is " << maxTemp.toBSON().toString();
        }

        if (chunk.tps < min) {
            NamespaceString nss(StringData(chunk.ns));
            if(!isShardCollection(nss, collections))
            {
                index_LOG(1) << "chunk of collection is not shard colleciton, chunk:" << chunk.toBSON().toString();
                continue;
            }
            minTemp = chunk;
            min = chunk.tps;
            isMinFind = true;
            index_LOG(1) << "now min is " << minTemp.toBSON().toString() << "min is " << min;
        }
    }
    if(isMaxFind &&  isMinFind )
    {
        index_LOG(1) << "get min tps chunk:" <<minTemp.chunkId.toString() << "of collections:"<< minTemp.ns << ", tps:" << minTemp.tps;
        index_LOG(1) << "get max tps chunk:" <<maxTemp.chunkId.toString() << "of collections:" << maxTemp.ns <<", tps:" << maxTemp.tps;
    }
    else
    {
        index_LOG(1) << "get none max or  tps chunk, all chunk tps is 0 or chunk is unsharded chunk.";
    }


    // 3 get mid tps chunk
    if (isMaxFind && isMinFind) {
        if (maxTemp.chunkId == minTemp.chunkId) {
            index_LOG(1) << " max tps chunk is same with min tps chunk" ;
            return std::string();
        } else {
            for (size_t i = 0; i <chunkSize; i++) {
                if (chunkStatistics[i].tps > min && chunkStatistics[i].tps < max) {
                    index_LOG(0) << " get mindle chunk :" << chunkStatistics[i].chunkId <<  "ns: " << chunkStatistics[i].ns << ",tps :"  <<chunkStatistics[i].tps;
                    NamespaceString nss(StringData(chunkStatistics[i].ns));
                    if(!isShardCollection(nss, collections))
                    {
                        index_LOG(1) << "chunk of collection is not shard colleciton, chunk:" << chunkStatistics[i].toBSON().toString();
                        continue;
                    }
                    if(!isChunkNumBalance(shardStats, chunkStatistics[i].ns,minShard->shardId,false))
                    {
                        index_LOG(1) << "chunk for collection:" << chunkStatistics[i].ns << "is not suitable for shard:" << minShard->shardId.toString();
                        continue;
                    }


                    if(!isChunkNumBalance(shardStats, chunkStatistics[i].ns, maxShard->shardId, true))
                    {
                        index_LOG(1) << "chunk for collection:" << chunkStatistics[i].ns << "can not offload frome :" << maxShard->shardId.toString();
                        continue;
                    }
                    if(minShard->chunkStatistics.size() >= maxChunkForShard)
                    {
                        index_LOG(1) << "chunks on min load shard :" << minShard->shardId.toString() << "exceed limit";
                        continue;
                    }
                    string destChunkId = canChunkMove(txn ,&chunkStatistics[i], shardStats,maxShard,minShard );
                    if(!destChunkId.empty())
                    {
                        index_LOG(1) << "chunk of collection can  move to dest shard:" << destChunkId;
                        return  destChunkId;;
                    }
                }
            }
        }
    }
    return "";
}


ClusterStatistics::ShardStatistics *
BalancerChunkSelectionPolicyImpl::_getMaxCpuUsageShard(ShardStatisticsVector &shardStats,
                                                       set<ShardId> &excludedShards) {
    ClusterStatistics::ShardStatistics *best  = nullptr;
    double cpuUsage = -1;
    size_t shardSize = shardStats.size();
    for (size_t i = 0; i < shardSize; i++) {
        if (excludedShards.count(shardStats[i].shardId))
            continue;
        if (shardStats[i].cpuInfo.CpuUsage > cpuUsage) {
            // TODO Judge chunks num of the shard
            cpuUsage = shardStats[i].cpuInfo.CpuUsage;
            best = &shardStats[i];
        }
    }
    return best;
}

ClusterStatistics::ShardStatistics *
BalancerChunkSelectionPolicyImpl::_getMinCpuUsageShard(ShardStatisticsVector &shardStats,
                                                       set<ShardId> &excludedShards) {

    ClusterStatistics::ShardStatistics *best = nullptr;
    int cpuUsage = 100000;
    size_t shardSize = shardStats.size();
    for (size_t i = 0; i < shardSize; i++) {
        if (excludedShards.count(shardStats[i].shardId))
            continue;
        if (shardStats[i].cpuInfo.CpuUsage < cpuUsage) {
            // TODO Judge chunks num of the shard
            cpuUsage = shardStats[i].cpuInfo.CpuUsage;
            best = &shardStats[i];
        }
    }
    return best;
}


const ChunkType * BalancerChunkSelectionPolicyImpl::getChunkTypeByChunkId(const std::vector<ChunkType> &chunks, const ChunkId &chunkId) {
    size_t chunkSize = chunks.size();
    for (size_t i = 0; i < chunkSize; i++) {
        index_LOG(1) << "getChunkTypeByChunkId get a chunid " << chunks[i].toBSON().toString();
        ChunkType changer;
        if (changer.toID(chunkId.toString()) == chunks[i].getID()) {
            index_LOG(1) << "getChunkTypeByChunkId dest chukid " << chunks[i].getID() ;
            return &(chunks[i]);
        }
    }
    return nullptr;
}

bool  BalancerChunkSelectionPolicyImpl::isShardCollection(NamespaceString& ns, std::vector<CollectionType> & collections) {
    for(CollectionType col: collections)
    {
        index_LOG(1) << "isShardCollection for " << col.fullNs() << ",tableType:" <<  static_cast<int>(col.getTabType()) ;
        if(col.getNs() == ns)
         {
             if( col.getTabType() == CollectionType::TableType::kSharded)
             {
                 return true;
             }
             return false;
         }
    }
    return false;
}

StatusWith<ChunkType> BalancerChunkSelectionPolicyImpl::getChunkById(OperationContext* txn, string chunkId)
{
    ReadPreferenceSetting kPrimaryOnlyReadPreference{ReadPreference::PrimaryOnly};
    auto findChunkStatus =
            Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                    txn,
                    kPrimaryOnlyReadPreference,
                    repl::ReadConcernLevel::kLocalReadConcern,
                    NamespaceString(ChunkType::ConfigNS),
                    BSON(ChunkType::name(chunkId)),
                    BSONObj(),
                    boost::none);

    if (!findChunkStatus.isOK()) {
        index_log() << "[_moveChunks] failed to get info of chunk " << chunkId;
        return {ErrorCodes::ChunkNotFound,
                str::stream() << "Unable to find constraints information for shard " << chunkId};

    }

    auto chunkDocs = findChunkStatus.getValue().docs;
    if (chunkDocs.size() == 0) {
        index_log() << "[_moveChunks] there is no chunk named " << chunkId << "is found";
        return {ErrorCodes::ChunkNotFound,
                str::stream() << "Unable to find constraints information for shard " << chunkId};
    }

    auto chunkDocStatus = ChunkType::fromBSON(chunkDocs.front());
    if (!chunkDocStatus.isOK()) {
        index_log() << "[_moveChunks] failed to get chunktype from BSON for " << chunkId;
        return {ErrorCodes::ChunkNotFound,
                str::stream() << "Unable to find constraints information for shard " << chunkId};
    }
    return chunkDocStatus.getValue();
}

StatusWith<MigrateInfoVector> BalancerChunkSelectionPolicyImpl::selectMinTpsChunksToMove(
        OperationContext *txn) {
    BalancerConfiguration *balancerConfig = Grid::get(txn)->getBalancerConfiguration();
    auto shardStatsStatus = _clusterStats->getStats(txn);
    if (!shardStatsStatus.isOK()) {
        index_LOG(0) << "get shardStatsStatus error.";
        return shardStatsStatus.getStatus();
    }
    auto shardStats = std::move(shardStatsStatus.getValue());
    if (shardStats.size() < 2) {
        index_LOG(0) << "no more than two shard, no chunk can be selected to move.";
        return MigrateInfoVector{};
    }
    std::vector<CollectionType> collections;
    Status collsStatus =
            Grid::get(txn)->catalogClient(txn)->getCollections(txn, nullptr, &collections, nullptr);
    if (!collsStatus.isOK()) {
        return collsStatus;
    }

    if (collections.empty()) {
        index_LOG(0) << "no collections now, no chunk can be selected to move.";
        return MigrateInfoVector{};
    }
    MigrateInfoVector candidateChunks;
    //TODO: if no min shard fullfilled , continue to find  little min shard ,usedShards store this
    set<ShardId> usedShards;
    size_t  maxChunkForShard = ConfigReader::getInstance()->getDecimalNumber<size_t>(
            "PublicOptions", "max_chunk_count_in_one_shard");
    if(maxChunkForShard <= 0)
    {
        index_LOG(1) << " get max chunk for shard failed";
        maxChunkForShard = 400; //TODO
    }
    const ClusterStatistics::ShardStatistics * max  = _getMaxCpuUsageShard(shardStats, usedShards);
    const ClusterStatistics::ShardStatistics * min = _getMinCpuUsageShard(shardStats, usedShards);
    index_LOG(1) << " get max cpu shard:" << max->shardId << ", cpuUsage" << max->cpuInfo.CpuUsage;
    index_LOG(1) << " get min cpu shard:" << min->shardId << ", cpuUsage" << min->cpuInfo.CpuUsage;
    if (max == min) {
        index_LOG(1) << "max cpu is same with min cpu shard, no chunk can be selected to move.";
        return MigrateInfoVector{};
    }
    if (nullptr != max && nullptr != min) {
        if ((max->cpuInfo.CpuUsage - min->cpuInfo.CpuUsage) <= balancerConfig->getThreshold()) {
            index_LOG(1) << "max cpu shard and min cpu shard diff less than " << balancerConfig->getThreshold();
            return MigrateInfoVector{};
        }

        unsigned long chunkSize = max->chunkStatistics.size();
        if (chunkSize <= 0) {
            index_LOG(1) << "shard :" << max->shardId << "has no chunk, no chunk can be selected to move.";
            return MigrateInfoVector{};
        }
        std::string candiChunkId = findChunk(txn, maxChunkForShard, collections, shardStats, max, min);
        if (!candiChunkId.empty()) {
            index_LOG(1) << "get dest Chunk" <<  candiChunkId ;
            {
                auto candiChunk = getChunkById(txn, candiChunkId);
                if(!candiChunk.getStatus().isOK())
                {
                    return MigrateInfoVector{};
                }
                index_LOG(1) << "get dest Chunk" <<  candiChunk.getValue().toString() ;
                MigrateInfo bestMig(min->shardId, candiChunk.getValue());
                candidateChunks.push_back(bestMig);
                return candidateChunks;
            }
        }
        usedShards.emplace(min->shardId);
        index_LOG(1) << "not chunks can be founded to move to shard:" <<  min->shardId.toString();
        return MigrateInfoVector{};
    }
    else {
        index_LOG(1) << "invalid shard .";
        return MigrateInfoVector{};
    }
}



StatusWith<MigrateInfoVector> BalancerChunkSelectionPolicyImpl::selectChunksToMove(
    OperationContext* txn, bool aggressiveBalanceHint) {
    auto shardStatsStatus = _clusterStats->getStats(txn);
    if (!shardStatsStatus.isOK()) {
        return shardStatsStatus.getStatus();
    }

    const auto shardStats = std::move(shardStatsStatus.getValue());

    if (shardStats.size() < 2) {
        return MigrateInfoVector{};
    }

    vector<CollectionType> collections;

    Status collsStatus =
        Grid::get(txn)->catalogClient(txn)->getCollections(txn, nullptr, &collections, nullptr);
    if (!collsStatus.isOK()) {
        return collsStatus;
    }

    if (collections.empty()) {
        return MigrateInfoVector{};
    }

    MigrateInfoVector candidateChunks;

    for (const auto& coll : collections) {
        if (coll.getTabType() != CollectionType::TableType::kSharded ||
            coll.getAllowBalance() == false) {
            continue;
        }
        const NamespaceString nss(coll.getNs());

        if (!coll.getAllowBalance()) {
            LOG(1) << "Not balancing collection " << nss << "; explicitly disabled.";
            continue;
        }

        auto candidatesStatus =
            _getMigrateCandidatesForCollection(txn, nss, shardStats, aggressiveBalanceHint);
        if (candidatesStatus == ErrorCodes::NamespaceNotFound) {
            // Namespace got dropped before we managed to get to it, so just skip it
            continue;
        } else if (!candidatesStatus.isOK()) {
            warning() << "Unable to balance collection " << nss.ns()
                      << causedBy(candidatesStatus.getStatus());
            continue;
        }

        candidateChunks.insert(candidateChunks.end(),
                               std::make_move_iterator(candidatesStatus.getValue().begin()),
                               std::make_move_iterator(candidatesStatus.getValue().end()));
    }

    return candidateChunks;
}

StatusWith<boost::optional<MigrateInfo>>
BalancerChunkSelectionPolicyImpl::selectSpecificChunkToMove(OperationContext* txn,
                                                            const ChunkType& chunk) {
    auto shardStatsStatus = _clusterStats->getStats(txn);
    if (!shardStatsStatus.isOK()) {
        return shardStatsStatus.getStatus();
    }

    const auto shardStats = std::move(shardStatsStatus.getValue());

    const NamespaceString nss(chunk.getNS());

    auto scopedCMStatus = ScopedChunkManager::getExisting(txn, nss);
    if (!scopedCMStatus.isOK()) {
        return scopedCMStatus.getStatus();
    }

    auto scopedCM = std::move(scopedCMStatus.getValue());
    ChunkManager* const cm = scopedCM.cm();

    const auto collInfoStatus = createCollectionDistributionStatus(txn, shardStats, cm);
    if (!collInfoStatus.isOK()) {
        return collInfoStatus.getStatus();
    }

    const DistributionStatus& distribution = collInfoStatus.getValue();

    return BalancerPolicy::balanceSingleChunk(chunk, shardStats, distribution);
}

Status BalancerChunkSelectionPolicyImpl::checkMoveAllowed(OperationContext* txn,
                                                          const ChunkType& chunk,
                                                          const ShardId& newShardId) {
    auto shardStatsStatus = _clusterStats->getStats(txn, true);
    if (!shardStatsStatus.isOK()) {
        return shardStatsStatus.getStatus();
    }

    auto shardStats = std::move(shardStatsStatus.getValue());

    const NamespaceString nss(chunk.getNS());

    auto scopedCMStatus = ScopedChunkManager::getExisting(txn, nss);
    if (!scopedCMStatus.isOK()) {
        return scopedCMStatus.getStatus();
    }

    auto scopedCM = std::move(scopedCMStatus.getValue());
    ChunkManager* const cm = scopedCM.cm();

    const auto collInfoStatus = createCollectionDistributionStatus(txn, shardStats, cm);
    if (!collInfoStatus.isOK()) {
        return collInfoStatus.getStatus();
    }

    const DistributionStatus& distribution = collInfoStatus.getValue();

    auto newShardIterator =
        std::find_if(shardStats.begin(),
                     shardStats.end(),
                     [&newShardId](const ClusterStatistics::ShardStatistics& stat) {
                         return stat.shardId == newShardId;
                     });
    if (newShardIterator == shardStats.end()) {
        return {ErrorCodes::ShardNotFound,
                str::stream() << "Unable to find constraints information for shard " << newShardId
                              << ". Move to this shard will be disallowed."};
    }

    return BalancerPolicy::isShardSuitableReceiver(*newShardIterator,
                                                   distribution.getTagForChunk(chunk));
}

StatusWith<IndexSplitInfoVector> BalancerChunkSelectionPolicyImpl::_getSplitCandidatesForCollection(
    OperationContext* txn, const NamespaceString& nss, const ShardStatisticsVector& shardStats) {
    auto scopedCMStatus = ScopedChunkManager::getExisting(txn, nss);
    if (!scopedCMStatus.isOK()) {
        return scopedCMStatus.getStatus();
    }

    auto scopedCM = std::move(scopedCMStatus.getValue());
    ChunkManager* const cm = scopedCM.cm();

    const auto& shardKeyPattern = cm->getShardKeyPattern().getKeyPattern();

    const auto collInfoStatus = createCollectionDistributionStatus(txn, shardStats, cm);
    if (!collInfoStatus.isOK()) {
        return collInfoStatus.getStatus();
    }

    const DistributionStatus& distribution = collInfoStatus.getValue();

    // Accumulate split points for the same chunk together
    IndexSplitInfoVector splits;

    for (const auto& tagRangeEntry : distribution.tagRanges()) {
        const auto& tagRange = tagRangeEntry.second;

        shared_ptr<Chunk> chunkAtZoneMin =
            cm->findIntersectingChunkWithSimpleCollation(txn, tagRange.min);
        invariant(chunkAtZoneMin->getMax().woCompare(tagRange.min) > 0);

        if (chunkAtZoneMin->getMin().woCompare(tagRange.min)) {
            ChunkType chunkType;
            chunkType.setNS(nss.ns());
            chunkAtZoneMin->constructChunkType(&chunkType);
            splits.emplace_back(chunkType, tagRange.min);
        }

        // The global max key can never fall in the middle of a chunk
        if (!tagRange.max.woCompare(shardKeyPattern.globalMax()))
            continue;

        shared_ptr<Chunk> chunkAtZoneMax =
            cm->findIntersectingChunkWithSimpleCollation(txn, tagRange.max);

        // We need to check that both the chunk's minKey does not match the zone's max and also that
        // the max is not equal, which would only happen in the case of the zone ending in MaxKey.
        if (chunkAtZoneMax->getMin().woCompare(tagRange.max) &&
            chunkAtZoneMax->getMax().woCompare(tagRange.max)) {
            ChunkType chunkType;
            chunkType.setNS(nss.ns());
            chunkAtZoneMax->constructChunkType(&chunkType);
            splits.emplace_back(chunkType, tagRange.max);
        }
    }

    return splits;
}


StatusWith<IndexSplitInfoVector>
BalancerChunkSelectionPolicyImpl::_indexGetSplitCandidatesForCollection(
    OperationContext* txn, const NamespaceString& nss, const ShardStatisticsVector& shardStats) {
    auto scopedCMStatus = ScopedChunkManager::getExisting(txn, nss);
    if (!scopedCMStatus.isOK()) {
        return scopedCMStatus.getStatus();
    }

    auto scopedCM = std::move(scopedCMStatus.getValue());
    ChunkManager* const cm = scopedCM.cm();

    const auto collInfoStatus = createCollectionDistributionStatus(txn, shardStats, cm);
    if (!collInfoStatus.isOK()) {
        return collInfoStatus.getStatus();
    }

    const DistributionStatus& distribution = collInfoStatus.getValue();


    index_LOG(2) << "[Auto-SplitChunk]_indexGetSplitCandidatesForCollection";
    map<string, ClusterStatistics::ChunkStatistics> chunkNameToStatis;
    for (const auto& shardStat : shardStats) {
        for (const auto& chunkStat : shardStat.chunkStatistics) {
            chunkNameToStatis[chunkStat.chunkId.toString()] = chunkStat;
            index_LOG(1) << "[Auto-SplitChunk]shardId: " << shardStat.shardId.toString()
                         << ", chunkId: " << chunkStat.chunkId.toString()
                         << ", ns: " << chunkStat.ns << ", currSizeMB: " << chunkStat.currSizeMB
                         << ", documentNum: " << chunkStat.documentNum << ", tps: " << chunkStat.tps
                         << ", sstfileNum: " << chunkStat.sstfileNum
                         << ", sstfilesize: " << chunkStat.sstfilesize;
        }
    }

    IndexSplitInfoVector splits;

    // Check for chunks, which are in above the split threshold
    int shardNum = int(shardStats.size());
    for (const auto& shardStat : shardStats) {
        const vector<ChunkType>& chunks = distribution.getChunks(shardStat.shardId);

        if (chunks.empty()) {
            index_LOG(2) << "[Auto-SplitChunk]Chunks is empty.";
            continue;
        }

        index_LOG(1) << "[Auto-SplitChunk]collectionNs: " << nss.toString()
                     << ", shardId: " << shardStat.shardId.toString()
                     << ", chunkcount: " << chunks.size();

        // Scan all chunks to find if we should perform a split operation
        for (const auto& chunk : chunks) {
            if (chunkNameToStatis.find(chunk.getName()) == chunkNameToStatis.end()) {
                index_err() << "[Auto-SplitChunk]Unable to find chunk from chunkNameToStatis map, "
                               "chunkName is "
                            << chunk.getName();
                continue;
            }

            auto chunkStat = chunkNameToStatis[chunk.getName()];
            auto splitThreshold = cm->getCurrentDesiredChunkSize(shardNum);

            if (!chunkStat.isAboveSplitThreshold(splitThreshold)) {
                index_LOG(2) << "[Auto-SplitChunk]chunk(" << chunk.getName()
                             << ") is not above SplitThreashold";
                continue;
            }

            splits.emplace_back(chunk, BSONObj());
        }
    }

    return splits;
}

StatusWith<MigrateInfoVector> BalancerChunkSelectionPolicyImpl::_getMigrateCandidatesForCollection(
    OperationContext* txn,
    const NamespaceString& nss,
    const ShardStatisticsVector& shardStats,
    bool aggressiveBalanceHint) {
    auto scopedCMStatus = ScopedChunkManager::getExisting(txn, nss);
    if (!scopedCMStatus.isOK()) {
        return scopedCMStatus.getStatus();
    }

    auto scopedCM = std::move(scopedCMStatus.getValue());
    ChunkManager* const cm = scopedCM.cm();

    const auto& shardKeyPattern = cm->getShardKeyPattern().getKeyPattern();

    const auto collInfoStatus = createCollectionDistributionStatus(txn, shardStats, cm);
    if (!collInfoStatus.isOK()) {
        return collInfoStatus.getStatus();
    }

    const DistributionStatus& distribution = collInfoStatus.getValue();

    for (const auto& tagRangeEntry : distribution.tagRanges()) {
        const auto& tagRange = tagRangeEntry.second;

        shared_ptr<Chunk> chunkAtZoneMin =
            cm->findIntersectingChunkWithSimpleCollation(txn, tagRange.min);

        if (chunkAtZoneMin->getMin().woCompare(tagRange.min)) {
            return {ErrorCodes::IllegalOperation,
                    str::stream()
                        << "Tag boundaries "
                        << tagRange.toString()
                        << " fall in the middle of an existing chunk "
                        << ChunkRange(chunkAtZoneMin->getMin(), chunkAtZoneMin->getMax()).toString()
                        << ". Balancing for collection "
                        << nss.ns()
                        << " will be postponed until the chunk is split appropriately."};
        }

        // The global max key can never fall in the middle of a chunk
        if (!tagRange.max.woCompare(shardKeyPattern.globalMax()))
            continue;

        shared_ptr<Chunk> chunkAtZoneMax =
            cm->findIntersectingChunkWithSimpleCollation(txn, tagRange.max);

        // We need to check that both the chunk's minKey does not match the zone's max and also that
        // the max is not equal, which would only happen in the case of the zone ending in MaxKey.
        if (chunkAtZoneMax->getMin().woCompare(tagRange.max) &&
            chunkAtZoneMax->getMax().woCompare(tagRange.max)) {
            return {ErrorCodes::IllegalOperation,
                    str::stream()
                        << "Tag boundaries "
                        << tagRange.toString()
                        << " fall in the middle of an existing chunk "
                        << ChunkRange(chunkAtZoneMax->getMin(), chunkAtZoneMax->getMax()).toString()
                        << ". Balancing for collection "
                        << nss.ns()
                        << " will be postponed until the chunk is split appropriately."};
        }
    }

    return BalancerPolicy::balance(shardStats, distribution, aggressiveBalanceHint);
}

}  // namespace mongo

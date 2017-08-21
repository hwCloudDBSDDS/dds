/**
 * Copyright (c) 2012-2014 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/pipeline_d.h"

#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/document_validation.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/exec/fetch.h"
#include "mongo/db/exec/index_iterator.h"
#include "mongo/db/exec/multi_iterator.h"
#include "mongo/db/exec/shard_filter.h"
#include "mongo/db/exec/working_set.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/matcher/extensions_callback_real.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_cursor.h"
#include "mongo/db/pipeline/document_source_match.h"
#include "mongo/db/pipeline/document_source_merge_cursors.h"
#include "mongo/db/pipeline/document_source_sample.h"
#include "mongo/db/pipeline/document_source_sample_from_random_cursor.h"
#include "mongo/db/pipeline/document_source_single_document_transformation.h"
#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/query/collation/collator_interface.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/db/query/plan_summary_stats.h"
#include "mongo/db/query/query_planner.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/service_context.h"
#include "mongo/db/stats/fill_locker_info.h"
#include "mongo/db/stats/storage_stats.h"
#include "mongo/db/stats/top.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/rpc/metadata/client_metadata_ismaster.h"
#include "mongo/s/chunk_version.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/net/sock.h"
#include "mongo/util/time_support.h"

namespace mongo {

using boost::intrusive_ptr;
using std::shared_ptr;
using std::string;
using std::unique_ptr;

namespace {
class MongodImplementation final : public DocumentSourceNeedsMongod::MongodInterface {
public:
    MongodImplementation(const intrusive_ptr<ExpressionContext>& ctx)
        : _ctx(ctx), _client(ctx->opCtx) {}

    void setOperationContext(OperationContext* opCtx) {
        invariant(_ctx->opCtx == opCtx);
        _client.setOpCtx(opCtx);
    }

    DBClientBase* directClient() final {
        return &_client;
    }

    bool isSharded(const NamespaceString& nss) final {
        AutoGetCollectionForReadCommand autoColl(_ctx->opCtx, nss);
        // TODO SERVER-24960: Use CollectionShardingState::collectionIsSharded() to confirm sharding
        // state.
        auto css = CollectionShardingState::get(_ctx->opCtx, nss);
        return bool(css->getMetadata());
    }

    BSONObj insert(const NamespaceString& ns, const std::vector<BSONObj>& objs) final {
        boost::optional<DisableDocumentValidation> maybeDisableValidation;
        if (_ctx->bypassDocumentValidation)
            maybeDisableValidation.emplace(_ctx->opCtx);

        _client.insert(ns.ns(), objs);
        return _client.getLastErrorDetailed();
    }

    CollectionIndexUsageMap getIndexStats(OperationContext* opCtx,
                                          const NamespaceString& ns) final {
        AutoGetCollectionForReadCommand autoColl(opCtx, ns);

        Collection* collection = autoColl.getCollection();
        if (!collection) {
            LOG(2) << "Collection not found on index stats retrieval: " << ns.ns();
            return CollectionIndexUsageMap();
        }

        return collection->infoCache()->getIndexUsageStats();
    }

    void appendLatencyStats(const NamespaceString& nss,
                            bool includeHistograms,
                            BSONObjBuilder* builder) const final {
        Top::get(_ctx->opCtx->getServiceContext())
            .appendLatencyStats(nss.ns(), includeHistograms, builder);
    }

    Status appendStorageStats(const NamespaceString& nss,
                              const BSONObj& param,
                              BSONObjBuilder* builder) const final {
        return appendCollectionStorageStats(_ctx->opCtx, nss, param, builder);
    }

    Status appendRecordCount(const NamespaceString& nss, BSONObjBuilder* builder) const final {
        return appendCollectionRecordCount(_ctx->opCtx, nss, builder);
    }

    BSONObj getCollectionOptions(const NamespaceString& nss) final {
        const auto infos =
            _client.getCollectionInfos(nss.db().toString(), BSON("name" << nss.coll()));
        return infos.empty() ? BSONObj() : infos.front().getObjectField("options").getOwned();
    }

    Status renameIfOptionsAndIndexesHaveNotChanged(
        const BSONObj& renameCommandObj,
        const NamespaceString& targetNs,
        const BSONObj& originalCollectionOptions,
        const std::list<BSONObj>& originalIndexes) final {
        Lock::GlobalWrite globalLock(_ctx->opCtx);

        if (SimpleBSONObjComparator::kInstance.evaluate(originalCollectionOptions !=
                                                        getCollectionOptions(targetNs))) {
            return {ErrorCodes::CommandFailed,
                    str::stream() << "collection options of target collection " << targetNs.ns()
                                  << " changed during processing. Original options: "
                                  << originalCollectionOptions
                                  << ", new options: "
                                  << getCollectionOptions(targetNs)};
        }

        auto currentIndexes = _client.getIndexSpecs(targetNs.ns());
        if (originalIndexes.size() != currentIndexes.size() ||
            !std::equal(originalIndexes.begin(),
                        originalIndexes.end(),
                        currentIndexes.begin(),
                        SimpleBSONObjComparator::kInstance.makeEqualTo())) {
            return {ErrorCodes::CommandFailed,
                    str::stream() << "indexes of target collection " << targetNs.ns()
                                  << " changed during processing."};
        }

        BSONObj info;
        bool ok = _client.runCommand("admin", renameCommandObj, info);
        return ok ? Status::OK() : Status{ErrorCodes::CommandFailed,
                                          str::stream() << "renameCollection failed: " << info};
    }

    StatusWith<std::unique_ptr<Pipeline, Pipeline::Deleter>> makePipeline(
        const std::vector<BSONObj>& rawPipeline,
        const boost::intrusive_ptr<ExpressionContext>& expCtx) final {
        // 'expCtx' may represent the settings for an aggregation pipeline on a different namespace
        // than the DocumentSource this MongodImplementation is injected into, but both
        // ExpressionContext instances should still have the same OperationContext.
        invariant(_ctx->opCtx == expCtx->opCtx);

        auto pipeline = Pipeline::parse(rawPipeline, expCtx);
        if (!pipeline.isOK()) {
            return pipeline.getStatus();
        }

        pipeline.getValue()->optimizePipeline();

        AutoGetCollectionForReadCommand autoColl(expCtx->opCtx, expCtx->ns);

        // makePipeline() is only called to perform secondary aggregation requests and expects the
        // collection representing the document source to be not-sharded. We confirm sharding state
        // here to avoid taking a collection lock elsewhere for this purpose alone.
        // TODO SERVER-27616: This check is incorrect in that we don't acquire a collection cursor
        // until after we release the lock, leaving room for a collection to be sharded inbetween.
        // TODO SERVER-24960: Use CollectionShardingState::collectionIsSharded() to confirm sharding
        // state.
        auto css = CollectionShardingState::get(_ctx->opCtx, expCtx->ns);
        uassert(4567, "from collection cannot be sharded", !bool(css->getMetadata()));

        PipelineD::prepareCursorSource(
            autoColl.getCollection(), expCtx->ns, nullptr, pipeline.getValue().get());

        return pipeline;
    }

    std::vector<BSONObj> getCurrentOps(CurrentOpConnectionsMode connMode,
                                       CurrentOpUserMode userMode,
                                       CurrentOpTruncateMode truncateMode) const {
        AuthorizationSession* ctxAuth = AuthorizationSession::get(_ctx->opCtx->getClient());

        const std::string hostName = getHostNameCachedAndPort();

        std::vector<BSONObj> ops;

        for (ServiceContext::LockedClientsCursor cursor(
                 _ctx->opCtx->getClient()->getServiceContext());
             Client* client = cursor.next();) {
            invariant(client);

            stdx::lock_guard<Client> lk(*client);

            // If auth is disabled, ignore the allUsers parameter.
            if (ctxAuth->getAuthorizationManager().isAuthEnabled() &&
                userMode == CurrentOpUserMode::kExcludeOthers &&
                !ctxAuth->isCoauthorizedWithClient(client)) {
                continue;
            }

            const OperationContext* clientOpCtx = client->getOperationContext();

            if (!clientOpCtx && connMode == CurrentOpConnectionsMode::kExcludeIdle) {
                continue;
            }

            BSONObjBuilder infoBuilder;

            infoBuilder.append("host", hostName);

            client->reportState(infoBuilder);

            const auto& clientMetadata =
                ClientMetadataIsMasterState::get(client).getClientMetadata();

            if (clientMetadata) {
                auto appName = clientMetadata.get().getApplicationName();
                if (!appName.empty()) {
                    infoBuilder.append("appName", appName);
                }

                auto clientMetadataDocument = clientMetadata.get().getDocument();
                infoBuilder.append("clientMetadata", clientMetadataDocument);
            }

            // Fill out the rest of the BSONObj with opCtx specific details.
            infoBuilder.appendBool("active", static_cast<bool>(clientOpCtx));
            infoBuilder.append(
                "currentOpTime",
                _ctx->opCtx->getServiceContext()->getPreciseClockSource()->now().toString());

            if (clientOpCtx) {
                infoBuilder.append("opid", clientOpCtx->getOpID());
                if (clientOpCtx->isKillPending()) {
                    infoBuilder.append("killPending", true);
                }

                if (clientOpCtx->getLogicalSessionId()) {
                    BSONObjBuilder bob(infoBuilder.subobjStart("lsid"));
                    clientOpCtx->getLogicalSessionId()->serialize(&bob);
                }

                CurOp::get(clientOpCtx)
                    ->reportState(&infoBuilder,
                                  (truncateMode == CurrentOpTruncateMode::kTruncateOps));

                Locker::LockerInfo lockerInfo;
                clientOpCtx->lockState()->getLockerInfo(&lockerInfo);
                fillLockerInfo(lockerInfo, infoBuilder);
            }

            ops.emplace_back(infoBuilder.obj());
        }

        return ops;
    }

    std::string getShardName(OperationContext* opCtx) const {
        if (ShardingState::get(opCtx)->enabled()) {
            return ShardingState::get(opCtx)->getShardName();
        }

        return std::string();
    }

private:
    intrusive_ptr<ExpressionContext> _ctx;
    DBDirectClient _client;
};

/**
 * Returns a PlanExecutor which uses a random cursor to sample documents if successful. Returns {}
 * if the storage engine doesn't support random cursors, or if 'sampleSize' is a large enough
 * percentage of the collection.
 */
StatusWith<unique_ptr<PlanExecutor, PlanExecutor::Deleter>> createRandomCursorExecutor(
    Collection* collection, OperationContext* opCtx, long long sampleSize, long long numRecords) {
    double kMaxSampleRatioForRandCursor = 0.05;
    if (sampleSize > numRecords * kMaxSampleRatioForRandCursor || numRecords <= 100) {
        return {nullptr};
    }

    // Attempt to get a random cursor from the RecordStore. If the RecordStore does not support
    // random cursors, attempt to get one from the _id index.
    std::unique_ptr<RecordCursor> rsRandCursor =
        collection->getRecordStore()->getRandomCursor(opCtx);

    auto ws = stdx::make_unique<WorkingSet>();
    std::unique_ptr<PlanStage> stage;

    if (rsRandCursor) {
        stage = stdx::make_unique<MultiIteratorStage>(opCtx, ws.get(), collection);
        static_cast<MultiIteratorStage*>(stage.get())->addIterator(std::move(rsRandCursor));

    } else {
        auto indexCatalog = collection->getIndexCatalog();
        auto indexDescriptor = indexCatalog->findIdIndex(opCtx);

        if (!indexDescriptor) {
            // There was no _id index.
            return {nullptr};
        }

        IndexAccessMethod* idIam = indexCatalog->getIndex(indexDescriptor);
        auto idxRandCursor = idIam->newRandomCursor(opCtx);

        if (!idxRandCursor) {
            // Storage engine does not support any type of random cursor.
            return {nullptr};
        }

        auto idxIterator = stdx::make_unique<IndexIteratorStage>(opCtx,
                                                                 ws.get(),
                                                                 collection,
                                                                 idIam,
                                                                 indexDescriptor->keyPattern(),
                                                                 std::move(idxRandCursor));
        stage = stdx::make_unique<FetchStage>(
            opCtx, ws.get(), idxIterator.release(), nullptr, collection);
    }

    {
        AutoGetCollectionForRead autoColl(opCtx, collection->ns());

        // If we're in a sharded environment, we need to filter out documents we don't own.
        if (ShardingState::get(opCtx)->needCollectionMetadata(opCtx, collection->ns().ns())) {
            auto shardFilterStage = stdx::make_unique<ShardFilterStage>(
                opCtx,
                CollectionShardingState::get(opCtx, collection->ns())->getMetadata(),
                ws.get(),
                stage.release());
            return PlanExecutor::make(opCtx,
                                      std::move(ws),
                                      std::move(shardFilterStage),
                                      collection,
                                      PlanExecutor::YIELD_AUTO);
        }
    }

    return PlanExecutor::make(
        opCtx, std::move(ws), std::move(stage), collection, PlanExecutor::YIELD_AUTO);
}

StatusWith<std::unique_ptr<PlanExecutor, PlanExecutor::Deleter>> attemptToGetExecutor(
    OperationContext* opCtx,
    Collection* collection,
    const NamespaceString& nss,
    const intrusive_ptr<ExpressionContext>& pExpCtx,
    BSONObj queryObj,
    BSONObj projectionObj,
    BSONObj sortObj,
    const AggregationRequest* aggRequest,
    const size_t plannerOpts) {
    auto qr = stdx::make_unique<QueryRequest>(nss);
    switch (pExpCtx->tailableMode) {
        case ExpressionContext::TailableMode::kNormal:
            break;
        case ExpressionContext::TailableMode::kTailableAndAwaitData:
            qr->setTailable(true);
            qr->setAwaitData(true);
            break;
    }
    qr->setFilter(queryObj);
    qr->setProj(projectionObj);
    qr->setSort(sortObj);
    if (aggRequest) {
        qr->setExplain(static_cast<bool>(aggRequest->getExplain()));
        qr->setHint(aggRequest->getHint());
    }

    // If the pipeline has a non-null collator, set the collation option to the result of
    // serializing the collator's spec back into BSON. We do this in order to fill in all options
    // that the user omitted.
    //
    // If pipeline has a null collator (representing the "simple" collation), we simply set the
    // collation option to the original user BSON, which is either the empty object (unspecified),
    // or the specification for the "simple" collation.
    qr->setCollation(pExpCtx->getCollator() ? pExpCtx->getCollator()->getSpec().toBSON()
                                            : pExpCtx->collation);

    const ExtensionsCallbackReal extensionsCallback(pExpCtx->opCtx, &nss);

    auto cq = CanonicalQuery::canonicalize(opCtx,
                                           std::move(qr),
                                           pExpCtx,
                                           extensionsCallback,
                                           MatchExpressionParser::AllowedFeatures::kText |
                                               MatchExpressionParser::AllowedFeatures::kExpr);

    if (!cq.isOK()) {
        // Return an error instead of uasserting, since there are cases where the combination of
        // sort and projection will result in a bad query, but when we try with a different
        // combination it will be ok. e.g. a sort by {$meta: 'textScore'}, without any projection
        // will fail, but will succeed when the corresponding '$meta' projection is passed in
        // another attempt.
        return {cq.getStatus()};
    }

    return getExecutor(
        opCtx, collection, std::move(cq.getValue()), PlanExecutor::YIELD_AUTO, plannerOpts);
}
}  // namespace

void PipelineD::prepareCursorSource(Collection* collection,
                                    const NamespaceString& nss,
                                    const AggregationRequest* aggRequest,
                                    Pipeline* pipeline) {
    auto expCtx = pipeline->getContext();

    // We will be modifying the source vector as we go.
    Pipeline::SourceContainer& sources = pipeline->_sources;

    // Inject a MongodImplementation to sources that need them.
    for (auto&& source : sources) {
        DocumentSourceNeedsMongod* needsMongod =
            dynamic_cast<DocumentSourceNeedsMongod*>(source.get());
        if (needsMongod) {
            needsMongod->injectMongodInterface(std::make_shared<MongodImplementation>(expCtx));
        }
    }

    if (!sources.empty() && !sources.front()->constraints().requiresInputDocSource) {
        return;
    }

    // We are going to generate an input cursor, so we need to be holding the collection lock.
    dassert(expCtx->opCtx->lockState()->isCollectionLockedForMode(nss.ns(), MODE_IS));

    if (!sources.empty()) {
        auto sampleStage = dynamic_cast<DocumentSourceSample*>(sources.front().get());
        // Optimize an initial $sample stage if possible.
        if (collection && sampleStage) {
            const long long sampleSize = sampleStage->getSampleSize();
            const long long numRecords = collection->getRecordStore()->numRecords(expCtx->opCtx);
            auto exec = uassertStatusOK(
                createRandomCursorExecutor(collection, expCtx->opCtx, sampleSize, numRecords));
            if (exec) {
                // Replace $sample stage with $sampleFromRandomCursor stage.
                sources.pop_front();
                std::string idString = collection->ns().isOplog() ? "ts" : "_id";
                sources.emplace_front(DocumentSourceSampleFromRandomCursor::create(
                    expCtx, sampleSize, idString, numRecords));

                addCursorSource(
                    collection,
                    pipeline,
                    expCtx,
                    std::move(exec),
                    pipeline->getDependencies(DepsTracker::MetadataAvailable::kNoMetadata));
                return;
            }
        }
    }

    // Look for an initial match. This works whether we got an initial query or not. If not, it
    // results in a "{}" query, which will be what we want in that case.
    const BSONObj queryObj = pipeline->getInitialQuery();
    if (!queryObj.isEmpty()) {
        if (dynamic_cast<DocumentSourceMatch*>(sources.front().get())) {
            // If a $match query is pulled into the cursor, the $match is redundant, and can be
            // removed from the pipeline.
            sources.pop_front();
        } else {
            // A $geoNear stage, the only other stage that can produce an initial query, is also
            // a valid initial stage and will be handled above.
            MONGO_UNREACHABLE;
        }
    }

    // Find the set of fields in the source documents depended on by this pipeline.
    DepsTracker deps = pipeline->getDependencies(DocumentSourceMatch::isTextQuery(queryObj)
                                                     ? DepsTracker::MetadataAvailable::kTextScore
                                                     : DepsTracker::MetadataAvailable::kNoMetadata);

    BSONObj projForQuery = deps.toProjection();

    /*
      Look for an initial sort; we'll try to add this to the
      Cursor we create.  If we're successful in doing that (further down),
      we'll remove the $sort from the pipeline, because the documents
      will already come sorted in the specified order as a result of the
      index scan.
    */
    intrusive_ptr<DocumentSourceSort> sortStage;
    BSONObj sortObj;
    if (!sources.empty()) {
        sortStage = dynamic_cast<DocumentSourceSort*>(sources.front().get());
        if (sortStage) {
            // build the sort key
            sortObj = sortStage->serializeSortKey(/*explain*/ false).toBson();
        }
    }

    // Create the PlanExecutor.
    auto exec = uassertStatusOK(prepareExecutor(expCtx->opCtx,
                                                collection,
                                                nss,
                                                pipeline,
                                                expCtx,
                                                sortStage,
                                                deps,
                                                queryObj,
                                                aggRequest,
                                                &sortObj,
                                                &projForQuery));


    if (!projForQuery.isEmpty() && !sources.empty()) {
        // Check for redundant $project in query with the same specification as the inclusion
        // projection generated by the dependency optimization.
        auto proj =
            dynamic_cast<DocumentSourceSingleDocumentTransformation*>(sources.front().get());
        if (proj && proj->isSubsetOfProjection(projForQuery)) {
            sources.pop_front();
        }
    }

    addCursorSource(
        collection, pipeline, expCtx, std::move(exec), deps, queryObj, sortObj, projForQuery);
}

StatusWith<std::unique_ptr<PlanExecutor, PlanExecutor::Deleter>> PipelineD::prepareExecutor(
    OperationContext* opCtx,
    Collection* collection,
    const NamespaceString& nss,
    Pipeline* pipeline,
    const intrusive_ptr<ExpressionContext>& expCtx,
    const intrusive_ptr<DocumentSourceSort>& sortStage,
    const DepsTracker& deps,
    const BSONObj& queryObj,
    const AggregationRequest* aggRequest,
    BSONObj* sortObj,
    BSONObj* projectionObj) {
    // The query system has the potential to use an index to provide a non-blocking sort and/or to
    // use the projection to generate a covered plan. If this is possible, it is more efficient to
    // let the query system handle those parts of the pipeline. If not, it is more efficient to use
    // a $sort and/or a ParsedDeps object. Thus, we will determine whether the query system can
    // provide a non-blocking sort or a covered projection before we commit to a PlanExecutor.
    //
    // To determine if the query system can provide a non-blocking sort, we pass the
    // NO_BLOCKING_SORT planning option, meaning 'getExecutor' will not produce a PlanExecutor if
    // the query system would use a blocking sort stage.
    //
    // To determine if the query system can provide a covered projection, we pass the
    // NO_UNCOVERED_PROJECTS planning option, meaning 'getExecutor' will not produce a PlanExecutor
    // if the query system would need to fetch the document to do the projection. The following
    // logic uses the above strategies, with multiple calls to 'attemptToGetExecutor' to determine
    // the most efficient way to handle the $sort and $project stages.
    //
    // LATER - We should attempt to determine if the results from the query are returned in some
    // order so we can then apply other optimizations there are tickets for, such as SERVER-4507.
    size_t plannerOpts = QueryPlannerParams::DEFAULT | QueryPlannerParams::NO_BLOCKING_SORT;

    // If we are connecting directly to the shard rather than through a mongos, don't filter out
    // orphaned documents.
    if (ShardingState::get(opCtx)->needCollectionMetadata(opCtx, nss.ns())) {
        plannerOpts |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
    }

    if (deps.hasNoRequirements()) {
        // If we don't need any fields from the input document, performing a count is faster, and
        // will output empty documents, which is okay.
        plannerOpts |= QueryPlannerParams::IS_COUNT;
    }

    // The only way to get a text score is to let the query system handle the projection. In all
    // other cases, unless the query system can do an index-covered projection and avoid going to
    // the raw record at all, it is faster to have ParsedDeps filter the fields we need.
    if (!deps.getNeedTextScore()) {
        plannerOpts |= QueryPlannerParams::NO_UNCOVERED_PROJECTIONS;
    }

    BSONObj emptyProjection;
    if (sortStage) {
        // See if the query system can provide a non-blocking sort.
        auto swExecutorSort = attemptToGetExecutor(opCtx,
                                                   collection,
                                                   nss,
                                                   expCtx,
                                                   queryObj,
                                                   emptyProjection,
                                                   *sortObj,
                                                   aggRequest,
                                                   plannerOpts);

        if (swExecutorSort.isOK()) {
            // Success! Now see if the query system can also cover the projection.
            auto swExecutorSortAndProj = attemptToGetExecutor(opCtx,
                                                              collection,
                                                              nss,
                                                              expCtx,
                                                              queryObj,
                                                              *projectionObj,
                                                              *sortObj,
                                                              aggRequest,
                                                              plannerOpts);

            std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> exec;
            if (swExecutorSortAndProj.isOK()) {
                // Success! We have a non-blocking sort and a covered projection.
                exec = std::move(swExecutorSortAndProj.getValue());
            } else if (swExecutorSortAndProj == ErrorCodes::QueryPlanKilled) {
                return {ErrorCodes::OperationFailed,
                        str::stream() << "Failed to determine whether query system can provide a "
                                         "covered projection in addition to a non-blocking sort: "
                                      << swExecutorSortAndProj.getStatus().toString()};
            } else {
                // The query system couldn't cover the projection.
                *projectionObj = BSONObj();
                exec = std::move(swExecutorSort.getValue());
            }

            // We know the sort is being handled by the query system, so remove the $sort stage.
            pipeline->_sources.pop_front();

            if (sortStage->getLimitSrc()) {
                // We need to reinsert the coalesced $limit after removing the $sort.
                pipeline->_sources.push_front(sortStage->getLimitSrc());
            }
            return std::move(exec);
        } else if (swExecutorSort == ErrorCodes::QueryPlanKilled) {
            return {
                ErrorCodes::OperationFailed,
                str::stream()
                    << "Failed to determine whether query system can provide a non-blocking sort: "
                    << swExecutorSort.getStatus().toString()};
        }
        // The query system can't provide a non-blocking sort.
        *sortObj = BSONObj();
    }

    // Either there was no $sort stage, or the query system could not provide a non-blocking
    // sort.
    dassert(sortObj->isEmpty());

    // See if the query system can cover the projection.
    auto swExecutorProj = attemptToGetExecutor(opCtx,
                                               collection,
                                               nss,
                                               expCtx,
                                               queryObj,
                                               *projectionObj,
                                               *sortObj,
                                               aggRequest,
                                               plannerOpts);
    if (swExecutorProj.isOK()) {
        // Success! We have a covered projection.
        return std::move(swExecutorProj.getValue());
    } else if (swExecutorProj == ErrorCodes::QueryPlanKilled) {
        return {ErrorCodes::OperationFailed,
                str::stream()
                    << "Failed to determine whether query system can provide a covered projection: "
                    << swExecutorProj.getStatus().toString()};
    }

    // The query system couldn't provide a covered projection.
    *projectionObj = BSONObj();
    // If this doesn't work, nothing will.
    return attemptToGetExecutor(opCtx,
                                collection,
                                nss,
                                expCtx,
                                queryObj,
                                *projectionObj,
                                *sortObj,
                                aggRequest,
                                plannerOpts);
}

void PipelineD::addCursorSource(Collection* collection,
                                Pipeline* pipeline,
                                const intrusive_ptr<ExpressionContext>& expCtx,
                                unique_ptr<PlanExecutor, PlanExecutor::Deleter> exec,
                                DepsTracker deps,
                                const BSONObj& queryObj,
                                const BSONObj& sortObj,
                                const BSONObj& projectionObj) {
    // DocumentSourceCursor expects a yielding PlanExecutor that has had its state saved.
    exec->saveState();

    // Put the PlanExecutor into a DocumentSourceCursor and add it to the front of the pipeline.
    intrusive_ptr<DocumentSourceCursor> pSource =
        DocumentSourceCursor::create(collection, std::move(exec), expCtx);

    // Note the query, sort, and projection for explain.
    pSource->setQuery(queryObj);
    pSource->setSort(sortObj);

    if (deps.hasNoRequirements()) {
        pSource->shouldProduceEmptyDocs();
    }

    if (!projectionObj.isEmpty()) {
        pSource->setProjection(projectionObj, boost::none);
    } else {
        // There may be fewer dependencies now if the sort was covered.
        if (!sortObj.isEmpty()) {
            deps = pipeline->getDependencies(DocumentSourceMatch::isTextQuery(queryObj)
                                                 ? DepsTracker::MetadataAvailable::kTextScore
                                                 : DepsTracker::MetadataAvailable::kNoMetadata);
        }

        pSource->setProjection(deps.toProjection(), deps.toParsedDeps());
    }

    // Add the initial DocumentSourceCursor to the front of the pipeline. Then optimize again in
    // case the new stage can be absorbed with the first stages of the pipeline.
    pipeline->addInitialSource(pSource);
    pipeline->optimizePipeline();
}

std::string PipelineD::getPlanSummaryStr(const Pipeline* pPipeline) {
    if (auto docSourceCursor =
            dynamic_cast<DocumentSourceCursor*>(pPipeline->_sources.front().get())) {
        return docSourceCursor->getPlanSummaryStr();
    }

    return "";
}

void PipelineD::getPlanSummaryStats(const Pipeline* pPipeline, PlanSummaryStats* statsOut) {
    invariant(statsOut);

    if (auto docSourceCursor =
            dynamic_cast<DocumentSourceCursor*>(pPipeline->_sources.front().get())) {
        *statsOut = docSourceCursor->getPlanSummaryStats();
    }

    bool hasSortStage{false};
    for (auto&& source : pPipeline->_sources) {
        if (dynamic_cast<DocumentSourceSort*>(source.get())) {
            hasSortStage = true;
            break;
        }
    }

    statsOut->hasSortStage = hasSortStage;
}

}  // namespace mongo

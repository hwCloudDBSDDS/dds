/**
 *    Copyright (C) 2014 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"


#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/exec/count.h"
#include "mongo/db/modules/rocks/src/gc_common.h"
#include "mongo/db/modules/rocks/src/gc_common.h"
#include "mongo/db/query/explain.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/db/query/plan_summary_stats.h"
#include "mongo/db/query/view_response_formatter.h"
#include "mongo/db/range_preserver.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/server_options.h"
#include "mongo/db/views/resolved_view.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"
#include "mongo/util/util_extend/GlobalConfig.h"

namespace mongo {
namespace {

using std::unique_ptr;
using std::string;
using std::stringstream;

/**
 * Implements the MongoD side of the count command.
 */
class CmdCount : public Command {
public:
    CmdCount() : Command("count") {}

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    virtual bool slaveOk() const {
        // ok on --slave setups
        return repl::getGlobalReplicationCoordinator()->getSettings().isSlave();
    }

    virtual bool slaveOverrideOk() const {
        return true;
    }

    virtual bool maintenanceOk() const {
        return false;
    }

    virtual bool adminOnly() const {
        return false;
    }

    bool supportsReadConcern() const final {
        return true;
    }

    ReadWriteType getReadWriteType() const {
        return ReadWriteType::kRead;
    }

    virtual void help(stringstream& help) const {
        help << "count objects in collection";
    }

    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
        ActionSet actions;
        actions.addAction(ActionType::find);
        out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
    }


    virtual Status explain(OperationContext* txn,
                           const std::string& dbname,
                           const BSONObj& cmdObj,
                           ExplainCommon::Verbosity verbosity,
                           const rpc::ServerSelectionMetadata&,
                           BSONObjBuilder* out) const {
        const bool isExplain = true;
        auto request = CountRequest::parseFromBSON(dbname, cmdObj, isExplain);
        if (!request.isOK()) {
            return request.getStatus();
        }

        if (!request.getValue().getCollation().isEmpty() &&
            serverGlobalParams.featureCompatibility.version.load() ==
                ServerGlobalParams::FeatureCompatibility::Version::k32) {
            return Status(ErrorCodes::InvalidOptions,
                          "The featureCompatibilityVersion must be 3.4 to use collation. See "
                          "http://dochub.mongodb.org/core/3.4-feature-compatibility.");
        }

        // Acquire the db read lock.
        AutoGetCollectionOrViewForRead ctx(txn, request.getValue().getNs());
        Collection* collection = ctx.getCollection();

        if (ctx.getView()) {
            ctx.releaseLocksForView();

            auto viewAggregation = request.getValue().asAggregationCommand();
            if (!viewAggregation.isOK()) {
                return viewAggregation.getStatus();
            }

            std::string errmsg;
            (void)Command::findCommand("aggregate")
                ->run(txn, dbname, viewAggregation.getValue(), 0, errmsg, *out);
            return Status::OK();
        }

        // Prevent chunks from being cleaned up during yields - this allows us to only check the
        // version on initial entry into count.
        RangePreserver preserver(collection);

        auto statusWithPlanExecutor = getExecutorCount(txn,
                                                       collection,
                                                       request.getValue(),
                                                       true,  // explain
                                                       PlanExecutor::YIELD_AUTO);
        if (!statusWithPlanExecutor.isOK()) {
            return statusWithPlanExecutor.getStatus();
        }

        unique_ptr<PlanExecutor> exec = std::move(statusWithPlanExecutor.getValue());

        Explain::explainStages(exec.get(), collection, verbosity, out);
        return Status::OK();
    }

    int getChunksNum(OperationContext* txn, const std::string& ns) {
        repl::ReadConcernLevel readConcern;
        if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
            readConcern = repl::ReadConcernLevel::kLocalReadConcern;
        }

        std::vector<ChunkType> chunks;
        auto status = grid.catalogClient(txn)->getChunks(
            txn, BSON(ChunkType::ns(ns)), BSONObj(), 0, &chunks, nullptr, readConcern);
        if (!status.isOK() || chunks.empty()) {
            index_err() << "getChunksNum error: " << status << "; ns: " << ns;
            return 0;
        }
        return chunks.size();
    }

    int gcCount(OperationContext* txn,
                const string& dbname,
                const NamespaceString& ns,
                const CountRequest& request) {
        if (dbname == "config" && GLOBAL_CONFIG_GET(IsShardingTest) &&
            !GLOBAL_CONFIG_GET(ShowInternalInfo) && GLOBAL_CONFIG_GET(enableGlobalGC) &&
            serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
            if (ns.ns() == "config.chunks") {
                if (!request.getQuery().isEmpty()) {
                    return 0;
                } else {
                    int num = getChunksNum(txn, GcRefNs) + getChunksNum(txn, GcRemoveInfoNs);
                    index_LOG(0) << "count dbname: " << dbname << "; num: " << num;
                    return num;
                }
            } else if (ns.ns() == "config.collections") {
                if (!request.getQuery().isEmpty()) {
                    return 0;
                } else {
                    return 2;
                }
            } else {
                return 0;
            }
        } else {
            return 0;
        }
    }

    virtual bool run(OperationContext* txn,
                     const string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     string& errmsg,
                     BSONObjBuilder& result) {
        const bool isExplain = false;
        const NamespaceString ns(parseNs(dbname, cmdObj));
        NamespaceString nss = ns2chunkHolder().getNsWithChunkId(ns);
        auto request = CountRequest::parseFromBSON(dbname, cmdObj, isExplain);
        if (!request.isOK()) {
            return appendCommandStatus(result, request.getStatus());
        }

        if (!request.getValue().getCollation().isEmpty() &&
            serverGlobalParams.featureCompatibility.version.load() ==
                ServerGlobalParams::FeatureCompatibility::Version::k32) {
            return appendCommandStatus(
                result,
                Status(ErrorCodes::InvalidOptions,
                       "The featureCompatibilityVersion must be 3.4 to use collation. See "
                       "http://dochub.mongodb.org/core/3.4-feature-compatibility."));
        }

        if (AuthorizationSession::get(txn->getClient())->isAuthWithCustomerOrNoAuthUser()) {

            if (request.getValue().getNs().ns() == "admin.system.users") {
                std::set<std::string> buildinUsers;
                UserName::getBuildinUsers(buildinUsers);
                BSONObj filterUsername =
                    BSON(AuthorizationManager::USER_NAME_FIELD_NAME << NIN << buildinUsers);
                BSONObj filterdbname =
                    BSON(AuthorizationManager::ROLE_DB_FIELD_NAME << NE << "admin");
                BSONObj filter = BSON("$or" << BSON_ARRAY(filterUsername << filterdbname));
                BSONObj query = BSON("$and" << BSON_ARRAY(request.getValue().getQuery() << filter));
                request.getValue().setQuery(query);
            }
            if (request.getValue().getNs().ns() == "admin.system.roles") {
                std::set<std::string> buildinRoles;
                RoleName::getBuildinRoles(buildinRoles);
                BSONObj filterUsername =
                    BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME << NIN << buildinRoles);
                BSONObj filterdbname =
                    BSON(AuthorizationManager::ROLE_DB_FIELD_NAME << NE << "admin");
                BSONObj filter = BSON("$or" << BSON_ARRAY(filterUsername << filterdbname));
                BSONObj query = BSON("$and" << BSON_ARRAY(request.getValue().getQuery() << filter));
                request.getValue().setQuery(query);
            }
        }

        int gc_count = gcCount(txn, dbname, ns, request.getValue());

        AutoGetCollectionOrViewForRead ctx(txn, nss);
        Collection* collection = ctx.getCollection();

        // if find with chunkid, but no collection here, we should return stale config, so mongos
        // will retry
        if (!collection && nss.isChunk()) {
            index_LOG(0) << "Collection:" << nss << " not found ";
            return appendCommandStatus(
                result, {ErrorCodes::SendStaleConfig, str::stream() << "Collection [" << nss.toString() << "] not found."});
        }

        auto css = CollectionShardingState::get(txn, request.getValue().getNs());
        css->checkChunkVersionOrThrow(txn);

        if (ctx.getView()) {
            ctx.releaseLocksForView();

            auto viewAggregation = request.getValue().asAggregationCommand();
            if (!viewAggregation.isOK()) {
                return appendCommandStatus(result, viewAggregation.getStatus());
            }

            BSONObjBuilder aggResult;
            (void)Command::findCommand("aggregate")
                ->run(txn, dbname, viewAggregation.getValue(), options, errmsg, aggResult);

            if (ResolvedView::isResolvedViewErrorResponse(aggResult.asTempObj())) {
                result.appendElements(aggResult.obj());
                return false;
            }

            ViewResponseFormatter formatter(aggResult.obj());
            Status formatStatus = formatter.appendAsCountResponse(&result);
            if (!formatStatus.isOK()) {
                return appendCommandStatus(result, formatStatus);
            }
            return true;
        }

        // Prevent chunks from being cleaned up during yields - this allows us to only check the
        // version on initial entry into count.
        RangePreserver preserver(collection);

        auto statusWithPlanExecutor = getExecutorCount(txn,
                                                       collection,
                                                       request.getValue(),
                                                       false,  // !explain
                                                       PlanExecutor::YIELD_AUTO);
        if (!statusWithPlanExecutor.isOK()) {
            return appendCommandStatus(result, statusWithPlanExecutor.getStatus());
        }

        unique_ptr<PlanExecutor> exec = std::move(statusWithPlanExecutor.getValue());

        // Store the plan summary string in CurOp.
        auto curOp = CurOp::get(txn);
        {
            stdx::lock_guard<Client> lk(*txn->getClient());
            curOp->setPlanSummary_inlock(Explain::getPlanSummary(exec.get()));
        }

        Status execPlanStatus = exec->executePlan();
        if (!execPlanStatus.isOK()) {
            return appendCommandStatus(result, execPlanStatus);
        }

        PlanSummaryStats summaryStats;
        Explain::getSummaryStats(*exec, &summaryStats);
        if (collection) {
            collection->infoCache()->notifyOfQuery(txn, summaryStats.indexesUsed);
        }
        curOp->debug().setPlanSummaryMetrics(summaryStats);

        if (curOp->shouldDBProfile()) {
            BSONObjBuilder execStatsBob;
            Explain::getWinningPlanStats(exec.get(), &execStatsBob);
            curOp->debug().execStats = execStatsBob.obj();
        }

        // Plan is done executing. We just need to pull the count out of the root stage.
        invariant(STAGE_COUNT == exec->getRootStage()->stageType());
        CountStage* countStage = static_cast<CountStage*>(exec->getRootStage());
        const CountStats* countStats =
            static_cast<const CountStats*>(countStage->getSpecificStats());
        result.appendNumber("n",
                            countStats->nCounted >= gc_count ? countStats->nCounted - gc_count
                                                             : countStats->nCounted);
        index_LOG(1) << "[CountCmd] nss: " << nss << " count return: " << countStats->nCounted;
        return true;
    }

} cmdCount;

}  // namespace
}  // namespace mongo

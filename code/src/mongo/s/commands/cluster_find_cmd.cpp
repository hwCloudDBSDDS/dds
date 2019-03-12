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

#include <boost/optional.hpp>

#include "mongo/client/read_preference.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/matcher/extensions_callback_noop.h"
#include "mongo/db/modules/rocks/src/gc_common.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/cursor_response.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/views/resolved_view.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/commands/cluster_aggregate.h"
#include "mongo/s/commands/strategy.h"
#include "mongo/s/query/cluster_find.h"
#include "mongo/util/log.h"
#include "mongo/util/util_extend/GlobalConfig.h"


namespace mongo {
namespace {

using std::unique_ptr;
using std::string;
using std::vector;

const char kTermField[] = "term";

/**
 * Implements the find command on mongos.
 */
class ClusterFindCmd : public Command {
    MONGO_DISALLOW_COPYING(ClusterFindCmd);

public:
    ClusterFindCmd() : Command("find") {}


    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    bool slaveOk() const final {
        return false;
    }

    bool slaveOverrideOk() const final {
        return true;
    }

    bool maintenanceOk() const final {
        return false;
    }

    bool adminOnly() const final {
        return false;
    }

    bool shouldAffectCommandCounter() const final {
        return false;
    }

    void help(std::stringstream& help) const final {
        help << "query for documents";
    }

    /**
     * In order to run the find command, you must be authorized for the "find" action
     * type on the collection.
     */
    Status checkAuthForCommand(Client* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) final {
        NamespaceString nss(parseNs(dbname, cmdObj));
        auto hasTerm = cmdObj.hasField(kTermField);
        return AuthorizationSession::get(client)->checkAuthForFind(nss, hasTerm);
    }

    Status explain(OperationContext* txn,
                   const std::string& dbname,
                   const BSONObj& cmdObj,
                   ExplainCommon::Verbosity verbosity,
                   const rpc::ServerSelectionMetadata& serverSelectionMetadata,
                   BSONObjBuilder* out) const final {
        const string fullns = parseNs(dbname, cmdObj);
        const NamespaceString nss(fullns);
        if (!nss.isValid()) {
            return {ErrorCodes::InvalidNamespace,
                    str::stream() << "Invalid collection name: " << nss.ns()};
        }

        // Parse the command BSON to a QueryRequest.
        bool isExplain = true;
        auto qr = QueryRequest::makeFromFindCommand(std::move(nss), cmdObj, isExplain);
        if (!qr.isOK()) {
            return qr.getStatus();
        }

        auto result = Strategy::explainFind(
            txn, cmdObj, *qr.getValue(), verbosity, serverSelectionMetadata, out);

        if (result == ErrorCodes::CommandOnShardedViewNotSupportedOnMongod) {
            auto resolvedView = ResolvedView::fromBSON(out->asTempObj());
            out->resetToEmpty();

            auto aggCmdOnView = qr.getValue().get()->asAggregationCommand();
            if (!aggCmdOnView.isOK()) {
                return aggCmdOnView.getStatus();
            }

            auto aggCmd = resolvedView.asExpandedViewAggregation(aggCmdOnView.getValue());
            if (!aggCmd.isOK()) {
                return aggCmd.getStatus();
            }

            int queryOptions = 0;
            ClusterAggregate::Namespaces nsStruct;
            nsStruct.requestedNss = std::move(nss);
            nsStruct.executionNss = std::move(resolvedView.getNamespace());
            auto status =
                ClusterAggregate::runAggregate(txn, nsStruct, aggCmd.getValue(), queryOptions, out);
            appendCommandStatus(*out, status);
            return status;
        }

        return result;
    }

    bool hasField(const BSONObj& obj, const std::string& field, std::vector<std::string>& values) {
        if (obj.hasField(field)) {

            if (obj.getField(field).type() != BSONType::String) {
                return false;
            }

            std::string value = obj.getField(field).String();

            index_log() << "mongos value: " << value;
            for (auto v : values) {
                if (value == v) {
                    return true;
                }
            }
        }

        return false;
    }

    bool hideInterInfo(const std::string& dbname, const BSONObj& obj) {
        if (dbname != "config") {
            return false;
        }

        if (GLOBAL_CONFIG_GET(ShowInternalInfo)) {
            return false;
        }

        std::vector<std::string> nss = {GcRefNs, GcRemoveInfoNs, GcDbName};

        if (hasField(obj, "ns", nss)) {
            return true;
        }

        if (hasField(obj, "_id", nss)) {
            return true;
        }

        return false;
    }

    bool run(OperationContext* txn,
             const std::string& dbname,
             BSONObj& cmdObj,
             int options,
             std::string& errmsg,
             BSONObjBuilder& result) final {
        // We count find command as a query op.
        globalOpCounters.gotQuery();

        const std::string fullns = parseNs(dbname, cmdObj);
        const NamespaceString nss(fullns);

        if (!nss.isValid()) {
            index_log() << "[MONGOS] find error ns: " << nss.ns() << " , Invalid collection name";
            return appendCommandStatus(result,
                                       {ErrorCodes::InvalidNamespace,
                                        str::stream() << "Invalid collection name: " << nss.ns()});
        }

        if (AuthorizationSession::get(txn->getClient())->isAuthWithCustomerOrNoAuthUser()) {
            bool flag = false;
            BSONObj buildinfilter;
            if (fullns == "admin.system.users") {
                std::set<std::string> buildinUsers;
                UserName::getBuildinUsers(buildinUsers);

                BSONObj filterUsername =
                    BSON(AuthorizationManager::USER_NAME_FIELD_NAME << NIN << buildinUsers);
                BSONObj filterdbname =
                    BSON(AuthorizationManager::ROLE_DB_FIELD_NAME << NE << "admin");
                buildinfilter = BSON("$or" << BSON_ARRAY(filterUsername << filterdbname));
                flag = true;
            }
            if (fullns == "admin.system.roles") {
                std::set<std::string> buildinRoles;
                RoleName::getBuildinRoles(buildinRoles);
                BSONObj filterUsername =
                    BSON(AuthorizationManager::ROLE_NAME_FIELD_NAME << NIN << buildinRoles);
                BSONObj filterdbname =
                    BSON(AuthorizationManager::ROLE_DB_FIELD_NAME << NE << "admin");
                buildinfilter = BSON("$or" << BSON_ARRAY(filterUsername << filterdbname));
                flag = true;
            }

            if (flag) {
                std::string filterName = "filter";
                BSONElement filterField = cmdObj[filterName];
                BSONObj newFilter;
                if (filterField.isABSONObj()) {
                    BSONObj filter = filterField.embeddedObject();
                    newFilter = BSON("$and" << BSON_ARRAY(filter << buildinfilter));
                } else {
                    newFilter = buildinfilter;
                }

                BSONObjBuilder nb(64);
                nb.append(filterName, newFilter);
                BSONForEach(e, cmdObj) {
                    if (!str::equals(filterName.c_str(), e.fieldName())) {
                        nb.append(e);
                    }
                }
                cmdObj = nb.obj();
            }
        }

        const bool isExplain = false;
        auto qr = QueryRequest::makeFromFindCommand(nss, cmdObj, isExplain);
        if (!qr.isOK()) {
            return appendCommandStatus(result, qr.getStatus());
        }

        auto cq =
            CanonicalQuery::canonicalize(txn, std::move(qr.getValue()), ExtensionsCallbackNoop());
        if (!cq.isOK()) {
            return appendCommandStatus(result, cq.getStatus());
        }

        // Extract read preference. If no read preference is specified in the query, will we pass
        // down a "primaryOnly" or "secondary" read pref, depending on the slaveOk setting.
        auto readPref =
            ClusterFind::extractUnwrappedReadPref(cmdObj, options & QueryOption_SlaveOk);
        if (!readPref.isOK()) {
            return appendCommandStatus(result, readPref.getStatus());
        }

        // Do the work to generate the first batch of results. This blocks waiting to get responses
        // from the shard(s).
        std::vector<BSONObj> batch;
        BSONObj viewDefinition;
        auto cursorId = ClusterFind::runQuery(
            txn, *cq.getValue(), readPref.getValue(), &batch, &viewDefinition);
        if (!cursorId.isOK()) {
            if (cursorId.getStatus() == ErrorCodes::CommandOnShardedViewNotSupportedOnMongod) {
                auto aggCmdOnView = cq.getValue()->getQueryRequest().asAggregationCommand();

                if (!aggCmdOnView.isOK()) {
                    return appendCommandStatus(result, aggCmdOnView.getStatus());
                }

                auto resolvedView = ResolvedView::fromBSON(viewDefinition);
                auto aggCmd = resolvedView.asExpandedViewAggregation(aggCmdOnView.getValue());
                if (!aggCmd.isOK()) {
                    return appendCommandStatus(result, aggCmd.getStatus());
                }

                // We pass both the underlying collection namespace and the view namespace here. The
                // underlying collection namespace is used to execute the aggregation on mongoD. Any
                // cursor returned will be registered under the view namespace so that subsequent
                // getMore and killCursors calls against the view have access.
                ClusterAggregate::Namespaces nsStruct;
                nsStruct.requestedNss = std::move(nss);
                nsStruct.executionNss = std::move(resolvedView.getNamespace());
                auto status = ClusterAggregate::runAggregate(
                    txn, nsStruct, aggCmd.getValue(), options, &result);
                appendCommandStatus(result, status);
                return status.isOK();
            }

            index_err() << "[MONGOS] Find status status:" << cursorId.getStatus();
            return appendCommandStatus(result, cursorId.getStatus());
        }

        // Build the response document.
        CursorResponseBuilder firstBatch(/*firstBatch*/ true, &result);
        for (const auto& obj : batch) {
            if (hideInterInfo(dbname, obj)) {
                continue;
            }

            firstBatch.append(obj);
        }
        firstBatch.done(cursorId.getValue(), nss.ns());
        return true;
    }

} cmdFindCluster;

}  // namespace
}  // namespace mongo

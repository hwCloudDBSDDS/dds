/******************************************************************************
                Copyright 1999 - 2017, Huawei Tech. Co., Ltd.
                           ALL RIGHTS RESERVED
  File Name     : assign_chunk_command.cpp
  Version       : Initial Draft
  Author        : 
  Created       : 2017/6/20
  Description   : assign chunk cmd
  History       :
  1.Date        : 2017/6/20
    Author      : 
    Modification: Created file

******************************************************************************/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/client/remote_command_targeter.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/range_deleter_service.h"
#include "mongo/db/s/chunk_move_write_concern_options.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/s/catalog/dist_lock_manager.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/offload_chunk_request.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/catalog/drop_collection.h"

namespace mongo {

using std::string;

namespace {

class OffloadChunkCommand : public Command {
public:
    OffloadChunkCommand() : Command("offloadChunk") {}

    void help(std::stringstream& help) const override {
        help << "should not be calling this directly";
    }

    bool slaveOk() const override {
        return false;
    }

    bool adminOnly() const override {
        return true;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    Status checkAuthForCommand(Client* client,
                               const string& dbname,
                               const BSONObj& cmdObj) override {
        if (!AuthorizationSession::get(client)->isAuthorizedForActionsOnResource(
                ResourcePattern::forClusterResource(), ActionType::internal)) {
            return Status(ErrorCodes::Unauthorized, "Unauthorized");
        }
        return Status::OK();
    }

    string parseNs(const string& dbname, const BSONObj& cmdObj) const override {
        return parseNsFullyQualified(dbname, cmdObj);
    }

    bool run(OperationContext* txn,
             const string& dbname,
             BSONObj& cmdObj,
             int ,
             string& errmsg,
             BSONObjBuilder& result) override {
        Date_t initExecTime = Date_t::now();
        const OffloadChunkRequest offloadChunkRequest = uassertStatusOK(
            OffloadChunkRequest::createFromCommand(cmdObj));
     
        index_LOG(1)<<"offloadChunk: "<<dbname<<" cmdobj:"<<cmdObj <<" Request:"<<offloadChunkRequest;

        // to generate the collection name for a given chunk
        NamespaceString nss (StringData( offloadChunkRequest.getNss()));

        // create new collection name: ns$chunkID
        NamespaceString nss_with_chunkID (StringData(nss.ns()+'$'+ offloadChunkRequest.getName()));

        index_log() << "[offloadChunk] shardSvr start nss: " << nss_with_chunkID;
        if ((repl::getGlobalReplicationCoordinator()->getReplicationMode() !=
             repl::ReplicationCoordinator::modeNone) &&
            nss_with_chunkID.isOplog()) {
            errmsg = "can't offload live oplog while replicating";
            return false;
        }

        //Offload command is now using drop collection command to remove metadata. Modified drop_ident command to not removing data
        //TODO to have seperate command for offload chunk
        txn->setCmdFlag(OperationContext::OFFLOAD);
        bool suc = appendCommandStatus(result, dropCollection(txn, nss_with_chunkID, result));
        index_log() << "[offloadChunk] shardSvr end nss: " << nss_with_chunkID << "; used Time(ms): " <<
            (Date_t::now()-initExecTime);
        return suc;
    }
} offloadChunkCmd;

}  // namespace
}  // namespace mongo

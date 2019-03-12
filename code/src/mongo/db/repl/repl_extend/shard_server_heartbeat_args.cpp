#include "mongo/platform/basic.h"

#include "mongo/db/repl/repl_extend/shard_server_heartbeat_args.h"

namespace mongo {
namespace repl {

namespace {

const std::string kSenderHostFieldName = "shardServerHeartbeat";

}  // namespace

Status ShardServerHeartbeatArgs::initialize(const BSONObj& argsObj) {
    std::string hostAndPortString;
    Status status = bsonExtractStringField(argsObj, kSenderHostFieldName, &hostAndPortString);
    if (!status.isOK())
        return status;
    if (!hostAndPortString.empty()) {
        status = _senderHost.initialize(hostAndPortString);
        if (!status.isOK())
            return status;
        _hasSender = true;
    }

    return Status::OK();
}

void ShardServerHeartbeatArgs::setSenderHost(const HostAndPort& newVal) {
    _senderHost = newVal;
    _hasSender = true;
}

BSONObj ShardServerHeartbeatArgs::toBSON() const {
    BSONObjBuilder builder;
    addToBSON(&builder);
    return builder.obj();
}

void ShardServerHeartbeatArgs::addToBSON(BSONObjBuilder* builder) const {
    if (_hasSender) {
        builder->append(kSenderHostFieldName, _senderHost.toString());
    }
}

}  // namespace repl
}  // namespace mongo

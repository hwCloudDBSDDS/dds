#include "mongo/platform/basic.h"

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/s/grid.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

#include "mongo/s/catalog/type_shard_server.h"

namespace mongo {

const std::string ShardServerType::ConfigNS = "config.shardservers";

const BSONField<std::string> ShardServerType::host("_id");
const BSONField<ShardServerType::ShardServerState> ShardServerType::state("state");
const BSONField<std::string> ShardServerType::shardName("shardName");
const BSONField<BSONArray> ShardServerType::tags("tags");

StatusWith<ShardServerType> ShardServerType::fromBSON(const BSONObj& source) {
    ShardServerType shardServer;

    {
        std::string shardServerHost;
        Status status = bsonExtractStringField(source, host.name(), &shardServerHost);

        if (!status.isOK()) {
            return status;
        }

        shardServer._host = shardServerHost;
    }

    {
        long long shardServerState;
        Status status = bsonExtractIntegerField(source, state.name(), &shardServerState);

        if (!status.isOK()) {
            return status;
        }

        // Make sure the state field falls within the valid range of ShardServerState values.
        if (!(shardServerState >= static_cast<std::underlying_type<ShardServerState>::type>(
                                      ShardServerState::kStandby) &&
              shardServerState <= static_cast<std::underlying_type<ShardServerState>::type>(
                                      ShardServerState::kActive))) {
            return Status(ErrorCodes::BadValue,
                          str::stream() << "Invalid shard server state value: "
                                        << shardServerState);
        }

        shardServer._state = static_cast<ShardServerState>(shardServerState);
    }


    {
        std::string shardNameStr;
        Status status = bsonExtractStringField(source, shardName.name(), &shardNameStr);

        if (status.isOK()) {
            if (shardServer._state != ShardServerState::kStandby) {
                shardServer._shardName = shardNameStr;
            } else {
                // shardName field should not be set if active == false
            }
        } else if (status == ErrorCodes::NoSuchKey) {
            if (shardServer._state != ShardServerState::kStandby) {
                return Status(ErrorCodes::NoSuchKey,
                              str::stream()
                                  << "empty shard id when shard server is active or toBeActive");
            } else {
                // shardName field should not be set if active == false
            }
        } else {
            return status;
        }
    }

    if (source.hasField(tags.name())) {
        shardServer._tags = std::vector<std::string>();
        BSONElement tagsElement;
        Status status = bsonExtractTypedField(source, tags.name(), Array, &tagsElement);

        if (!status.isOK()) {
            return status;
        }

        BSONObjIterator it(tagsElement.Obj());

        while (it.more()) {
            BSONElement tagElement = it.next();

            if (tagElement.type() != String) {
                return Status(ErrorCodes::TypeMismatch,
                              str::stream() << "Elements in \"" << tags.name()
                                            << "\" array must be strings but found "
                                            << typeName(tagElement.type()));
            }

            shardServer._tags->push_back(tagElement.String());
        }
    }

    return shardServer;
}

Status ShardServerType::validate() const {
    if (!_host.is_initialized() || _host->empty()) {
        return Status(ErrorCodes::NoSuchKey,
                      str::stream() << "missing " << host.name() << " field");
    }

    if (!_state.is_initialized()) {
        return Status(ErrorCodes::NoSuchKey,
                      str::stream() << "missing " << state.name() << " field");
    }

    if ((getState() != ShardServerState::kStandby) && (!_shardName.is_initialized())) {
        return Status(ErrorCodes::NoSuchKey,
                      str::stream() << "missing " << shardName.name() << " field");
    }

    return Status::OK();
}

BSONObj ShardServerType::toBSON() const {
    BSONObjBuilder builder;

    if (_host) {
        builder.append(host(), getHost());
    }

    if (_state) {
        builder.append(state(),
                       static_cast<std::underlying_type<ShardServerState>::type>(getState()));
    }

    if (_shardName) {
        builder.append(shardName(), getShardName());
    }

    if (_tags) {
        builder.append(tags(), getTags());
    }

    return builder.obj();
}

std::string ShardServerType::toString() const {
    return toBSON().toString();
}

void ShardServerType::setHost(const std::string& hostStr) {
    invariant(!hostStr.empty());
    _host = hostStr;
}

void ShardServerType::setState(const ShardServerState state) {
    _state = state;
}

void ShardServerType::setShardName(const std::string& shardName) {
    invariant(!shardName.empty());
    _shardName = shardName;
}

void ShardServerType::setTags(const std::vector<std::string>& tags) {
    invariant(tags.size() > 0);
    _tags = tags;
}

}  // namespace mongo

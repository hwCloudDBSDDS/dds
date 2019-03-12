#pragma once

#include <boost/optional.hpp>
#include <string>
#include <vector>

#include "mongo/db/jsobj.h"
#include "mongo/s/shard_id.h"
#include "mongo/s/write_ops/batched_update_request.h"
#include "mongo/util/net/hostandport.h"

namespace mongo {

struct BSONArray;
class BSONObj;
class Status;
template <typename T>
class StatusWith;

/**
 * This class represents the layout and contents of documents contained in the
 * config.shardservers collection. All manipulation of documents coming from that
 * collection should be done with this class.
 */
class ShardServerType {
public:
    enum class ShardServerState : int { kStandby = 0, kToBeActive = 1, kActive = 2 };

    enum class ShardServerRole : int { kStandby = 0, kActive = 1 };

    // Name of the shardservers collection in the config server.
    static const std::string ConfigNS;

    // Field names and types in the shardservers collection type.
    static const BSONField<std::string> host;
    static const BSONField<ShardServerState> state;
    static const BSONField<std::string> shardName;
    static const BSONField<BSONArray> tags;

    bool operator==(const ShardServerType& shardServer) const {
        return this->toString() == shardServer.toString();
    }

    bool operator<(const ShardServerType& shardServer) const {
        return this->toString() < shardServer.toString();
    }

    /**
     * Constructs a new ShardServerType object from BSON.
     * Also does validation of the contents.
     */
    static StatusWith<ShardServerType> fromBSON(const BSONObj& source);

    /**
     * Returns OK if all fields have been set. Otherwise returns NoSuchKey
     * and information about the first field that is missing.
     */
    Status validate() const;

    /**
     * Returns the BSON representation of the entry.
     */
    BSONObj toBSON() const;

    /**
     * Returns a std::string representation of the current internal state.
     */
    std::string toString() const;

    const std::string& getHost() const {
        return _host.get();
    }
    void setHost(const std::string& host);

    ShardServerState getState() const {
        return _state.value_or(ShardServerState::kStandby);
    }
    void setState(const ShardServerState state);

    bool hasShardName() const {
        if (_shardName) {
            return true;
        } else {
            return false;
        }
    }

    const std::string& getShardName() const {
        return _shardName.get();
    }
    void setShardName(const std::string& shardName);

    std::vector<std::string> getTags() const {
        return _tags.value_or(std::vector<std::string>());
    }
    void setTags(const std::vector<std::string>& tags);

private:
    // Convention: (M)andatory, (O)ptional, (S)pecial rule.

    // (M)  shard server's host and port
    boost::optional<std::string> _host;
    // (M)  shard server state
    boost::optional<ShardServerState> _state;
    // (O)  shard's id for an active shard server
    boost::optional<std::string> _shardName;
    // (O)  shard server tags
    boost::optional<std::vector<std::string>> _tags;
};

}  // namespace mongo

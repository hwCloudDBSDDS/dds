#pragma once

#include <string>

#include "mongo/bson/util/bson_check.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/jsobj.h"
#include "mongo/util/net/hostandport.h"

namespace mongo {

class BSONObj;
class BSONObjBuilder;
class Status;

namespace repl {

/**
 * Arguments to the shardServerHeartbeat command.
 */
class ShardServerHeartbeatArgs {
public:
    /**
     * Initializes this ShardServerHeartbeatArgs from the contents of args.
     */
    Status initialize(const BSONObj& argsObj);

    /**
     * Returns whether or not the HostAndPort of the sender is set.
     */
    bool hasSender() const {
        return _hasSender;
    }

    /**
     * The below methods set the value in the method name to 'newVal'.
     */
    void setSenderHost(const HostAndPort& newVal);

    /**
     * Gets the HostAndPort of the sender.
     */
    HostAndPort getSenderHost() const {
        return _senderHost;
    }

    /**
     * Returns a BSONified version of the object.
     * Should only be called if the mandatory fields have been set.
     * Optional fields are only included if they have been set.
     */
    BSONObj toBSON() const;

    void addToBSON(BSONObjBuilder* builder) const;

private:
    bool _hasSender = false;
    HostAndPort _senderHost;
};

}  // namespace repl
}  // namespace mongo

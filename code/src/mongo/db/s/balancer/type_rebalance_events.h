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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#pragma once

#include "mongo/bson/bsonobj.h"
#include "mongo/db/s/balancer/balance_event_engine.h"
#include "mongo/db/s/balancer/balancer_policy.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/client/shard.h"

namespace mongo {
class BalanceEventEngine;
class IRebalanceEvent;
// This class represents the layout and contents of documents contained in the
// config.rebalanceevents
// collection. All manipulation of documents coming from that collection should be done with this
// class.
class RebalanceEventType {
public:
    // Name of the statemachine collection in the config server.
    static const std::string ConfigNS;

    // Field names and types in the statemachine collection type.
    static const BSONField<std::string> eventId;
    static const BSONField<int> eventType;
    static const BSONField<BSONObj> eventData;

    // The rebalance modules encapsulates reblance information in BalanceEventEngineInfo objects,
    // so this facilitates conversion to a config.migrations entry format.
    explicit RebalanceEventType(IRebalanceEvent* info);

    // Constructs a new RebalanceEventType object from BSON. Expects all fields to be present,
    // and errors if they are not.
    static StatusWith<RebalanceEventType> fromBSON(const BSONObj& source);

    // Returns the BSON representation of the config.statemachines document entry.
    BSONObj toBSON() const;

    // Returns a std::string representation of the current internal state.
    std::string toString() const;

    // Uniquely identifies a statemachine.
    const std::string& getEventId() const;

    // Type of statemachine, real type is BalanceEventEngine::Type
    int getEventType() const;

    // Returns the internal data of statemachine
    const BSONObj& getEventData() const;

    bool operator==(const RebalanceEventType& e) const {
        return ((_eventData.woCompare(e.getEventData()) == 0) && (_eventId == e.getEventId()) &&
                (_eventType == e.getEventType()));
    }

private:
    RebalanceEventType();

    // All required fields for config.rebalanceevents
    std::string _eventId;
    int _eventType = 0;
    BSONObj _eventData;
};

}  // namespace mongo

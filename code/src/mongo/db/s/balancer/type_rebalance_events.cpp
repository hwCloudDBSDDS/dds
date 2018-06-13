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

#include "mongo/platform/basic.h"

#include "mongo/db/s/balancer/type_rebalance_events.h"

#include "mongo/bson/util/bson_extract.h"
#include "mongo/s/catalog/type_chunk.h"

namespace mongo {

const std::string RebalanceEventType::ConfigNS = "config.rebalanceevents";

const BSONField<std::string> RebalanceEventType::eventId("_id");
const BSONField<int> RebalanceEventType::eventType("eventType");
const BSONField<BSONObj> RebalanceEventType::eventData("eventData");

RebalanceEventType::RebalanceEventType() = default;

RebalanceEventType::RebalanceEventType(IRebalanceEvent* info) {
    _eventId = info->getEventId();
    _eventType = static_cast<int>(info->getEventType());
    _eventData = info->dataToBSON();
}

StatusWith<RebalanceEventType> RebalanceEventType::fromBSON(const BSONObj& source) {
    RebalanceEventType rebalanceEventType;

    {
        std::string eventid;
        Status status = bsonExtractStringField(source, eventId.name(), &eventid);
        if (!status.isOK())
            return status;

        rebalanceEventType._eventId = std::move(eventid);
    }

    {
        long long eventtype;
        Status status = bsonExtractIntegerField(source, eventType.name(), &eventtype);
        if (!status.isOK())
            return status;

        rebalanceEventType._eventType = static_cast<int>(eventtype);
    }

    {
        BSONElement eventdata;
        Status status  = bsonExtractTypedField(source, eventData.name(), Object, &eventdata);
        if (!status.isOK()) {
            return {status.code(),
                    str::stream() << "Invalid statemachine data due to " << status.reason()};
        }
        
        if (eventdata.Obj().isEmpty()) {
            return {ErrorCodes::BadValue, "The statemachine data cannot be empty"};
        }

        rebalanceEventType._eventData = std::move(eventdata.Obj().getOwned());
    }

    return rebalanceEventType;
}

BSONObj RebalanceEventType::toBSON() const {
    BSONObjBuilder builder;

    builder.append(eventId.name(), getEventId());
    builder.append(eventType.name(), getEventType());
    builder.append(eventData.name(), getEventData());

    return builder.obj();
}

std::string RebalanceEventType::toString() const {
    return toBSON().toString();
}

int RebalanceEventType::getEventType() const {
    return _eventType;
}

const std::string& RebalanceEventType::getEventId() const {
    return _eventId;
}

const BSONObj& RebalanceEventType::getEventData() const {
    return _eventData;
}

}  // namespace mongo

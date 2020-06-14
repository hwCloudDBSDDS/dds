/*    Copyright 2014 10gen Inc.
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

#include "mongo/logger/audit_event_utf8_encoder.h"

#include <iostream>
#include <vector>

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/util/time_support.h"

namespace mongo {
namespace logger {

const std::string USER_NAME_FIELD_NAME = "user";
const std::string USER_DB_FIELD_NAME = "db";
const std::string ROLE_NAME_FIELD_NAME = "role";
const std::string ROLE_DB_FIELD_NAME = "db";

BSONArray usersVectorToBSONArray(const std::vector<UserName>& users) {
    BSONArrayBuilder usersArrayBuilder;
    for (std::vector<UserName>::const_iterator it = users.begin(); it != users.end(); ++it) {
        const UserName& user = *it;
        usersArrayBuilder.append(BSON(USER_NAME_FIELD_NAME
                                      << user.getUser() << USER_DB_FIELD_NAME
                                      << user.getDB()));
    }
    return usersArrayBuilder.arr();
}

// TODO: audit lib should not dependent on serveronly lib,
// so duplicate code here(copied from db/commands/user_management_commands.cpp),
// should put it elsewhere
BSONArray rolesVectorToBSONArray(const std::vector<RoleName>& roles) {
    BSONArrayBuilder rolesArrayBuilder;
    for (std::vector<RoleName>::const_iterator it = roles.begin(); it != roles.end(); ++it) {
        const RoleName& role = *it;
        rolesArrayBuilder.append(BSON(ROLE_NAME_FIELD_NAME
                                      << role.getRole() << ROLE_DB_FIELD_NAME
                                      << role.getDB()));
    }
    return rolesArrayBuilder.arr();
}

AuditEventJSONEncoder::~AuditEventJSONEncoder() {}

std::ostream& AuditEventJSONEncoder::encode(const AuditEventEphemeral& event,
                                            std::ostream& os) {
    BSONObjBuilder builder;
    builder.append("atype", event.getAtype());
    builder.append("ts", event.getTs());

    BSONObj local = BSON("ip" << event.getLocalHost() << "port" << event.getLocalPort());
    BSONObj remote = BSON("ip" << event.getRemoteHost() << "port" << event.getRemotePort());

    builder.append("local", local);
    builder.append("remote", remote);

    BSONArray userNameArr = usersVectorToBSONArray(*(event.getUserNames()));
    BSONArray roleNameArr = rolesVectorToBSONArray(*(event.getRoleNames()));

    builder.append("users", userNameArr);
    builder.append("roles", roleNameArr);
    builder.append("param", *(event.getParam()));
    builder.append("result", event.getResult());

    os << builder.obj().jsonString();
    os << '\n';
    return os;
}

void AuditEventJSONEncoder::encode(const AuditEventEphemeral& event,
                                            std::stringstream& s) {
    BSONObjBuilder builder;
    builder.append("atype", event.getAtype());
    builder.append("ts", event.getTs());

    BSONObj local = BSON("ip" << event.getLocalHost() << "port" << event.getLocalPort());
    BSONObj remote = BSON("ip" << event.getRemoteHost() << "port" << event.getRemotePort());

    builder.append("local", local);
    builder.append("remote", remote);

    BSONArray userNameArr = usersVectorToBSONArray(*(event.getUserNames()));
    BSONArray roleNameArr = rolesVectorToBSONArray(*(event.getRoleNames()));

    builder.append("users", userNameArr);
    builder.append("roles", roleNameArr);
    builder.append("param", *(event.getParam()));
    builder.append("result", event.getResult());

    s << builder.obj().jsonString();
    s << '\n';
}


AuditEventHWDDSEncoder::~AuditEventHWDDSEncoder() {}
std::ostream& AuditEventHWDDSEncoder::encode(const AuditEventEphemeral& event,
                                                  std::ostream& os) {
    // tid
    os << event.getThreadId();
    os << '\t';

    // ip
    os << event.getRemoteHost();
    os << '\t';

    // user
    for (std::vector<UserName>::const_iterator it = event.getUserNames()->begin(); it != event.getUserNames()->end(); ++it) {
        const UserName& user = *it;
        os << user.getUser();
        os << ';';
    }
    os << '\t';

    // db
    for (std::vector<UserName>::const_iterator it = event.getUserNames()->begin(); it != event.getUserNames()->end(); ++it) {
        const UserName& user = *it;
        os << user.getDB();
        os << ';';
    }
    os << '\t';

    // time(micro seconds)
    os << event.getTs().toMillisSinceEpoch() * 1000;
    os << '\t';

    // fail(0 means success)
    int result = event.getResult();
    os << result;
    os << '\t';

    // latency(micro seconds)
    os << event.getLatencyMicros();
    os << '\t';

    // return_rows
    os << 0;
    os << '\t';

    // update_rows
    os << 0;
    os << '\t';

    // check_rows
    os << 0;
    os << '\t';

    // isbind
    os << 0;
    os << '\t';

    // sql
    BSONObjBuilder builder;
    builder.append("atype", event.getAtype());
    builder.append("param", *(event.getParam()));
    builder.append("result", event.getResult());
    os << builder.obj().jsonString();

    os << "\1\n";

    return os;
}
void AuditEventHWDDSEncoder::encode(const AuditEventEphemeral& event,
                                         std::stringstream& s) {
    // tid
    s << event.getThreadId();
    s << '\t';

    // ip
    s << event.getRemoteHost();
    s << '\t';

    // user
    for (std::vector<UserName>::const_iterator it = event.getUserNames()->begin(); it != event.getUserNames()->end(); ++it) {
        const UserName& user = *it;
        s << user.getUser();
        s << ';';
    }
    s << '\t';

    // db
    for (std::vector<UserName>::const_iterator it = event.getUserNames()->begin(); it != event.getUserNames()->end(); ++it) {
        const UserName& user = *it;
        s << user.getDB();
        s << ';';
    }
    s << '\t';

    // time(micro seconds)
    s << event.getTs().toMillisSinceEpoch() * 1000;
    s << '\t';

    // fail(0 means success)
    int result = event.getResult();
    s << result;
    s << '\t';

    // latency(micro seconds)
    s << event.getLatencyMicros();
    s << '\t';

    // return_rows
    s << 0;
    s << '\t';

    // update_rows
    s << 0;
    s << '\t';

    // check_rows
    s << 0;
    s << '\t';

    // isbind
    s << 0;
    s << '\t';

    // sql
    s << "{ \"atype\" : \"" << event.getAtype() << "\", \"param\" : ";
    event.getParam()->jsonString(s);
    s << ", \"result\": \"" << event.getResult() << "\" }";

    // end of line
    s << "\1\n";
}

}  // namespace logger
}  // namespace mongo

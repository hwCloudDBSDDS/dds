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


#pragma once

#include <cstdint>
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/base/string_data.h"
#include "mongo/logger/log_severity.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/time_support.h"

namespace mongo {

class BSONObj;
class UserName;
class RoleName;

namespace logger {

/**
 * Free form text log message object that does not own the storage behind its message and
 * contextName.
 *
 * Used and owned by one thread.  This is the message type used by AuditLogDomain.
 */
class AuditEventEphemeral {
public:
    AuditEventEphemeral(StringData atype,
                        Date_t ts,
                        const std::string& localHost,
                        int localPort,
                        const std::string& remoteHost,
                        int remotePort,
                        std::vector<UserName>* userNames,
                        std::vector<RoleName>* roleNames,
                        long long latencyMicros,
                        BSONObj* param,
                        ErrorCodes::Error result)
        : _threadId(stdx::this_thread::get_id()),
          _atype(atype),
          _ts(ts),
          _localHost(localHost),
          _localPort(localPort),
          _remoteHost(remoteHost),
          _remotePort(remotePort),
          _userNames(userNames),
          _roleNames(roleNames),
          _latencyMicros(latencyMicros),
          _param(param),
          _result(result) {}

    // Auditing does not need severity but syslog_appender.h requires event.getSeverity()
    LogSeverity getSeverity() const {
        return LogSeverity::Log();
    }
    const stdx::thread::id getThreadId() const {
        return _threadId;
    }
    const StringData getAtype() const {
        return _atype;
    }
    const Date_t getTs() const {
        return _ts;
    }
    const std::string& getLocalHost() const {
        return _localHost;
    }
    const int getLocalPort() const {
        return _localPort;
    }
    const std::string& getRemoteHost() const {
        return _remoteHost;
    }
    const int getRemotePort() const {
        return _remotePort;
    }
    const std::vector<UserName>* getUserNames() const {
        return _userNames;
    }
    const std::vector<RoleName>* getRoleNames() const {
        return _roleNames;
    }
    const long long getLatencyMicros() const {
        return _latencyMicros;
    }
    const BSONObj* getParam() const {
        return _param;
    }
    const ErrorCodes::Error getResult() const {
        return _result;
    }

private:
    stdx::thread::id _threadId;
    StringData _atype;
    Date_t _ts;
    std::string _localHost;
    int _localPort;
    std::string _remoteHost;
    int _remotePort;
    std::vector<UserName>* _userNames;
    std::vector<RoleName>* _roleNames;
    long long _latencyMicros;
    BSONObj* _param;
    ErrorCodes::Error _result;
};

enum AuditOp {
    opInvalid = 0,
    opAuth = 1,
    opAdmin = 1 << 1,
    opSlow = 1 << 2,
    opInsert = 1 << 3,
    opUpdate = 1 << 4,
    opDelete = 1 << 5,
    opCommand = 1 << 6,
    opQuery = 1 << 7,
};

static inline AuditOp auditOpFromString(const std::string& auditOp) {
    if (auditOp == "auth") {
        return opAuth;
    } else if (auditOp == "admin") {
        return opAdmin;
    } else if (auditOp == "slow") {
        return opSlow;
    } else if (auditOp == "insert") {
        return opInsert;
    } else if (auditOp == "update") {
        return opUpdate;
    } else if (auditOp == "delete") {
        return opDelete;
    } else if (auditOp == "command") {
        return opCommand;
    } else if (auditOp == "query") {
        return opQuery;
    } else {
        return opInvalid;
    }
}

}  // namespace logger
}  // namespace mongo

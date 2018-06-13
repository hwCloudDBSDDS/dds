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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <vector>

#include "mongo/base/status_with.h"

#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_string.h"
#include "mongo/client/connection_string.h"

namespace mongo {

/**
 * Provides support for parsing and serialization of arguments to the config server regShardSvr 
 * command.
 */
class RegShardSvrRequest {
public:
    RegShardSvrRequest(NamespaceString nss,
                       ConnectionString hostAndPort,
                       std::string extendIPs,
                       std::string processIdentity);

    /**
     * Parses the provided BSON content as the internal _configsvrRegShardSvrCommand command, and if
     * it contains the correct types, constructs a RegShardSvrRequest object from it.
     *
     * {
     *   _configsvrRegShardSvrCommand: <string namespace>,
     *   hostAndPort: <ConnectionString ipPort>,
     * }
     */
    static StatusWith<RegShardSvrRequest> parseFromConfigCommand(const BSONObj& cmdObj);

    /**
     * Creates a BSONObjBuilder and uses it to create and return a BSONObj from this
     * RegShardSvrRequest instance. Calls appendAsConfigCommand and tracks on the passed-in
     * writeConcern.
     */
    BSONObj toConfigCommandBSON(const BSONObj& writeConcern);

    /**
     * Creates a serialized BSONObj of the internal _configsvrRegShardSvrCommand command from this
     * RegShardSvrRequest instance.
     */
    void appendAsConfigCommand(BSONObjBuilder* cmdBuilder);

    const NamespaceString& getNamespace() const; 
    const ConnectionString& getConnString() const;
    const std::string& getExtendIPs() const;
    const std::string& getProcessIdentity() const;

	std::string toString() const;

private:
    /**
     * Returns a validation Status for this RegShardSvrRequest instance. Performs checks for
     * valid Namespace and ConnectionString.
     */
    Status _validate();

    NamespaceString _nss;
    ConnectionString _connectionString;
    std::string _extendIPs;
    std::string _processIdentity;
};

}  // namespace mongo

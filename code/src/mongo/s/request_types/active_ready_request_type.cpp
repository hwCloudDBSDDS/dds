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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding
#include "mongo/platform/basic.h"

#include "mongo/s/request_types/active_ready_request_type.h"

#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/server_options.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/log.h"

namespace mongo {

using std::string;
using str::stream;

namespace {

const char kConfigsvrRegShardSvr[] = "_configsvrActiveReady";
const char kHostAndPort[] = "hostAndPort";
// const char kInit[] = "init";
const char kShardName[] = "shardName";
const char kProcessIdentity[] = "processIdentity";

}  // namespace

ActiveReadyRequest::ActiveReadyRequest(NamespaceString nss,
                                       ConnectionString connectionString,
                                       std::string shardName,
                                       std::string processIdentity)
    : _nss(std::move(nss)),
      _connectionString(std::move(connectionString)),
      _shardName(std::move(shardName)),
      _processIdentity(std::move(processIdentity)) {} 

StatusWith<ActiveReadyRequest> ActiveReadyRequest::parseFromConfigCommand(const BSONObj& cmdObj) {
    string ns;
    auto parseNamespaceStatus = bsonExtractStringField(cmdObj, kConfigsvrRegShardSvr, &ns);
    if (!parseNamespaceStatus.isOK()) {
        return parseNamespaceStatus;
    }

    string str;
    auto parseConnStringStatus = bsonExtractStringField(cmdObj, kHostAndPort, &str);
    if (!parseConnStringStatus.isOK()) {
        log() << "parse connection string failed";
        return parseConnStringStatus;
    }

    auto swConnString = ConnectionString::parse(str);
    if (!swConnString.isOK()) {
        log() << "connection string is illegal";
        return swConnString.getStatus();
    }
    ConnectionString connString = std::move(swConnString.getValue());


    std::string shardName;
    auto parseShardNameStatus = bsonExtractStringField(cmdObj, kShardName, &shardName);
    if (!parseShardNameStatus.isOK()) {
        log() << "shard name is bad value";
        return parseShardNameStatus;
    }

    std::string processIdentity;
    auto parseProcessIdentityStatus = bsonExtractStringField(cmdObj, kProcessIdentity, &processIdentity);
    if (!parseProcessIdentityStatus.isOK()) {
        return parseProcessIdentityStatus;
    }

    auto request = ActiveReadyRequest(NamespaceString(ns),
                                      std::move(connString),
                                      std::move(shardName),
                                      std::move(processIdentity));

    Status validationStatus = request._validate();
    if (!validationStatus.isOK()) {
        log() << "request validation failed";
        return validationStatus;
    }

    return request;
}

BSONObj ActiveReadyRequest::toConfigCommandBSON(const BSONObj& writeConcern) {
    BSONObjBuilder cmdBuilder;
    appendAsConfigCommand(&cmdBuilder);

    // Tack on passed-in writeConcern
    cmdBuilder.appendElements(writeConcern);

    return cmdBuilder.obj();
}

void ActiveReadyRequest::appendAsConfigCommand(BSONObjBuilder* cmdBuilder) {
    cmdBuilder->append(kConfigsvrRegShardSvr, _nss.ns());
    cmdBuilder->append(kHostAndPort, _connectionString.toString());
    cmdBuilder->append(kShardName, _shardName);
    cmdBuilder->append(kProcessIdentity, _processIdentity);
}

const NamespaceString& ActiveReadyRequest::getNamespace() const {
    return _nss;
}

const ConnectionString& ActiveReadyRequest::getConnString() const {
    return _connectionString;
}

const std::string& ActiveReadyRequest::getName() const {
    return _shardName;
}


const std::string& ActiveReadyRequest::getProcessIdentity() const {
    return _processIdentity;
}

Status ActiveReadyRequest::_validate() {
    if (!getConnString().isValid()) {
        return Status(ErrorCodes::HostNotFound,
                      str::stream() << "invalid host '" << _connectionString.toString()
                                    << "' specified for request");
    }

    return Status::OK();
}

}  // namespace mongo

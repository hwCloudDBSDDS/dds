
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

#include "mongo/s/request_types/request_types_extend/grant_active_request_type.h"

#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/server_options.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/log.h"

namespace mongo {

using std::string;
using str::stream;

namespace {

const char kConfigsvrGrantActive[] = "_configsvrGrantActive";
const char kShardName[] = "shardName";
const char kRootPlogList[] = "rootPlogList";

}  // namespace

GrantActiveRequest::GrantActiveRequest() {}
GrantActiveRequest::GrantActiveRequest(const NamespaceString& nss,
                                       const std::string& shardName,
                                       const RootPlogList& rootPlogList) {
    _nss = nss;
    _shardName = shardName;
    _rootPlogList = rootPlogList;
}

GrantActiveRequest::GrantActiveRequest(const GrantActiveRequest& request) {
    _nss = request.getNamespace();
    _shardName = request.getShardName();
    _rootPlogList = request.getRootPlogList();
}

GrantActiveRequest& GrantActiveRequest::operator =(const GrantActiveRequest& request) {
    this->_nss = request.getNamespace();
    this->_shardName = request.getShardName();
    this->_rootPlogList = request.getRootPlogList();
    return *this;
}

Status GrantActiveRequest::parseFromConfigCommand(const BSONObj& cmdObj, GrantActiveRequest& request) {
    string ns;
    auto parseNamespaceStatus = bsonExtractStringField(cmdObj, kConfigsvrGrantActive, &ns);
    if (!parseNamespaceStatus.isOK()) {
        return parseNamespaceStatus;
    }

    std::string shardName;
    auto parseShardNameStatus = bsonExtractStringField(cmdObj, kShardName, &shardName);
    if (!parseShardNameStatus.isOK()) {
        log() << "shard name is bad value";
        return parseShardNameStatus;
    }

    BSONElement beRootPlogList;
    auto parseRootPlogStatus = bsonExtractTypedField(cmdObj, kRootPlogList, Object,
                                                     &beRootPlogList);
    if (!parseRootPlogStatus.isOK()) {
        log() << "root plog list is bad value";
        return parseRootPlogStatus;
    }

    BSONObj obj = beRootPlogList.Obj();
    if (obj.isEmpty()) {
        return Status(ErrorCodes::BadValue, "empty root plog list");
    }

    RootPlogList rootPlogList;
    rootPlogList.fromBSON(obj);

    request = GrantActiveRequest(NamespaceString(ns),
                                      shardName,
                                      rootPlogList);
    return Status::OK();
}

BSONObj GrantActiveRequest::toConfigCommandBSON(const BSONObj& writeConcern) {
    BSONObjBuilder cmdBuilder;
    appendAsConfigCommand(&cmdBuilder);

    // Tack on passed-in writeConcern
    cmdBuilder.appendElements(writeConcern);

    return cmdBuilder.obj();
}

void GrantActiveRequest::appendAsConfigCommand(BSONObjBuilder* cmdBuilder) {
    cmdBuilder->append(kConfigsvrGrantActive, _nss.ns());
    cmdBuilder->append(kShardName, _shardName);
    cmdBuilder->append(kRootPlogList, _rootPlogList.toBSON().getOwned());
}

const NamespaceString& GrantActiveRequest::getNamespace() const {
    return _nss;
}

const RootPlogList& GrantActiveRequest::getRootPlogList() const {
    return _rootPlogList;
}

const std::string& GrantActiveRequest::getShardName() const {
    return _shardName;
}

std::string GrantActiveRequest::toString() const {
    stream ss;
    ss << "GrantActiveRequest shard: " << _shardName;
    ss << ", rootPlogList: " << _rootPlogList.toBSON().toString();
    return ss;
}

}  // namespace mongo

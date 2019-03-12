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


#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include <fstream>
#include <map>
#include <stdio.h>
#include <string>

#include "mongo/util/concurrency/rwlock.h"
#include "mongo/util/log.h"
#include "mongo/util/net/externalconfig.h"
#include "mongo/util/text.h"
#include <boost/algorithm/string.hpp>


namespace mongo {

bool ExternalConfig::parseFromFile(const std::string& path, std::string& errmsg) {
    if (path.empty() || path[0] != '/') {
        errmsg = "must be absolute path";
        return false;
    }

    _path = path;

    bool hasReloadPublicIpPrivateIpRange = false;
    bool hasReloadPrivateIpPrivateIpRange = false;

    std::ifstream f(path.c_str(), std::ifstream::in);
    if (f.is_open()) {
        int readCnt = 0;
        std::string line;
        while (std::getline(f, line) && (readCnt < 1000)) {
            boost::trim(line);
            std::vector<std::string> keys = StringSplitter::split(line, ":");

            if (!keys.empty()) {
                if (keys[0] == "publicIpPrivateRange") {
                    hasReloadPublicIpPrivateIpRange = true;
                    publicIpPrivateIpRange.parseFromString(keys);
                } else if (keys[0] == "privateIpPrivateRange") {
                    hasReloadPrivateIpPrivateIpRange = true;
                    privateIpPrivateIpRange.parseFromString(keys);
                }
            }
            readCnt++;
        }
    }

    if (!hasReloadPublicIpPrivateIpRange) {
        // do not reload means no configuration for publicIpPrivateRange, need reset it.
        publicIpPrivateIpRange.reset();
    }

    if (!hasReloadPrivateIpPrivateIpRange) {
        // do not reload means no configuration for privateIpPrivateRange, need reset it.
        privateIpPrivateIpRange.reset();
    }

    log() << "parseFromFile is done." << std::endl;
    log() << "publicIpPrivateIpRange is " << publicIpPrivateIpRange.toString() << std::endl;
    log() << "privateIpPrivateIpRange is " << privateIpPrivateIpRange.toString() << std::endl;

    return true;
}

std::string ExternalConfig::toString() {
    std::stringstream ss;
    ss << publicIpPrivateIpRange.toString();
    ss << privateIpPrivateIpRange.toString();
    return ss.str();
}
}

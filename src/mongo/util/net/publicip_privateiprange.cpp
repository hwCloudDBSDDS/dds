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

#include <arpa/inet.h>
#include <map>
#include <netinet/in.h>
#include <stdio.h>
#include <string>

#include "mongo/util/log.h"
#include "mongo/util/text.h"
#include <boost/algorithm/string.hpp>
// #include "mongo/util/concurrency/rwlock.h"
#include "mongo/util/net/publicip_privateiprange.h"

namespace mongo {

// eg: 192.168.1.100,192.168.1.100/24
bool PublicIpPrivateIpRange::parseFromString(const std::string& line) {

    std::vector<std::string> keys;
    const std::string separator = line.find(":") < line.find(";") ? ":" : ";";
    boost::split(keys, line, boost::is_any_of(separator));

    if (!keys.empty()) {
        if (keys[0] == "publicIpPrivateRange") {
            if (keys[1] == "001" && separator == ":") {
                return parseFromString001(keys);
            } else if (keys[1] == "002" && separator == ";") {
                // 0001 and 002 use same parse function
                return parseFromString001(keys);
            }
        }
    }

    return false;
}

bool PublicIpPrivateIpRange::parseFromString(std::vector<std::string>& keys) {
    if (!keys.empty()) {
        if (keys[0] == "publicIpPrivateRange") {
            if (keys[1] == "001" || keys[1] == "002") {
                return parseFromString001(keys);
            }
        }
    }

    return false;
}

bool PublicIpPrivateIpRange::parseFromString001(std::vector<std::string>& keys) {

    if (keys.empty()) {
        return false;
    }

    if (keys.size() < 3) {
        return false;
    }

    if (!(keys[0] == "publicIpPrivateRange" && (keys[1] == "001" || keys[1] == "002"))) {
        return false;
    }

    if (keys.size() < 4) {
        reset();
        return true;
    }


    if (keys[3] == "") {
        reset();
        return true;
    }
    std::map<std::string, std::vector<std::string>> tmpPublicIpMap;
    std::map<__uint128_t, IpRange> tmpRangeMap;

    parsePublicIp(keys[2], tmpPublicIpMap);
    parsePrivateIpRange(keys[3], tmpRangeMap);

    // swap to update
    // rwlock lk(_lock, true);
    publicIpMap.swap(tmpPublicIpMap);
    privateIpRangeMap.swap(tmpRangeMap);

    return true;
}

bool PublicIpPrivateIpRange::parsePrivateIpRange(std::string& range,
                                                 std::map<__uint128_t, IpRange>& tmpRangeMap) {
    // parse each item
    std::multimap<__uint128_t, IpRange> tmpRangeMultiMap;
    std::vector<std::string> items = StringSplitter::split(range, ",");

    for (auto it = items.begin(); it != items.end(); it++) {
        IpRange range;
        if (!IpRange::parseItem(*it, range)) {
            return false;
        }
        tmpRangeMultiMap.insert(std::make_pair(range.min, range));
    }


    // merge overlapped items

    IpRange last;

    for (auto mit = tmpRangeMultiMap.begin(); mit != tmpRangeMultiMap.end(); mit++) {
        if (last.invalid()) {
            last = mit->second;
        } else {
            if (last.max >= mit->second.min) {
                last.max = std::max(last.max, mit->second.max);
            } else {
                tmpRangeMap.insert(std::make_pair(last.min, last));
                last = mit->second;
            }
        }
    }

    if (!last.invalid()) {
        tmpRangeMap.insert(std::make_pair(last.min, last));
    }

    return true;
}

void PublicIpPrivateIpRange::parsePublicIp(
    std::string publicIp, std::map<std::string, std::vector<std::string>>& tmpPublicIpMap) {

    if (publicIp.empty()) {
        tmpPublicIpMap.clear();
        return;
    }

    std::vector<std::string> items = StringSplitter::split(publicIp, ",");
    for (auto& item : items) {
        std::vector<std::string> key_value = StringSplitter::split(item, "=");
        if (key_value.size() != 2) {
            continue;
        }
        boost::trim(key_value[0]);
        boost::trim(key_value[1]);

        int type = IpRange::ipType(key_value[1]);
        if (type == -1) {
            continue;
        }
        auto& ips = tmpPublicIpMap[(key_value[0])];
        if (ips.size() < 2) {
            ips.resize(2);
        }
        ips[type] = key_value[1];
    }
}
bool PublicIpPrivateIpRange::isInPrivateIpRange(const std::string& ipstr) {
    __uint128_t ipval = 0;
    if (!IpRange::addrToUint(ipstr, ipval)) {
        return false;
    }
    return isInPrivateIpRange(ipval);
}

bool PublicIpPrivateIpRange::isPublicIp(const std::string& ipstr) {

    if (!isPublicIpUsed()) {
        return false;
    }

    __uint128_t ipval = 0;
    if (!IpRange::addrToUint(ipstr, ipval)) {
        return false;
    }

    return !isInPrivateIpRange(ipval);
}

int PublicIpPrivateIpRange::rangeSize() {
    // rwlock lk(_lock, false);
    return privateIpRangeMap.size();
}

bool PublicIpPrivateIpRange::isInPrivateIpRange(const __uint128_t& ip) {
    // rwlock lk(_lock, false);

    if (privateIpRangeMap.empty()) {
        return false;
    }

    auto it = privateIpRangeMap.lower_bound(ip);
    if (it != privateIpRangeMap.end() && it->second.include(ip)) {
        return true;
    }

    if (it != privateIpRangeMap.begin()) {
        it--;
        if (it->second.include(ip)) {
            return true;
        }
    }

    return false;
}

std::string PublicIpPrivateIpRange::toString() {
    std::stringstream ss;
    // rwlock lk(_lock, false);
    ss << " publice Ip: [ ";
    for (auto it = publicIpMap.begin(); it != publicIpMap.end(); it++) {
        if (it->second[0] != "") {
            ss << it->first << "=" << it->second[0] << " ";
        }
        if (it->second[1] != "") {
            ss << it->first << "=" << it->second[1] << " ";
        }
    }
    ss << "]. privateIpRange: size is " << privateIpRangeMap.size() << ". ";
    for (auto it = privateIpRangeMap.begin(); it != privateIpRangeMap.end(); it++) {
        it->second.toString(ss);
    }
    return ss.str();
}
}
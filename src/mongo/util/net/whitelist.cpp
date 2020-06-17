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
#include <ctype.h>
#include <map>
#include <netinet/in.h>
#include <sstream>
#include <stdio.h>
#include <string>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "mongo/util/text.h"
#include <boost/algorithm/string.hpp>
// #include "mongo/util/concurrency/rwlock.h"
#include "mongo/util/net/whitelist.h"

namespace mongo {

int IpRange::ipType(const std::string& ipstr) {
    __uint128_t tmp = 0;

    int ret = inet_pton(AF_INET, ipstr.c_str(), &tmp);

    if (ret > 0) {
        return 0;
    }

    if (ret == 0) {
        ret = inet_pton(AF_INET6, ipstr.c_str(), &tmp);
    }

    if (ret <= 0) {
        return -1;
    }

    return 1;
}

bool IpRange::addrToUint(const std::string& addr, __uint128_t& ipval) {
    in6_addr ip6addr;

    for (int i = 0; i < 16; i++) {
        ip6addr.s6_addr[i] = 0;
    }

    int ret = inet_pton(AF_INET, addr.c_str(), &ip6addr);
    if (ret > 0) {
        for (int i = 0; i < 4; i++) {
            ip6addr.s6_addr[i + 12] = ip6addr.s6_addr[i];
            ip6addr.s6_addr[i] = 0;
        }
    }

    if (ret == 0) {
        ret = inet_pton(AF_INET6, addr.c_str(), &ip6addr);
    }

    if (ret <= 0) {
        return false;
    }

    ipval = 0;
    for (int i = 0; i < 16; i++) {
        ipval |= ip6addr.s6_addr[i];
        if (i < 15) {
            ipval <<= 8;
        }
    }

    return true;
}

void IpRange::uintToAddr(const __uint128_t& ipval, std::string& addr) {
    char str[INET6_ADDRSTRLEN];
    __uint128_t tmpValue = ipval;
    in6_addr ip6addr;
    int mode = ((tmpValue >> 32) == 0) ? AF_INET : AF_INET6;

    for (int i = 15; i >= 0; i--) {
        ip6addr.s6_addr[i] = tmpValue & 0xff;
        tmpValue >>= 8;
    }

    if (mode == AF_INET) {
        for (int i = 0; i < 4; i++) {
            ip6addr.s6_addr[i] = ip6addr.s6_addr[i + 12];
            ip6addr.s6_addr[i + 12] = 0;
        }
    }

    if (inet_ntop(mode, &ip6addr, str, INET6_ADDRSTRLEN) == NULL) {
        return;
    }
    addr.assign(str);
}
// valid format
// 192.168.1.100 or 192.168.1.100/24  or 192.168.1.100-192.168.1.200
// rds rule: 0.0.0.0/0 means all ips
bool IpRange::parseItem(const std::string& raw, IpRange& range) {
    std::string item = raw;
    boost::trim(item);

    if (item.find("/") != std::string::npos) {
        std::vector<std::string> fields = StringSplitter::split(item, "/");
        if (fields.size() != 2) {
            return false;
        }

        __uint128_t val = 0;
        if (!IpRange::addrToUint(fields[0], val)) {
            return false;
        }

        bool ipv4 = (fields[0].find(":") == std::string::npos);

        int mask = atoi(fields[1].c_str());

        if (ipv4) {
            mask += (128 - 32);
        }

        if (mask < 0 || mask > 128) {
            return false;
        }

        if (mask == 0) {
            range.min = 0;
            range.max = IpRange::maxNum;
        } else {
            range.min = (val & (IpRange::maxNum << (128 - mask)));
            range.max = (val | (~(IpRange::maxNum & (IpRange::maxNum << (128 - mask)))));
        }
    } else if (item.find("-") != std::string::npos) {
        std::vector<std::string> fields = StringSplitter::split(item, "-");
        if (fields.size() != 2) {
            return false;
        }

        if (!IpRange::addrToUint(fields[0], range.min) ||
            !IpRange::addrToUint(fields[1], range.max) || range.min > range.max) {
            return false;
        }
    } else {
        if (!IpRange::addrToUint(item, range.min)) {
            return false;
        }
        range.max = range.min;
    }

    return true;
}

void IpRange::toString(std::stringstream& ss) {

    std::string min_ipstr;
    std::string max_ipstr;

    IpRange::uintToAddr(min, min_ipstr);
    IpRange::uintToAddr(max, max_ipstr);

    ss << "[" << min_ipstr << "," << max_ipstr << "] ";
}

bool WhiteList::parseFromFile(const std::string& path, std::string& errmsg) {
    if (path.empty() || path[0] != '/') {
        errmsg = "must be absolute path";
        return false;
    }

    // support max file size 16KB
    const int kMaxFileSize = 65536;
    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        errmsg = strerror(errno);
        return false;
    }

    if (st.st_size > kMaxFileSize) {
        errmsg = "file size exceed 16KB";
        return false;
    }

    FILE* f = fopen(path.c_str(), "r");
    if (f == NULL) {
        errmsg = strerror(errno);
        return false;
    }

    char line[kMaxFileSize + 1] = {0};
    if (fgets(line, kMaxFileSize, f) == NULL) {
        return false;
    }
    fclose(f);

    std::string content = line;
    boost::trim(content);

    if (!parseFromString(content)) {
        errmsg = "whitelist format invalid";
        return false;
    }

    _path = path;
    return true;
}

// eg: 192.168.1.100,192.168.1.100/24
bool WhiteList::parseFromString(const std::string& line) {
    if (line.empty()) {
        // rwlock lk(_lock, true);
        _whiteList.clear();
        return true;
    }

    // parse each item
    std::multimap<__uint128_t, IpRange> whiteMap;
    std::vector<std::string> items = StringSplitter::split(line, ",");
    auto it = items.begin();
    for (; it != items.end(); it++) {
        IpRange range;
        if (!IpRange::parseItem(*it, range)) {
            return false;
        }

        whiteMap.insert(std::make_pair(range.min, range));
    }

    // merge overlapped items
    std::map<__uint128_t, IpRange> whiteList;
    IpRange last;
    auto mit = whiteMap.begin();
    for (; mit != whiteMap.end(); mit++) {
        if (last.invalid()) {
            last = mit->second;
        } else {
            if (last.max >= mit->second.min) {
                last.max = std::max(last.max, mit->second.max);
            } else {
                whiteList.insert(std::make_pair(last.min, last));
                last = mit->second;
            }
        }
    }

    if (!last.invalid()) {
        whiteList.insert(std::make_pair(last.min, last));
    }

    // swap to update
    // rwlock lk(_lock, true);
    _whiteList.swap(whiteList);

    return true;
}

int WhiteList::rangeSize() {
    // rwlock lk(_lock, false);
    return _whiteList.size();
}

bool WhiteList::include(const __uint128_t& ip) {
    // rwlock lk(_lock, false);
    if (_whiteList.empty()) {
        return false;
    }

    std::map<__uint128_t, IpRange>::iterator it = _whiteList.lower_bound(ip);
    if (it != _whiteList.end() && it->second.include(ip)) {
        return true;
    }

    if (it != _whiteList.begin()) {
        it--;
        if (it->second.include(ip)) {
            return true;
        }
    }

    return false;
}

bool WhiteList::include(const std::string& ipstr) {
    __uint128_t ipval = 0;
    if (!IpRange::addrToUint(ipstr, ipval)) {
        return false;
    }
    return include(ipval);
}

void WhiteList::setMatchAll() {
    // rwlock lk(_lock, true);
    IpRange range;
    range.min = 0;
    range.max = IpRange::maxNum;
    _whiteList.clear();
    _whiteList.insert(std::make_pair(range.min, range));
}

bool WhiteList::isMatchAll() {
    // rwlock lk(_lock, false);
    if (_whiteList.size() != 1) {
        return false;
    }

    auto it = _whiteList.begin();
    return it->second.min == 0 && it->second.max == IpRange::maxNum;
}

void WhiteList::setMatchNone() {
    // rwlock lk(_lock, true);
    _whiteList.clear();
}

bool WhiteList::isMatchNone() {
    // rwlock lk(_lock, false);
    return _whiteList.empty();
}

std::string WhiteList::toString() {
    std::stringstream ss;
    // rwlock lk(_lock, false);
    auto it = _whiteList.begin();
    for (; it != _whiteList.end(); it++) {
        it->second.toString(ss);
    }
    return ss.str();
}
}
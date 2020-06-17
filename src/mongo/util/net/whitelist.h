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

// #include "mongo/util/concurrency/rwlock.h"

namespace mongo {

struct IpRange {
    __uint128_t min;
    __uint128_t max;
    static constexpr __uint128_t maxNum = ~0;

    IpRange() : min(0), max(0) {}

    bool include(const __uint128_t& ip) const {
        return ip >= min && ip <= max;
    }

    void reset() {
        min = 0;
        max = 0;
    }

    bool invalid() {
        return min == 0 && max == 0;
    }

    static bool addrToUint(const std::string& addr, __uint128_t& ipval);

    static void uintToAddr(const __uint128_t& ipval, std::string& addr);

    // ret value: 0: ipv4, 1: ipv6, -1: ip is unkonw
    static int ipType(const std::string& ipstr);
    static bool parseItem(const std::string& raw, IpRange& range);

    void toString(std::stringstream& ss);
};

/*
 * WhiteList is a comma separated string
 * suppoted format
 * 1. single ip, 192.168.1.100
 * 2. netmask, 192.168.1.100/24
 * 3. net range, 192.168.1.100-192.168.1.200
 */
class WhiteList {
public:
    // WhiteList():_lock("whitelistMutex") {  }
    bool parseFromFile(const std::string& path, std::string& errmsg);
    bool parseFromString(const std::string& line);
    bool include(const __uint128_t& ip);
    bool include(const std::string& ipstr);
    int rangeSize();
    std::string toString();

    void setMatchAll();
    bool isMatchAll();
    void setMatchNone();
    bool isMatchNone();

    std::string path() {
        return _path;
    }

private:
    std::map<__uint128_t, IpRange> _whiteList;
    std::string _path;
    // RWLock _lock;
};
}
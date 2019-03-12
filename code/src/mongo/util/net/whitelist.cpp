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
#include <stdio.h>
#include <string>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "mongo/util/concurrency/rwlock.h"
#include "mongo/util/log.h"
#include "mongo/util/net/whitelist.h"
#include "mongo/util/text.h"
#include <boost/algorithm/string.hpp>

using namespace std;

namespace mongo {

bool WhiteList::parseFromFile(const string& path, string& errmsg) {
    if (path.empty() || path[0] != '/') {
        errmsg = "must be absolute path";
        return false;
    }

    // support max file size 4KB
    const int kMaxFileSize = 4 * 1024;
    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        errmsg = strerror(errno);
        index_err() << "parseFromFile fail path: " << path << "; error: " << errmsg;
        return false;
    }

    if (st.st_size > kMaxFileSize) {
        errmsg = "file size exceed 16KB";
        index_err() << "parseFromFile file size exceed 16KB path: " << path;
        return false;
    }

    FILE* f = fopen(path.c_str(), "r");
    if (f == NULL) {
        errmsg = strerror(errno);
        index_err() << "parseFromFile opfile error path: " << path << "; err: " << errmsg;
        return false;
    }

    char line[kMaxFileSize + 1] = {0};
    if (fgets(line, kMaxFileSize, f) == NULL) {
        index_err() << "parseFromFile fgets error path: " << path;
        fclose(f);
        return false;
    }
    fclose(f);

    std::string content = line;
    boost::trim(content);

    if (!content.empty() && !isdigit(content[0])) {
        errmsg = "whitelist line must starts with [0-9]";
        index_err() << "parseFromFile whitelist line must starts with [0-9] path: " << path;
        return false;
    }

    if (!parseFromString(content)) {
        errmsg = "whitelist format invalid";
        index_err() << "parseFromFile whitelist format invalid path: " << path;
        return false;
    }

    index_LOG(2) << "parseFromFile whitelist suc path: " << path
                 << "; ipnum: " << _whiteList.size();
    _path = path;
    return true;
}

// eg: 192.168.1.100,192.168.1.100/24
bool WhiteList::parseFromString(const string& line) {
    if (line.empty()) {
        rwlock lk(_lock, true);
        _whiteList.clear();
        return true;
    }

    // parse each item
    multimap<uint32_t, IpRange> whiteMap;
    vector<string> items = StringSplitter::split(line, ",");
    vector<string>::iterator it = items.begin();
    for (; it != items.end(); it++) {
        IpRange range;
        if (!parseItem(*it, range)) {
            return false;
        }

        whiteMap.insert(make_pair(range.min, range));
    }

    // merge overlapped items
    map<uint32_t, IpRange> whiteList;
    IpRange last;
    multimap<uint32_t, IpRange>::iterator mit = whiteMap.begin();
    for (; mit != whiteMap.end(); mit++) {
        if (last.invalid()) {
            last = mit->second;
        } else {
            if (last.max >= mit->second.min) {
                last.max = std::max(last.max, mit->second.max);
            } else {
                whiteList.insert(make_pair(last.min, last));
                last = mit->second;
            }
        }
    }

    if (!last.invalid()) {
        whiteList.insert(make_pair(last.min, last));
    }

    // swap to update
    rwlock lk(_lock, true);
    _whiteList.swap(whiteList);

    return true;
}

int WhiteList::rangeSize() {
    rwlock lk(_lock, false);
    return _whiteList.size();
}

bool WhiteList::include(const uint32_t ip) {
    rwlock lk(_lock, false);
    if (_whiteList.empty()) {
        return false;
    }

    map<uint32_t, IpRange>::iterator it = _whiteList.lower_bound(ip);
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
    uint32_t ipval = 0;
    if (!addrToUint(ipstr, ipval)) {
        return false;
    }
    return include(ipval);
}

// valid format
// 192.168.1.100 or 192.168.1.100/24  or 192.168.1.100-192.168.1.200
// rds rule: 0.0.0.0/0 means all ips
bool WhiteList::parseItem(const string& raw, IpRange& range) {
    std::string item = raw;
    boost::trim(item);

    if (item.find("/") != string::npos) {
        vector<string> fields = StringSplitter::split(item, "/");
        if (fields.size() != 2) {
            return false;
        }

        uint32_t val = 0;
        if (!addrToUint(fields[0], val)) {
            return false;
        }

        int mask = atoi(fields[1].c_str());
        if (mask < 0 || mask > 32) {
            return false;
        }

        if (mask == 0) {
            range.min = 0;
            range.max = 0xFFFFFFFF;
        } else {
            range.min = (val & (0xFFFFFFFF << (32 - mask)));
            range.max = (val | ((1 << (32 - mask)) - 1));
        }
    } else if (item.find("-") != string::npos) {
        vector<string> fields = StringSplitter::split(item, "-");
        if (fields.size() != 2) {
            return false;
        }

        if (!addrToUint(fields[0], range.min) || !addrToUint(fields[1], range.max) ||
            range.min > range.max) {
            return false;
        }
    } else {
        if (!addrToUint(item, range.min)) {
            return false;
        }
        range.max = range.min;
    }

    return true;
}

void WhiteList::setMatchAll() {
    rwlock lk(_lock, true);
    IpRange range;
    range.min = 0;
    range.max = 0xFFFFFFFF;
    _whiteList.clear();
    _whiteList.insert(std::make_pair(range.min, range));
}

bool WhiteList::isMatchAll() {
    rwlock lk(_lock, false);
    if (_whiteList.size() != 1) {
        return false;
    }

    std::map<uint32_t, IpRange>::iterator it = _whiteList.begin();
    return it->second.min == 0 && it->second.max == 0xFFFFFFFF;
}

void WhiteList::setMatchNone() {
    rwlock lk(_lock, true);
    _whiteList.clear();
}

bool WhiteList::isMatchNone() {
    rwlock lk(_lock, false);
    return _whiteList.empty();
}

string WhiteList::toString() {
    stringstream ss;
    rwlock lk(_lock, false);
    map<uint32_t, IpRange>::iterator it = _whiteList.begin();
    for (; it != _whiteList.end(); it++) {
        string min_ipstr;
        string max_ipstr;
        uintToAddr(it->second.min, min_ipstr);
        uintToAddr(it->second.max, max_ipstr);
        ss << "[" << min_ipstr << ":" << max_ipstr << "]";
    }
    return ss.str();
}

bool WhiteList::addrToUint(const string& addr, uint32_t& ipval) {
    struct in_addr inaddr;
    int ret = inet_aton(addr.c_str(), &inaddr);
    if (!ret) {
        return false;
    }

    ipval = ntohl(inaddr.s_addr);
    return true;
}

void WhiteList::uintToAddr(uint32_t ipval, string& addr) {
    uint32_t nipval = htonl(ipval);
    char str[64] = {0};
    unsigned char* bytes = (unsigned char*)&nipval;
    snprintf(
        str,  sizeof(str) - 1, "%d.%d.%d.%d", bytes[0], bytes[1], bytes[2], bytes[3]);
    addr.assign(str);
}
}

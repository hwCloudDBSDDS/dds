/**
 *    Copyright (C) 2014 MongoDB, Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "mongo/db/global_timestamp.h"
#include "mongo/db/service_context.h"
#include "mongo/util/clock_source.h"
#include "mongo/util/concurrency/spin_lock.h"
#include "mongo/util/log.h"
#include "mongo/util/time_support.h"

namespace mongo {
namespace {
// This is the value of the next timestamp to handed out.
uint64_t globalTimestamp(Timestamp(0, 0).asULL());
unsigned globalMemSecs(0);
SpinLock globalTimestampLock;
}  // namespace

void setGlobalTimestamp(const Timestamp& newTime) {
    scoped_spinlock lk(globalTimestampLock);
    globalTimestamp = newTime.asULL() + 1;
}

Timestamp getLastSetTimestamp() {
    scoped_spinlock lk(globalTimestampLock);
    return Timestamp(globalTimestamp - 1 );
}

Timestamp getNextGlobalTimestamp(unsigned count) {
    invariant((count != 0) && (count <= uint32_t(std::numeric_limits<int32_t>::max())));
    scoped_spinlock lk(globalTimestampLock);

    const unsigned now = durationCount<Seconds>(
        getGlobalServiceContext()->getFastClockSource()->now().toDurationSinceEpoch());
    if(0 == globalMemSecs){ globalMemSecs = now;}
    if(0 == globalTimestamp){ globalTimestamp = Timestamp(now, 1).asULL();}

    // Optimistic approach: just increment the timestamp, assuming the seconds still match.
    uint64_t first = globalTimestamp;
    auto currentTimestamp = first + count;  // What we just set it to.

    // Fail if time is not moving forward for 2**31 calls to getNextGlobalTimestamp.
    if (Timestamp(currentTimestamp).getInc() >= 1U << 31) {
        mongo::severe() << "not moving forward for 2**31 calls ";
        fassertFailed(17449);
    }

    auto difSecs = now - globalMemSecs;
    if(difSecs){
        globalMemSecs = now; //update global seconds.
        unsigned g_secs = Timestamp(globalTimestamp).getSecs() + difSecs;
        first = Timestamp(g_secs, 1).asULL();
    }

    globalTimestamp = first + count;

    return Timestamp(first);
}
}  // namespace mongo

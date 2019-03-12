/**
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

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <fstream>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <time.h>
#include <stdlib.h>
#include <fcntl.h>

#include "mongo/util/util_extend/status_renewal.h"
#include "mongo/util/util_extend/GlobalConfig.h"

#include "mongo/base/status.h"
#include "mongo/db/client.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/concurrency/thread_name.h"
#include "mongo/util/log.h"
#include "mongo/util/exit.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/util_extend/default_parameters.h"

namespace fs = boost::filesystem;

namespace mongo {

namespace {

StatusRenewal* globalStatusRenewal = NULL;

using std::string;
using std::stringstream;
using std::ofstream;
using std::ios_base;
using std::ios;
using str::stream;

}  // namespace

StatusRenewal* getGlobalStatusRenewal() {
    fassert(50002, globalStatusRenewal);

    return globalStatusRenewal;
}

void setGlobalStatusRenewal(
    std::unique_ptr<StatusRenewal>&& statusRenewal) {

    fassert(50003, statusRenewal.get());

    delete globalStatusRenewal;

    globalStatusRenewal = statusRenewal.release();
}

StatusRenewal::StatusRenewal() {
}

StatusRenewal::~StatusRenewal() {
    _removeWorker();
}

Status StatusRenewal::Init(const std::string& name) {
    index_log() << "Init status renewal worker for " << name;
    _name = name;
    _stop = false;
    
    // create thread to update the status file
    _workThread.reset(new (std::nothrow) stdx::thread(stdx::bind(&StatusRenewal::_doTask, this)));
    if(!_workThread) {
        return Status(ErrorCodes::ExceededMemoryLimit, "failed to allocate _workThread.");
    }

    sched_param sch = {80};
    int policy = 0;
    // SCHED_NORMAL: 0, SCHED_FIFO: 1, SCHED_RR: 2, SCHED_BATCH: 3, SCHED_IDLE: 5
    if (0 != pthread_setschedparam(_workThread->native_handle(), SCHED_RR, &sch))
    {
        index_err() << "failed to set schedparam for thread StatusRenewal: " << strerror(errno);
        return Status::OK();
    }

    pthread_getschedparam(_workThread->native_handle(), &policy, &sch);
    index_log() << "thread StatusRenewal - policy: " << policy << ", priority: " << sch.sched_priority;
    
    return Status::OK();
}

void StatusRenewal::enableShardHeartbeatCheck() {
    _ssHbCheckFlag.store(true);
}

void StatusRenewal::disableShardHeartbeatCheck() {
    _ssHbCheckFlag.store(false);
}

void StatusRenewal::updateShardServerHeartbeatTime(const Date_t& hitTime) {
    _ssLastHbtHitTime.store(hitTime.asInt64());
}

void StatusRenewal::_checkShardServerHeartbeatTimeOut(const Date_t& now) {
    Milliseconds alreadyElapsed(now.asInt64() - _ssLastHbtHitTime.load());
    if (alreadyElapsed >= kDefaultShardExitTimeoutPeriod) {
        index_log() << "[Heartbeat detection] Exit due to heartbeat timeout , lastHbHitTime : "
                    << _ssLastHbtHitTime.load() << "; now : " << now;
        ::_Exit(EXIT_FAILURE);
    }
}

void StatusRenewal::_removeWorker() {
    index_log() << "remove status renewal worker";
    _stop = true;
    _workThread->join();
}

void StatusRenewal::_doTask() {
    Client::initThread("StatusRenewal");
    index_log() << "status renewal thread is running";

    Date_t startTime;
    Milliseconds timeUsed(0);
    while (!_stop) {
        startTime = Date_t::now();

        if(_ssHbCheckFlag.load()) {
            _checkShardServerHeartbeatTimeOut(startTime);
        }

        stdx::this_thread::sleep_for(kDefaultStatusRenewalInterval.toSteadyDuration());
    } //lint !e1788  

}

} //namespace mongo

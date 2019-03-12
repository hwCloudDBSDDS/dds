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

#pragma once

#include <string>
#include <list>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/status_with.h"
#include "mongo/db/server_options.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/functional.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/net/hostandport.h"
//#include "mongo/util/util_extend/job_registry.h"

namespace mongo {

/*
 * StatusRenewal-
 *
 * ConfigServer, ShardServer and Mongos now periodically write a status message into
 * a file named status under dbpath to indicate it's alive. The message consists of
 * a timestamp, the hostAndPort of the service and the role of the service(Standby
 * or Active). For ShardServer, the role is useful but for ConfigServer and Mongos
 * it is useless.
 */
class StatusRenewal {
    MONGO_DISALLOW_COPYING(StatusRenewal);

public:
    // register/unregister
    Status Init(const std::string& name);

    void enableShardHeartbeatCheck();
    void disableShardHeartbeatCheck();
    void updateShardServerHeartbeatTime(const Date_t& hitTime);
      
    explicit StatusRenewal();
    ~StatusRenewal();

private:

    void _removeWorker();

    // check if shardserver needs to exit
    void _checkShardServerHeartbeatTimeOut(const Date_t& now);
    
    // do renewal task periodically
    void _doTask();

    std::string _name;  // Name of worker
    std::unique_ptr<stdx::thread> _workThread; // thread to run _doTask()
    std::string _statusFilePath; // The path of status file
    bool _stop = false; // indicate whether _workThread should stop
    std::atomic<bool> _restart{false};

    std::atomic<bool> _ssHbCheckFlag{false};
    std::atomic<int64_t> _ssLastHbtHitTime{0};

};

StatusRenewal* getGlobalStatusRenewal();

void setGlobalStatusRenewal(
    std::unique_ptr<StatusRenewal>&& statusRenewal);


} // namespace mongo

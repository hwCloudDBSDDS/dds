/*    Copyright 2013 10gen Inc.
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

#include "mongo/db/jsobj.h"
#include "mongo/platform/process_id.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/catalog/type_shard_server.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/concurrency/notification.h"
#include "mongo/util/net/listen.h"  // For DEFAULT_MAX_CONN
#include "mongo/util/net/whitelist.h"
#include "mongo/util/time_support.h"

namespace mongo {
const int DEFAULT_UNIX_PERMS = 0700;

enum class ClusterRole { None, ShardServer, ConfigServer };

struct ServerGlobalParams {
    std::string binaryName;  // mongod or mongos
    std::string cwd;         // cwd of when process started

    int port = DefaultDBPort;  // --port
    enum { DefaultDBPort = 27017, ConfigServerPort = 27019, ShardServerPort = 27018 };
    bool isDefaultPort() const {
        return port == DefaultDBPort;
    }

    std::string bind_ip;          // --bind_ip
    std::string extendIPs;        // manage ips
    std::string bindIp;           // bind ip for register,active,heartbeat
    std::string processIdentity;  // add for shard process re-start after failover, used as identity
                                  // in register flow

    bool rest = false;   // --rest
    bool jsonp = false;  // --jsonp

    bool indexBuildRetry = true;  // --noIndexBuildRetry

    std::atomic<bool> quiet{false};  // --quiet NOLINT

    ClusterRole clusterRole = ClusterRole::None;  // --configsvr/--shardsvr
    std::string shardName = "";  // shardName to be used when adding shardsvr to a shard
    ShardServerType::ShardServerState shardServerState =
        ShardServerType::ShardServerState::kStandby;
    ShardType::ShardState shardState = ShardType::ShardState::kShardRegistering;
    std::shared_ptr<Notification<void>>
        activeNotification;  // standby will be blocked until receive grant active message

    stdx::mutex shardServerStateMutex;
    stdx::mutex shardStateMutex;

    // Auto-increment value used to generate chunk id. Initialized when CS become a leader, by
    // loading the maximum id from the chunk collection.
    std::atomic<uint64_t> chunkCount{0};  // --quiet NOLINT

    bool cpu = false;  // --cpu show cpu time periodically

    bool objcheck = true;  // --objcheck

    int defaultProfile = 0;                // --profile
    int slowMS = 100;                      // --time in ms that is "slow"
    int profileSizeMB = 1;                 // --the maxsize of system.profile collection
    int defaultLocalThresholdMillis = 15;  // --localThreshold in ms to consider a node local
    bool moveParanoia = false;             // for move chunk paranoia

    bool noUnixSocket = false;    // --nounixsocket
    bool doFork = false;          // --fork
    std::string socket = "/tmp";  // UNIX domain socket directory

    int maxConns = DEFAULT_MAX_CONN;  // Maximum number of simultaneous open connections.
    int maxInternalConns =
        DEFAULT_MAX_CONN_INTERNAL;  // Maximum number of simultaneous open internal connections.

    int unixSocketPermissions = DEFAULT_UNIX_PERMS;  // permissions for the UNIX domain socket

    std::string keyFile;  // Path to keyfile, or empty if none.
    std::string pidFile;  // Path to pid file, or empty if none.

    std::string logpath;            // Path to log file, if logging to a file; otherwise, empty.
    bool logAppend = false;         // True if logging to a file in append mode.
    bool logRenameOnRotate = true;  // True if logging should rename log files on rotate
    bool logWithSyslog = false;     // True if logging to syslog; must not be set if logpath is set.
    int syslogFacility;             // Facility used when appending messages to the syslog.

    bool isHttpInterfaceEnabled = false;  // True if the dbwebserver should be enabled.
    bool limitVerifyTimes = true;
    std::string auditLogpath;  // Path to audit log file, if logging to a file; otherwise, empty.
    std::string auditLogFormat = "JSON";  // Format of audit log
    std::string auditOpFilterStr;         // Filter ops that need to be audited, string format.
    int auditOpFilter = 0xFFFFFFFF;  // Bitwise or result of ops that need to be audited, parsed
                                     // from auditOpFilterStr.
    bool auditAuthSuccess = true;    // True if audit authorization success requests.
    bool readOnly = false;

#ifndef _WIN32
    ProcessId parentProc;  // --fork pid of initial process
    ProcessId leaderProc;  // --fork pid of leader process
#endif

    /**
     * Switches to enable experimental (unsupported) features.
     */
    struct ExperimentalFeatures {
        ExperimentalFeatures() : storageDetailsCmdEnabled(false) {}
        bool storageDetailsCmdEnabled;  // -- enableExperimentalStorageDetailsCmd
    } experimental;

    Date_t started = Date_t::now();

    BSONArray argvArray;
    BSONObj parsedOpts;

    enum AuthState { kEnabled, kDisabled, kUndefined };

    AuthState authState = AuthState::kUndefined;

    bool transitionToAuth = false;  // --transitionToAuth, mixed mode for rolling auth upgrade
    AtomicInt32 clusterAuthMode;    // --clusterAuthMode, the internal cluster auth mode

    enum ClusterAuthModes {
        ClusterAuthMode_undefined,
        /**
        * Authenticate using keyfile, accept only keyfiles
        */
        ClusterAuthMode_keyFile,

        /**
        * Authenticate using keyfile, accept both keyfiles and X.509
        */
        ClusterAuthMode_sendKeyFile,

        /**
        * Authenticate using X.509, accept both keyfiles and X.509
        */
        ClusterAuthMode_sendX509,

        /**
        * Authenticate using X.509, accept only X.509
        */
        ClusterAuthMode_x509
    };

    // for the YAML config, sharding._overrideShardIdentity. Can only be used when in
    // queryableBackupMode.
    BSONObj overrideShardIdentity;

    struct FeatureCompatibility {
        enum class Version {
            /**
             * In this mode, the cluster will expose a 3.2-like API. Attempts by a client to use new
             * features in 3.4, such as read-only views, collation, or the decimal128 BSON type,
             * will be rejected.
             */
            k32,

            /**
             * In this mode, new features in 3.4 are allowed. The system should guarantee that no
             * 3.2 node can participate in a cluster whose feature compatibility version is 3.4.
             */
            k34,
        };

        // Read-only parameter featureCompatibilityVersion.
        AtomicWord<Version> version{Version::k34};

        // Feature validation differs depending on the role of a mongod in a replica set or
        // master/slave configuration. Masters/primaries can accept user-initiated writes and
        // validate based on the feature compatibility version. A secondary/slave (which is not also
        // a master) always validates in "3.4" mode so that it can sync 3.4 features, even when in
        // "3.2" feature compatibility mode.
        AtomicWord<bool> validateFeaturesAsMaster{true};
    } featureCompatibility;

    /**
     * host  in adminWhiteList manage and monitor mongodb instances
     * It's not limited by 'maxIncomingConnections'
     */
    WhiteList adminWhiteList;
};

extern ServerGlobalParams serverGlobalParams;
}

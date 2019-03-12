//
// Tests of cleanupOrphaned command permissions.
//

(function() {
    'use strict';

    function assertUnauthorized(res, msg) {
        if (assert._debug && msg)
            print("in assert for: " + msg);

        if (res.ok == 0 && res.errmsg.startsWith('not authorized'))
            return;

        var finalMsg = "command worked when it should have been unauthorized: " + tojson(res);
        if (msg) {
            finalMsg += " : " + msg;
        }
        doassert(finalMsg);
    }

    var st =
        new ShardingTest({auth: true, other: {keyFile: 'jstests/libs/key1', useHostname: false}});

    var shardAdmin = st.shard0.getDB('admin');
    shardAdmin.createUser({
        user: 'admin',
        pwd: 'WEak@2password',
        roles: ['clusterAdmin', 'userAdminAnyDatabase'],
        passwordDigestor: "server"
    });
    shardAdmin.auth('admin', 'WEak@2password');

    var mongos = st.s0;
    var mongosAdmin = mongos.getDB('admin');
    var coll = mongos.getCollection('foo.bar');

    mongosAdmin.createUser({
        user: 'admin',
        pwd: 'WEak@2password',
        roles: ['clusterAdmin', 'userAdminAnyDatabase'],
        passwordDigestor: "server"
    });
    mongosAdmin.auth('admin', 'WEak@2password');

    assert.commandWorked(mongosAdmin.runCommand({enableSharding: coll.getDB().getName()}));

    assert.commandWorked(
        mongosAdmin.runCommand({shardCollection: coll.getFullName(), key: {_id: 'hashed'}}));

    // cleanupOrphaned requires auth as admin user.
    assert.commandWorked(shardAdmin.logout());
    assertUnauthorized(shardAdmin.runCommand({cleanupOrphaned: 'foo.bar'}));

    var fooDB = st.shard0.getDB('foo');
    shardAdmin.auth('admin', 'WEak@2password');
    fooDB.createUser({
        user: 'user',
        pwd: 'WEak@2password',
        roles: ['readWrite', 'dbAdmin'],
        passwordDigestor: "server"
    });
    shardAdmin.logout();
    fooDB.auth('user', 'WEak@2password');
    assertUnauthorized(shardAdmin.runCommand({cleanupOrphaned: 'foo.bar'}));

    st.stop();
})();

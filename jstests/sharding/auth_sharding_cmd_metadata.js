/**
 * Tests that only the internal user will be able to advance the config server opTime.
 */
(function() {

    "use strict";

    // TODO: Remove 'shardAsReplicaSet: false' when SERVER-32672 is fixed.
    var st = new ShardingTest(
        {shards: 1, other: {keyFile: 'jstests/libs/key1', shardAsReplicaSet: false}});

    var adminUser = {db: "admin", username: "admin", password: "Password@a1b"};

    st.s.getDB(adminUser.db).createUser({
        user: 'admin',
        pwd: 'Password@a1b',
        roles: jsTest.adminUserRoles, "passwordDigestor": "server"
    });

    st.s.getDB('admin').auth('admin', 'Password@a1b');

    st.adminCommand({enableSharding: 'test'});
    st.adminCommand({shardCollection: 'test.user', key: {x: 1}});

    st.d0.getDB('admin').createUser({
        user: 'omuser',
        pwd: 'Password@a1b',
        roles: jsTest.adminUserRoles, "passwordDigestor": "server"
    });
    st.d0.getDB('admin').auth('omuser', 'Password@a1b');

    var maxSecs = Math.pow(2, 32) - 1;
    var metadata = {$configServerState: {opTime: {ts: Timestamp(maxSecs, 0), t: maxSecs}}};
    var res = st.d0.getDB('test').runCommandWithMetadata({ping: 1}, metadata);

    assert.commandFailedWithCode(res.commandReply, ErrorCodes.Unauthorized);

    // Make sure that the config server optime did not advance.
    var status = st.d0.getDB('test').runCommand({serverStatus: 1});
    assert.neq(null, status.sharding);
    assert.lt(status.sharding.lastSeenConfigServerOpTime.t, maxSecs);

    st.d0.getDB('admin').createUser(
        {user: 'internal', pwd: 'Password@a1b', roles: ['__system'], "passwordDigestor": "server"});
    st.d0.getDB('admin').auth('internal', 'Password@a1b');

    res = st.d0.getDB('test').runCommandWithMetadata({ping: 1}, metadata);
    assert.commandWorked(res.commandReply);

    status = st.d0.getDB('test').runCommand({serverStatus: 1});
    assert.neq(null, status.sharding);
    assert.eq(status.sharding.lastSeenConfigServerOpTime.t, maxSecs);

    st.stop();

})();

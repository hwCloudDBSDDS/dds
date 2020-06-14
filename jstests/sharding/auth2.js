(function() {
    'use strict';

    // TODO: Remove 'shardAsReplicaSet: false' when SERVER-32672 is fixed.
    var st = new ShardingTest({
        shards: 2,
        other: {
            chunkSize: 1,
            useHostname: true,
            keyFile: 'jstests/libs/key1',
            shardAsReplicaSet: false
        },
    });

    var mongos = st.s;
    var adminDB = mongos.getDB('admin');
    var db = mongos.getDB('test');

    adminDB.createUser({
        user: 'admin',
        pwd: 'Password@a1b',
        roles: jsTest.adminUserRoles, "passwordDigestor": "server"
    });

    jsTestLog("Add user was successful");

    // Test for SERVER-6549, make sure that repeatedly logging in always passes.
    for (var i = 0; i < 100; i++) {
        adminDB = new Mongo(mongos.host).getDB('admin');
        assert(adminDB.auth('admin', 'Password@a1b'), "Auth failed on attempt #: " + i);
    }

    st.stop();
})();

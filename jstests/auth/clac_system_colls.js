/**
 * This tests that CLAC (collection level access control) handles system collections properly.
 * @tags: [requires_sharding]
 */

// Verify that system collections are treated correctly
function runTest(admindb) {
    var authzErrorCode = 13;

    admindb.createUser({
        user: "admin",
        pwd: "Password@a1b",
        roles: ["userAdminAnyDatabase"], "passwordDigestor": "server"
    });
    assert.eq(1, admindb.auth("admin", "Password@a1b"));

    var sysCollections = [
        "system.indexes",
        "system.js",
        "system.namespaces",
        "system.profile",
        "system.roles",
        "system.users"
    ];
    var sysPrivs = new Array();
    for (var i in sysCollections) {
        sysPrivs.push(
            {resource: {db: admindb.getName(), collection: sysCollections[i]}, actions: ['find']});
    }

    var findPriv = {resource: {db: admindb.getName(), collection: ""}, actions: ['find']};

    admindb.createRole({role: "FindInDB", roles: [], privileges: [findPriv]});
    admindb.createRole({role: "FindOnSysRes", roles: [], privileges: sysPrivs});

    admindb.createUser({
        user: "sysUser",
        pwd: "Password@a1b",
        roles: ["FindOnSysRes"], "passwordDigestor": "server"
    });
    admindb.createUser(
        {user: "user", pwd: "Password@a1b", roles: ["FindInDB"], "passwordDigestor": "server"});

    // Verify the find on all collections exludes system collections
    assert.eq(1, admindb.auth("user", "Password@a1b"));

    assert.doesNotThrow(function() {
        admindb.foo.findOne();
    });
    for (var i in sysCollections) {
        assert.commandFailed(admindb.runCommand({count: sysCollections[i]}));
    }

    // Verify that find on system collections gives find permissions
    assert.eq(1, admindb.auth("sysUser", "Password@a1b"));

    assert.throws(function() {
        admindb.foo.findOne();
    });
    for (var i in sysCollections) {
        assert.commandWorked(admindb.runCommand({count: sysCollections[i]}));
    }

    admindb.logout();
}

jsTest.log('Test standalone');
var conn = MongoRunner.runMongod({auth: ''});
runTest(conn.getDB("admin"));
MongoRunner.stopMongod(conn);

jsTest.log('Test sharding');
// TODO: Remove 'shardAsReplicaSet: false' when SERVER-32672 is fixed.
var st = new ShardingTest(
    {shards: 2, config: 3, keyFile: 'jstests/libs/key1', other: {shardAsReplicaSet: false}});
runTest(st.s.getDB("admin"));
st.stop();

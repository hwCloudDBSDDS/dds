/**
 * This tests that all the different commands for user manipulation all work properly for all valid
 * forms of input.
 */
function runAllUserManagementCommandsTests(conn, writeConcern) {
    'use strict';

    var hasAuthzError = function(result) {
        assert(result instanceof WriteCommandError);
        assert.eq(ErrorCodes.Unauthorized, result.code);
    };

    conn.getDB('admin').createUser(
        {user: 'admin', pwd: 'Password@a1b', roles: ['root'], "passwordDigestor": "server"},
        writeConcern);
    conn.getDB('admin').auth('admin', 'Password@a1b');
    conn.getDB('admin').createUser({
        user: 'backupuser',
        pwd: 'Password@a1b',
        roles: ['userAdminAnyDatabase'], "passwordDigestor": "server",
        customData: {userAdmin: true}
    },
                                   writeConcern);

    conn.getDB('test').createRole({
        role: 'testRole',
        roles: [],
        privileges: [{resource: {db: 'test', collection: ''}, actions: ['viewRole']}],
    },
                                  writeConcern);
    conn.getDB('admin').createRole({
        role: 'adminRole',
        roles: [],
        privileges: [{resource: {cluster: true}, actions: ['connPoolSync']}]
    },
                                   writeConcern);

    conn.getDB('admin').createUser({
        user: "monitor",
        pwd: "Password@a1b",
        customData: {zipCode: 10028},
        roles: [
            {'role': 'readWrite', db: 'test'},
            {role: 'testRole', db: 'test'},
            {role: 'adminRole', db: 'admin'}
        ],
        "passwordDigestor": "server"
    },
                                   writeConcern);

    conn.getDB('admin').logout();

    var userAdminConn = new Mongo(conn.host);
    userAdminConn.getDB('admin').auth('backupuser', 'Password@a1b');
    var testUserAdmin = userAdminConn.getDB('test');
    var testUserAdmin2 = userAdminConn.getDB('admin');

    var db = conn.getDB('test');

    // At this point there are 2 handles to the "test" database in use - "testUserAdmin" and "db".
    // "testUserAdmin" is on a connection which has been auth'd as a user with the
    // 'userAdminAnyDatabase' role.  This will be used for manipulating the user defined roles
    // used in the test.  "db" is a handle to the test database on a connection that has not
    // yet been authenticated as anyone.  This is the connection that will be used to log in as
    // various users and test that their access control is correct.

    (function testCreateUser() {
        jsTestLog("Testing createUser");

        testUserAdmin.createUser(
            {user: "andy", pwd: "Password@a1b", roles: [], "passwordDigestor": "server"},
            writeConcern);
        var user = testUserAdmin2.getUser('monitor');
        assert.eq(10028, user.customData.zipCode);
        assert(db.getSisterDB('admin').auth('monitor', 'Password@a1b'));
        assert.writeOK(db.foo.insert({a: 1}));
        assert.eq(1, db.foo.findOne().a);
        assert.doesNotThrow(function() {
            db.getRole('testRole');
        });
        assert.commandWorked(db.adminCommand('connPoolSync'));

        db.getSisterDB('admin').logout();
        assert(db.auth('andy', 'Password@a1b'));
        var res = db.runCommand({connectionStatus: 1});
        var res = db.foo.insert({a: 1});
        hasAuthzError(db.foo.insert({a: 1}));
        assert.throws(function() {
            db.foo.findOne();
        });
        assert.throws(function() {
            db.getRole('testRole');
        });
    })();

    (function testUpdateUser() {
        jsTestLog("Testing updateUser");

        conn.getDB('test').logout();

        testUserAdmin.getSisterDB('admin').updateUser(
            'monitor',
            {pwd: 'Password@a1b4', customData: {}, "passwordDigestor": "server"},
            writeConcern);
        var user = testUserAdmin.getSisterDB('admin').getUser('monitor');
        assert.eq(null, user.customData.zipCode);
        assert(!db.getSisterDB('admin').auth('monitor', 'Password@a1b'));
        assert(db.getSisterDB('admin').auth('monitor', 'Password@a1b4'));

        testUserAdmin.getSisterDB('admin').updateUser(
            'monitor',
            {
              customData: {zipCode: 10036},
              roles: [{role: "read", db: 'test'}, {role: "testRole", db: 'test'}]
            },
            writeConcern);
        var user = testUserAdmin.getSisterDB('admin').getUser('monitor');
        assert.eq(10036, user.customData.zipCode);
        hasAuthzError(db.foo.insert({a: 1}));
        assert.eq(1, db.foo.findOne().a);
        assert.eq(1, db.foo.count());
        assert.doesNotThrow(function() {
            db.getRole('testRole');
        });
        assert.commandFailedWithCode(db.adminCommand('connPoolSync'), ErrorCodes.Unauthorized);

        testUserAdmin.getSisterDB('admin').updateUser(
            'monitor',
            {roles: [{role: "readWrite", db: 'test'}, {role: 'adminRole', db: 'admin'}]},
            writeConcern);
        assert.writeOK(db.foo.update({}, {$inc: {a: 1}}));
        assert.eq(2, db.foo.findOne().a);
        assert.eq(1, db.foo.count());
        assert.throws(function() {
            db.getRole('testRole');
        });
        assert.commandWorked(db.adminCommand('connPoolSync'));
    })();

    (function testGrantRolesToUser() {
        jsTestLog("Testing grantRolesToUser");

        assert.commandFailedWithCode(db.runCommand({collMod: 'foo', usePowerOf2Sizes: true}),
                                     ErrorCodes.Unauthorized);

        testUserAdmin.getSisterDB('admin').grantRolesToUser('monitor',
                                                            [
                                                              {role: 'readWrite', db: 'test'},
                                                              {role: 'dbAdmin', db: 'test'},
                                                              {role: 'readWrite', db: 'test'},
                                                              {role: 'testRole', db: 'test'},
                                                              {role: 'readWrite', db: 'test'},
                                                            ],
                                                            writeConcern);

        assert.commandWorked(db.runCommand({collMod: 'foo', usePowerOf2Sizes: true}));
        assert.writeOK(db.foo.update({}, {$inc: {a: 1}}));
        assert.eq(3, db.foo.findOne().a);
        assert.eq(1, db.foo.count());
        assert.doesNotThrow(function() {
            db.getRole('testRole');
        });
        assert.commandWorked(db.adminCommand('connPoolSync'));
    })();

    (function testRevokeRolesFromUser() {
        jsTestLog("Testing revokeRolesFromUser");

        testUserAdmin.getSisterDB('admin').revokeRolesFromUser(
            'monitor',
            [
              {role: 'readWrite', db: 'test'},
              {role: 'dbAdmin', db: 'test2'},  // role user doesnt have
              {role: 'testRole', db: 'test'}
            ],
            writeConcern);

        assert.commandWorked(db.runCommand({collMod: 'foo', usePowerOf2Sizes: true}));
        hasAuthzError(db.foo.update({}, {$inc: {a: 1}}));
        assert.throws(function() {
            db.foo.findOne();
        });
        assert.throws(function() {
            db.getRole('testRole');
        });
        assert.commandWorked(db.adminCommand('connPoolSync'));

        testUserAdmin.getSisterDB('admin').revokeRolesFromUser(
            'monitor', [{role: 'adminRole', db: 'admin'}], writeConcern);

        hasAuthzError(db.foo.update({}, {$inc: {a: 1}}));
        assert.throws(function() {
            db.foo.findOne();
        });
        assert.throws(function() {
            db.getRole('testRole');
        });
        assert.commandFailedWithCode(db.adminCommand('connPoolSync'), ErrorCodes.Unauthorized);

    })();

    (function testUsersInfo() {
        jsTestLog("Testing usersInfo");

        var res = testUserAdmin.getSisterDB('admin').runCommand({usersInfo: 'monitor'});
        printjson(res);
        assert.eq(1, res.users.length);
        assert.eq(10036, res.users[0].customData.zipCode);

        res = testUserAdmin.getSisterDB('admin').runCommand(
            {usersInfo: {user: 'monitor', db: 'admin'}});
        assert.eq(1, res.users.length);
        assert.eq(10036, res.users[0].customData.zipCode);

        // UsersInfo results are ordered alphabetically by user field then db field,
        // not by user insertion order
        res = testUserAdmin.getSisterDB('admin').runCommand(
            {usersInfo: ['monitor', {user: 'backupuser', db: 'admin'}]});
        printjson(res);
        assert.eq(2, res.users.length);
        assert.eq("monitor", res.users[1].user);
        assert.eq(10036, res.users[1].customData.zipCode);
        assert(res.users[0].customData.userAdmin);
        assert.eq("backupuser", res.users[0].user);

        res = testUserAdmin.runCommand({usersInfo: 1});
        assert.eq(1, res.users.length);
        assert.eq("andy", res.users[0].user);
        //       assert.eq("spencer", res.users[1].user);
        assert(!res.users[0].customData);
        //       assert.eq(10036, res.users[1].customData.zipCode);

        res = testUserAdmin.runCommand({usersInfo: {forAllDBs: true}});
        printjson(res);
        assert.eq(4, res.users.length);
        assert.eq("admin", res.users[0].user);
        assert.eq("andy", res.users[1].user);
        assert.eq("backupuser", res.users[2].user);
        assert.eq("monitor", res.users[3].user);
    })();

    (function testDropUser() {
        jsTestLog("Testing dropUser");

        assert(db.getSisterDB('admin').auth('monitor', 'Password@a1b4'));
        assert(db.auth('andy', 'Password@a1b'));

        testUserAdmin.getSisterDB('admin').dropUser('monitor', writeConcern);

        assert(!db.getSisterDB('admin').auth('monitor', 'Password@a1b4'));
        assert(db.auth('andy', 'Password@a1b'));

        assert.eq(1, testUserAdmin.getUsers().length);
    })();

    (function testDropAllUsersFromDatabase() {
        jsTestLog("Testing dropAllUsersFromDatabase");

        assert.eq(1, testUserAdmin.getUsers().length);
        assert(db.auth('andy', 'Password@a1b'));

        testUserAdmin.dropAllUsers(writeConcern);

        assert(!db.auth('andy', 'Password@a1b'));
        assert.eq(0, testUserAdmin.getUsers().length);
    })();
}

// Verifies that we've fixed SERVER-33158 by creating large user lsid refresh records (via large
// usernames)

(function() {
    'use strict';

    // This test makes assertions about the number of sessions, which are not compatible with
    // implicit sessions.
    TestData.disableImplicitSessions = true;

    const mongod = MongoRunner.runMongod({auth: ""});

    const refresh = {refreshLogicalSessionCacheNow: 1};
    const startSession = {startSession: 1};

    const admin = mongod.getDB('admin');
    const db = mongod.getDB("test");
    const config = mongod.getDB("config");

    admin.createUser({
        user: 'admin',
        pwd: 'Password@a1b',
        roles: jsTest.adminUserRoles, "passwordDigestor": "server"
    });
    assert(admin.auth('admin', 'Password@a1b'));

    const longUserName = "x".repeat(1000);

    // Create a user with a long name, so that the refresh records have a chance to blow out the
    // 16MB limit, if all the sessions are flushed in one batch
    db.createUser({
        user: longUserName,
        pwd: 'Password@a1b',
        roles: jsTest.basicUserRoles, "passwordDigestor": "server"
    });
    admin.logout();

    assert(db.auth(longUserName, 'Password@a1b'));

    // 20k * 1k = 20mb which is greater than 16mb
    const numSessions = 20000;
    for (var i = 0; i < numSessions; i++) {
        assert.commandWorked(admin.runCommand(startSession), "unable to start session");
    }

    assert.commandWorked(admin.runCommand(refresh), "failed to refresh");

    // Make sure we actually flushed the sessions
    assert.eq(numSessions,
              config.system.sessions.aggregate([{'$listSessions': {}}, {'$count': "count"}])
                  .next()
                  .count);

    MongoRunner.stopMongod(mongod);
})();

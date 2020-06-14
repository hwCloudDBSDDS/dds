(function() {
    'use strict';

    // This test makes assertions about the number of sessions, which are not compatible with
    // implicit sessions.
    TestData.disableImplicitSessions = true;

    var conn;
    var admin;
    var foo;
    var result;
    const request = {startSession: 1};

    conn = MongoRunner.runMongod({setParameter: {maxSessions: 2}});
    admin = conn.getDB("admin");

    // ensure that the cache is empty
    var serverStatus = assert.commandWorked(admin.adminCommand({serverStatus: 1}));
    assert.eq(0, serverStatus.logicalSessionRecordCache.activeSessionsCount);

    // test that we can run startSession unauthenticated when the server is running without --auth

    result = admin.runCommand(request);
    assert.commandWorked(
        result,
        "failed test that we can run startSession unauthenticated when the server is running without --auth");
    assert(result.id, "failed test that our session response has an id");
    assert.eq(
        result.timeoutMinutes, 30, "failed test that our session record has the correct timeout");

    // test that startSession added to the cache
    serverStatus = assert.commandWorked(admin.adminCommand({serverStatus: 1}));
    assert.eq(1, serverStatus.logicalSessionRecordCache.activeSessionsCount);

    // test that we can run startSession authenticated when the server is running without --auth

    admin.createUser({user: 'user0', pwd: 'Password@a1b', roles: [], "passwordDigestor": "server"});
    admin.auth("user0", "Password@a1b");

    result = admin.runCommand(request);
    assert.commandWorked(
        result,
        "failed test that we can run startSession authenticated when the server is running without --auth");
    assert(result.id, "failed test that our session response has an id");
    assert.eq(
        result.timeoutMinutes, 30, "failed test that our session record has the correct timeout");

    assert.commandFailed(admin.runCommand(request),
                         "failed test that we can't run startSession when the cache is full");
    MongoRunner.stopMongod(conn);

    //

    conn = MongoRunner.runMongod({auth: "", nojournal: ""});
    admin = conn.getDB("admin");
    foo = conn.getDB("foo");

    // test that we can't run startSession unauthenticated when the server is running with --auth

    assert.commandFailed(
        admin.runCommand(request),
        "failed test that we can't run startSession unauthenticated when the server is running with --auth");

    //

    admin.createUser({
        user: 'admin',
        pwd: 'Password@a1b',
        roles: jsTest.adminUserRoles, "passwordDigestor": "server"
    });
    admin.auth("admin", "Password@a1b");
    admin.createUser({
        user: 'user0',
        pwd: 'Password@a1b',
        roles: jsTest.basicUserRoles, "passwordDigestor": "server"
    });
    foo.createUser({
        user: 'user1',
        pwd: 'Password@a1b',
        roles: jsTest.basicUserRoles, "passwordDigestor": "server"
    });
    admin.createUser({user: 'user2', pwd: 'Password@a1b', roles: [], "passwordDigestor": "server"});
    admin.logout();

    // test that we can run startSession authenticated as one user with proper permissions

    admin.auth("user0", "Password@a1b");
    result = admin.runCommand(request);
    assert.commandWorked(
        result,
        "failed test that we can run startSession authenticated as one user with proper permissions");
    assert(result.id, "failed test that our session response has an id");
    assert.eq(
        result.timeoutMinutes, 30, "failed test that our session record has the correct timeout");

    // test that we cant run startSession authenticated as two users with proper permissions

    foo.auth("user1", "Password@a1b");
    assert.commandFailed(
        admin.runCommand(request),
        "failed test that we cant run startSession authenticated as two users with proper permissions");

    // test that we cant run startSession authenticated as one user without proper permissions

    admin.logout();
    admin.auth("user2", "Password@a1b");
    assert.commandFailed(
        admin.runCommand(request),
        "failed test that we cant run startSession authenticated as one user without proper permissions");

    //

    MongoRunner.stopMongod(conn);

})();

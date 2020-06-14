// @tags: [requires_profiling]
var conn = MongoRunner.runMongod({auth: ""});

var adminDb = conn.getDB("admin");
var testDb = conn.getDB("testdb");

adminDb.createUser({
    user: 'admin',
    pwd: 'Password@a1b',
    roles: ['userAdminAnyDatabase', 'dbAdminAnyDatabase', 'readWriteAnyDatabase'],
    "passwordDigestor": "server"
});

adminDb.auth('admin', 'Password@a1b');
testDb.createUser(
    {user: 'readUser', pwd: 'Password@a1b', roles: ['read'], "passwordDigestor": "server"});
adminDb.createUser({
    user: 'monitor',
    pwd: 'Password@a1b',
    roles: [{'role': 'dbAdmin', 'db': 'testdb'}], "passwordDigestor": "server"
});
adminDb.getSiblingDB('admin').createUser({
    user: 'backupuser',
    pwd: 'Password@a1b',
    roles: [{role: 'dbAdminAnyDatabase', db: 'admin'}], "passwordDigestor": "server"
});
testDb.setProfilingLevel(2);
testDb.foo.findOne();
adminDb.logout();
testDb.auth('readUser', 'Password@a1b');
assert.throws(function() {
    testDb.system.profile.findOne();
});
testDb.logout();

// SERVER-14355
testDb.getSiblingDB('admin').auth('monitor', 'Password@a1b');
testDb.setProfilingLevel(0);
testDb.system.profile.drop();
assert.commandWorked(testDb.createCollection("system.profile", {capped: true, size: 1024}));
testDb.logout();

// SERVER-16944
testDb.getSiblingDB('admin').auth('backupuser', 'Password@a1b');
testDb.setProfilingLevel(0);
testDb.system.profile.drop();
assert.commandWorked(testDb.createCollection("system.profile", {capped: true, size: 1024}));
MongoRunner.stopMongod(conn, null, {user: 'admin', pwd: 'Password@a1b'});

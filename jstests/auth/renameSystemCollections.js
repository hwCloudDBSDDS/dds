// SERVER-8623: Test that renameCollection can't be used to bypass auth checks on system namespaces
var conn = MongoRunner.runMongod({auth: ""});

var adminDB = conn.getDB("admin");
var testDB = conn.getDB("testdb");
var testDB2 = conn.getDB("testdb2");

var CodeUnauthorized = 13;

var backdoorUserDoc = {user: 'backdoor', db: 'admin', pwd: 'hashed', roles: ['root']};

adminDB.createUser({
    user: 'userAdmin',
    pwd: 'Password@a1b',
    roles: ['userAdminAnyDatabase'], "passwordDigestor": "server"
});

adminDB.auth('userAdmin', 'Password@a1b');
adminDB.createUser({
    user: 'readWriteAdmin',
    pwd: 'Password@a1b',
    roles: ['readWriteAnyDatabase'], "passwordDigestor": "server"
});
adminDB.createUser({
    user: 'readWriteAndUserAdmin',
    pwd: 'Password@a1b',
    roles: ['readWriteAnyDatabase', 'userAdminAnyDatabase'], "passwordDigestor": "server"
});
adminDB.createUser(
    {user: 'root', pwd: 'Password@a1b', roles: ['root'], "passwordDigestor": "server"});

adminDB.logout();

jsTestLog("Test that a readWrite user can't rename system.profile to something they can read");
adminDB.auth('readWriteAdmin', 'Password@a1b');
res = adminDB.system.profile.renameCollection("profile");
assert.eq(0, res.ok);
assert.eq(CodeUnauthorized, res.code);

jsTestLog("Test that a readWrite user can't rename system.users to something they can read");
var res = adminDB.system.users.renameCollection("users");
assert.eq(0, res.ok);
assert.eq(CodeUnauthorized, res.code);
assert.eq(0, adminDB.users.count());

jsTestLog("Test that a readWrite user can't use renameCollection to override system.users");
// adminDB.users.insert(backdoorUserDoc);
res = adminDB.users.renameCollection("system.users", true);
assert.eq(0, res.ok);
assert.eq(CodeUnauthorized, res.code);
// adminDB.users.drop();

jsTestLog("Test that a userAdmin can't rename system.users without readWrite");
adminDB.logout();
adminDB.auth('userAdmin', 'Password@a1b');
var res = adminDB.system.users.renameCollection("users");
assert.eq(0, res.ok);
assert.eq(CodeUnauthorized, res.code);
assert.eq(4, adminDB.system.users.count());

adminDB.auth('readWriteAndUserAdmin', 'Password@a1b');
assert.eq(0, adminDB.users.count());

jsTestLog("Test that even with userAdmin AND dbAdmin you CANNOT rename to/from system.users");
var res = adminDB.system.users.renameCollection("users");
assert.eq(0, res.ok);
assert.eq(CodeUnauthorized, res.code);
assert.eq(4, adminDB.system.users.count());

// adminDB.users.drop();
// adminDB.users.insert(backdoorUserDoc);
var res = adminDB.users.renameCollection("system.users");
assert.eq(0, res.ok);
assert.eq(CodeUnauthorized, res.code);

assert.eq(null, adminDB.system.users.findOne({user: backdoorUserDoc.user}));
assert.neq(null, adminDB.system.users.findOne({user: 'userAdmin'}));

// At this point, all the user documents are gone, so further activity may be unauthorized,
// depending on cluster configuration.  So, this is the end of the test.
MongoRunner.stopMongod(conn, {user: 'userAdmin', pwd: 'Password@a1b'});

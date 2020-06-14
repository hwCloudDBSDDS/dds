/**
 * Test that the replica set connections to the secondaries will have the right auth credentials
 * even when these connections are shared within the same connection pool.
 * @tags: [requires_replication]
 */

var NUM_NODES = 3;
var rsTest = new ReplSetTest({nodes: NUM_NODES});
rsTest.startSet({oplogSize: 10, keyFile: 'jstests/libs/key1'});
rsTest.initiate();
rsTest.awaitSecondaryNodes();

var setupConn = rsTest.getPrimary();
var admin = setupConn.getDB('admin');

// Setup initial data
admin.createUser({
    user: 'admin',
    pwd: 'Password@a1b',
    roles: jsTest.adminUserRoles, "passwordDigestor": "server"
});
admin.auth('admin', 'Password@a1b');

setupConn.getDB('foo').createUser(
    {user: 'foo', pwd: 'Password@a1b', roles: jsTest.basicUserRoles, "passwordDigestor": "server"},
    {w: NUM_NODES});
setupConn.getDB('foo').logout();
setupConn.getDB('bar').createUser(
    {user: 'bar', pwd: 'Password@a1b', roles: jsTest.basicUserRoles, "passwordDigestor": "server"},
    {w: NUM_NODES});
setupConn.getDB('bar').logout();

var replConn0 = new Mongo(rsTest.getURL());
var replConn1 = new Mongo(rsTest.getURL());
var fooDB0 = replConn0.getDB('foo');
var barDB0 = replConn0.getDB('bar');
var fooDB1 = replConn1.getDB('foo');
var barDB1 = replConn1.getDB('bar');

fooDB0.auth('foo', 'Password@a1b');
barDB1.auth('bar', 'Password@a1b');

assert.writeOK(fooDB0.user.insert({x: 1}, {writeConcern: {w: NUM_NODES}}));
assert.writeError(barDB0.user.insert({x: 1}, {writeConcern: {w: NUM_NODES}}));

assert.writeError(fooDB1.user.insert({x: 2}, {writeConcern: {w: NUM_NODES}}));
assert.writeOK(barDB1.user.insert({x: 2}, {writeConcern: {w: NUM_NODES}}));

// Make sure replica set connection in the shell is ready.
_awaitRSHostViaRSMonitor(rsTest.getPrimary().name, {ok: true, ismaster: true}, rsTest.name);
rsTest.getSecondaries().forEach(function(sec) {
    _awaitRSHostViaRSMonitor(sec.name, {ok: true, secondary: true}, rsTest.name);
});

// Note: secondary nodes are selected randomly and connections will only be returned to the
// pool if a different secondary is selected from the previous one so we have to iterate
// a couple of times.
for (var x = 0; x < 20; x++) {
    var explain = fooDB0.user.find().readPref('secondary').explain('executionStats');
    assert.eq(1, explain.executionStats.nReturned);

    assert.throws(function() {
        explain = barDB0.user.find().readPref('secondary').explain('executionStats');
    });

    assert.throws(function() {
        explain = fooDB1.user.find().readPref('secondary').explain('executionStats');
    });

    explain = barDB1.user.find().readPref('secondary').explain('executionStats');
    assert.eq(1, explain.executionStats.nReturned);
}

admin.logout();
fooDB0.logout();
barDB1.logout();

rsTest.stopSet();

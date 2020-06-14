/**
 * Test that user modifications on replica set primaries
 * will invalidate cached user credentials on secondaries
 * @tags: [requires_replication]
 */

var NUM_NODES = 3;
var rsTest = new ReplSetTest({nodes: NUM_NODES});
rsTest.startSet({oplogSize: 10, keyFile: 'jstests/libs/key1'});
rsTest.initiate();
rsTest.awaitSecondaryNodes();

var primary = rsTest.getPrimary();
var secondary = rsTest.getSecondary();
var admin = primary.getDB('admin');

// Setup initial data
admin.createUser({
    user: 'admin',
    pwd: 'Password@a1b',
    roles: jsTest.adminUserRoles, "passwordDigestor": "server"
});
admin.auth('admin', 'Password@a1b');

primary.getDB('foo').createUser(
    {user: 'foo', pwd: 'Password@a1b', roles: [], "passwordDigestor": "server"}, {w: NUM_NODES});

secondaryFoo = secondary.getDB('foo');
secondaryFoo.auth('foo', 'Password@a1b');
assert.throws(function() {
    secondaryFoo.col.findOne();
}, [], "Secondary read worked without permissions");

primary.getDB('foo').updateUser('foo', {roles: jsTest.basicUserRoles}, {w: NUM_NODES});
assert.doesNotThrow(function() {
    secondaryFoo.col.findOne();
}, [], "Secondary read did not work with permissions");

admin.logout();
secondaryFoo.logout();

rsTest.stopSet();

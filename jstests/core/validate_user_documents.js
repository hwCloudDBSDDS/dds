// @tags: [
//      requires_non_retryable_commands,
//      requires_auth,
//      assumes_write_concern_unchanged
//      ]

// Ensure that inserts and updates of the system.users collection validate the schema of inserted
// documents.

mydb = db.getSisterDB("validate_user_documents");

function assertGLEOK(status) {
    assert(status.ok && status.err === null, "Expected OK status object; found " + tojson(status));
}

function assertGLENotOK(status) {
    assert(status.ok && status.err !== null,
           "Expected not-OK status object; found " + tojson(status));
}

mydb.dropDatabase();
mydb.dropAllUsers();

//
// Tests of the insert path
//

// V0 user document document; insert should fail.
assert.commandFailed(mydb.runCommand(
    {createUser: 1, user: "spencer", pwd: "Password@a1b", readOnly: true, "digestPassword": true}));

// V1 user document; insert should fail.
assert.commandFailed(mydb.runCommand({
    createUser: 1,
    user: "spencer",
    userSource: "test2",
    roles: ["dbAdmin"], "digestPassword": true
}));

// Valid V2 user document; insert should succeed.
assert.commandWorked(mydb.runCommand(
    {createUser: "spencer", pwd: "Password@a1b", roles: ["dbAdmin"], "digestPassword": true}));

// Valid V2 user document; insert should succeed.
assert.commandWorked(mydb.runCommand({
    createUser: "andy",
    pwd: "Password@a1b",
    roles: [{role: "dbAdmin", db: "validate_user_documents", hasRole: true, canDelegate: false}],
    "digestPassword": true
}));

// Non-existent role; insert should fail
assert.commandFailed(mydb.runCommand(
    {createUser: "bob", pwd: "Password@a1b", roles: ["fakeRole123"], "digestPassword": true}));

//
// Tests of the update path
//

// Update a document in a legal way, expect success.
assert.commandWorked(mydb.runCommand({updateUser: 'spencer', roles: ['read']}));

// Update a document in a way that is illegal, expect failure.
assert.commandFailed(mydb.runCommand({updateUser: 'spencer', readOnly: true}));
assert.commandFailed(mydb.runCommand({updateUser: 'spencer', pwd: "", "digestPassword": true}));
assert.commandFailed(mydb.runCommand({updateUser: 'spencer', roles: ['fakeRole123']}));

mydb.dropDatabase();

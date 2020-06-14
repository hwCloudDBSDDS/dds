(function() {
    'use strict';

    var conn = MongoRunner.runMongod({auth: ""});
    var admin = conn.getDB("admin");
    var db = conn.getDB("otherdb");

    admin.createUser({
        user: "admin",
        pwd: "Password@a1b",
        roles: jsTest.adminUserRoles, "passwordDigestor": "server"
    });
    admin.auth("admin", "Password@a1b");
    db.createUser({
        user: "lily",
        pwd: "Password@a1b",
        roles: jsTest.basicUserRoles, "passwordDigestor": "server"
    });
    admin.logout();

    var testCommand = function(cmd) {
        // Test that we can run a pre-auth command without authenticating.
        var command = {[cmd]: 1};

        assert.commandWorked(admin.runCommand(command));

        // Test that we can authenticate and start a session
        db.auth("lily", "Password@a1b");
        var res = admin.runCommand({startSession: 1});
        assert.commandWorked(res);
        var id = res.id;

        var commandWithSession = {[cmd]: 1, lsid: res.id};

        // Test that we can run a pre-auth command with a session while
        // the session owner is logged in (and the session gets ignored)
        assert.commandWorked(db.runCommand(command),
                             "failed to run command " + cmd + " while logged in");
        assert.commandWorked(db.runCommand(commandWithSession),
                             "failed to run command " + cmd + " with session while logged in");

        // Test that we can run a pre-auth command with a session while
        // nobody is logged in (and the session gets ignored)
        db.logout();
        assert.commandWorked(db.runCommand(command),
                             "failed to run command " + cmd + " without being logged in");
        assert.commandWorked(
            db.runCommand(commandWithSession),
            "failed to run command " + cmd + " with session without being logged in");

        // Test that we can run a pre-auth command with a session while
        // multiple users are logged in (and the session gets ignored)
        db.auth("lily", "Password@a1b");
        admin.auth("admin", "Password@a1b");
        assert.commandWorked(admin.runCommand(command),
                             "failed to run command " + cmd + " with multiple users logged in");
        assert.commandWorked(
            admin.runCommand(commandWithSession),
            "failed to run command " + cmd + " with session with multiple users logged in");

        db.logout();
        admin.logout();
    };

    var commands = ["ping", "ismaster"];
    for (var i = 0; i < commands.length; i++) {
        testCommand(commands[i]);
    }
    MongoRunner.stopMongod(conn, null, {user: "admin", pwd: "Password@a1b"});
})();

// Ensure that attempting to use SCRAM-SHA-1 auth on a
// user with invalid SCRAM-SHA-1 credentials fails gracefully.

(function() {
    'use strict';

    function runTest(mongod) {
        assert(mongod);
        const admin = mongod.getDB('admin');
        const test = mongod.getDB('test');

        admin.createUser({
            user: 'admin',
            pwd: 'Password@a1b',
            roles: jsTest.adminUserRoles, "passwordDigestor": "server"
        });
        assert(admin.auth('admin', 'Password@a1b'));

        test.createUser({
            user: 'user',
            pwd: 'Password@a1b',
            roles: jsTest.basicUserRoles, "passwordDigestor": "server"
        });

        admin.system.users.find().forEach(function(doc) {
            print(tojson(doc));
        });
        // Give the test user an invalid set of SCRAM-SHA-1 credentials.
        assert.eq(admin.system.users
                  .update({_id: "test.user"}, {
                      $set: {
                          "credentials.SCRAM-SHA-1": {
                              salt: "AAAA",
                              storedKey: "AAAA",
                              serverKey: "AAAA",
                              iterationCount: 10000
                          }
                      }
                  })
                  .nModified,
                  1,
                  "Should have updated one document for user@test");
        admin.system.users.find().forEach(function(doc) {
            print(tojson(doc));
        });
        admin.logout();

        assert(!test.auth({user: 'user', pwd: 'Password@a1b'}));
    }

    const mongod = MongoRunner.runMongod({auth: "", useLogFiles: true});
    runTest(mongod);
    MongoRunner.stopMongod(mongod);
})();

// Test passwords on private keys for SSL
// This tests that providing a proper password works and that providing no password or incorrect
// password fails.  It uses both mongod and mongo to run the tests, since the mongod binary
// does not return error statuses to indicate an error.
// This test requires ssl support in mongo-tools
// @tags: [requires_ssl_mongo_tools]

load('jstests/ssl/libs/ssl_helpers.js');
requireSSLProvider('openssl', function() {
    var baseName = "jstests_ssl_ssl_cert_password";
    var dbpath = MongoRunner.dataPath + baseName;
    var external_scratch_dir = MongoRunner.dataPath + baseName + "/external/";
    resetDbpath(dbpath);
    mkdir(external_scratch_dir);

    // Password is correct
    var md = MongoRunner.runMongod({
        nopreallocj: "",
        dbpath: dbpath,
        sslMode: "requireSSL",
        sslPEMKeyFile: "jstests/libs/password_protected.pem",
        sslPEMKeyPassword: "qwerty"
    });
    // MongoRunner.runMongod connects a Mongo shell, so if we get here, the test is successful.

    // Password incorrect; error logged is:
    //  error:06065064:digital envelope routines:EVP_DecryptFinal_ex:bad decrypt
    var exit_code = runMongoProgram("mongo",
                                    "--port",
                                    md.port,
                                    "--ssl",
                                    "--sslAllowInvalidCertificates",
                                    "--sslCAFile",
                                    "jstests/libs/ca.pem",
                                    "--sslPEMKeyFile",
                                    "jstests/libs/password_protected.pem",
                                    "--sslPEMKeyPassword",
                                    "barf");

    // 1 is the exit code for failure
    assert(exit_code == 1);

    // Test that mongodump and mongorestore support ssl
    c = md.getDB("dumprestore_ssl").getCollection("foo");
    assert.eq(0, c.count(), "dumprestore_ssl.foo collection is not initially empty");
    c.save({a: 22});
    assert.eq(1, c.count(), "failed to insert document into dumprestore_ssl.foo collection");

    exit_code = MongoRunner.runMongoTool("mongodump", {
        out: external_scratch_dir,
        port: md.port,
        ssl: "",
        sslPEMKeyFile: "jstests/libs/password_protected.pem",
        sslCAFile: "jstests/libs/ca.pem",
        sslPEMKeyPassword: "qwerty",
    });

    assert.eq(exit_code, 0, "Failed to start mongodump with ssl");

    c.drop();
    assert.eq(0, c.count(), "dumprestore_ssl.foo collection is not empty after drop");

    exit_code = MongoRunner.runMongoTool("mongorestore", {
        dir: external_scratch_dir,
        port: md.port,
        ssl: "",
        sslCAFile: "jstests/libs/ca.pem",
        sslPEMKeyFile: "jstests/libs/password_protected.pem",
        sslPEMKeyPassword: "qwerty",
    });

    assert.eq(exit_code, 0, "Failed to start mongorestore with ssl");

    assert.soon("c.findOne()",
                "no data after sleep.  Expected a document after calling mongorestore");
    assert.eq(
        1,
        c.count(),
        "did not find expected document in dumprestore_ssl.foo collection after mongorestore");
    assert.eq(22, c.findOne().a, "did not find correct value in document after mongorestore");

    if (!_isWindows()) {
        // Stop the server
        var exitCode = MongoRunner.stopMongod(md);
        assert(exitCode == 0);
    } else {
        MongoRunner.stopMongod(md);
    }
});

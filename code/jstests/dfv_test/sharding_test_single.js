// Test of complex sharding initialization

function shardingTestUsingObjects() {
    var ss = MongoRunner.runMongod({"configdb": "x/1.1.1", "bind_ip": "localhost"});
    var ss2 = MongoRunner.runMongod({"configdb": "x/1.1.1", "bind_ip": "localhost"});
    var db = ss.getDB("test");
    for (i = 0; i < 100; i++) {
        db.bar.save({n: 1});
    }
}
shardingTestUsingObjects();

(function() {
    var s1 = new ShardingTest({name: "route_refresh4", shards: 2, mongos: 2});
    var s2 = s1._mongos[1];

    var shard0 = s1.shard0.shardName;
    var shard1 = s1.shard1.shardName;

    assert.writeOK(s1.getDB("test1").foo.insert({num: 1}));

    if (s1.configRS) {
        // Ensure that the second mongos will see the movePrimary
        s1.configRS.awaitLastOpCommitted();
    }

    assert.eq(1, s2.getDB("test1").foo.find({num: 1}).count(), "origin route flush failed");

    var s1primaryshard = s1.getPrimaryShard("test1").name;

    if (s1primaryshard == shard0) {
        assert.commandWorked(s1.getDB('admin').runCommand({movePrimary: "test1", to: shard1}));
    } else {
        assert.commandWorked(s1.getDB('admin').runCommand({movePrimary: "test1", to: shard0}));
    }

    assert.neq(s1primaryshard, s1.getPrimaryShard("test1").name, "move shard  failed");
    // delay
    sleep(12000);

    assert.eq(1, s2.getDB("test1").foo.find({num: 1}).count(), "route flush failed");

    s1.stop();
})();

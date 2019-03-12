// Test for SERVER-8786 - if the first operation on an authenticated shard is moveChunk, it breaks
// the cluster.
(function() {
    'use strict';

    var st = new ShardingTest(
        {shards: 2, other: {keyFile: 'jstests/libs/key1', useHostname: true, chunkSize: 1}});

    var mongos = st.s;
    var adminDB = mongos.getDB('admin');
    var db = mongos.getDB('test');

    adminDB.createUser({
        user: 'admin',
        pwd: 'TEST@1password',
        roles: jsTest.adminUserRoles,
        passwordDigestor: "server"
    });

    adminDB.auth('admin', 'TEST@1password');

    adminDB.runCommand({enableSharding: "test"});
    st.ensurePrimaryShard('test', 'shard0001');
    adminDB.runCommand({shardCollection: "test.foo", key: {x: 1}});

    for (var i = 0; i < 100; i++) {
        db.foo.insert({x: i});
    }

    adminDB.runCommand({split: "test.foo", middle: {x: 50}});
    var curShard = st.getShard("test.foo", {x: 75});
    var otherShard = st.getOther(curShard).name;
    adminDB.runCommand(
        {moveChunk: "test.foo", find: {x: 25}, to: otherShard, _waitForDelete: true});

    st.printShardingStatus();
    var ss0 = st.shard0;
    MongoRunner.stopMongod(st.shard0);
    st.shard0 = MongoRunner.runMongod({restart: st.shard0});
    sleep(10*1000);
    var a=st.configRS.getURL();
    var str=String(a);
    var c=str.split(/[\,\:]/);
    var port=Math.floor(c[1]);
    var ssp1=port+4;  
    var addss1=c[2]+":"+ssp1; 
    jsTest.log("ssp1 : " + addss1);
    assert.commandWorked(adminDB.runCommand({addShard:addss1}));
    st.printShardingStatus();
    // May fail the first couple times due to socket exceptions
    assert.soon(function() {
        var res = adminDB.runCommand({moveChunk: "test.foo", find: {x: 75}, to: otherShard});
        printjson(res);
        return res.ok;
    });

    printjson(db.foo.findOne({x: 25}));
    printjson(db.foo.findOne({x: 75}));

    st.stop();
})();

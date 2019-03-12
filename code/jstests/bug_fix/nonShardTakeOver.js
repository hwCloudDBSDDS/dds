(function() {
    'use strict';

    var st = new ShardingTest({shards: 2, mongos: 1});
    var mgs=st.s0;
    var cfg=mgs.getDB('config');
    var admin = mgs.getDB('admin');
    var coll=mgs.getCollection("test.foo");
		
	var shards = mgs.getCollection('config.shards').find().toArray();
	var shard0 = shards[0]._id;
	var shard1 = shards[1]._id;
	var ssHbTimeout = 15;
		
	coll.insert({a:1});
		
	st.ensurePrimaryShard('test', shard0);

	MongoRunner.stopMongod(st._connections[0]);
	sleep((ssHbTimeout-2) * 1000);
        assert.soon(function() {
		var chunk = cfg.chunks.find({"ns" : "test.foo"}).toArray();
		return chunk[0].shard == shard1;
        }, "send assign chunk successfully", 6 * 1000);
	
	MongoRunner.stopMongod(st._configServers[0],9);
	
	var host0 = shards[0].host;
	var conn0 = MongoRunner.runMongod(st._connections[0]);
	var pid0 = conn0.pid;
	assert.soon(function() {
                var shard = cfg.shards.find({"host" : host0}).toArray();
                if (shard.length != 0){
                   var shardPid = shard[0].processIdentity; 
                   var ind = shardPid.indexOf("_");
                   var spid = shardPid.substr(0,ind);
                   var numLong = NumberLong(spid);
                   print(numLong);
                   print(pid0);
                   if (numLong.toString() == pid0.toString()) {
                        print("try to add shard");
                        var result = admin.runCommand({addShard: host0});
                        return result.ok;
                  }   
                }
    }, "failed to add shard", 5 * 1000);
	sleep(2 * 1000);
		
	assert.eq(1, coll.find().itcount());	
    st.stop();
})();
    


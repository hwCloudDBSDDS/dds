(function() {
    'use strict';

    var st = new ShardingTest({shards: 2, mongos: 1});
		
	var mgs=st.s0;
    var cfg=mgs.getDB('config');
	var admin = mgs.getDB("admin");
		
	var shards = mgs.getCollection('config.shards').find().toArray();
	sleep(5*1000);
	var conn0 = shards[0].host;
	var conn1 = shards[1].host;
        st.startBalancer();
	var res = admin.runCommand({removeShard: conn0});
	var dbsToMove = res["dbsToMove"];
	var dbToMove = dbsToMove[0];
	assert.commandWorked(admin.runCommand({movePrimary:dbToMove,auto:1}));
    assert.soon(function() {
        var result = admin.runCommand({removeShard: conn0});
        printjson(result);

        return result.ok && result.state == "completed";
    }, "failed to drain shard completely", 60 * 1000);

	MongoRunner.stopMongod(st._connections[0]);
	MongoRunner.runMongod(st._connections[0]);
	assert.soon(function() {
		var result = admin.runCommand({addShard: conn0});
        return result.ok && cfg.shards.find({"host" : conn0}).itcount() == 1;
    }, "failed to drain shard completely", 5 * 1000);
	
	assert.soon(function() {
        var result = admin.runCommand({removeShard: conn1});
        printjson(result);
        return result.ok && result.state == "started";
    }, "failed to drain shard completely", 5 * 1000);
	
	assert.commandWorked(admin.runCommand({movePrimary:dbToMove,auto:1}));
    st.stop();
})();
    


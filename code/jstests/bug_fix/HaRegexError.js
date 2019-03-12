(function() {
    'use strict';

        var st = new ShardingTest({shards: 2, mongos: 1});
		
	var mgs=st.s0;
        var cfg=mgs.getDB('config');
        var collA=mgs.getCollection("a.a");
	var collAA=mgs.getCollection("aa.a");
		
	var shards = mgs.getCollection('config.shards').find().toArray();
	var shard0 = shards[0]._id;
	var shard1 = shards[1]._id;
		
	collA.insert({a:1});
	collAA.insert({aa:1});	
		
	st.ensurePrimaryShard('a', shard0);
	st.ensurePrimaryShard('aa', shard0);

	assert(collA.drop());
	MongoRunner.stopMongod(st._connections[0]);
	assert.eq(1, collAA.find().itcount());
	sleep (10 * 1000);
		
	var time = 0;
	while (1)
	{
		var chunks = cfg.chunks.find().toArray();
		var num = cfg.chunks.find({"status":1}).itcount();
			
		jsTest.log("--------num-----"+num);
		if (num == 1) {
			break;
		}
		sleep (1 * 1000);
		time ++;
		jsTest.log("--------time-----"+time);
		if (time > 20){
			break;
		}
	}
		
	assert.eq(1, collAA.find().itcount());	
        assert.eq(0, collA.find().itcount());
        st.stop();
})();
    


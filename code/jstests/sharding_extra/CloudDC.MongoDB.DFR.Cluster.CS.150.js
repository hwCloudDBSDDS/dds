(function() {
    'use strict';

       var st = new ShardingTest({
        shards: 3,
        other: {
            c0: {},  // Make sure 1st config server is primary
            c1: {rsConfig: {priority: 0}},
            c2: {rsConfig: {priority: 0}}
        }
    });
		
        var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
		var numchunk=Math.floor(Math.random()+50);
		jsTest.log("--------numchunk------------------------------"+numchunk);
        assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:1}}));
		assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{a:"hashed"},numInitialChunks:numchunk}))
        var cfg=mgs.getDB('config');
        var cfg=mgs.getDB('config');
        var coll=mgs.getCollection("testDB.foo");
		var coll1=mgs.getCollection("testDB.foo1");
        var testdb=mgs.getDB('testDB');
        //coll.drop();
        assert.writeOK(coll.insert({"a":1,"num":1},{ writeConcern: { w: 1,j:true}}));
		assert.writeOK(coll1.insert({"a":1,"num":1},{ writeConcern: { w: 1,j:true}}));
        printShardingStatus(st.config,false);
		var shards = cfg.shards.find().toArray();
		assert.eq(3,shards.length);
        jsTest.log("--------insert ok------------------------------");
		var a=Math.floor(Math.random()*3+1);
		jsTest.log("--------number------------------------------"+a);
		//assert.eq(st.config0, st.configRS.getPrimary());
         MongoRunner.stopMongod(primarycs);
		MongoRunner.stopMongod(configSecondaryList[0]);
		MongoRunner.stopMongod(configSecondaryList[1]);
		MongoRunner.runMongod(configSecondaryList[1]);
        MongoRunner.runMongod(primarycs);
		MongoRunner.runMongod(configSecondaryList[0]);
		sleep (30 * 1000);
        printShardingStatus(st.config,false);
        var shards = cfg.shards.find().toArray();
        //assert.eq("shard0000",shards[0]._id);
        //assert.eq("shard0001",shards[1]._id);
		//assert.eq("shard0002",shards[2]._id);
        assert.eq(1,shards[0].state);
        assert.eq(1,shards[1].state);
		assert.eq(1,shards[2].state);
        assert.eq(3,shards.length);
	var chunks = cfg.chunks.find().toArray();
	assert.eq(numchunk+1,chunks.length);
	coll.update({"a":3},{"$set":{"num":1}},{upsert:true});
	coll1.update({"a":1},{"$set":{"num":10}},{upsert:true});
	assert.eq(2, coll.find().itcount());   
	assert.eq(1, coll1.find().itcount()); 
		assert.commandWorked(admin.runCommand({enableSharding:"testAB"}));
        assert.commandWorked(admin.runCommand({shardCollection:"testAB.foo",key:{a:1}}));
		assert.commandWorked(admin.runCommand({shardCollection:"testAB.foo1",key:{a:"hashed"},numInitialChunks:numchunk}))	
		var collec=mgs.getCollection("testAB.foo");
		var collec1=mgs.getCollection("testAB.foo1");
		 assert.writeOK(collec.insert({"a":1,"num":1},{ writeConcern: { w: 1,j:true}}));
		assert.writeOK(collec1.insert({"a":1,"num":1},{ writeConcern: { w: 1,j:true}}));
		collec.update({"a":3},{"$set":{"num":1}},{upsert:true});
		collec1.update({"a":1},{"$set":{"num":10}},{upsert:true});
		assert.eq(2, collec.find().itcount());   
		assert.eq(1, collec1.find().itcount());
		var chunks = cfg.chunks.find().toArray();
	  assert.eq(numchunk*2+2,chunks.length);
        assert.neq(null, coll.getIndexes());
        st.stop();
})();

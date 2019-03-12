(function() {
    'use strict';

        var st = new ShardingTest({shards: 3, mongos: 1,other: {enableAutoSplit: false}});
        var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var coll=mgs.getCollection("testDB.foo");
        var coll1=mgs.getCollection("testDB.foo1");
        var testdb=mgs.getDB('testDB');
        st.stopBalancer();
        assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:1}}));
        jsTest.log("-------------------insert data-------------------");
		var bigString = "";
		while (bigString.length < 1024 * 1024){
			bigString += "asocsancdnsjfnsdnfsjdhfasdfasdfasdfnsadofnsadlkfnsaldknfsad";}
		var i = 0;
		var cyclenum = 500;
		var bulk = coll.initializeUnorderedBulkOp();
		for (; i < cyclenum; i++) {
			bulk.insert({a: i, b:i,c:i,s: bigString});
		}
		assert.writeOK(bulk.execute());
		assert.commandWorked(coll.ensureIndex({"name": 11}));
		assert.eq(3, coll.getIndexes().length);
        printShardingStatus(st.config,false);
        jsTest.log("-------------------splite-------------------");
        var ransp = Math.floor(Math.random()*cyclenum+cyclenum);
		assert.commandWorked(admin.runCommand({split: "testDB.foo",find :{a : ransp}}));
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        var chunks = cfg.chunks.find().toArray();
        var max = chunks[0].max.a;
		var min = chunks[1].min.a;
		assert.eq(max,min);
		var insert=max;
		st.printChunks();
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,2);
        jsTest.log("-------------------confirm update normal-------------------");
        jsTest.log("-------------------insert-------------------"+insert);
		st.printChunks();
        jsTest.log("-------------------confirm update normal-------------------");
        var ranso = Math.floor(Math.random()*insert);
        var ransq = Math.floor(Math.random()*insert + insert + 1);
        assert.writeOK(coll.insert({a: ranso,c: ranso}));
		assert.writeOK(coll.insert({a: ransq,c: ransq}));
        assert.writeOK(coll.update({a: ranso},{$set : {c : 1006}}, false,true));
        assert.writeOK(coll.update({a: ransq},{$set : {c : 1007}}, false,true));
        assert.eq(1006,coll.find({a: ranso}).toArray()[0].c, "update  failed");
        assert.eq(1007,coll.find({a: ransq}).toArray()[0].c, "update  failed");
		assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
		assert.commandWorked(coll.dropIndex("name_11"));
		assert.eq(2, coll.getIndexes().length);	
        st.stop();
})();

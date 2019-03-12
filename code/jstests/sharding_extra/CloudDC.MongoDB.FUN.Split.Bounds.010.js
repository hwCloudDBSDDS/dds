(function() {
    'use strict';

        var st = new ShardingTest({shards: 3, mongos: 1,other: {enableAutoSplit: false}});
        var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var coll=mgs.getCollection("testDB.foo");
        var coll1=mgs.getCollection("testDB.foo1");
        var testdb=mgs.getDB('testDB');
        st.startBalancer();
        assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{"a":1}}));
        jsTest.log("-------------------insert data-------------------");
		var maxx=Number(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999)
        printShardingStatus(st.config,false);
        jsTest.log("-------------------splitmiddle-------------------");	
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : maxx}}));
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : 1}}));
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,3);
		var bigString = "";
		while (bigString.length < 1024 * 1024){
			bigString += "asocsancdnsjfnsdnfsjdhfasdfasdfasdfnsadofnsadlkfnsaldknfsad";}
		var i = 2;
		var cyclenum = 400;
		var bulk = coll.initializeUnorderedBulkOp();
		for (; i < cyclenum; i++) {
			bulk.insert({a: i, c:i,s: bigString});
		}
		assert.writeOK(bulk.execute());
		jsTest.log("-------------------splitbounds-------------------");
		assert.commandWorked(admin.runCommand({split:"testDB.foo",middle:{a:100000}}));
        jsTest.log("-------------------confirm update normal-------------------");
		printShardingStatus(st.config,false);
		var num = cfg.chunks.find().itcount();
        assert.eq(num,4);
        for (i=0; i < 1000; i++) {
			assert.writeOK(coll.insert({a: i, c:i}));
		}
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
        assert.writeOK(coll.update({c: 1},{$set : {c : 1003}}, false,true));
        assert.writeOK(coll.update({c: 100},{$set : {c : 1004}}, false,true));
        assert.eq(1003,coll.find({a: 1}).toArray()[0].c, "update  failed");
        assert.eq(1004,coll.find({a: 100}).toArray()[0].c, "update  failed");
        st.stop();
})();

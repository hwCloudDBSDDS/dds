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
		assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:1}}));
        jsTest.log("-------------------insert data-------------------");
		var bigString = "";
		while (bigString.length < 1024 * 1024){
			bigString += "asocsancdnsjfnsdnfsjdhfasdfasdfasdfnsadofnsadlkfnsaldknfsad";}
		var i = 0;
		var cyclenum = 1000;
		var bulk = coll.initializeUnorderedBulkOp();
		for (; i < cyclenum; i++) {
			bulk.insert({a: i, b:i,c:1,s: bigString});
		}
		assert.writeOK(bulk.execute());
		assert.commandWorked(coll.ensureIndex({"name": 11}));
		assert.eq(3, coll.getIndexes().length);	
		var maxx=Number(205000000)
        printShardingStatus(st.config,false);
        jsTest.log("-------------------splitmiddle-------------------");	
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : maxx}}));
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : 1}}));
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,3);		
		printShardingStatus(st.config,false);
		var stats = coll.stats()
		printjson(stats);
		assert.commandFailed(admin.runCommand({split:"testDB.foo",bounds:[{a:205000000},{a:1}]}));
        jsTest.log("-------------------confirm update normal-------------------");
		var max2=[];
		var min2=[];
		for (var n=0;n<=2;n++){
		var chunks = cfg.chunks.find().toArray();
        max2[n] = String(chunks[n].max.a);
		min2[n] = String(chunks[n].min.a);
		}
		max2.sort(function(a,b){return a>b?1:-1});
		min2.sort(function(a,b){return a>b?1:-1});
		if (max2[0]!==1){
		for (var m=1;m<max2.length-1;m++){
			assert.eq(max2[m],min2[m]);
		}
		}
		if (max2[0]==1){
		for (var m=1;m<max2.length-1;m++){
			assert.eq(max2[m],min2[m]);
		}
		}
		jsTest.log(min2);
		jsTest.log(max2);
		if (max2[0]!=1){
		var insert = max2[2]
		}
		if (max2[0]==1){
		var insert = max2[1]
		}		
		printShardingStatus(st.config,false);
		var num = cfg.chunks.find().itcount();
        assert.eq(num,3);
		jsTest.log("-------------------insert-------------------"+insert);
		st.printChunks();
        jsTest.log("-------------------confirm update normal-------------------");
        var ranso = Math.floor(Math.random()*insert);
        var ransq = Math.floor(Math.random()*insert + insert+1);
        var ransq = Math.floor(Math.random()*insert + insert + 1);
        assert.writeOK(coll.insert({a: ranso,c: ranso}));
		assert.writeOK(coll.insert({a: ransq,c: ransq}));
        assert.writeOK(coll.update({a: ranso},{$set : {c : 1003}}, false,true));
        assert.writeOK(coll.update({a: ransq},{$set : {c : 1004}}, false,true));
        assert.eq(1003,coll.find({a: ranso}).toArray()[0].c, "update  failed");
        assert.eq(1004,coll.find({a: ransq}).toArray()[0].c, "update  failed");
		assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
		assert.commandWorked(coll.dropIndex("name_11"));
		assert.eq(2, coll.getIndexes().length);	
        st.stop();
})();

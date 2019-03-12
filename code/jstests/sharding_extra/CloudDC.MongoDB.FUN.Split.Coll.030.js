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
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",unique:true,key:{"a":1}}));
	jsTest.log("-------------------splitmiddle-------------------");
        var maxx=Number(205000000)
	assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : 1}}));
        assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : maxx}}));
        printShardingStatus(st.config,false);
	jsTest.log("-------------------insert data-------------------");
	assert.commandWorked(coll.ensureIndex({"name": 11}));
	var bigString = "";
	while (bigString.length < 1024 * 1024){
	bigString += "asocsancdnsjfnsdnfsjdhfasdfasdfasdfnsadofnsadlkfnsaldknfsad";}
	var bulk = coll.initializeUnorderedBulkOp();
	for (var i = 0;i <1000; i++) {
	bulk.insert({a: i,s: bigString},{writeConcern:{w:1}});
	}
	assert.writeOK(bulk.execute());
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,3);
	printShardingStatus(st.config,false);
	jsTest.log("-------------------splitbounds-------------------");
	var sps = cfg.chunks.find({min:{"a": 1}}).toArray()[0].shard;
	var stats = coll.stats();
	printjson(stats);
	jsTest.log("-------------------split bounds-------------------");
	assert.commandFailed(admin.runCommand({split:"testDB.dog",bounds:[{a: 1},{a: maxx}]}));
	assert.commandWorked(admin.runCommand({split:"testDB.foo",bounds:[{a: 1},{a: maxx}]}));
	printShardingStatus(st.config,false);
	jsTest.log("-------------------split find-------------------");
	assert.commandFailed(admin.runCommand({split:"testDB.cat",find:{a : 1000}}));
        assert.commandWorked(admin.runCommand({split:"testDB.foo",find:{a : 1000}}));
	jsTest.log("-------------------split middle-------------------");
        assert.commandFailed(admin.runCommand({split:"testDB.baby",middle:{a : "aaa"}}));
        assert.commandWorked(admin.runCommand({split:"testDB.foo",middle:{a : "aaa"}}));
        jsTest.log("-------------------confirm normal-------------------");
	printShardingStatus(st.config,false);
	var num = cfg.chunks.find().itcount();
        assert.eq(num,6);
	st.printChunks();
        jsTest.log("-------------------confirm update normal-------------------");
	var ransp = Math.floor(Math.random()*1000);
        var ransl = ransp - 1000;
        var ransr = ransp + 1000;
        assert.writeOK(coll.insert({a: ransl,c: 1001}));
        assert.writeOK(coll.insert({a: ransr,c: 1002}));
        assert.writeOK(coll.update({c: 1001},{$set : {c : 10001}}, false,true));
        assert.writeOK(coll.update({c: 1002},{$set : {c : 10002}}, false,true));
        assert.eq(10001,coll.find({a: ransl}).toArray()[0].c, "update  failed");
        assert.eq(10002,coll.find({a: ransr}).toArray()[0].c, "update  failed");
	assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
	assert.commandWorked(coll.dropIndex("name_11"));
	assert.eq(2, coll.getIndexes().length);	
        st.stop();
})();

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
	st.disableAutoSplit();
        assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{"a":1}}));
	jsTest.log("-------------------splitmiddle-------------------");
        assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : "a"}}));
        assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : "z"}}));
        printShardingStatus(st.config,false);
	jsTest.log("-------------------insert data-------------------");
	assert.commandWorked(coll.ensureIndex({"name": 11}));
	var bigString = "";
	while (bigString.length < 1024 * 1024){
	bigString += "asocsancdnsjfnsdnfsjdhfasdfasdfasdfnsadofnsadlkfnsaldknfsad";}
	var bulk = coll.initializeUnorderedBulkOp();
	for (var i = 0;i <1000; i++) {
	var j = "b" + i;
	bulk.insert({a: j,s: bigString},{writeConcern:{w:1}});
	}
	assert.writeOK(bulk.execute());
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,3);
	printShardingStatus(st.config,false);
	jsTest.log("-------------------splitbounds-------------------");
	var sps = cfg.chunks.find({min:{"a":"a"}}).toArray()[0].shard;
	var stats = coll.stats();
	printjson(stats);
	assert.commandWorked(admin.runCommand({split:"testDB.foo",bounds:[{a:"a"},{a:"z"}]}));
	printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm update normal-------------------");
	var max2=[];
	var min2=[];
	for (var n=0;n<=3;n++){
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
        assert.eq(num,4);
	jsTest.log("-------------------insert-------------------"+insert);
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

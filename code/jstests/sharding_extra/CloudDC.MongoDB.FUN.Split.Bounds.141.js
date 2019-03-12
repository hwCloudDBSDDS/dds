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
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{"num":1 , "list":1}}));
	jsTest.log("-------------------splitmiddle-------------------");
        var ransp = Math.floor(Math.random()*10000) ;
	//var value = Math.floor(Math.random()*1000) ;
	var ransl = ransp - 501 ;
	var ransr = ransp + 501 ;
	assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{"num" : ransp  , "list" : ransl}}));
        assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{"num" : ransp  , "list" : ransr}}));
        printShardingStatus(st.config,false);
	jsTest.log("-------------------insert data-------------------");
	assert.commandWorked(coll.ensureIndex({"name": 11}));
	var bigString = "";
	while (bigString.length < 1024 * 1024){
	bigString += "asocsancdnsjfnsdnfsjdhfasdfasdfasdfnsadofnsadlkfnsaldknfsad";}
	var bulk = coll.initializeUnorderedBulkOp();
	for (var i = 0;i <1000; i++) {
	var j = ransl + i ;
	bulk.insert({"num": ransp,"list": j, s: bigString},{writeConcern:{w:1}});
	}
	assert.writeOK(bulk.execute());
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,3);
	printShardingStatus(st.config,false);
	jsTest.log("-------------------splitbounds-------------------");
	var sps = cfg.chunks.find({min:{"num" : ransp , "list" : ransl}}).toArray()[0].shard;
	var stats = coll.stats();
	printjson(stats);
	assert.commandWorked(admin.runCommand({split:"testDB.foo",bounds:[{"num" : ransp ,"list": ransl},{"num" : ransp ,"list" : ransr}]}));
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
	var ransp = Math.floor(Math.random()*10000);
        var ransl = ransp + 10000;
        var ransr = ransp + 20000;
        assert.writeOK(coll.insert({"num": ransl,"list": ransr,c: 1001}));
        assert.writeOK(coll.insert({"num": ransr,"list": ransl,c: 1002}));
        assert.writeOK(coll.update({"num": ransl},{$set : {c : 10001}}, false,true));
        assert.writeOK(coll.update({"num": ransr},{$set : {c : 10002}}, false,true));
        assert.eq(10001,coll.find({"num": ransl}).toArray()[0].c, "update  failed");
        assert.eq(10002,coll.find({"num": ransr}).toArray()[0].c, "update  failed");
	assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{"num":1,"list":1}}));
        assert.writeOK(coll1.insert({"num": 10, "list": 20, "path": "chengdu"}));
	assert.commandWorked(coll.dropIndex("name_11"));
	assert.eq(2, coll.getIndexes().length);	
        st.stop();
})();

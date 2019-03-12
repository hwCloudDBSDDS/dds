(function() {
    'use strict';

        var st = new ShardingTest({shards: 3, mongos: 1});
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
        printShardingStatus(st.config,false);
        jsTest.log("-------------------splite Digital-------------------");
        //assert.commandWorked(admin.runCommand({split: "testDB.foo",manualsplit:true,find :{a : "123"}}));
        assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : "123"}}));
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,2);
        //assert.commandWorked(admin.runCommand({split: "testDB.foo",manualsplit:true,find :{a : 123}}));
        assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : 123}}));
        jsTest.log("-------------------confirm chunks is 3-------------------");
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,3);
	var max=[];
	var min=[];
	for (var n=0;n<=1;n++){
	var chunks = cfg.chunks.find().toArray();
        max[n] = String(chunks[n].max.a);
	min[n] = String(chunks[n].min.a);
	}
	max.sort(function(a,b){return a>b?1:-1});
	min.sort(function(a,b){return a>b?1:-1});
	jsTest.log(max[0]);
	jsTest.log(min[0]);
	jsTest.log(max[1]);
	jsTest.log(min[1]);
	if (max[0]!="123"){
	for (var m=1;m<max.length;m++){
		assert.eq(max[m],min[m]);
		assert.eq(max[m],array[m-1]);
	}
	}
	if (max[0]=="123"){
	for (var m=1;m<max.length-1;m++){
		assert.eq(max[m],min[m]);
		assert.eq(max[m],array[m]);
	}
	} 
        printShardingStatus(st.config,false);
	var shards = cfg.shards.find().toArray();
	//assert.eq("shard0000",shards[0]._id);
	//assert.eq("shard0001",shards[1]._id);
	//assert.eq("shard0002",shards[2]._id);
	for (var i=0;i<1000;i++){
        assert.writeOK(coll.insert({a: i, c: i}));}
	for (var i=1;i<10;i++){
        assert.commandWorked(coll.ensureIndex({"name": i}));}
        assert.eq(1000, coll.find().itcount());
        assert.eq(11, coll.getIndexes().length);
        st.stop();
})();


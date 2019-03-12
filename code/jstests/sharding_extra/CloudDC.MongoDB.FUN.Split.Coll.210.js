(function() {
    'use strict';

        var st = new ShardingTest({shards: 3, mongos: 1});
        var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var coll=mgs.getCollection("testDB.foo");
		var coll2=mgs.getCollection("testDB.fooo");
        var coll1=mgs.getCollection("testDB.foo1");
        var testdb=mgs.getDB('testDB');
        st.stopBalancer();
        assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:1}}));
		assert.commandWorked(admin.runCommand({shardCollection:"testDB.fooo",key:{a:"hashed"},numInitialChunks:2}));
        jsTest.log("-------------------insert data-------------------");
		st.disableAutoSplit();
		
		var bigString = "";
		for (var i =0; i < 300; i++) {  
        assert.writeOK(coll.insert({a: -i,b:-i}));
		assert.writeOK(coll2.insert({a: -i,b:-i}));
    }

    while (bigString.length < 1024 * 1024){bigString += "asocsancdnsjfnsdnfsjdhfasdfasdfasdfnsadofnsadlkfnsaldknfsad";}
    var bulk = coll.initializeUnorderedBulkOp();
    for (var i =-2000; i < -1800; i++) {
        bulk.insert({a: i, s: bigString});
    }
    assert.writeOK(bulk.execute());
	var bulk = coll2.initializeUnorderedBulkOp();
    for (var i =-2000; i < -1500; i++) {
        bulk.insert({a: i, s: bigString});
    }
    assert.writeOK(bulk.execute());
		
        
        for (var i=1;i<10;i++){
			var doc={};
			var key="name"+i
		doc[key]=1
        assert.commandWorked(coll.ensureIndex(doc));
		assert.commandWorked(coll2.ensureIndex(doc));}
        printShardingStatus(st.config,false);
		var array=[];
        for (var j=0;j<1;){
        var ransp = -150;
		
        //assert.commandWorked(admin.runCommand({split: "testDB.foo", manualsplit:true,find :{a : ransp}}));
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : ransp}}));
		
		assert.commandWorked(admin.runCommand({split: "testDB.foo",find :{a : -1000}}));
		assert.commandWorked(admin.runCommand({split: "testDB.fooo",find :{a : -1000}}));
		assert.commandWorked(admin.runCommand({split: "testDB.fooo",middle :{a : ransp}}));
		assert.writeOK(coll.insert({a: ransp-1, "name1": 20}));
		assert.writeOK(coll.insert({a: ransp+1, "name1": 20}));
		assert.writeOK(coll2.insert({a: ransp-1, "name1": 20}));
		assert.writeOK(coll2.insert({a: ransp+1, "name1": 20}));
		
        printShardingStatus(st.config,false);
		

		jsTest.log("-------------------confirm size normal-------------------");
        jsTest.log(j);
        var chunks = cfg.chunks.find().toArray();
        var num1 = cfg.chunks.find({ns:"testDB.foo"}).itcount();
		var num11 = cfg.chunks.find({ns:"testDB.fooo"}).itcount();
        var num2 = j + 3;
		var num22 = j + 4;
        assert.eq(num1,num2);
		assert.eq(num11,num22);
        jsTest.log("-------------------confirm update normal-------------------");
        jsTest.log(j);
        jsTest.log(num1);
		j++;
        }
        jsTest.log("-------------------create coll1 normal-------------------");
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: -10, d: -20}));
		printShardingStatus(st.config,false);
        var shards = cfg.shards.find().toArray();
        //assert.eq("shard0000",shards[0]._id);
        //assert.eq("shard0001",shards[1]._id);
        //assert.eq("shard0002",shards[2]._id);
        assert.writeOK(coll.update({a:-1},{"$set":{b:-10000}},false,true));
        assert.eq(502, coll.find().itcount());
		assert.eq(2, coll.find({"name1":20}).itcount());
		assert.eq(1, coll.find({b:-10000}).itcount());
        assert.commandWorked(coll.dropIndex("name1_1"));
        assert.eq(10, coll.getIndexes().length);
		assert.writeOK(coll2.update({a:-1},{"$set":{b:-10000}},false,true));
        assert.eq(802, coll2.find().itcount());
		assert.eq(2, coll2.find({"name1":20}).itcount());
		assert.eq(1, coll2.find({b:-10000}).itcount());
        assert.commandWorked(coll2.dropIndex("name1_1"));
        assert.eq(10, coll2.getIndexes().length);

        st.stop();
})();


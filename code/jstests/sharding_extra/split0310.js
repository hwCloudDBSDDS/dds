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
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:1,b:1}}));
        jsTest.log("-------------------insert data-------------------");
        for (var i=0;i<1000;i++){
        assert.writeOK(coll.insert({a: i, b: i}));}
        for (var i=1;i<10;i++){
        assert.commandWorked(coll.ensureIndex({"name": i}));}
        printShardingStatus(st.config,false);
        for (var j=0;j<10;){
        var ransp = Math.floor(Math.random()*1000);
		var ran=String("a"+ransp);
		
        //assert.commandFailed(admin.runCommand({split: "testDB.foo", manualsplit:true,find :{a : ransp}}));
		assert.commandFailed(admin.runCommand({split: "testDB.foo",middle :{a : ransp}}));
        printShardingStatus(st.config,false);
		
		j++;
        }
        jsTest.log("-------------------create coll1 normal-------------------");
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
		printShardingStatus(st.config,false);
        var shards = cfg.shards.find().toArray();
        //assert.eq("shard0000",shards[0]._id);
        //assert.eq("shard0001",shards[1]._id);
        //assert.eq("shard0002",shards[2]._id);
        coll.update({"a":2900,"b":112},{"$set":{"value":20}},{upsert:true});
        assert.eq(1001, coll.find().itcount());
        assert.commandWorked(coll.dropIndex("name_9"));
        assert.eq(10, coll.getIndexes().length);

        st.stop();
})();


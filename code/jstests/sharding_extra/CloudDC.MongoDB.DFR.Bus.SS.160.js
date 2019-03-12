(function() {
    'use strict';

        var st = new ShardingTest({shards: 3, mongos: 1});
		var array=[0,1,2];
	var rdm=[];
	for (var j=0;j<3;j++){
	var index=Math.floor(Math.random()*array.length);
	rdm[j]=array.splice(index,1)[0];
	}
	var sort1=rdm[0];
	var sort2=rdm[1];
	var sort3=rdm[2];
        var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var coll=mgs.getCollection("testDB.foo");
		var colll=mgs.getCollection("testDB.fooo");
		var coll1=mgs.getCollection("testDB.foo1");
		var colll1=mgs.getCollection("testDB.fooo1");
        var testdb=mgs.getDB('testDB');
	st.startBalancer();
	assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:"hashed"},numInitialChunks:50}));
		assert.commandWorked(admin.runCommand({shardCollection:"testDB.fooo",key:{a:1}}));
 	jsTest.log("-------------------query 51 chunk OK-------------------");
	var chunks = cfg.chunks.find().toArray();
	var num = cfg.chunks.find().itcount();
        var shard0=0;
        var shard1=0;
        var shard2=0;
	assert.eq(num,51);
	for (var i = 0; i < num; i++) {
	  if(chunks[i].shard == "shard0000"){
	     ( shard0++ );}
	  if(chunks[i].shard == "shard0001"){
             ( shard1++ );}
	  if(chunks[i].shard == "shard0002"){
            ( shard2++ );}
	}
	assert.gte(shard0,16,"balance failed");
	assert.gte(shard1,16,"balance failed");
	assert.gte(shard2,16,"balance failed");
        printShardingStatus(st.config,false);
        jsTest.log("-------------------insert OK-------------------");
        assert.writeOK(coll.insert({a: -10, c: 10},{ writeConcern: { w: 1,j:true}}));
		assert.writeOK(colll.insert({a: -10, c: 10},{ writeConcern: { w: 1,j:true}}));

		jsTest.log("-------------------kill primary shard-------------------");
        // MongoRunner.stopMongod(primarycs);
		MongoRunner.stopMongod(st._connections[sort1]);
		sleep (20 * 1000)
		MongoRunner.runMongod(st._connections[sort1]);
		sleep (240 * 1000)
		printShardingStatus(st.config,false);
        jsTest.log("-------------------cofirm chunk balance normal-------------------");
        //query chunk
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        var shard1=0;
        var shard2=0;
        var shard3=0;
		var shdname1="shard000"+String(sort2);
		var shdname2="shard000"+String(sort3);
        assert.eq(num,51);
        for (var i = 0; i < num; i++) {
          if(chunks[i].shard == shdname1){
             ( shard1++ );}
          if(chunks[i].shard == shdname2){
             ( shard2++ );}
        }
        assert.gte(shard1,24,"balance failed");
        assert.gte(shard2,24,"balance failed");
		printShardingStatus(st.config,false);
		jsTest.log("-------------------update coll OK!2!-------------------");
		printShardingStatus(st.config,false);
		var a=coll.find().toArray()[0];
		jsTest.log("++++++++++"+a)
        assert.writeOK(coll.update({c: 10},{$set : {c : 102}}, false,true));
		assert.writeOK(colll.update({c: 10},{$set : {c : 22}}, false,true));
		assert.eq(102,coll.find({a:-10}).toArray()[0].c, "update 4 failed");
		assert.eq(22,colll.find({a:-10}).toArray()[0].c, "update 5 failed");
        printShardingStatus(st.config,false);
		assert.eq(1, coll.find().itcount());
		assert.eq(1, colll.find().itcount());
        assert.neq(null, coll.getIndexes());
        st.stop();
})();
    

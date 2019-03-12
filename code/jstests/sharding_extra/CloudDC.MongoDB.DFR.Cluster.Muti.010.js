(function() {
    'use strict';
	for(var i=0;i<1;i++){
        var st = new ShardingTest({shards: 3, mongos: 1});
	var array=[0,1,2];
	var rdm=[];
	for (var j=0;j<1;j++){
	var index=Math.floor(Math.random()*array.length);
	rdm[j]=array.splice(index,1)[0];
	}
	var rdm1=rdm[0];
	rdm.sort(function(a,b){return a>b?1:-1});
	var sort1=rdm[0];
	var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var coll=mgs.getCollection("testDB.foo");
        var coll1=mgs.getCollection("testDB.foo1");
        var testdb=mgs.getDB('testDB');   
        st.startBalancer();
        assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:1}}));
        assert.writeOK(coll.insert({a: -10, c: 10},{ writeConcern: { w: 1,j:true}}));
		printShardingStatus(st.config,false);
		jsTest.log(rdm1);
		MongoRunner.stopMongod(st._configServers[sort1]);
		MongoRunner.stopMongod(st._connections[0]);
		sleep (5 *500);
		MongoRunner.stopMongod(st._connections[1]);
		sleep (5 *500);
		MongoRunner.stopMongod(st._connections[2]);
		sleep (40 *1000);
		printShardingStatus(st.config,false);
		//assert.eq(1,cfg.shards.find().itcount(),"shard counts is wrong");
		assert.commandFailed(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));	
		assert.writeError(coll.insert({a: 10},{ writeConcern: { w: 1,j:true}}));		
		assert.commandFailed(admin.runCommand({moveChunk: "testDB.foo",find:{a: -10},to: "shard0000"}));
        
	st.stop();
	}
	
})();	

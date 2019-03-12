(function() {
    'use strict';
	for(var i=0;i<2;i++){
        var st = new ShardingTest({shards: 3, mongos: 1});
	var array=[0,1,2];
	var rdm=[];
	for (var j=0;j<1;j++){
	var index=Math.floor(Math.random()*array.length);
	rdm[j]=array.splice(index,1)[0];
	}
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
		MongoRunner.stopMongod(st._configServers[0]);
		MongoRunner.stopMongod(st._configServers[1]);
		MongoRunner.stopMongod(st._configServers[2]);
		MongoRunner.stopMongod(st._connections[0]);
		MongoRunner.stopMongod(st._connections[1]);
		MongoRunner.stopMongod(st._connections[2]);
		//printShardingStatus(st.config,false);
		MongoRunner.runMongod(st._configServers[sort1]);
		MongoRunner.runMongod(st._connections[0]);
		MongoRunner.runMongod(st._connections[1]);
		MongoRunner.runMongod(st._connections[2]);
		var a=st.configRS.getURL();
        var str=String(a);
        var c=str.split(/[\,\:]/);
        var port=Math.floor(c[1]);
        var ssp1=port+4+0;
        var addss1=c[2]+":"+ssp1;
		var ssp2=port+4+1;
        var addss2=c[2]+":"+ssp2;
		var ssp2=port+4+2;
        var addss3=c[2]+":"+ssp2;
		jsTest.log("++++++++++++++"+addss1+"------------");
		assert.commandFailed(admin.runCommand({addshard: addss1}), "Failed to add shard1");
		assert.commandFailed(admin.runCommand({addshard: addss2}), "Failed to add shard2");
		assert.commandFailed(admin.runCommand({addshard: addss3}), "Failed to add shard2");
		sleep (240 *1000);
		

		
		assert.commandFailed(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));	
		assert.writeError(coll.insert({a: 10},{ writeConcern: { w: 1,j:true}}));		
		assert.commandFailed(admin.runCommand({moveChunk: "testDB.foo",find:{a: -10},to: "shard0000"}));
			
	
	st.stop();
	}
	
})();	

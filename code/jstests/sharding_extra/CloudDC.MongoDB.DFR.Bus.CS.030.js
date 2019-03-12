(function() {
    'use strict';

        var st = new ShardingTest({
        shards: 3,
        other: {
            c0: {},  // Make sure 1st config server is primary
            c1: {rsConfig: {priority: 0}},
            c2: {rsConfig: {priority: 0}}
       	 }
    	});
	st.startBalancer();
	var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var testdb=mgs.getDB('testDB');
	var coll=mgs.getCollection("testDB.foo");
	var chunkCountOnShard = function(z){
		var ct=cfg.chunks.find().itcount();
	        var cks=cfg.chunks.find().toArray();
        	var a0=0;
	        var a1=0;
        	var a2=0;
		var z0=z;
		var z1=z+1;
		var z2=z+2;
		var shd0=String("shard000"+z0);
		var shd1=String("shard000"+z1);
		var shd2=String("shard000"+z2);
	        for (var m=0;m<ct;m++){
        	        if(cks[m].shard==shd0)
                	        a0++;
        		if(cks[m].shard==shd1)
				a1++;
			if(cks[m].shard==shd2)
                        	a2++;
                }
		var array=[a0,a1,a2];
		return array;
	}
	assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
	assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:"hashed"},numInitialChunks:50}));
	sleep (10*1000);
	var a=[];
	a=chunkCountOnShard(0);
	assert.lte(16,a[0],"1-a0");
	assert.lte(16,a[1],"1-a1");
	assert.lte(16,a[2],"1-a2");
	st.stopBalancer();
	for(var i=-50;i<80;i++){
		assert.writeOK(coll.insert({"a":i},{ writeConcern: { w: 1,j:true}}));
		assert.commandWorked(mgs.adminCommand({moveChunk: 'testDB.foo',find: {a: i},to:"shard0002"}));
	}
	sleep (5*1000);
        var a=[];
        a=chunkCountOnShard(0);
	assert.lte(45,a[2],"2-a2");
	st.startBalancer();
	
	for (var j=0;j<401;j++){
		var a=[];
        a=chunkCountOnShard(0);
		if(a[2]<40)
		{break;}
		sleep (1*1000);
		jsTest.log("waiting...");
		assert.lt(j,400,"Timeout");
	}
	printShardingStatus(st.config,false);
	MongoRunner.stopMongod(configSecondaryList[0]);
	MongoRunner.stopMongod(configSecondaryList[1]);
	MongoRunner.runMongod(configSecondaryList[0]);
	sleep (180 *1000);

	printShardingStatus(st.config,false);
	var ct=cfg.shards.find().itcount();
	assert.eq(3,ct,"ss kill itself");
		for(var i=-50;i<80;i++){
	assert.writeOK(coll.insert({"a":i},{ writeConcern: { w: 1,j:true}}));}
	var ct=cfg.chunks.find().itcount();
        var cks=cfg.chunks.find().toArray();
        var a0=0;
        var a1=0;
        var a2=0;
        for (var m=0;m<ct;m++){
        	if(cks[m].shard=="shard0000")
                        a0++;
                if(cks[m].shard=="shard0001")
                        a1++;
                if(cks[m].shard=="shard0002")
                        a2++;
        }	
	assert.lte(15,a0,"1-a3");
    assert.lte(15,a2,"1-a4");
	assert.lte(15,a1,"1-a4");
	for(var i=-50;i<80;i++){
	assert.writeOK(coll.insert({"a":i},{ writeConcern: { w: 1,j:true}}));}
	st.stop();
})();	

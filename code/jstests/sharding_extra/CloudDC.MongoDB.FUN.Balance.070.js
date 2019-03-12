(function() {
    'use strict';

        var st = new ShardingTest({shards: 3, mongos: 1,other: {enableAutoSplit: true}});
        var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var testdb=mgs.getDB('testDB');
		st.startBalancer();
        st.enableAutoSplit();
		st.startBalancer();

        var collNames= [];
		assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
		var bigString = "";
            while (bigString.length < 1000*1000)
                 bigString += "asocsancdnsjfnsdnfsjdhfasdfasdfasdfnsadofnsadlkfnsaldknfsad";
        for (var i=0;i<1;i++){
            assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo"+i,key:{a:1}}));
            collNames[i]=mgs.getCollection("testDB.foo"+i);
            //var ("coll"+i)=mgs.getCollection("testDB"+i+".foo");
            
            var bulk=collNames[i].initializeUnorderedBulkOp();
            for (var j=0; j < 1000; j++) {
                 bulk.insert({a: j, s: bigString},{ writeConcern: { w: 1,j:true}});
            }
            assert.writeOK(bulk.execute());
            jsTest.log("----------"+i+"------------");
        }
        printShardingStatus(st.config,false);
        sleep (240*1000);
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
		jsTest.log("--------------------"+a0+"----------------------*****");
		jsTest.log("--------------------"+a1+"---------------------*****");
		jsTest.log("--------------------"+a2+"---------------------*****");	
        assert.lte(2,a0,"3-a1");
		assert.lte(2,a1,"3-a1");
		assert.lte(2,a2,"3-a1");
        st.stop();
})();

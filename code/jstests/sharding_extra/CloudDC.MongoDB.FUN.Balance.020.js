(function() {
    'use strict';

        var st = new ShardingTest({shards: 3, mongos: 1,other: {enableAutoSplit: true}});
        var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var testdb=mgs.getDB('testDB');
		st.startBalancer();
        //assert.commandWorked(admin.runCommand({enableAutoSplit:"true"}));
        var collNames= [];
		            assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
        for (var i=0;i<50;i++){
            assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo"+i,key:{a:"hashed"},numInitialChunks:15}));
            collNames[i]=mgs.getCollection("testDB.foo"+i);
            //var ("coll"+i)=mgs.getCollection("testDB"+i+".foo");
    
            
            collNames[i].insert({a: 1},{ writeConcern: { w: 1,j:true}});
           
            jsTest.log("----------"+i+"------------");
            }
        sleep (5*1000);
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
		jsTest.log("**************"+a0+"*******************");
		jsTest.log("**************"+a1+"*******************");
		jsTest.log("**************"+a2+"*******************");		
        assert.lte(240,a0,"3-a1");
		assert.lte(240,a1,"3-a1");
		assert.lte(240,a2,"3-a1");
        st.stop();
})();

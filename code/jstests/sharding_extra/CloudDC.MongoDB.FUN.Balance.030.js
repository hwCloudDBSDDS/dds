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
					var rdm = Math.floor(Math.random()*15+1);
        for (var i=0;i<20;i++){
            assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo"+i,key:{a:"hashed"},numInitialChunks:rdm*3}));
            collNames[i]=mgs.getCollection("testDB.foo"+i);
            //var ("coll"+i)=mgs.getCollection("testDB"+i+".foo");
            for (var j=0; j < 10+i*2; j++) {
                 collNames[i].insert({a: j},{ writeConcern: { w: 1,j:true}});
            }
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
		var a=(rdm*18);
		jsTest.log("**************"+a+"*******************");	
        assert.lte(a,a0,"3-a1");
		assert.lte(a,a1,"3-a1");
		assert.lte(a,a2,"3-a1");
        st.stop();
})();

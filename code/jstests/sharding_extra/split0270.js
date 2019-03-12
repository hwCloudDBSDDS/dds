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
        for (var i=-500;i<1000;i++){
        assert.writeOK(coll.insert({a: i, c: i}));}
        for (var i=1;i<10;i++){
        assert.commandWorked(coll.ensureIndex({"name": i}));}
        printShardingStatus(st.config,false);
		var array=[];
        for (var j=0;j<10;){
        var ransp = Math.floor(Math.random()*1000)-500;
		array[j]=String(ransp);
		array.sort(function(a,b){return a>b?1:-1});
		var m=0;
		for(;m<j;m++){
			if(array[m]==array[m+1]){
				m=100;
				break;
			}	
		}
		if (m>j){
			break
		}
        //assert.commandWorked(admin.runCommand({split: "testDB.foo", manualsplit:true,find :{a : ransp}}));
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : ransp}}));
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
		var max=[];
		var min=[];
		for (var n=0;n<=j+1;n++){
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
		if (max[0]!=array[0]){
		for (var m=1;m<max.length;m++){
			assert.eq(max[m],min[m]);
			assert.eq(max[m],array[m-1]);
		}
		}
		if (max[0]==array[0]){
		for (var m=1;m<max.length-1;m++){
			assert.eq(max[m],min[m]);
			assert.eq(max[m],array[m]);
		}
		}
		jsTest.log("-------------------confirm size normal-------------------");
        jsTest.log(j);
        var chunks = cfg.chunks.find().toArray();
        var num1 = cfg.chunks.find().itcount();
        var num2 = j + 2;
        assert.eq(num1,num2);
        jsTest.log("-------------------confirm update normal-------------------");
        jsTest.log(j);
        jsTest.log(num1);
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
        coll.update({"a":2900},{"$set":{"value":20}},{upsert:true});
        assert.eq(1501, coll.find().itcount());
        assert.commandWorked(coll.dropIndex("name_9"));
        assert.eq(10, coll.getIndexes().length);

        st.stop();
})();


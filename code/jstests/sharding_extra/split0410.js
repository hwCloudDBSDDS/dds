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
        for (var i=0;i<1000;i++){
        assert.writeOK(coll.insert({a: i, c: i}));}
        printShardingStatus(st.config,false);
		var array=[];
		var ransp = Math.floor(Math.random()+100);
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : ransp}}));
        for (var j=0;j<9;){
        array[0]=String(ransp);
        array[j+1]=String(ransp+100*j+100);
	array.sort(function(a,b){return a>b?1:-1});
        //assert.commandWorked(admin.runCommand({split: "testDB.foo", manualsplit:true,find :{a : ransp}}));
		//assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : ransp}}));
        printShardingStatus(st.config,false);
		jsTest.log("-------------------split again-----------------------------");
        var ransr = ransp + j*100+100;
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : ransr}}));
        jsTest.log("-------------------confirm chunks normal-------------------");
		var max=[];
		var min=[];
		for (var n=0;n<j+3;n++){
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
		jsTest.log(array[0]);
		jsTest.log(array[1]);
		if (max[0]!=array[0]){
		for (var m=1;m<max.length;m++){
			assert.eq(max[m],min[m]);
			assert.eq(max[m],array[m-1]);
		}
		}
		if (max[0]==array[0]){
		for (var m=0;m<max.length-1;m++){
			assert.eq(max[m],min[m]);
			assert.eq(max[m],array[m]);
		}
		}
		jsTest.log("-------------------confirm size normal-------------------");
        jsTest.log(j);
        var chunks = cfg.chunks.find().toArray();
        var num1 = cfg.chunks.find().itcount();
		var num2 = 1;
        var num2 = j+3;
        assert.eq(num1,num2);
		j++;
        }
        jsTest.log("-------------------create coll1 normal-------------------");
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
        st.stop();
})();


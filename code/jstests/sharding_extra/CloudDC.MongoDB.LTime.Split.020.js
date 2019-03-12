(function() {
    'use strict';

        var st = new ShardingTest({shards: 3, mongos: 1});
        var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var coll=mgs.getCollection("testDB.foo");
        var coll1=mgs.getCollection("testDB.foo1");
        var testdb=mgs.getDB('testDB');
        st.startBalancer();
        assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:1}}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:"hashed"},numInitialChunks:50}));
		for (var j=0;j<1000;j++){
		//assert.writeOK(coll.insert({a: (-1-j),b: (-1-j)}));		
		assert.writeOK(coll.insert({a: 1111*(j+5)}));
		assert.writeOK(coll1.insert({b: 1111*(j+5)}));
		}

	jsTest.log("-------------------splite-------------------");
	for (var i=0;i<4;i++) {
	var array=[];
	for (var j=0;j<10;){
        var ransp = 100*(i+1)+(j+1);
		array[j]=String(ransp);
		array.sort(function(a,b){return a>b?1:-1});
		var m=0;
		for(;m<j;m++){
			if(array[m]==array[m+1]){
				m=100000;
				break;
			}	
		}
	if (m>j){
		continue;
	}	
	assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : ransp}}));
	sleep(2*1000);
        assert.commandWorked(admin.runCommand({split: "testDB.foo1",middle :{b : ransp}}));
		sleep(2*1000);
	j++;
	}

	printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        //var chunks = cfg.chunks.find().toArray();
        //var max = chunks[0].max.a;
	//var min = chunks[1].min.a;
        //assert.eq(max,ransp);
	//assert.eq(min,ransp);
        //var chunks = cfg.chunks.find().toArray();
        var chunknum = cfg.chunks.find().itcount();
        assert.eq(chunknum , 20*(i+1)+51 ,"chunks not equal");
        jsTest.log("-------------------confirm update normal-------------------");
        var ransp = Math.floor(Math.random()*1000);
	var ransl = ransp - 10000000;
        var ransr = ransp + 10000000;
        var ranso = Math.floor(Math.random()*ransp);
        var num = 1000 - ransp ;
        var ransq = Math.floor(Math.random()*num + ransp);
        assert.writeOK(coll.insert({a: ransl,c: 1001}));
        assert.writeOK(coll.insert({a: ransr,c: 1002}));
        assert.writeOK(coll1.insert({b: ransl,c: 1003}));
        assert.writeOK(coll1.insert({b: ransr,c: 1004}));
        assert.writeOK(coll.update({c: 1001},{$set : {c : 10001}}, false,true));
        assert.writeOK(coll.update({c: 1002},{$set : {c : 10002}}, false,true));
		assert.writeOK(coll1.update({c: 1003},{$set : {c : 10003}}, false,true));
        assert.writeOK(coll1.update({c: 1004},{$set : {c : 10004}}, false,true));
        assert.eq(10001,coll.find({a: ransl}).toArray()[0].c, "update  failed");
        assert.eq(10002,coll.find({a: ransr}).toArray()[0].c, "update  failed");
        assert.eq(10003,coll1.find({b: ransl}).toArray()[0].c, "update  failed");
        assert.eq(10004,coll1.find({b: ransr}).toArray()[0].c, "update  failed");
	}
	st.stop();
	
})();

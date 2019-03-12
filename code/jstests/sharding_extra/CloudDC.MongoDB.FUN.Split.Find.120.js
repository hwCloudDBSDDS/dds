(function() {
    'use strict';
	   
        var st = new ShardingTest({shards: 3, mongos: 1,other: {enableAutoSplit: false}});
        var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var coll=mgs.getCollection("testDB.foo");
        var coll1=mgs.getCollection("testDB.foo1");
        var testdb=mgs.getDB('testDB');
        st.startBalancer();
        assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:1}}));
        jsTest.log("-------------------insert data-------------------");
		assert.commandWorked(coll.ensureIndex({"name": 11}));
		assert.eq(3, coll.getIndexes().length);
        printShardingStatus(st.config,false);
		var common=["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"]
		var rdm2=[];
		var str2="";
		var a=Math.floor(Math.random()*30);
		for (var j=0;j<=a;j++){
		var index=Math.floor(Math.random()*common.length);
		rdm2[j]=common[index];
		str2=str2+rdm2[j];
		}
        jsTest.log("-------------------splite-------------------");
		jsTest.log("-------------------str2-------------------"+str2);
		assert.commandFailed(admin.runCommand({split: "testDB.foo",find :{a : str2}}));
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,1);
        jsTest.log("-------------------confirm update normal-------------------");
		var max=Math.floor(Math.random()*1000+500);
		var ranspinsert=max/2;
        var ransl = ranspinsert - max/4;
        var ransr = ranspinsert + max/4;
        var ranso = Math.floor(Math.random()*ranspinsert);
        var num = max - ranspinsert ;
        var ransq = Math.floor(Math.random()*num + ranspinsert);
        assert.writeOK(coll.insert({a: ransl,c: 1001}));
        assert.writeOK(coll.insert({a: ransr,c: 1002}));
        assert.writeOK(coll.update({a: ransl},{$set : {c : 1003}}, false,true));
        assert.writeOK(coll.update({a: ransr},{$set : {c : 1004}}, false,true));
        assert.eq(1003,coll.find({a: ransl}).toArray()[0].c, "update  failed");
        assert.eq(1004,coll.find({a: ransr}).toArray()[0].c, "update  failed");
		assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
		assert.commandWorked(coll.dropIndex("name_11"));
		assert.eq(2, coll.getIndexes().length);	
        st.stop();
})();

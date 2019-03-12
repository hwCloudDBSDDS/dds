(function() {
    'use strict';

        var st = new ShardingTest({shards: 3, mongos: 1});
		// mid2有重复的490位特殊字符
		var special=["!","@","￥","%","+","*","[","#","a","1","&","-","=","|",":",";","<","?",">","}","{","(",")","]"]
		var rdm2=[];
		var mid2="";
		for (var j=0;j<300;j++){
		var index=Math.floor(Math.random()*special.length);
		rdm2[j]=special[index];
		mid2=mid2+rdm2[j];
		}
        var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var coll=mgs.getCollection("testDB.foo");
        var coll1=mgs.getCollection("testDB.foo1");
		var coll2=mgs.getCollection("testDB.foo2");
		var coll3=mgs.getCollection("testDB.foo3");
        var testdb=mgs.getDB('testDB');
        st.startBalancer();
        assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:1}}));
		assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{a:1}}));
		assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo2",key:{a:1,c:1}}));
		assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo3",key:{a:"hashed"}}));
        jsTest.log("-------------------insert data-------------------");
        for (var i=0;i<1000;i++){
        assert.writeOK(coll.insert({a: i, c: i}));
		assert.writeOK(coll1.insert({a: i, c: i}));
		assert.writeOK(coll2.insert({a: i, c: i}));
		assert.writeOK(coll3.insert({a: i, c: i}));
		}
        printShardingStatus(st.config,false);
        jsTest.log("-------------------splite-------------------");
		
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : mid2}}));
		for (var n=0;n<220;n++){
			var index=Math.floor(Math.random()*special.length);
			mid2=mid2+special[index];
		}
		assert.commandFailed(admin.runCommand({split: "testDB.foo",middle :{a : mid2}}));
		
		var mid="";
		for (var n=0;n<499;n++){
			mid=mid+"a";
			
		}
		assert.commandWorked(admin.runCommand({split: "testDB.foo1",middle :{a : mid}}));
		mid=mid+"a";
		assert.commandFailed(admin.runCommand({split: "testDB.foo1",middle :{a : mid}}));
		
		var mid="";
		for (var n=0;n<245;n++){
			mid=mid+"a";
			
		}
		assert.commandWorked(admin.runCommand({split: "testDB.foo2",middle :{a : mid,c: mid}}));
		mid=mid+"a";
		assert.commandFailed(admin.runCommand({split: "testDB.foo2",middle :{a : mid,c: mid}}));
			
		var mid="";
		for (var n=0;n<499;n++){
			mid=mid+"a";
			
		}
		assert.commandWorked(admin.runCommand({split: "testDB.foo3",middle :{a : mid}}));
		mid=mid+"a";
		assert.commandFailed(admin.runCommand({split: "testDB.foo3",middle :{a : mid}}));
        //assert.commandFailed(admin.runCommand({split: "testDB.fooo",manualsplit:true,find :{a : mid}}));
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,13);
        st.stop();
})();

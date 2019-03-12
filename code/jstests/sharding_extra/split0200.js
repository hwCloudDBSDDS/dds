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
        jsTest.log("-------------------insert data-------------------");
		
		for (var i=0;i<1000;i++){
			 var b=Number(i-100);
        assert.writeOK(coll.insert({a: b, c: b}));}
        printShardingStatus(st.config,false);
        jsTest.log("-------------------splite-------------------");
		var special=["!","@","￥","%","+","*","[","#","a","1","&","-","=","|",":",";","<","?",">","}","{","(",")","]"]
		var rdm2=[];
		var str2="";
		var a=Math.floor(Math.random()*100);
		for (var j=0;j<=a;j++){
		var index=Math.floor(Math.random()*special.length);
		rdm2[j]=special[index];
		str2=str2+rdm2[j];
		
		}


        //assert.commandWorked(admin.runCommand({split: "testDB.foo",manualsplit:true,find :{a : str2}}));
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : str2}}));
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        var chunks = cfg.chunks.find().toArray();
        var max = chunks[0].max.a;
		var min = chunks[1].min.a;
        assert.eq(max,str2);
		assert.eq(min,str2);
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,2);
        jsTest.log("-------------------confirm update normal-------------------");
        var ransl = 100;
        var ransr = -100;
        assert.writeOK(coll.insert({a: ransl,c: 1001}));
        assert.writeOK(coll.insert({a: ransr,c: 1002}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
        assert.writeOK(coll.update({c: -1},{$set : {c : 1003}}, false,true));
        assert.writeOK(coll.update({c: 100},{$set : {c : 1004}}, false,true));
        assert.eq(1003,coll.find({a: -1}).toArray()[0].c, "update  failed");
        assert.eq(1004,coll.find({a: 100}).toArray()[0].c, "update  failed");
        st.stop();
})();

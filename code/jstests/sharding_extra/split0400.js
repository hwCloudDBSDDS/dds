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
	var ransp = Math.floor(Math.random()*100+500);
	assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : ransp}}));
        for (var j=0;j<5;){
        //assert.commandWorked(admin.runCommand({split: "testDB.foo", manualsplit:true,find :{a : ransp}}));
		//assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : ransp}}));
        printShardingStatus(st.config,false);
		jsTest.log("-------------------split again-----------------------------");
	var ransl = ransp - j*100-100;
        var ransr = ransp + j*100+100;
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : ransl}}));
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : ransr}}));
		jsTest.log("-------------------confirm size normal-------------------");
        jsTest.log(j);
        var chunks = cfg.chunks.find().toArray();
        var num1 = cfg.chunks.find().itcount();
        var num2 = 2*j	+ 4;
        assert.eq(num1,num2);
        jsTest.log("-------------------confirm update normal-------------------");
        jsTest.log(j);
        jsTest.log(num1);
        var ransl = ransp - 100;
        var ransr = ransp + 100;
        var m = 1000 + j;
        var n = 1001 + j;
        var ranso = ransp-5*j;
        var num = 1000 - ransp ;
        var ransq = ransp+5*(j+1);
        var x = 1002 + j;
        var y = 1003 + j;
        assert.writeOK(coll.insert({a: ransl,c: m}));
        assert.writeOK(coll.insert({a: ransr,c: n}));
	//assert.writeOK(coll1.insert({b: 10, d: 20}));
        assert.writeOK(coll.update({c: ranso},{$set : {c : x}}, false,true));
        assert.writeOK(coll.update({c: ransq},{$set : {c : y}}, false,true));
        assert.eq(x,coll.find({a: ranso}).toArray()[0].c, "update  failed");
        assert.eq(y,coll.find({a: ransq}).toArray()[0].c, "update  failed");
		j++;
        }
        jsTest.log("-------------------create coll1 normal-------------------");
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
        st.stop();
})();


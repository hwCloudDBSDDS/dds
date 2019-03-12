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
	assert.commandWorked(coll.ensureIndex({"path": 1 }));
	jsTest.log("-------------------insert data-------------------");
        var bigString = "";
        while (bigString.length < 1024 * 1024){
        bigString += "asocsancdnsjfnsdnfsjdhfasdfasdfasdfnsadofnsadlkfnsaldknfsad";}
        var bulk = coll.initializeUnorderedBulkOp();
        for (var i = 0;i <1000; i++) {
        bulk.insert({a: i,s: bigString},{writeConcern:{w:1}});
        }
        assert.writeOK(bulk.execute());
        printShardingStatus(st.config,false);
	jsTest.log("-------------------Random split point-------------------");
	function createRandom(num ,min ,max) {
        var arr = [],res=[],newArr;
        var json = {};
        while (arr.length < num) {
        var ranNum = Math.ceil(Math.random() * (max - min)) + min;
        //通过判断json对象的索引值是否存在 来标记 是否重复
        if (!json[ranNum]) {
        json[ranNum] = 1;
        arr.push(ranNum);
        }
        }
        return arr;
        }
	jsTest.log("-------------------splite-------------------");
	var num = 10;
	var min = 0;
	var max = 10000;
	var sp= [];
	sp= createRandom(num ,min ,max);
	sleep(20*1000);
	for (var i =0;i<10;i++){
	assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : sp[i]}}));
	}
	printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        var chunknum = cfg.chunks.find().itcount();
        assert.eq(chunknum , 11 ,"chunks not equal");
        jsTest.log("-------------------confirm update normal-------------------");
        var ransp = Math.floor(Math.random()*1000);
	var ransl = ransp - 1000;
        var ransr = ransp + 1000;
        var ranso = Math.floor(Math.random()*ransp);
        var num = 1000 - ransp ;
        var ransq = Math.floor(Math.random()*num + ransp);
        assert.writeOK(coll.insert({a: ransl,"path": 1001}));
        assert.writeOK(coll.insert({a: ransr,"path": 1002}));
        assert.writeOK(coll.update({"path": 1001},{$set : {"path" : 10001}}, false,true));
        assert.writeOK(coll.update({"path": 1002},{$set : {"path" : 10002}}, false,true));
        assert.eq(10001,coll.find({a: ransl}).toArray()[0].path, "update  failed");
        assert.eq(10002,coll.find({a: ransr}).toArray()[0].path, "update  failed");
	st.stop();
})();

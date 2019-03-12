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
		
		var bigString = "";
		while (bigString.length < 1024 * 100){bigString += "11111111111111111333333333333333222222222222222";}
		var bulk = coll.initializeUnorderedBulkOp();
		for (var i = 200; i < 700; i++) {
			bulk.insert({a: i, s: bigString});
		}
		assert.writeOK(bulk.execute());
		
		for (var i=0;i<30000;i++){
			 var b=Number(i-100);
        assert.writeOK(coll.insert({a: b, c: b}));}
        printShardingStatus(st.config,false);
        jsTest.log("-------------------splite-------------------");

        //assert.commandWorked(admin.runCommand({split: "testDB.foo",manualsplit:true,find :{a : 20}}));
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : 20}}));
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        var chunks = cfg.chunks.find().toArray();
        var max = chunks[0].max.a;
		var min = chunks[1].min.a;
        assert.eq(max,20);
		assert.eq(min,20);
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,2);
		st.stopBalancer();
				for (var i=200;i<30000;i++){
			 var b=Number(i-100);
        assert.writeOK(coll.remove({a: b, c: b}));}
        //assert.commandWorked(admin.runCommand({split: "testDB.foo",manualsplit:true,find :{a : 10}}));
		assert.commandWorked(admin.runCommand({split: "testDB.foo",middle :{a : 10}}));		
		var max=[];
		var min=[];
		for (var n=0;n<=2;n++){
		var chunks = cfg.chunks.find().toArray();
        max[n] = String(chunks[n].max.a);
		min[n] = String(chunks[n].min.a);
		}
		max.sort(function(a,b){return a>b?1:-1});
		min.sort(function(a,b){return a>b?1:-1});
		if (max[0]!="10"){
		for (var m=1;m<max.length;m++){
			assert.eq(max[m],min[m]);
			assert.eq(max[m],10*m);
		}
		}
		if (max[0]=="10"){
		for (var m=0;m<max.length-1;m++){
			assert.eq(max[m],min[m]);
			assert.eq(max[m],10*(m+1));
		}
		}
		assert.eq(700,coll.find().itcount());
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,3);
		printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm update normal-------------------");
        assert.writeOK(coll.insert({a: 1,c: 1001}));
        assert.writeOK(coll.insert({a: 11,c: 1001}));
		assert.writeOK(coll.insert({a: 21,c: 1001}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
        assert.writeOK(coll.update({c: -10},{$set : {c : 1003}}, false,true));
		assert.writeOK(coll.update({c: 15},{$set : {c : 1005}}, false,true));
        assert.writeOK(coll.update({c: 50},{$set : {c : 1004}}, false,true));
        assert.eq(1003,coll.find({a: -10}).toArray()[0].c, "update  failed");
        assert.eq(1004,coll.find({a: 50}).toArray()[0].c, "update  failed");
		assert.eq(1005,coll.find({a: 15}).toArray()[0].c, "update  failed");
        st.stop();
})();

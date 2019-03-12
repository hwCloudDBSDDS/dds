(function() {
    'use strict';

        var st = new ShardingTest({shards: 3, mongos: 1,other: {enableAutoSplit: false}});
        var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var coll=mgs.getCollection("testDB.foo");
        var coll1=mgs.getCollection("testDB.foo1");
        var testdb=mgs.getDB('testDB');
        st.stopBalancer();
        assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:1,b:1,c:1,d:1,e:1,f:1}}));
        jsTest.log("-------------------insert data-------------------");
		var bigString = "";
		while (bigString.length < 1024 * 1024){
			bigString += "asocsancdnsjfnsdnfsjdhfasdfasdfasdfnsadofnsadlkfnsaldknfsad";}
		var i = 0;
		var cyclenum = 1000;
		var bulk = coll.initializeUnorderedBulkOp();
		for (; i < cyclenum; i++) {
			bulk.insert({a:i,b:i,c:i,d:i,e:i,f:i,wf:i,s: bigString});
		}
		assert.writeOK(bulk.execute());
	    assert.commandWorked(coll.ensureIndex({"name": 11}));
		assert.eq(3, coll.getIndexes().length);
        //for (var i=0;i<1000;i++){
        //assert.writeOK(coll.insert({a: i, c: i}));}
        printShardingStatus(st.config,false);
        jsTest.log("-------------------splite-------------------");
        var ransp = Math.floor(Math.random()*1000);
		assert.commandWorked(admin.runCommand({split: "testDB.foo",find :{a : ransp,b:ransp,c : ransp,d:ransp,e: ransp,f:ransp}}));
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        var chunks = cfg.chunks.find().toArray();
        var max = chunks[0].max.a;
		var min = chunks[1].min.a;
		assert.eq(max,min);
		var ranspinsert=max/2;
		jsTest.log("-------------------ranspinsert-------------------"+ranspinsert);
		st.printChunks();
        var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,2);
        jsTest.log("-------------------confirm update normal-------------------");
        var ransl = ranspinsert - max/4;
        var ransr = ranspinsert + max/4;
        var ranso = Math.floor(Math.random()*ranspinsert);
        var num = max - ranspinsert ;
        var ransq = Math.floor(Math.random()*num + ranspinsert);
        assert.writeOK(coll.insert({a: ransl,b: ransl,c: ransl,d: ransl,e: ransl,f: ransl,wf: 1001}));
        assert.writeOK(coll.insert({a: ransr,b: ransr,c: ransr,d: ransr,e: ransr,f: ransr,wf: 1002}));
        assert.writeOK(coll.update({wf: ranso},{$set : {wf : 1003}}, false,true));
        assert.writeOK(coll.update({wf: ransq},{$set : {wf : 1004}}, false,true));
        assert.eq(1003,coll.find({a: ranso}).toArray()[0].wf, "update  failed");
        assert.eq(1004,coll.find({a: ransq}).toArray()[0].wf, "update  failed");
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
		assert.commandWorked(admin.runCommand({split: "testDB.foo",find :{a : str2,b : str2,c : str2,d: str2,e : str2,f : str2}}));
		printShardingStatus(st.config,false);
		var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,3);
        jsTest.log("-------------------confirm update normal-------------------");
		var max2=[];
		var min2=[];
		for (var n=0;n<=2;n++){
		var chunks = cfg.chunks.find().toArray();
        max2[n] = String(chunks[n].max.a);
		min2[n] = String(chunks[n].min.a);
		}
		max2.sort(function(a,b){return a>b?1:-1});
		min2.sort(function(a,b){return a>b?1:-1});
		if (max2[0]!=max){
		for (var m=1;m<max2.length;m++){
			assert.eq(max2[m],min2[m]);
		}
		}
		if (max2[0]==max){
		for (var m=1;m<max2.length-1;m++){
			assert.eq(max2[m],min2[m]);
		}
		}
		jsTest.log(min2);
		jsTest.log(max2);
		if (max2[0]!=max){
		var insert = max2[2]
		}
		if (max2[0]==max){
			var insert = max2[1]
		}		
		jsTest.log("-------------------insert-------------------"+insert);
		var ranspinsert2=insert/2;
		jsTest.log("-------------------ranspinsert-------------------"+ranspinsert2);
		var ransl = ranspinsert2 - insert/4;
        var ransr = ranspinsert2 + insert/4;
        var ranso = Math.floor(Math.random()*ranspinsert2);
        var num = insert - ranspinsert2 ;
        var ransq = Math.floor(Math.random()*num + ranspinsert2);
        assert.writeOK(coll.insert({a: ransl,b: ransl,c: ransl,d: ransl,e: ransl,f: ransl,wf: 1001}));
        assert.writeOK(coll.insert({a: ransr,b: ransr,c: ransr,d: ransr,e: ransr,f: ransr,wf: 1002}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
        assert.writeOK(coll.update({a: ranso},{$set : {wf : 1005}}, false,true));
        assert.writeOK(coll.update({a: ransq},{$set : {wf : 1006}}, false,true));
        assert.eq(1005,coll.find({a: ranso}).toArray()[0].wf, "update  failed");
        assert.eq(1006,coll.find({a: ransq}).toArray()[0].wf, "update  failed");
		assert.commandWorked(coll.dropIndex("name_11"));
		assert.eq(2, coll.getIndexes().length);	
        st.stop();
})();

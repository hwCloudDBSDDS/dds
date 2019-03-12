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
        printShardingStatus(st.config,false);
        jsTest.log("-------------------splite-------------------");
		var Arrayransp=[]
		for (var j=0;j<=6;j++){
			var ransp = Math.floor(Math.random()*1000);
			Arrayransp.push(ransp)		
		}
		jsTest.log("-------------------Arrayransp-------------------");
		jsTest.log(Arrayransp);
		var common=["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"]
		var rdm2=[];
		var Arraystr2=[]
		for(var k=0;k<6;k++){
		var a=Math.floor(Math.random()*30);
		for (var j=0;j<=a;j++){
		var str2="";
		var index=Math.floor(Math.random()*common.length);
		rdm2[j]=common[index];
		str2=str2+rdm2[j];
		}
		Arraystr2.push(str2)
		}
		jsTest.log("-------------------Arraystr2-------------------");
		jsTest.log(Arraystr2);
        jsTest.log("-------------------splite-------------------");
		assert.commandWorked(admin.runCommand({split: "testDB.foo",find :{a : Arrayransp[0],b:Arrayransp[1],c : Arrayransp[2],d:Arraystr2[0],e: Arraystr2[1],f:Arraystr2[2]}}));
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
        var chunks = cfg.chunks.find().toArray();
        var max = chunks[0].max.a;
		var min = chunks[1].min.a;
		assert.eq(max,min);
		var ranspinsert=max/2;
		jsTest.log("-------------------ranspinsert-------------------"+ranspinsert);
		st.printChunks();
        //assert.eq(max,ranspinsert);
		//assert.eq(min,ranspinsert);
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
        assert.writeOK(coll.update({a: ranso},{$set : {wf : 1003}}, false,true));
        assert.writeOK(coll.update({a: ransq},{$set : {wf : 1004}}, false,true));
        assert.eq(1003,coll.find({a: ranso}).toArray()[0].wf, "update  failed");
        assert.eq(1004,coll.find({a: ransq}).toArray()[0].wf, "update  failed");
		printShardingStatus(st.config,false);
		assert.commandWorked(admin.runCommand({split: "testDB.foo",find :{a : Arraystr2[3],b : Arraystr2[4],c : Arraystr2[5],d: Arrayransp[3],e : Arrayransp[4],f : Arrayransp[5]}}));
		printShardingStatus(st.config,false);
		var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,3);
        jsTest.log("-------------------confirm update normal-------------------");
		var chunks = cfg.chunks.find().toArray();
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
		var ranspinsert2=insert/2;
		jsTest.log("-------------------ranspinsert-------------------"+ranspinsert);
		var ransl = ranspinsert2 - insert/4;
        var ransr = ranspinsert2 + insert/4;
        var ranso = Math.floor(Math.random()*ranspinsert2);
        var num = insert - ranspinsert2 ;
        var ransq = Math.floor(Math.random()*num + ranspinsert2);
        assert.writeOK(coll.insert({a: ransl,b: ransl,c: ransl,d: ransl,e: ransl,f: ransl,wf: 1001}));
        assert.writeOK(coll.insert({a: ransr,b: ransr,c: ransr,d: ransr,e: ransr,f: ransr,wf: 1002}));
        assert.writeOK(coll.update({a: ranso},{$set : {wf : 1003}}, false,true));
        assert.writeOK(coll.update({a: ransq},{$set : {wf : 1004}}, false,true));
        assert.eq(1003,coll.find({a: ranso}).toArray()[0].wf, "update  failed");
        assert.eq(1004,coll.find({a: ransq}).toArray()[0].wf, "update  failed");
		printShardingStatus(st.config,false);
		var Randomvalue=[];
		for (var i=0;i<6;i++){			
			var value=String(Arrayransp[i])+String(Arraystr2[i]);
			Randomvalue.push(value);
		}
		jsTest.log("-------------------Randomvalue-------------------");
		jsTest.log(Randomvalue);
		assert.commandWorked(admin.runCommand({split: "testDB.foo",find :{a : Randomvalue[0],b : Randomvalue[1],c : Randomvalue[2],d: Randomvalue[3],e : Randomvalue[4],f : Randomvalue[5]}}));
		printShardingStatus(st.config,false);
		var chunks = cfg.chunks.find().toArray();
        var num = cfg.chunks.find().itcount();
        assert.eq(num,4);
        jsTest.log("-------------------confirm update normal-------------------");
		var chunks = cfg.chunks.find().toArray();
		var max3=[];
		var min3=[];
		for (var n=0;n<=3;n++){
		var chunks = cfg.chunks.find().toArray();
        max3[n] = String(chunks[n].max.a);
		min3[n] = String(chunks[n].min.a);
		}
		max3.sort(function(a,b){return a>b?1:-1});
		min3.sort(function(a,b){return a>b?1:-1});
		if (max3[0]!=max){
		for (var m=1;m<max3.length;m++){
			assert.eq(max3[m],min3[m]);
		}
		}
		if (max3[0]==max){
		for (var m=1;m<max3.length-1;m++){
			assert.eq(max3[m],min3[m]);
		}
		}
		jsTest.log(min2);
		jsTest.log(min3);
		if (max3[0]!=max){
		var insert = max3[2]
		}
		if (max3[0]==max){
			var insert = max3[1]
		}		
		var ranspinsert2=insert/2;
		jsTest.log("-------------------ranspinsert2-------------------"+ranspinsert2);
		var ransl = ranspinsert2 - insert/4;
        var ransr = ranspinsert2 + insert/4;
        var ranso = Math.floor(Math.random()*ranspinsert2);
        var num = insert - ranspinsert2 ;
        var ransq = Math.floor(Math.random()*num + ranspinsert2);
        assert.writeOK(coll.insert({a: ransl,b: ransl,c: ransl,d: ransl,e: ransl,f: ransl,wf: 1001}));
        assert.writeOK(coll.insert({a: ransr,b: ransr,c: ransr,d: ransr,e: ransr,f: ransr,wf: 1002}));
        assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
        assert.writeOK(coll.update({a: ranso},{$set : {wf : 1003}}, false,true));
        assert.writeOK(coll.update({a: ransq},{$set : {wf : 1004}}, false,true));
        assert.eq(1003,coll.find({a: ranso}).toArray()[0].wf, "update  failed");
        assert.eq(1004,coll.find({a: ransq}).toArray()[0].wf, "update  failed");
		assert.commandWorked(coll.dropIndex("name_11"));
		assert.eq(2, coll.getIndexes().length);	
        st.stop();
})();

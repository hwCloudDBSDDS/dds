(function() {
    'use strict';
		var special=["!","@","￥","%","+","*","[","#","a","1","&","-","=","|",":",";","<","?",">","}","{","(",")","]"]
		var rdm2=[];
		var str2="";
		var a=Math.floor(Math.random()*30);
		for (var j=0;j<=a;j++){
		var index=Math.floor(Math.random()*special.length);
		rdm2[j]=special[index];
		str2=str2+rdm2[j];
		}

		var rdm1=[];
		var a=Math.floor(Math.random()*20);
		var str1="";
		
		for (var j=0;j<=a;j++){
		var index=Math.floor(Math.random()*special.length);
		rdm1[j]=special.splice(index,1)[0];
		str1=str1+rdm1[j];
		}
		jsTest.log("--------------------------------------"+str1);
		jsTest.log("--------------------------------------"+str2);
        var st = new ShardingTest({shards: 3, mongos: 1,other: {enableAutoSplit: false}});
        var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var coll=mgs.getCollection(str2+"."+str1);
		var coll1=mgs.getCollection(str2+"."+str2);
        var testdb=mgs.getDB(str2);
        st.startBalancer();
        assert.commandWorked(admin.runCommand({enableSharding:str2}));
        assert.commandWorked(admin.runCommand({shardCollection:str2+"."+str1,key:{a:1}}));
        jsTest.log("-------------------insert data-------------------");
		var bigString = "";
		while (bigString.length < 1024 * 1024){
			bigString += "asocsancdnsjfnsdnfsjdhfasdfasdfasdfnsadofnsadlkfnsaldknfsad";}
		var i = 0;
		var cyclenum = 1000;
		var bulk = coll.initializeUnorderedBulkOp();
		for (; i < cyclenum; i++) {
			bulk.insert({a: i, c:i,s: bigString});
		}
		assert.writeOK(bulk.execute());
		assert.commandWorked(coll.ensureIndex({"a": 11}));
		assert.eq(3, coll.getIndexes().length);	
        printShardingStatus(st.config,false);
        jsTest.log("-------------------splite-------------------");
		assert.commandWorked(admin.runCommand({split: str2+"."+str1,middle :{a : 402}}));
        printShardingStatus(st.config,false);
		assert.commandWorked(admin.runCommand({split: str2+"."+str1,middle :{a : str2}}));
        jsTest.log("-------------------confirm chunks normal-------------------");
		printShardingStatus(st.config,false);
        var chunks = cfg.chunks.find().toArray();
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
		jsTest.log(min2);
		jsTest.log(max2);
		jsTest.log("-------------------confirm update normal-------------------");
		
		if ((max2[0]!=1)&& (max2[0]==402)){
		var split1 = max2[0]
		split1=Number(split1)
		var split2 = max2[1]
		}
		if ((max2[0]!=1) && (max2[0]!=402)){
		var split2 = max2[0]
		split1=Number(split1)
		var split1 = max2[1]
		}
		if ((max2[0]==1)&& (max2[0]==402)){
		var split1 = max2[0]
		split1=Number(split1)
		var split2 = max2[1]
		}	
		if ((max2[0]==1)&& (max2[0]!=402)){
		var split2 = max2[0]
		split1=Number(split1)
		var split1 = max2[1]
		}
		assert.commandWorked(admin.runCommand({split:str2+"."+str1,bounds:[{a:402},{a:str2}]}));
		var num = cfg.chunks.find().itcount();
        assert.eq(num,4);
        jsTest.log("-------------------confirm update normal-------------------");
		st.printChunks();
        jsTest.log("-------------------confirm update normal-------------------");
		var ranso = Math.floor(Math.random()*402);
        var ransq = Math.floor(Math.random()*402 + 402);
        assert.writeOK(coll.update({a: ranso},{$set : {c : 1003}}, false,true));
        assert.writeOK(coll.update({a: ransq},{$set : {c : 1004}}, false,true));
        assert.eq(1003,coll.find({a: ranso}).toArray()[0].c, "update  failed");
        assert.eq(1004,coll.find({a: ransq}).toArray()[0].c, "update  failed");
		var ranso = Math.floor(Math.random()*1000);
        var ransq = Math.floor(Math.random()*1000 + 1000);
		assert.writeOK(coll.insert({a: ransq,c: ransq}));
        assert.writeOK(coll.update({a: ranso},{$set : {c : 1003}}, false,true));
        assert.writeOK(coll.update({a: ransq},{$set : {c : 1004}}, false,true));
        assert.eq(1003,coll.find({a: ranso}).toArray()[0].c, "update  failed");
        assert.eq(1004,coll.find({a: ransq}).toArray()[0].c, "update  failed");
		assert.commandWorked(admin.runCommand({shardCollection:str2+"."+str2,key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
		assert.commandWorked(coll.dropIndex("a_11"));
		assert.eq(2, coll.getIndexes().length);	
        st.stop();
})();

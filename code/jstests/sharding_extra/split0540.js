(function() {
    'use strict';

        var st = new ShardingTest({shards: 3, mongos: 1});
        var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
        var admin=mgs.getDB('admin');
        var cfg=mgs.getDB('config');
        var coll=mgs.getCollection("testDB.foo");
        var coll1=mgs.getCollection("testDB1.foo1");
        var testdb=mgs.getDB('testDB');
        st.startBalancer();
		for (var k=0;k<5;k++){
			assert.commandWorked(admin.runCommand({enableSharding:"testDB"+k}));
			assert.commandWorked(admin.runCommand({shardCollection:"testDB"+k+".foo",key:{a:1}}));
			jsTest.log("-------------------insert data-------------------");
			var collx=mgs.getCollection("testDB"+k+".foo");
			for (var i=0;i<1000;i++){
				assert.writeOK(collx.insert({a: i, c: i}));}
		}
        printShardingStatus(st.config,false);
		sleep(120*1000);
		var array=[];
        for (var j=0;j<2;){
        var ransp = 100+6*j;
		array[j]=String(ransp);
		array.sort(function(a,b){return a>b?1:-1});
		var m=0;
		for(;m<j;m++){
			if(array[m]==array[m+1]){
				m=1000;
				break;
			}	
		}
		if (m>j){
			break
		}
		for (var k=0;k<5;k++){
        //assert.commandWorked(admin.runCommand({split: "testDB.foo", manualsplit:true,find :{a : ransp}}));
		assert.commandWorked(admin.runCommand({split: "testDB"+k+".foo",middle :{a : ransp}}));
		}
        printShardingStatus(st.config,false);
        jsTest.log("-------------------confirm chunks normal-------------------");
		var max=[];
		var min=[];
		for (var n=0;n<=(j+1)*5;n++){
		var chunks = cfg.chunks.find().toArray();
        max[n] = String(chunks[n].max.a);
		min[n] = String(chunks[n].min.a);
		}
		max.sort(function(a,b){return a>b?1:-1});
		min.sort(function(a,b){return a>b?1:-1});
		if (max[0]!=array[0]){
		for (var m=5;m<max.length;m++){
			assert.eq(max[m],min[m]);
			assert.eq(max[m],array[((m-5)/5)]);
		}
		}
		if (max[0]==array[0]){
		for (var m=5;m<max.length-5;m++){
			assert.eq(max[m],min[m]);
			assert.eq(max[m],array[(m/5)]);
		}
		}
        jsTest.log(j);
        var chunks = cfg.chunks.find().toArray();
        var num1 = cfg.chunks.find().itcount();
        var num2 = (j + 2)*5;
        assert.eq(num1,num2);
        var num3 = cfg.databases.find().itcount();
        assert.eq(5,num3);
		var num4 = cfg.collections.find().itcount();
        assert.eq(5,num4);
		for (var k=0;k<5;k++){
			var collx=mgs.getCollection("testDB"+k+".foo");
		var num5=collx.getIndexes().length;
		assert.eq(2,num5);
        jsTest.log("-------------------confirm update normal-------------------");
        jsTest.log(j);
        jsTest.log(num1);
        var ransl = ransp - 1;
        var ransr = ransp + 1;
        var m = 1000 + j;
        var n = 1001 + j;
       var ranso = ransp - 2;
        var num = 1000 - ransp ;
        var ransq = ransp + 2;

        var x = 1002 + j;
        var y = 1003 + j;
        assert.writeOK(collx.insert({a: ransl,c: m}));
        assert.writeOK(collx.insert({a: ransr,c: n}));
	//assert.writeOK(coll1.insert({b: 10, d: 20}));
        assert.writeOK(collx.update({c: ranso},{$set : {c : x}}, false,true));
        assert.writeOK(collx.update({c: ransq},{$set : {c : y}}, false,true));
        assert.eq(x,collx.find({a: ranso}).toArray()[0].c, "update  failed");
        assert.eq(y,collx.find({a: ransq}).toArray()[0].c, "update  failed");
		}
		j++;
        }
        jsTest.log("-------------------create coll1 normal-------------------");
        assert.commandWorked(admin.runCommand({shardCollection:"testDB1.foo1",key:{b:1}}));
        assert.writeOK(coll1.insert({b: 10, d: 20}));
        st.stop();
})();

(function() {
 'use strict';
 var xx=0;
 while (xx<1)
 {
 var check=0;
 var st = new ShardingTest({shards: 3, mongos: 1,other: {enableAutoSplit: true}});
 var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
 var admin=mgs.getDB('admin');
 var cfg=mgs.getDB('config');
 var coll=mgs.getCollection("testDB.foo");
 var coll1=mgs.getCollection("testDB.foo1");
 var coll2=mgs.getCollection("testDB1.fooo1");
 var coll3=mgs.getCollection("testDB.fooo1");
 var testdb=mgs.getDB('testDB');
 st.startBalancer();
 assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
 assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:1}}));
 assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{a:"hashed"},numInitialChunks:2}));
 assert.commandWorked(coll.createIndex({b: 1}));
 assert.commandWorked(coll1.createIndex({b: 1}));
 st.disableAutoSplit();

 jsTest.log("-------------------insert data-------------------");
 var bigString = "";
 while (bigString.length < 1024 * 1024){bigString += "asdfgerertfdfdfdssdsf";}
 //var bulk = coll.initializeUnorderedBulkOp();
 for (var i = 0; i < 2000; i++) {
     assert.writeOK(coll.insert({a: i, s: bigString,b:i},{ writeConcern: { w: 1,j:true}}));
 }
 //assert.writeOK(bulk.execute());
 st.enableAutoSplit();
 //确认autosplit开始
 for (var j=0;j<401;j++){
     var chunknumm = cfg.chunks.find({ns:"testDB.foo"}).itcount();
     if(chunknumm>1)
     {
         jsTest.log("++++++started in "+j+"seconds...");
         break;}
     sleep (5*100);
     jsTest.log("waiting...");
     assert.lt(j,400,"Timeout");
 }
 for (var j=0;j<30;j++){
     var mid=5000*(j+1);
     admin.runCommand({split: "testDB.foo",middle :{a : mid}});

 }
 sleep(20*1000);
 printShardingStatus(st.config,false);

 var ct=0;
 var chunkn=cfg.chunks.find({ns:"testDB.foo"}).itcount();
 jsTest.log("chunkfoo======"+chunkn)
     for (var n=0;n<chunkn;n++){
         var chunks = cfg.chunks.find({ns:"testDB.foo"}).toArray();
         if ((parseInt(chunks[n].max.a)%5000==0)&&(parseInt(chunks[n].max.a)/5000>0))
           ct+=1
     }
 jsTest.log("ct======"+ct)
     if (ct==30)
       check+=1;
 assert.gte(chunkn,2);
 sleep (10*1000);
 printShardingStatus(st.config,false);
 var shards = cfg.shards.find().itcount();
 assert.eq(shards,3);
 //test.b
 st.disableAutoSplit();
 //var bulk = coll1.initializeUnorderedBulkOp();
 for (var i = 0; i < 2000; i++) {
      assert.writeOK(coll1.insert({a: i, s: bigString,b:i},{ writeConcern: { w: 1,j:true}}));
 }
 //assert.writeOK(bulk.execute());
 st.enableAutoSplit();
 //确认autosplit开始
 for (var j=0;j<401;j++){
     var chunknumm = cfg.chunks.find({ns:"testDB.foo1"}).itcount();
     if(chunknumm>2)
     {
         jsTest.log("++++++started in "+j+"seconds...");
         break;}
     sleep (5*100);
     jsTest.log("waiting...");
     assert.lt(j,400,"Timeout");
 }
 for (var j=0;j<30;j++){
     var mid=5000*(j+1);
     admin.runCommand({split: "testDB.foo1",middle :{a : mid}});		
 }
 sleep (20*1000);
 printShardingStatus(st.config,false);
 //判断2次都成功
 var ct=0;
 var chunkn=cfg.chunks.find({ns:"testDB.foo1"}).itcount();
 for (var n=0;n<chunkn;n++){
     var chunks = cfg.chunks.find({ns:"testDB.foo1"}).toArray();
     if ((parseInt(chunks[n].max.a)%5000==0)&&(parseInt(chunks[n].max.a)/5000>0))
       ct+=1
 }
 jsTest.log("ct2======"+ct)
     if (ct==30)
       check+=1;
 assert.gte(chunkn,3);
 //chunk 正常
 /*var max=[];
 var min=[];
 var chunkn=cfg.chunks.find().itcount();
 for (var n=0;n<chunkn;n++){
     var chunks = cfg.chunks.find().toArray();
     max[n] = String(chunks[n].max.a);
     min[n] = String(chunks[n].min.a);
 }
 max.sort(function(a,b){return a>b?1:-1});
 min.sort(function(a,b){return a>b?1:-1});
 if (max[0]!=min[0]){
     for (var m=2;m<max.length;m++){
         assert.eq(max[m],min[m]);
     }
 }
 if (max[0]==min[0]){
     for (var m=0;m<max.length-2;m++){
         assert.eq(max[m],min[m]);
     }
 }*/

 assert.eq(2000,coll.count());
 assert.eq(2000,coll1.count());
 assert.eq(1,coll.find({a:1,b:1}).itcount());
 assert.eq(1,coll1.find({a:1,b:1}).itcount());
 printShardingStatus(st.config,false);
 var shards = cfg.shards.find().itcount();
 assert.eq(shards,3);


 jsTest.log("-------------------confirm update normal-------------------");
 assert.writeOK(coll.insert({a: -1,b: 1001}));
 assert.writeOK(coll.insert({a: 10000000,b: 1001}));
 assert.writeOK(coll1.insert({a: -1,b: 1001}));
 assert.writeOK(coll1.insert({a: 10000000,b: 1001}));
 assert.commandWorked(admin.runCommand({enableSharding:"testDB1"}));
 assert.commandWorked(admin.runCommand({shardCollection:"testDB.fooo1",key:{b:1}}));
 assert.commandWorked(admin.runCommand({shardCollection:"testDB1.fooo1",key:{b:1}}));
 assert.writeOK(coll2.insert({b: 10, d: 20}));
 assert.writeOK(coll3.insert({b: 10, d: 20}));
 assert.writeOK(coll.update({b: 10},{$set : {b : 1003}}, false,true));
 assert.writeOK(coll1.update({b: 10},{$set : {b : 1003}}, false,true));
 assert.eq(1003,coll.find({a: 10}).toArray()[0].b, "update  failed");
 assert.eq(1003,coll1.find({a: 10}).toArray()[0].b, "update  failed");
 assert.eq(check,2);
 st.stop();
 xx+=1;
 jsTest.log(xx+" times end")
 }
})();

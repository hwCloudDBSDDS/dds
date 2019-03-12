(function() {
 'use strict';

 var st = new ShardingTest({
shards: 3,
other: {
c0: {},  // Make sure 1st config server is primary
c1: {rsConfig: {priority: 0}},
c2: {rsConfig: {priority: 0}}
}
});
 st.startBalancer();
 var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
 var admin=mgs.getDB('admin');
 var cfg=mgs.getDB('config');
 var testdb=mgs.getDB('testDB');
 var coll=mgs.getCollection("testDB.foo");
 var chunkCountOnShard = function(z){
     var ct=cfg.chunks.find().itcount();
     var cks=cfg.chunks.find().toArray();
     var a0=0;
     var a1=0;
     var a2=0;
     var z0=z;
     var z1=z+1;
     var z2=z+2;
     var shd0=String("shard000"+z0);
     var shd1=String("shard000"+z1);
     var shd2=String("shard000"+z2);
     for (var m=0;m<ct;m++){
	 if(cks[m].shard==shd0)
		 a0++;
	 if(cks[m].shard==shd1)
		 a1++;
	 if(cks[m].shard==shd2)
		 a2++;
     }
     var array=[a0,a1,a2];
     return array;
 }
assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:"hashed"},numInitialChunks:50}));
sleep (10*1000);
var a=[];
a=chunkCountOnShard(0);
assert.lte(16,a[0],"1-a0");
assert.lte(16,a[1],"1-a1");
assert.lte(16,a[2],"1-a2");
st.stopBalancer();
for(var i=-120;i<120;i++){
	assert.writeOK(coll.insert({"a":i},{ writeConcern: { w: 1,j:true}}));
	assert.commandWorked(mgs.adminCommand({moveChunk: 'testDB.foo',find: {a: i},to:"shard0002"}));
}
var a=[];
a=chunkCountOnShard(0);
assert.lte(40,a[2],"2-a2");
st.startBalancer();
sleep (240*1000);
var a=[];
a=chunkCountOnShard(0);
assert.lte(15,a[0],"1-a0");
assert.lte(15,a[1],"1-a1");
assert.lte(15,a[2],"1-a2");
st.stop();
})();	




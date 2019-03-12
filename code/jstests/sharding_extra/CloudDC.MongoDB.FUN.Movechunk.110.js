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

 var primarycs=st.configRS.getPrimary();var configSecondaryList = st.configRS.getSecondaries();var mgs=st.s0;
 var admin=mgs.getDB('admin');
 var numchunk=Math.floor(Math.random()+50);
 jsTest.log("--------numchunk------------------------------"+numchunk);
 assert.commandWorked(admin.runCommand({enableSharding:"testDB"}));
 assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo",key:{a:1}}));
 assert.commandWorked(admin.runCommand({shardCollection:"testDB.foo1",key:{a:"hashed"},numInitialChunks:numchunk}))
 var cfg=mgs.getDB('config');
 var coll=mgs.getCollection("testDB.foo");
var testdb=mgs.getDB('testDB');
// coll.drop();
assert.writeOK(coll.insert({a: 1, Value: 'Test value 1'}));
assert.writeOK(coll.insert({a: 10, Value: 'Test value 10'}));
assert.writeOK(coll.insert({a: 20, Value: 'Test value 20'}));
assert.writeOK(coll.insert({a: 30, Value: 'Test value 30'}));
printShardingStatus(st.config,false);

assert.commandWorked(st.moveChunk('testDB.foo', {a: 20}, "shard0000"));
var chunks = cfg.chunks.find({ns:"testDB.foo"}).toArray();
assert.eq("shard0000",chunks[0].shard);
assert.eq(4, coll.find().itcount());
MongoRunner.stopMongod(st.shard2);
assert.commandWorked(st.moveChunk('testDB.foo', {a: 20}, "shard0001"));
jsTest.log("--------------------------------------");
sleep(20*1000);
printShardingStatus(st.config,false);
var shards = cfg.shards.find().toArray();
//assert.eq("shard0000",shards[0]._id);
//assert.eq("shard0001",shards[1]._id);
assert.eq(1,shards[0].state);
assert.eq(1,shards[1].state);
assert.eq(2,shards.length);
var chunks = cfg.chunks.find().toArray();
assert.eq(numchunk+1,chunks.length);
//	var shards = cfg.shards.find().toArray();
//      for(var i=0;i<=shards.length;i++){
//        assert.eq("shard000"+i,shards[i]._id);
//      assert.eq(1,shards[i].state);
//}
var chunks = cfg.chunks.find({ns:"testDB.foo"}).toArray();
assert.eq("shard0001",chunks[0].shard);
coll.update({"a":11},{"$set":{"value":20}},{upsert:true});
assert.eq(5, coll.find().itcount());
st.stop();
})();



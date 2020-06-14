 (function() {	
    var s1 = new ShardingTest({name: "route_refresh1", shards: 2, mongos: 2});
    var s2 = s1._mongos[1];

	assert.writeOK(s1.getDB("test1").foo.insert({num:1}));
	assert.writeOK(s1.getDB("test2").foo.insert({num:1}));
	assert.writeOK(s1.getDB("test3").foo.insert({num:1}));
	assert.writeOK(s1.getDB("test4").foo.insert({num:1}));
	assert.writeOK(s1.getDB("test5").foo.insert({num:1}));
	assert.writeOK(s1.getDB("test6").foo.insert({num:1}));
	assert.writeOK(s1.getDB("test7").foo.insert({num:1}));
	
	if (s1.configRS) {
    // Ensure that the second mongos will see the movePrimary
     s1.configRS.awaitLastOpCommitted();
    }
	
	assert.eq(1, s2.getDB("test1").foo.find({num:1}).count(),"origin route flush failed");
	
	var s1primaryshard = s1.getPrimaryShard("test1").name;
	
	if(s1primaryshard != s1.getPrimaryShard("test2").name ) { 
	    assert.commandWorked(
        s1.getDB("test2").dropDatabase());
	}
	
	if(s1primaryshard != s1.getPrimaryShard("test3").name ) {
		assert.commandWorked(
        s1.getDB("test3").dropDatabase());
	}
	
	if(s1primaryshard != s1.getPrimaryShard("test4").name ) {
		assert.commandWorked(
        s1.getDB("test4").dropDatabase());
	}
	
	if(s1primaryshard != s1.getPrimaryShard("test5").name ) {
		assert.commandWorked(
        s1.getDB("test5").dropDatabase());
	}
	
	if(s1primaryshard != s1.getPrimaryShard("test6").name ) {
		assert.commandWorked(
        s1.getDB("test6").dropDatabase());
	}
	
	if(s1primaryshard != s1.getPrimaryShard("test7").name ) {
		assert.commandWorked(
        s1.getDB("test7").dropDatabase());
	}
	
	//dropDatabase test1
	assert.commandWorked(s1.getDB("test1").dropDatabase());
	
	//recreate test1 on other shard
	assert.writeOK(s1.getDB("test1").foo.insert({num:2}));
	assert.writeOK(s1.getDB("test1").foo.insert({num:2}));
	assert.writeOK(s1.getDB("test1").foo.insert({num:2}));
	assert.writeOK(s1.getDB("test1").foo.insert({num:2}));
	assert.writeOK(s1.getDB("test1").foo.insert({num:2}));
	assert.writeOK(s1.getDB("test1").foo.insert({num:2}));
	assert.writeOK(s1.getDB("test1").foo.insert({num:2}));
	assert.writeOK(s1.getDB("test1").foo.insert({num:2}));
	
	//延迟
	sleep(12000);
	
	//s2 insert data into test1
    assert.writeOK(s2.getDB("test1").foo.insert({num:3}));
	assert.writeOK(s2.getDB("test1").foo.insert({num:3}));
	assert.writeOK(s2.getDB("test1").foo.insert({num:3}));
	assert.writeOK(s2.getDB("test1").foo.insert({num:3}));
	assert.writeOK(s2.getDB("test1").foo.insert({num:3}));

	assert.eq(8, s2.getDB("test1").foo.find({num:2}).count(), "route flush failed");
	assert.eq(5, s1.getDB("test1").foo.find({num:3}).count(), "route flush failed");
	assert.eq(13,s1.getDB("test1").foo.find({}).count(), "route flush failed");
	
	s1.stop();
})();

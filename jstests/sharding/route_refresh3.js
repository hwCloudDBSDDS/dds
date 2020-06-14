  (function() {	
    var s1 = new ShardingTest({name: "route_refresh3", shards: 2, mongos: 2});
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
	assert.writeOK(s1.getDB("test1").foo.insert({name: "aaa"}));
	assert.writeOK(s1.getDB("test1").foo.insert({name: "bbb"}));
	assert.writeOK(s1.getDB("test1").foo.insert({name: "ccc"}));
	assert.writeOK(s1.getDB("test1").foo.insert({name: "ddd"}));
	assert.writeOK(s1.getDB("test1").foo.insert({name: "eee"}));
	assert.writeOK(s1.getDB("test1").foo.insert({name: "fff"}));
	assert.writeOK(s1.getDB("test1").foo.insert({name: "ggg"}));
	
	assert.eq(1, s1.getDB("test1").foo.find({name: "aaa"}).count(), "route flush failed");
	assert.eq(1, s1.getDB("test1").foo.find({name: "bbb"}).count(), "route flush failed");
	assert.eq(1, s1.getDB("test1").foo.find({name: "ccc"}).count(), "route flush failed");
	assert.eq(1, s1.getDB("test1").foo.find({name: "ddd"}).count(), "route flush failed");
	assert.eq(1, s1.getDB("test1").foo.find({name: "eee"}).count(), "route flush failed");
	assert.eq(1, s1.getDB("test1").foo.find({name: "fff"}).count(), "route flush failed");
	assert.eq(1, s1.getDB("test1").foo.find({name: "ggg"}).count(), "route flush failed");
	
	//delay
	sleep(12000);	
	
	//s2 drop new database
	assert.commandWorked(s2.getDB("test1").dropDatabase());
	
	//s1 check new database data is new or not
	assert.eq(0, s1.getDB("test1").foo.find({name: "aaa"}).count(), "route flush failed");
	assert.eq(0, s1.getDB("test1").foo.find({name: "bbb"}).count(), "route flush failed");
	assert.eq(0, s1.getDB("test1").foo.find({name: "ccc"}).count(), "route flush failed");
	assert.eq(0, s1.getDB("test1").foo.find({name: "ddd"}).count(), "route flush failed");
	assert.eq(0, s1.getDB("test1").foo.find({name: "eee"}).count(), "route flush failed");
	assert.eq(0, s1.getDB("test1").foo.find({name: "fff"}).count(), "route flush failed");
	assert.eq(0, s1.getDB("test1").foo.find({name: "ggg"}).count(), "route flush failed");
	
	assert.eq(0, s1.getDB("test1").foo.find({}).count(), "route flush failed");
	
	s1.stop();
})();

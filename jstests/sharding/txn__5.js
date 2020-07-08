(function() {
    'use strict';

    var st = new ShardingTest({shards: 2, mongos: 1});

    var mongos = st.s0;
    var testDbyj1 = st.s0.getDB('testyj1');
    var testDbyj2 = st.s0.getDB('testyj2');
    var testDbyj3 = st.s0.getDB('testyj3');
    var testDbyj4 = st.s0.getDB('testyj4');
    
    var admin = mongos.getDB("admin");
    var shards = mongos.getCollection("config.shards").find().toArray();
    printjson(admin.runCommand({movePrimary: "testyj1", to: shards[0]._id}));
    printjson(admin.runCommand({movePrimary: "testyj2", to: shards[0]._id}));
    printjson(admin.runCommand({movePrimary: "testyj3", to: shards[0]._id}));
    printjson(admin.runCommand({movePrimary: "testyj4", to: shards[0]._id}));

    assert.commandWorked(testDbyj1.testc.insert({"hello": "aaa"}));
    assert.commandWorked(testDbyj2.testc.insert({"hello": "bbb"}));
    assert.commandWorked(testDbyj3.testc.insert({"hello": "vvv"}));
    assert.commandWorked(testDbyj4.testc.insert({"hello": "kkk"}));
    var res = testDbyj1.testc.find({"hello": "aaa"}).toArray(); 
    assert.eq(res.length, 1);
    assert.commandWorked(testDbyj1.testc.insert({"hello": "aaa"}));
    res = testDbyj1.testc.find({"hello": "aaa"}).toArray(); 
    assert.eq(res.length, 2);

    var session = testDbyj1.getMongo().startSession();
    var sessionDbyj1 = session.getDatabase('testyj1');
    var sessionDbyj2 = session.getDatabase('testyj2');
    var sessionDbyj3 = session.getDatabase('testyj3');
    var sessionDbyj4 = session.getDatabase('testyj4');
    session.startTransaction();
    res = sessionDbyj1.testc.find().toArray(); 
    assert.eq(res.length, 2);
    
    assert.commandWorked(sessionDbyj2.testc.insert({"hello": "aaa"}));
    assert.commandWorked(sessionDbyj3.testc.insert({"hello": "aaa"}));
    assert.commandWorked(sessionDbyj4.testc.insert({"hello": "aaa"}));
    res = sessionDbyj2.testc.find({"hello": "aaa"}).toArray(); 
    assert.eq(res.length, 1);
    
    assert.commandWorked(sessionDbyj3.testc.deleteOne({"hello": "aaa"})); 
    res = sessionDbyj3.testc.find({"hello": "aaa"}).toArray(); 
    assert.eq(res.length, 0);
    
    assert.commandWorked(sessionDbyj4.testc.deleteMany({"hello": "aaa"})); 
    res = sessionDbyj4.testc.find({"hello": "aaa"}).toArray(); 
    assert.eq(res.length, 0);
    
    assert.commandWorked(sessionDbyj2.testc.update({"hello": "bbb"}, {"hello":"bbb123"}));
    res = sessionDbyj2.testc.find({"hello": "bbb"}).toArray(); 
    assert.eq(res.length, 0);
    res = sessionDbyj2.testc.find({"hello": "bbb123"}).toArray(); 
    assert.eq(res.length, 1);

    
    session.commitTransaction();
    
    res = testDbyj2.testc.find().toArray(); 
    assert.eq(res.length, 2);
    
    session.endSession();

    st.stop();
})();

(function() {
    'use strict';

    var st = new ShardingTest({shards: 2});
    var testDb = st.s0.getDB('testyj');
    assert.commandWorked(testDb.testc.insert({"hello": "aaa"}));
    assert.commandWorked(testDb.testc.insert({"hello": "bbb"}));
    assert.commandWorked(testDb.testc.insert({"hello": "vvv"}));
    var res = testDb.testc.find({"hello": "aaa"}).toArray(); 
    assert.eq(res.length, 1);
    assert.commandWorked(testDb.testc.insert({"hello": "aaa"}));
    res = testDb.testc.find({"hello": "aaa"}).toArray(); 
    assert.eq(res.length, 2);
    var session = testDb.getMongo().startSession();
    var sessionDb = session.getDatabase('testyj');
    session.startTransaction();
    res = sessionDb.testc.find().toArray(); 
    assert.eq(res.length, 4);
    assert.commandWorked(sessionDb.testc.insert({"hello": "aaa"}));
    assert.commandWorked(sessionDb.testc.insert({"hello": "aaa"}));
    assert.commandWorked(sessionDb.testc.insert({"hello": "aaa"}));
    res = sessionDb.testc.find({"hello": "aaa"}).toArray(); 
    assert.eq(res.length, 5);
    assert.commandWorked(sessionDb.testc.deleteOne({"hello": "aaa"})); 
    res = sessionDb.testc.find({"hello": "aaa"}).toArray(); 
    assert.eq(res.length, 4);
    
    session.abortTransaction();
    session.endSession();
    st.stop();
})();

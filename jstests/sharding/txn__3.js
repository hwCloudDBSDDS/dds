(function() {
    'use strict';

    var st = new ShardingTest({shards: 2});    
    var testDb = st.s0.getDB('testyj');
    assert.commandWorked(testDb.testc.insert({"hello": "bbb1"}));
    assert.commandWorked(testDb.testc.insert({"hello": "bbb2"}));
    assert.commandWorked(testDb.testc.insert({"hello": "bbb3"}));
    assert.commandWorked(testDb.testc.insert({"hello": "bbb4"}));
    var res = testDb.testc.distinct("hello"); 

    var session = testDb.getMongo().startSession();
    var sessionDb = session.getDatabase('testyj');
    session.startTransaction();
    var res = sessionDb.testc.distinct("hello");
 
    session.commitTransaction();
    res = testDb.testc.find().toArray();
    printjson(res);
    
    session.endSession();

    st.stop();
})();

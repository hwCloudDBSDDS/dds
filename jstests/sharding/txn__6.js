(function() {
    'use strict';

    var st = new ShardingTest({shards: 2});    


    var testDb = st.s0.getDB('testyj');
    assert.commandWorked(testDb.testc.insert({"hello": "bbb"}));

    var session = testDb.getMongo().startSession();
    var sessionDb = session.getDatabase('testyj');
    session.startTransaction();
    var res = sessionDb.testc.findAndModify({query:{"hello": "bbb"}, update:{"hello": "bbb123123"}});
    printjson(res);
 
    session.commitTransaction();
    res = testDb.testc.find().toArray();
    printjson(res);
    assert.eq("bbb123123", res[0]['hello']);
    
    session.endSession();

    st.stop();
})();

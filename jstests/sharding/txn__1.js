(function() {
   'use strict';

    var st = new ShardingTest({shards: 2});

    var testDb = st.s0.getDB('test');
//   st.s0.setLogLevel(5);
    assert.commandWorked(testDb.testc.insert({x: 0}));
    var res = testDb.testc.find({x: 0}).toArray(); assert.eq(res.length, 1);
    assert.commandWorked(testDb.testc.insert({x: 0}));
    res = testDb.testc.find({x: 0}).toArray(); assert.eq(res.length, 2);
    var session = testDb.getMongo().startSession();
    var sessionDb = session.getDatabase('test');
//    assert.commandWorked(sessionDb.runCommand({listCollections: 1, nameOnly: true}));
    session.startTransaction();
    res = sessionDb.testc.find({x: 0}).toArray(); assert.eq(res.length, 2);
    assert.commandWorked(sessionDb.testc.insert({x: 0}));
    res = sessionDb.testc.find({x: 0}).toArray(); assert.eq(res.length, 3);
    let findRes = assert.commandWorked(sessionDb.runCommand({find: "testc", batchSize: 0}));
    let cursorId = findRes.cursor.id;
    assert.neq(0, cursorId);
    assert.commandWorked(sessionDb.runCommand({getMore: cursorId, collection: "testc"}));
    assert.commandWorked(sessionDb.testc.insert({x: 0}));
    res = sessionDb.testc.find({x: 0}).toArray(); assert.eq(res.length, 4);
    assert.commandWorked(sessionDb.testc.insert({x: 0}));
    res = sessionDb.testc.find({x: 0}).toArray(); assert.eq(res.length, 5);
    res = testDb.testc.find({x: 0}).toArray(); assert.eq(res.length, 2);
    assert.commandWorked(sessionDb.runCommand({aggregate: "testc", pipeline: [], cursor: {}, txnNumber: NumberLong(0)}));

    session.commitTransaction();
    res = testDb.testc.find({x: 0}).toArray(); assert.eq(res.length, 5);


//    session.endSession();

   st.stop();

})();

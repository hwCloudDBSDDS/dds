// Test $text explain.  SERVER-12037.

/*var coll = db.fts_explain;
var res;

coll.drop();
res = coll.ensureIndex({content: "text"}, {default_language: "none"});
assert.commandWorked(res);

res = coll.insert({content: "some data"});
assert.writeOK(res);

var explain = coll.find({$text: {$search: "\"a\" -b -\"c\""}}).explain(true);
var stage = explain.executionStats.executionStages;
if ("SINGLE_SHARD" === stage.stage) {
    stage = stage.shards[0].executionStages;
}
assert.eq(stage.stage, "TEXT");
assert.gte(stage.textIndexVersion, 1, "textIndexVersion incorrect or missing.");
assert.eq(stage.inputStage.stage, "TEXT_MATCH");
assert.eq(stage.inputStage.inputStage.stage, "TEXT_OR");
assert.eq(stage.parsedTextQuery.terms, ["a"]);
assert.eq(stage.parsedTextQuery.negatedTerms, ["b"]);
assert.eq(stage.parsedTextQuery.phrases, ["a"]);
assert.eq(stage.parsedTextQuery.negatedPhrases, ["c"]);
*/
(function() {
    "use strict";
     const coll = db.fts_explain;
    let res;
     coll.drop();
    res = coll.ensureIndex({content: "text"}, {default_language: "none"});
    assert.commandWorked(res);
     res = coll.insert({content: "some data"});
    assert.writeOK(res);
     const explain =
        coll.find({$text: {$search: "\"a\" -b -\"c\""}}, {content: 1, score: {$meta: "textScore"}})
            .explain(true);
    let stage = explain.executionStats.executionStages;
    if ("SINGLE_SHARD" === stage.stage) {
        stage = stage.shards[0].executionStages;
    }
     assert.eq(stage.stage, "PROJECTION");
     let textStage = stage.inputStage;
    assert.eq(textStage.stage, "TEXT");
    assert.gte(textStage.textIndexVersion, 1, "textIndexVersion incorrect or missing.");
    assert.eq(textStage.inputStage.stage, "TEXT_MATCH");
    assert.eq(textStage.inputStage.inputStage.stage, "TEXT_OR");
    assert.eq(textStage.parsedTextQuery.terms, ["a"]);
    assert.eq(textStage.parsedTextQuery.negatedTerms, ["b"]);
    assert.eq(textStage.parsedTextQuery.phrases, ["a"]);
    assert.eq(textStage.parsedTextQuery.negatedPhrases, ["c"]);
})();

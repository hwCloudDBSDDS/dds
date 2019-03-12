/**
 * Tests the behavior of operations when interacting with a view's default collation.
 */
(function() {
    "use strict";

    function runTest(conn) {
        let viewsDB = conn.getDB("views_collation");
        assert.commandWorked(viewsDB.dropDatabase());
        assert.commandWorked(viewsDB.runCommand({create: "simpleCollection"}));
        assert.commandWorked(
            viewsDB.runCommand({create: "ukCollection", collation: {locale: "uk"}}));
        assert.commandWorked(
            viewsDB.runCommand({create: "filCollection", collation: {locale: "fil"}}));

        assert.commandWorked(viewsDB.runCommand({create: "simpleView", viewOn: "ukCollection"}));
        let listCollectionsOutput =
            viewsDB.runCommand({listCollections: 1, filter: {type: "view"}});
        assert.commandWorked(listCollectionsOutput);
        assert(!listCollectionsOutput.cursor.firstBatch[0].options.hasOwnProperty("collation"));

        assert.commandWorked(viewsDB.runCommand({aggregate: "simpleView", pipeline: []}));
        assert.commandWorked(viewsDB.runCommand({find: "simpleView"}));
        assert.commandWorked(viewsDB.runCommand({count: "simpleView"}));
        assert.commandWorked(viewsDB.runCommand({distinct: "simpleView", key: "x"}));

        assert.commandWorked(viewsDB.runCommand(
            {aggregate: "simpleView", pipeline: [], collation: {locale: "simple"}}));
        assert.commandWorked(
            viewsDB.runCommand({find: "simpleView", collation: {locale: "simple"}}));
        assert.commandWorked(
            viewsDB.runCommand({count: "simpleView", collation: {locale: "simple"}}));
        assert.commandWorked(
            viewsDB.runCommand({distinct: "simpleView", key: "x", collation: {locale: "simple"}}));

        assert.commandFailedWithCode(
            viewsDB.runCommand({aggregate: "simpleView", pipeline: [], collation: {locale: "en"}}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand({find: "simpleView", collation: {locale: "fr"}}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand({count: "simpleView", collation: {locale: "fil"}}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand({distinct: "simpleView", key: "x", collation: {locale: "es"}}),
            ErrorCodes.OptionNotSupportedOnView);

        assert.commandWorked(
            viewsDB.createView("filView", "ukCollection", [], {collation: {locale: "fil"}}));
        listCollectionsOutput = viewsDB.runCommand({listCollections: 1, filter: {name: "filView"}});
        assert.commandWorked(listCollectionsOutput);
        assert.eq(listCollectionsOutput.cursor.firstBatch[0].options.collation.locale, "fil");

        assert.commandWorked(viewsDB.runCommand({aggregate: "filView", pipeline: []}));
        assert.commandWorked(viewsDB.runCommand({find: "filView"}));
        assert.commandWorked(viewsDB.runCommand({count: "filView"}));
        assert.commandWorked(viewsDB.runCommand({distinct: "filView", key: "x"}));

        assert.commandWorked(
            viewsDB.runCommand({aggregate: "filView", pipeline: [], collation: {locale: "fil"}}));
        assert.commandWorked(viewsDB.runCommand({find: "filView", collation: {locale: "fil"}}));
        assert.commandWorked(viewsDB.runCommand({count: "filView", collation: {locale: "fil"}}));
        assert.commandWorked(
            viewsDB.runCommand({distinct: "filView", key: "x", collation: {locale: "fil"}}));

        assert.commandFailedWithCode(
            viewsDB.runCommand({aggregate: "filView", pipeline: [], collation: {locale: "en"}}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand({aggregate: "filView", pipeline: [], collation: {locale: "simple"}}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand({find: "filView", collation: {locale: "fr"}}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand({find: "filView", collation: {locale: "simple"}}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand({count: "filView", collation: {locale: "zh"}}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand({count: "filView", collation: {locale: "simple"}}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand({distinct: "filView", key: "x", collation: {locale: "es"}}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand({distinct: "filView", key: "x", collation: {locale: "simple"}}),
            ErrorCodes.OptionNotSupportedOnView);

        const lookupSimpleView = {
            $lookup: {from: "simpleView", localField: "x", foreignField: "x", as: "result"}
        };
        const graphLookupSimpleView = {
            $graphLookup: {
                from: "simpleView",
                startWith: "$_id",
                connectFromField: "_id",
                connectToField: "matchedId",
                as: "matched"
            }
        };

        assert.commandWorked(
            viewsDB.runCommand({aggregate: "simpleCollection", pipeline: [lookupSimpleView]}));
        assert.commandWorked(
            viewsDB.runCommand({aggregate: "simpleCollection", pipeline: [graphLookupSimpleView]}));

        assert.commandWorked(viewsDB.runCommand({
            aggregate: "ukCollection",
            pipeline: [lookupSimpleView],
            collation: {locale: "simple"}
        }));
        assert.commandWorked(viewsDB.runCommand({
            aggregate: "ukCollection",
            pipeline: [graphLookupSimpleView],
            collation: {locale: "simple"}
        }));

        assert.commandFailedWithCode(viewsDB.runCommand({
            aggregate: "simpleCollection",
            pipeline: [lookupSimpleView],
            collation: {locale: "en"}
        }),
                                     ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(viewsDB.runCommand({
            aggregate: "simpleCollection",
            pipeline: [graphLookupSimpleView],
            collation: {locale: "zh"}
        }),
                                     ErrorCodes.OptionNotSupportedOnView);

        const lookupFilView = {
            $lookup: {from: "filView", localField: "x", foreignField: "x", as: "result"}
        };
        const graphLookupFilView = {
            $graphLookup: {
                from: "filView",
                startWith: "$_id",
                connectFromField: "_id",
                connectToField: "matchedId",
                as: "matched"
            }
        };

        assert.commandWorked(
            viewsDB.runCommand({aggregate: "filCollection", pipeline: [lookupFilView]}));
        assert.commandWorked(
            viewsDB.runCommand({aggregate: "filCollection", pipeline: [graphLookupFilView]}));

        assert.commandWorked(viewsDB.runCommand(
            {aggregate: "ukCollection", pipeline: [lookupFilView], collation: {locale: "fil"}}));
        assert.commandWorked(viewsDB.runCommand({
            aggregate: "ukCollection",
            pipeline: [graphLookupFilView],
            collation: {locale: "fil"}
        }));

        assert.commandFailedWithCode(
            viewsDB.runCommand({aggregate: "simpleCollection", pipeline: [lookupFilView]}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand({aggregate: "simpleCollection", pipeline: [graphLookupFilView]}),
            ErrorCodes.OptionNotSupportedOnView);

        assert.commandFailedWithCode(
            viewsDB.runCommand(
                {aggregate: "filCollection", pipeline: [lookupFilView], collation: {locale: "zh"}}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(viewsDB.runCommand({
            aggregate: "filCollection",
            pipeline: [graphLookupFilView],
            collation: {locale: "zh"}
        }),
                                     ErrorCodes.OptionNotSupportedOnView);

        assert.commandWorked(viewsDB.runCommand(
            {create: "simpleView2", viewOn: "simpleCollection", collation: {locale: "simple"}}));
        assert.commandWorked(
            viewsDB.runCommand({aggregate: "simpleView2", pipeline: [lookupSimpleView]}));
        assert.commandWorked(
            viewsDB.runCommand({aggregate: "simpleView2", pipeline: [graphLookupSimpleView]}));

        const graphLookupUkCollection = {
            $graphLookup: {
                from: "ukCollection",
                startWith: "$_id",
                connectFromField: "_id",
                connectToField: "matchedId",
                as: "matched"
            }
        };
        assert.commandWorked(viewsDB.runCommand(
            {aggregate: "simpleView2", pipeline: [lookupSimpleView, graphLookupUkCollection]}));

        // You cannot perform an aggregation involving multiple views if the views don't all have
        // the same default collation.
        assert.commandFailedWithCode(
            viewsDB.runCommand({aggregate: "filView", pipeline: [lookupSimpleView]}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand({aggregate: "simpleView", pipeline: [lookupFilView]}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand(
                {aggregate: "simpleCollection", pipeline: [lookupFilView, graphLookupSimpleView]}),
            ErrorCodes.OptionNotSupportedOnView);

        // You cannot create a view that depends on another view with a different default collation.
        assert.commandFailedWithCode(
            viewsDB.runCommand({create: "zhView", viewOn: "filView", collation: {locale: "zh"}}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(viewsDB.runCommand({
            create: "zhView",
            viewOn: "simpleCollection",
            pipeline: [lookupFilView],
            collation: {locale: "zh"}
        }),
                                     ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(viewsDB.runCommand({
            create: "zhView",
            viewOn: "simpleCollection",
            pipeline: [graphLookupSimpleView],
            collation: {locale: "zh"}
        }),
                                     ErrorCodes.OptionNotSupportedOnView);

        // You cannot modify a view to depend on another view with a different default collation.
        assert.commandWorked(viewsDB.runCommand(
            {create: "esView", viewOn: "simpleCollection", collation: {locale: "es"}}));
        assert.commandFailedWithCode(viewsDB.runCommand({collMod: "esView", viewOn: "filView"}),
                                     ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand(
                {collMod: "esView", viewOn: "simpleCollection", pipeline: [lookupSimpleView]}),
            ErrorCodes.OptionNotSupportedOnView);
        assert.commandFailedWithCode(
            viewsDB.runCommand(
                {collMod: "esView", viewOn: "simpleCollection", pipeline: [graphLookupFilView]}),
            ErrorCodes.OptionNotSupportedOnView);
    }

    // Run the test on a standalone.
/*    let mongod = MongoRunner.runMongod({});
    runTest(mongod);
    MongoRunner.stopMongod(mongod);
*/
    // Run the test on a sharded cluster.
    let cluster = new ShardingTest({shards: 1, mongos: 1});
    runTest(cluster);
    cluster.stop();

}());

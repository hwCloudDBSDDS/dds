/**
 * Basic test for the 'sharding' section of the serverStatus response object for
 * both mongos and the shard.
 */

(function() {
    "use strict";

    var st = new ShardingTest({shards: 1});

    var testDB = st.s.getDB('test');
    testDB.adminCommand({enableSharding: 'test'});
    testDB.adminCommand({shardCollection: 'test.user', key: {_id: 1}});

    // Initialize shard metadata in shards
    testDB.user.insert({x: 1});

    var checkShardingServerStatus = function(st,doc, isCSRS) {
        var shardingSection = doc.sharding;
        assert.neq(shardingSection, null);

        //var configConnStr = shardingSection.configsvrConnectionString;
        var configConnStr = st.configRS.getURL();
        print("xiangyu "+configConnStr);
        var configConn = new Mongo(configConnStr);
        //var configConn = new Mongo(st.configRS.getURL());
        var configIsMaster = configConn.getDB('admin').runCommand({isMaster: 1});

        var configOpTimeObj = shardingSection.lastSeenConfigServerOpTime;

        if (isCSRS) {
            assert.gt(configConnStr.indexOf('/'), 0);
            assert.gte(configIsMaster.configsvr, 1);  // If it's a shard, this field won't exist.
            assert.neq(null, configOpTimeObj);
            assert.neq(null, configOpTimeObj.ts);
            assert.neq(null, configOpTimeObj.t);
        } else {
            assert.eq(-1, configConnStr.indexOf('/'));
            assert.gt(configConnStr.indexOf(','), 0);
            assert.eq(0, configIsMaster.configsvr);
            assert.eq(null, configOpTimeObj);
        }
    };

    var mongosServerStatus = testDB.adminCommand({serverStatus: 1});
    var isCSRS = st.configRS != null;
    checkShardingServerStatus(st,mongosServerStatus, isCSRS);

    var mongodServerStatus = st.d0.getDB('admin').runCommand({serverStatus: 1});
    checkShardingServerStatus(st,mongodServerStatus, isCSRS);

    st.stop();
})();

// Test of complex sharding initialization

function shardingTestUsingObjects() {
    var st = new ShardingTest({

        mongos: {s0: {verbose: 6}, s1: {verbose: 5}},
        config: {c0: {verbose: 4}},
        shards: {
            d0: {verbose: 3},
            rs1: {nodes: {d0: {verbose: 2}, a1: {verbose: 1}}},
            rs2: {nodes: {d0: {verbose: 4}, a1: {verbose: 5}}}
        }
    });
    var st2 = new ShardingTest({

        mongos: {s0: {verbose: 6}, s1: {verbose: 5}},
        config: {c0: {verbose: 4}},
        shards: {
            d0: {verbose: 3},
            rs1: {nodes: {d0: {verbose: 2}, a1: {verbose: 1}}},
            rs2: {nodes: {d0: {verbose: 4}, a1: {verbose: 5}}}
        }
    });
    var ss = MongoRunner.runMongod(
        {'shardsvr': "", "configdb": st.configRS.getURL(), "bind_ip": "localhost"});
    var ss2 = MongoRunner.runMongod(
        {'shardsvr': "", "configdb": st2.configRS.getURL(), "bind_ip": "localhost"});
    var ss3 = MongoRunner.runMongos({"configdb": st.configRS.getURL(), "bind_ip": "localhost"});
    var ss4 = MongoRunner.runMongos({"configdb": st2.configRS.getURL(), "bind_ip": "localhost"});

    var s0 = st.s0;
    assert.eq(s0, st._mongos[0]);

    var s1 = st.s1;
    assert.eq(s1, st._mongos[1]);

    var d0 = st.d0;
    assert.eq(d0, st._connections[0]);

    assert(s0.commandLine.hasOwnProperty("vvvvvv"));
    assert(s1.commandLine.hasOwnProperty("vvvvv"));
    assert(d0.commandLine.hasOwnProperty("vvv"));

    var db = st.getDB("test");
    for (i = 0; i < 100; i++) {
        db.bar.save({n: 1});
    }

    st.stop();
    st2.stop();
}
shardingTestUsingObjects();

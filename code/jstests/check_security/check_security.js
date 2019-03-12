//
// Test the security info in LOG
//

/*--------------- Prepared Sharding Cluster -----------------*/
var st = new ShardingTest({shards: 2, mongos: 2});
var mongos = st.s0;

// TODO: add more case about user operation

printjson("------------ user operation start -----------");

var myDbCreated = mongos.getDB("MyDbCreated");
var myCollCreated = myDbCreated.getCollection("MyCollCreated");
for(var i=1;i<10000;i++){myCollCreated.insert({"MyItemRandom":"MyValue"+i});}
myCollCreated.insert({"MyItem":"MyValue"});

printjson("------------ user operation end -----------");

/*--------------- finish -----------------*/
st.stop();

/* TODO: Execute Shell in JS [ Can not execute now ]
printjson("------------ callfile.execFile start -----------");
function callback(err, stdout, stderr)
{
    printjson("------------ callback start -----------");
    printjson(err);
    printjson(stdout);
    printjson(stderr);
    printjson("------------ callback end -----------");
    return 1;
};
var callfile = require('child_process');
callfile.execFile('check_security.sh',null,null,function (err, stdout, stderr) {
    callback(err, stdout, stderr);
});
printjson("------------ callfile.execFile end -----------");
*/
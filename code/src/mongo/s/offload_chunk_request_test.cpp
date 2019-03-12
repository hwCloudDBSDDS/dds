#include "mongo/platform/basic.h"

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/s/offload_chunk_request.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

using unittest::assertGet;

namespace {

TEST(OffloadChunkRequest, Roundtrip) {
    const ChunkVersion chunkVersion(2, 3, OID::gen());
    NamespaceString nss("TestDB", "TestColl");

    ChunkType chunk;
    chunk.setNS(nss.ns());
    chunk.setName("0001");
    chunk.setMin(BSON("Key" << -100));
    chunk.setMax(BSON("Key" << 100));
    chunk.setVersion(chunkVersion);
    chunk.setShard(ShardId("shard0001"));
    chunk.setRootFolder("plogcnt1,123");

    BSONObjBuilder builder;
    OffloadChunkRequest::appendAsCommand(&builder, chunk);

    BSONObj cmdObj = builder.obj();

    auto request = assertGet(OffloadChunkRequest::createFromCommand(cmdObj));
    request.toString();

    ASSERT_BSONOBJ_EQ(BSON("Key" << -100), request.getMinKey());
    ASSERT_BSONOBJ_EQ(BSON("Key" << 100), request.getMaxKey());
}

}  // namespace
}  // namespace mongo

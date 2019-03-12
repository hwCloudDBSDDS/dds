#include "mongo/platform/basic.h"

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/s/split_chunk_request.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

using unittest::assertGet;

namespace {

TEST(SplitChunkReq, Roundtrip) {
    const ChunkVersion collectionVersion(3, 1, OID::gen());
    const ChunkVersion chunkVersion(2, 3, OID::gen());
    NamespaceString nss("TestDB", "TestColl");

    ChunkType chunk;
    chunk.setNS(nss.ns());
    chunk.setName("0001");
    chunk.setMin(BSON("Key" << -100));
    chunk.setMax(BSON("Key" << 100));
    chunk.setVersion(chunkVersion);
    chunk.setShard(ShardId("shard0001"));
    chunk.setName("1");
    chunk.setRootFolder("plogcnt1,123");

    CollectionType coll;
    coll.setNs(NamespaceString{nss.ns()});
    coll.setEpoch(collectionVersion.epoch());
    coll.setUpdatedAt(Date_t::fromMillisSinceEpoch(collectionVersion.toLong()));
    coll.setUnique(false);
    coll.setKeyPattern(BSON("a" << 1));

    BSONObj splitPoint = BSON("splitPoint"
                              << "x");
    std::string rightDBPath = "/test/point";
    std::string rightChunkID = "22";

    BSONObjBuilder builder;
    SplitChunkReq::appendAsCommand(&builder, chunk, coll, rightDBPath, rightChunkID, splitPoint);

    BSONObj cmdObj = builder.obj();
    auto request = assertGet(SplitChunkReq::createFromCommand(cmdObj));

    ASSERT_EQ("TestDB.TestColl", request.getNss().ns());
    ASSERT_BSONOBJ_EQ(BSON("Key" << -100), request.getMinKey());
    ASSERT_BSONOBJ_EQ(BSON("Key" << 100), request.getMaxKey());
    ASSERT_EQ(splitPoint.toString(), request.getSplitPoint().toString());
    ASSERT_EQ(rightDBPath, request.getRightDBPath());
    ASSERT_EQ(rightChunkID, request.getRightChunkName());

    BSONObjBuilder builder2;
    SplitChunkReq::appendAsCommand(&builder2, chunk, coll, rightDBPath, rightChunkID);

    cmdObj = builder2.obj();

    request = assertGet(SplitChunkReq::createFromCommand(cmdObj));

    ASSERT_EQ("TestDB.TestColl", request.getNss().ns());
    ASSERT_BSONOBJ_EQ(BSON("Key" << -100), request.getMinKey());
    ASSERT_BSONOBJ_EQ(BSON("Key" << 100), request.getMaxKey());
    ASSERT_TRUE(request.getSplitPoint().isEmpty());
    ASSERT_EQ(rightDBPath, request.getRightDBPath());
    ASSERT_EQ(rightChunkID, request.getRightChunkName());
}
}  // namespace
}  // namespace mongo

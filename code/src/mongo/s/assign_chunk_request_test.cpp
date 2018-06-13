#include "mongo/platform/basic.h"

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/s/assign_chunk_request.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

using unittest::assertGet;

namespace {

TEST(AssignChunkRequest, Roundtrip) {
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

    BSONObjBuilder builder;
    AssignChunkRequest::appendAsCommand(&builder, chunk, coll, true);

    BSONObj cmdObj = builder.obj();

    auto request = assertGet(AssignChunkRequest::createFromCommand(cmdObj));

    ASSERT_EQ("TestDB.TestColl", request.getNss().ns());
    ASSERT_BSONOBJ_EQ(BSON("Key" << -100), request.getMinKey());
    ASSERT_BSONOBJ_EQ(BSON("Key" << 100), request.getMaxKey());
}

}  // namespace
}  // namespace mongo



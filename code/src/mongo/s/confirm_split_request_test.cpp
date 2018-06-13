#include "mongo/platform/basic.h"

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/s/confirm_split_request.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

    using unittest::assertGet;
    namespace {

        TEST(ConfirmSplitRequest, Roundtrip) {
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

            bool splitSuc = true;

            BSONObjBuilder builder;
            mongo::ConfirmSplitRequest::appendAsCommand(&builder, chunk, coll, splitSuc);

            BSONObj cmdObj = builder.obj();
            auto request = assertGet(mongo::ConfirmSplitRequest::createFromCommand(cmdObj));

            ASSERT_EQ("TestDB.TestColl", request.getNss().ns());
            ASSERT_EQ(splitSuc, request.splitSuccess());
            ASSERT_EQ(chunkVersion.toString(), request.getChunkVersion().toString());

            splitSuc = false;
            BSONObjBuilder builder2;
            ConfirmSplitRequest::appendAsCommand(&builder2, chunk, coll, splitSuc);

            cmdObj = builder2.obj();
            request = assertGet(ConfirmSplitRequest::createFromCommand(cmdObj));

            ASSERT_EQ("TestDB.TestColl", request.getNss().ns());
            ASSERT_EQ(splitSuc, request.splitSuccess());
            ASSERT_EQ(chunkVersion.toString(), request.getChunkVersion().toString());
        }
    }  // namespace
}  // namespace mongo



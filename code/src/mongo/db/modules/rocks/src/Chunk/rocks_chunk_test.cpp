
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include <mongo/util/log.h>
#include "mongo/unittest/unittest.h"
#include "ChunkMetadata.h"
#include <mongo/bson/json.h>
#include "../GlobalConfig.h"
#include "ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage.h"
#include "../rocks_engine.h"
#include <mongo/db/commands.h>
#include <mongo/db/s/operation_sharding_state.h>
#include <mongo/db/s/sharded_connection_info.h>


#ifdef ASSERT_OK
#undef ASSERT_OK
#endif
#define ASSERT_OK(expr)   ASSERT_TRUE(ResultHandling::IsOK(expr))



///////////////////////////////////////////////////////////////////////////////////////////////////
//
//  In this section we define some functions from libraries which we don't want to link for some reasons.
//  Dump approach but it saves time. Need to fix it someday.
//
extern "C"{
uint32_t IndexCRC32(uint32_t crc, char *buffer, uint64_t size)
{
    return 0;
}

}


#include "base/index_lock.h"

void IndexRwlock::WriteLock() { }
void IndexRwlock::WriteUnlock() { }
void IndexRwlock::ReadLock() { }
void IndexRwlock::ReadUnlock() { }

void ManualFlushLog() { }
void IndexCRC32() { }

void IndexLogEntry(
         const uint64_t     module,
         const char*        chunk_id,
         const uint32_t     log_level,
         const char*        log_file_name,
         const char*        log_func_name,
         const uint32_t     log_line,
         const char*        pszFormat, ...)
{ }

IndexThreadLocal *g_pindex_thread_local = NULL;
void* IndexThreadLocal::GetKey(eThreadLocalKey eKey, int *pvalue_size)
{
    return nullptr;
}

void IndexThreadLocal::SetKey(eThreadLocalKey eKey, void *pvalue, int size)
{
}

void BaseModuleDestroy() { }


namespace rocksdb
{
std::shared_ptr<Cache> NewLRUCache(size_t capacity, int num_shard_bits,
                                   bool strict_capacity_limit,
                                   double high_pri_pool_ratio)
{
    return nullptr;
}

}

namespace mongo
{

bool RocksEngine::initRsOplogBackgroundThread(StringData ns)
{
    return false;
}

void Command::registerError(OperationContext* txn, const DBException& exception) {}

void usingAShardConnection(const std::string& addr) {}

OperationShardingState operationShardingState;
OperationShardingState& OperationShardingState::get(OperationContext* txn) {
    return operationShardingState;
}

OperationShardingState::OperationShardingState()  { }
bool OperationShardingState::hasShardVersion() const { return false; }

ChunkVersion OperationShardingState::getShardVersion(const NamespaceString& nss) const { return ChunkVersion::UNSHARDED(); }

void OperationShardingState::setShardVersion(NamespaceString nss, ChunkVersion newVersion) { }


ShardedConnectionInfo* ShardedConnectionInfo::get(Client* client, bool create) { return nullptr; }
ChunkVersion ShardedConnectionInfo::getVersion(const std::string& ns) const { return ChunkVersion::UNSHARDED(); }

bool isMongos() { return false; }


void assembleResponse(OperationContext* txn, Message& m, DbResponse& dbresponse, const HostAndPort& remote)
{}
///////////////////////////////////////////////////////////////////////////////////////////////////



/*
///////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Tests for PartitionedRocksEngine-related staff.
//
TEST(RocksRecordChunkTest, Metadata)
{
    std::string assignmentCommand = 
        "{" //  1:1
        "metadataVersion: 1,"   // 19:20
        "indexedRecordStorageId: 1,"    //  26:46
        "collection: "  //  12:58
        "{" //  1:59
        "    _id: 'mydb.coll1', lastmodEpoch: ObjectId('594cb211ca101a9e55e067b0'), lastmod: new Date(1), dropped: false, prefix: 3, key: { a: 1 }, unique: false," //  153:212
        "    index: [ { v: 2, name: '_id_', ns: 'mydb.coll1', key: { _id: 1 }, prefix: 2 }, { ns: 'mydb.coll1', v: 2, key: { uid: 1.0 }, name: 'uid_1', prefix: 3 } ]"  //  156:368
        "}, "   //  3:371
        "chunk: "   //  7:378
        "{" //  1:379
        "    _id: 'mydb.coll1-uid_\"A\"', ns: 'mydb.coll1', min: { uid: 'A' }, max: { uid: 'B' }, shard: 'shard00000000', lastmod: 0,"
        "    lastmodEpoch: ObjectId('594b996b77d7de05cce31839'), state: 1, rootFolder: 'plogcnt1,18446744071691134375-0,' }"
        "}"
        "}";

    mongo::BSONObj obj = fromjson(assignmentCommand);
    std::unique_ptr<mongo::ChunkMetadata> metadata;
    ASSERT_TRUE(mongo::ChunkMetadata::Parse(obj, metadata).ok());

    mongo::BSONObj bson = metadata->ToBSON();

    rocksdb::Slice slice(bson.objdata(), bson.objsize());
    std::unique_ptr<mongo::ChunkMetadata> metadata1;
    ASSERT_TRUE(mongo::ChunkMetadata::Parse(slice, metadata1).ok());

    ASSERT_TRUE(metadata1->GetIndexedRecordStorageId() == IndexedRecordStorageId::ShardKeyPrefixedDataWithStandardIndexColumnFamily);
    //ASSERT_TRUE(metadata1->GetCollection().getPrefix() == 3);
    ASSERT_EQUALS(metadata1->GetChunk().getName(), "mydb.coll1-uid_\"A\"");

    const IndexDefinition* index = metadata1->GetIndex(3);
    ASSERT_TRUE(index != nullptr);
    ASSERT_TRUE(index->GetIndexId() == 3);
    ASSERT_TRUE(SimpleBSONObjComparator::kInstance.evaluate(index->GetKeyPattern() == fromjson("{ uid: 1.0 }")));
}
///////////////////////////////////////////////////////////////////////////////////////////////////
*/


///////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Tests for GlobalConfig
//
class GlobalConfigTest
{
public:
    static void MainTest()
    {
        std::string config = 
            "{"
            "UseMultiRocksDBInstanceEngine: true,"
            "ConfigSettingsVersion:111"
            "}";

        GlobalConfig::Load(config);

        ASSERT_TRUE(GLOBAL_CONFIG_GET(ConfigSettingsVersion) == 111);
        ASSERT_TRUE(GLOBAL_CONFIG_GET(UseMultiRocksDBInstanceEngine) == true);
    }
};

TEST(GlobalConfigTestClass, GlobalConfigMainTest)
{
    GlobalConfigTest::MainTest();
}
///////////////////////////////////////////////////////////////////////////////////////////////////



///////////////////////////////////////////////////////////////////////////////////////////////////
//
//
//
class ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorageTest
{
    static constexpr const char* const ChunkMetadataText = 
    "{" //  1:1
    "metadataVersion: 1,"   //  19:20
    "indexedRecordStorageId: 1,"    //  26:46
    "dataSize: 3,"
    "numRecords: 4,"
    "chunk:"
    "{"
    "	_id: 'TestDB.TestCol-bucket_MinKeyobject_MinKey',"
    "	ns: 'TestDB.TestCol',"
    "	min: { bucket: 'AMinKey', object: 'AMinKey' },"
    "	max: { bucket: 'BMaxKey', object: 'BMaxKey' },"
    "	shard: 'shard00000000000000000001',"
    "	lastmod: new Date(1),"
    "	lastmodEpoch: ObjectId('59a60b59080b36c033ab675b'),"
    "	state: 0,"
    "   status: 1,"
    "	rootFolder: 'plogcnt4,4758145404648494219-0,18446744073463842483-0,16464705921138542587-0,5122289316238896394-0',"
    "   processIdentity: 'test'"
    "},"
    "collection:"
    "{"
    "	_id: 'TestDB.TestCol$TestDB.TestCol-bucket_MinKeyobject_MinKey',"
    "	lastmodEpoch: ObjectId('59a60b59080b36c033ab675b'),"
    "	lastmod: new Date(4294967296),"
    "	dropped: false,"
    "	prefix: 1,"
    "	key: { bucket: 1.0, object: 1.0 },"
    "	unique: false,"
    "	indexes:"
    "	["
    "		{ v: 2, name: '_id_', ns: 'TestDB.TestCol', key: { _id: 1 }, prefix: 2 },"
    "		{ ns: 'TestDB.TestCol', v: 2, key: { bucket: 1.0, object: 1.0, _id: 1.0 }, name: 'bucket_1_object_1__id_1', prefix: 3 }"
    "	]"
    "}"
    "}";

    static constexpr const char* const ChunkMetadataText1 = 
    "{"
    "metadataVersion: 1,"
    "indexedRecordStorageId: 1,"
    "chunk:"
    "{"
    "   _id: \"test1.splitTest2-_id_MinKey\", ns: \"test1.splitTest2\", min: { _id: 'AMinKey'  }, max: { _id: 'BMaxKey' }, "
    "   shard: \"shard01\", lastmod: new Date(1), lastmodEpoch: ObjectId('59c22ec6014763d2c9efdc28'), state: 0,"
    "   rootFolder: \"plogcnt4,18446744072050574944-0,18446744072831205376-0,4985351734516790617-0,18446744072530172253-0\","
    "   status: 1,"
    "   processIdentity: 'test'"
    "},"
    "collection: { _id: \"test1.splitTest2$test1.splitTest2-_id_MinKey\","
    "lastmodEpoch: ObjectId('59c22ec6014763d2c9efdc28'), lastmod: new Date(1), dropped: false, prefix: 1, key: { _id: 1.0 }, unique: false,"
    "indexes: [ { v: 2, name: \"_id_\", ns: \"test1.splitTest2\", key: { _id: 1 }, prefix: 2 } ] }"    
    "}";

public:
/*
#define KEY_S "\000\000\000\001<user1000001338446751948\000\004"
#define VALUE_S "\036\000<user1000001338446751948\000\004\000\000\000@"

    static void MainTest()
    {
        std::string key(KEY_S, sizeof(KEY_S));
        std::string value(VALUE_S, sizeof(VALUE_S));

        SecondaryKeyFormatter reader;
        ASSERT_OK(reader.DecodeHeader(key, value));

        rocksdb::Slice shardKey;
        ASSERT_OK(reader.GetShardKey(shardKey));

        ASSERT_TRUE(!shardKey.empty());        
    }
*/

    static void CheckUniqueIndexFormatter(
        const std::unique_ptr<IIndexRecordFormatter>& formatter,
        const RecordId& recordId,
        const char* keyJson        
        )
    {
        std::pair<std::string, std::string> recordKeyValuePair;
        mongo::BSONObj keyObj = stripFieldNames(fromjson(keyJson));
        ASSERT_OK(formatter->GenerateRecord(keyObj, recordId, recordKeyValuePair));
        recordKeyValuePair.second = std::string("12", 2) + recordKeyValuePair.second;

        RecordId recordId2;
        mongo::BSONObj keyObj2;
        ASSERT_OK(formatter->ParseRecord(recordKeyValuePair, &keyObj2, &recordId2));
        ASSERT_TRUE(recordId == recordId2);
        ASSERT_TRUE(SimpleBSONObjComparator::kInstance.evaluate(keyObj == keyObj2));

        //  Check shard key
        SecondaryKeyFormatter reader;
        invariantRocksOK(reader.DecodeHeader(recordKeyValuePair.first, recordKeyValuePair.second));

        rocksdb::Slice shardKey;
        invariantRocksOK(reader.GetShardKey(shardKey));

        invariant(!shardKey.empty());
    }

    static void CrashTest(const std::unique_ptr<IIndexRecordFormatter>& formatter)
    {
        uint8_t indexValue[] = { 18, 0, 100, 89, 191, 44 , 191,  248, 157,  197, 244, 90, 186, 255, 126, 4, 0, 0, 0, 81 };
        uint8_t indexKey[] = { 0, 0,  0,  1, 100,  89,  191,  44, 191,  248 , 157,  197 , 244 , 90,  186 , 255 , 126 , 4 };

        std::string key((char*)indexKey, sizeof(indexKey)); 
        std::string value((char*)indexValue, sizeof(indexValue));

        auto recordKeyValuePair = std::make_pair(key, value);
        RecordId recordId2;
        mongo::BSONObj keyObj2;
        ASSERT_OK(formatter->ParseRecord(recordKeyValuePair, &keyObj2, &recordId2));

        SecondaryKeyFormatter reader;
        invariantRocksOK(reader.DecodeHeader(key, value));

        rocksdb::Slice shardKey;
        invariantRocksOK(reader.GetShardKey(shardKey));

        ASSERT_TRUE(!shardKey.empty());
    }

    static void TestShardKey(
        ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage& storage,
        const char* shardKeys,
        const char* otherKeys
        )
    {
        std::string document = std::string("{") + shardKeys + "," + otherKeys + "}";
        std::string shardKey = std::string("{") + shardKeys + "}";

        mongo::BSONObj obj = fromjson(document);

        RecordId recordId;
        ASSERT_OK(storage.GetRecordId(obj, recordId));
        BSONObj shardKeyBSON = storage.GetShardKeyBSON(recordId);
        BSONObj shardKeyBSONExpect = fromjson(shardKey);
        std::cout << "TestShardKey expected=" << shardKeyBSONExpect.toString() << " actual=" << shardKeyBSON.toString() << std::endl;
        ASSERT_TRUE(SimpleBSONObjComparator::kInstance.evaluate(shardKeyBSON == shardKeyBSONExpect));
    }

    static void MainTest()
    {
        mongo::BSONObj obj = fromjson(ChunkMetadataText);
        std::unique_ptr<mongo::ChunkMetadata> metadata;
        ASSERT_OK(mongo::ChunkMetadata::Parse(obj, metadata));
        ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage storage(*metadata.get());

        static const char* documentText = "{ bucket: 'TestBucket', object: 'TestObject', _id: 'TestId' }";
        obj = fromjson(documentText);

        RecordId recordId;
        ASSERT_OK(storage.GetRecordId(obj, recordId));

        //  Test unique index
        {
            const IndexDefinition* idIndex = metadata->GetIndex(2);
            ASSERT_TRUE(idIndex->IsUnique() && idIndex->IsId());

            auto indexFormatter = storage.GetSecondaryIndexFormatter(*idIndex);

            //CrashTest(indexFormatter);

            CheckUniqueIndexFormatter(indexFormatter, recordId, "{ _id: 'TestId' }" );
            CheckUniqueIndexFormatter(indexFormatter, recordId, "{ _id: 'TestIdTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT' }" );
        }

        //  Test standard index
        {
            const IndexDefinition* index = metadata->GetIndex(3);
            ASSERT_TRUE(!index->IsUnique() && !index->IsId());

            auto indexFormatter = storage.GetSecondaryIndexFormatter(*index);

            std::pair<std::string, std::string> recordKeyValuePair;
            mongo::BSONObj keyObj = stripFieldNames(fromjson(documentText));
            ASSERT_OK(indexFormatter->GenerateRecord(keyObj, recordId, recordKeyValuePair));

            RecordId recordId2;
            mongo::BSONObj keyObj2;
            ASSERT_OK(indexFormatter->ParseRecord(recordKeyValuePair, &keyObj2, &recordId2));
            ASSERT_TRUE(recordId == recordId2);
            ASSERT_TRUE(SimpleBSONObjComparator::kInstance.evaluate(keyObj == keyObj2));

            //  Test ShardKeyExtractor
            rocksdb::Slice primaryKey(recordId2.repr().Data(), recordId2.repr().Size());
            ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage::ShardKeyFromPrimaryKeyExtractor extractor;
            ASSERT_TRUE(extractor.InDomain(primaryKey));            
            rocksdb::Slice shardKey = extractor.Transform(primaryKey);
            ASSERT_TRUE(!shardKey.empty());
            
            //  Test Shard Key
            BSONObj shardKeyBSON = storage.GetShardKeyBSON(recordId2);
            //BSONObj shardKeyBSONExpect = stripFieldNames(fromjson("{ bucket: 'TestBucket', object: 'TestObject' }"));
            BSONObj shardKeyBSONExpect = fromjson("{ bucket: 'TestBucket', object: 'TestObject' }");
            ASSERT_TRUE(SimpleBSONObjComparator::kInstance.evaluate(shardKeyBSON == shardKeyBSONExpect));
        }

        //  Shard key test
        TestShardKey(storage, "bucket: 'TestBucket', object: 1.1", "_id: 'TestId'");
        TestShardKey(storage, "bucket: 999999999999999999, object: ObjectId('59bf2cbff89dc5f45abaff7e')", "_id: 'TestId'");
    }

    static void TestId()
    {
        mongo::BSONObj obj = fromjson(ChunkMetadataText1);
        std::unique_ptr<mongo::ChunkMetadata> metadata;
        ASSERT_OK(mongo::ChunkMetadata::Parse(obj, metadata));
        ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorage storage(*metadata.get());

        static const char* documentText = "{ 'uid':ObjectId('59bf2cbff89dc5f45abaff7e'), '_id':ObjectId('59bf2cbff89dc5f45abaff7a') }";
        obj = fromjson(documentText);

        RecordId recordId;
        ASSERT_OK(storage.GetRecordId(obj, recordId));

        //  Test unique index
        {
            const IndexDefinition* idIndex = metadata->GetIndex(2);
            ASSERT_TRUE(idIndex->IsUnique() && idIndex->IsId());

            auto indexFormatter = storage.GetSecondaryIndexFormatter(*idIndex);

            CheckUniqueIndexFormatter(indexFormatter, recordId, documentText);
        }
    }
};

TEST(PartitionedRocksEngineTestClass, Indexer)
{
    ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorageTest::MainTest();
    ShardKeyPrefixedDataWithStandardIndexColumnFamilyStorageTest::TestId();
}
///////////////////////////////////////////////////////////////////////////////////////////////////

}

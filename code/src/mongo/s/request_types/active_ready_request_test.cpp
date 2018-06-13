#include "mongo/s/request_types/active_ready_request_type.h"
#include "mongo/db/jsobj.h"
#include "mongo/unittest/unittest.h"

namespace mongo{
namespace {

using unittest::assertGet;
const char kConfigsvrRegShardSvr[] = "_configsvrActiveReady";
const char kHostAndPort[] = "hostAndPort";
const char kShardName[] = "shardName";


TEST(CfgActiveReadyRequest, BasicValidConfigCommand) {

	auto request = assertGet(ActiveReadyRequest::parseFromConfigCommand(
		BSON(kConfigsvrRegShardSvr
			 << "TestActiveReady.TestConfigCommand" 
			 << kHostAndPort 
			 << ConnectionString(HostAndPort("test0:1234")).toString() 
			 << kShardName 
			 << "shard0")));

	ASSERT_EQ(NamespaceString("TestActiveReady.TestConfigCommand"), request.getNamespace());
	ASSERT_EQ(
		assertGet(ConnectionString::parse("test0:1234")).toString(),
		request.getConnString().toString()
		);
	ASSERT_EQ("shard0", request.getName());

	request = assertGet(ActiveReadyRequest::parseFromConfigCommand(
		BSON(kConfigsvrRegShardSvr
			 << "TestActiveReady.TestConfigCommand" 
			 << kHostAndPort
			 << ConnectionString(HostAndPort("test0:1234")).toString() 
			 << kShardName 
			 << "shard0")));

	ASSERT_EQ(NamespaceString("TestActiveReady.TestConfigCommand"), request.getNamespace());
	ASSERT_EQ(
		assertGet(ConnectionString::parse("test0:1234")).toString(),
		request.getConnString().toString()
		);
	ASSERT_EQ("shard0", request.getName()); 
}

TEST(CfgActiveReadyRequest, ConfigCommandToBSON) {
	BSONObj serializedRequest =
		BSON(kConfigsvrRegShardSvr
			<< "TestActiveReady.TestConfigCommand" 
			<< kHostAndPort
			<< ConnectionString(HostAndPort("test0:1234")).toString() 
			<< kShardName
			<< "shard0");
			
	BSONObj writeConcernObj = BSON("writeConcern" << BSON("w"
                                                          << "majority"));

    BSONObjBuilder cmdBuilder;
    {
        cmdBuilder.appendElements(serializedRequest);
        cmdBuilder.appendElements(writeConcernObj);
    }

    auto request = assertGet(ActiveReadyRequest::parseFromConfigCommand(serializedRequest));
    auto requestToBSON = request.toConfigCommandBSON(writeConcernObj);

    ASSERT_BSONOBJ_EQ(cmdBuilder.obj(), requestToBSON);
}

TEST(CfgActiveReadyRequest, MissingNameSpaceErrors) {
	auto request = ActiveReadyRequest::parseFromConfigCommand(
        BSON(kHostAndPort
			 << ConnectionString(HostAndPort("test0:1234")).toString() 
			 << kShardName
			 << "shard0"));
    ASSERT_EQ(ErrorCodes::NoSuchKey, request.getStatus());
}

TEST(CfgActiveReadyRequest, MissingConnectionErrors) {
	auto request = ActiveReadyRequest::parseFromConfigCommand(
        BSON(kConfigsvrRegShardSvr 
			 << "TestActiveReady.TestConfigCommand" 
			 << kShardName 
			 << "shard0"));
    ASSERT_EQ(ErrorCodes::NoSuchKey, request.getStatus());
}


TEST(CfgActiveReadyRequest, MissingShardNameErrors) {
	auto request = ActiveReadyRequest::parseFromConfigCommand(
		BSON(kConfigsvrRegShardSvr
			 << "TestActiveReady.TestConfigCommand"
			 << kHostAndPort
			 << ConnectionString(HostAndPort("test0:1234")).toString()));
	ASSERT_EQ(ErrorCodes::NoSuchKey, request.getStatus());
}

TEST(CfgActiveReadyRequest, WrongNameSpaceTypeErrors) {
	auto request = ActiveReadyRequest::parseFromConfigCommand(
		BSON(kConfigsvrRegShardSvr
			 << 1234
			 << kHostAndPort
			 << ConnectionString(HostAndPort("test0:1234")).toString() 
			 << kShardName
			 << "shard0"));
	ASSERT_EQ(ErrorCodes::TypeMismatch, request.getStatus());
}

TEST(CfgActiveReadyRequest, WrongConnectionTypeErrors) {
	auto request = ActiveReadyRequest::parseFromConfigCommand(
		BSON(kConfigsvrRegShardSvr
			 << "TestActiveReady.TestConfigCommand"
			 << kHostAndPort
			 << 1234
			 << kShardName
			 << "shard0"));
	ASSERT_EQ(ErrorCodes::TypeMismatch, request.getStatus());
}


TEST(CfgActiveReadyRequest, WrongShardNameTypeErrors) {
	auto request = ActiveReadyRequest::parseFromConfigCommand(
		BSON(kConfigsvrRegShardSvr
			 << "TestActiveReady.TestConfigCommand"
			 << kHostAndPort
			 << ConnectionString(HostAndPort("test0:1234")).toString() 
			 << kShardName
			 << 1234));
	ASSERT_EQ(ErrorCodes::TypeMismatch, request.getStatus());
}


	
}  // namespace
}  // namespace mongo
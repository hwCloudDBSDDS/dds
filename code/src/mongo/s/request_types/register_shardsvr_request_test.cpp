#include "mongo/s/request_types/register_shardsvr_request_type.h"
#include "mongo/db/jsobj.h"
#include "mongo/unittest/unittest.h"

namespace mongo{
namespace{

using unittest::assertGet;

const char kConfigsvrRegShardSvr[] = "_configsvrRegShardSvr";
const char kHostAndPort[] = "hostAndPort";

TEST(CfgRegisterShardsvrRequest, BasicValidConfigCommand) {
	auto request = assertGet(RegShardSvrRequest::parseFromConfigCommand(
		BSON(kConfigsvrRegShardSvr
			 << "TestRegisterShardSvr.TestConfigCommand"
			 << kHostAndPort
			 << ConnectionString(HostAndPort("test0:1234")).toString() )));

	ASSERT_EQ(NamespaceString("TestRegisterShardSvr", "TestConfigCommand"), request.getNamespace());
	ASSERT_EQ(
		assertGet(ConnectionString::parse("test0:1234")).toString(),
		request.getConnString().toString()
		);
}

TEST(CfgRegisterShardsvrRequest, ConfigCommandToBSON) {
	BSONObj serializedRequest = 
		BSON(kConfigsvrRegShardSvr
			 << "TestRegisterShardSvr.TestConfigCommand"
			 << kHostAndPort
			 << ConnectionString(HostAndPort("test0:1234")).toString());
	
	BSONObj writeConcernObj = BSON("writeConcernObj" << BSON("w" 
															  << "majority"));

	BSONObjBuilder cmdBuilder;
	{
		cmdBuilder.appendElements(serializedRequest);
		cmdBuilder.appendElements(writeConcernObj);
	}

	auto request = assertGet(RegShardSvrRequest::parseFromConfigCommand(serializedRequest));
	auto requestToBSON = request.toConfigCommandBSON(writeConcernObj);

	ASSERT_BSONOBJ_EQ(cmdBuilder.obj(), requestToBSON);
}

TEST(CfgRegisterShardsvrRequest, MissingNameSpaceErrors) {
	auto request = RegShardSvrRequest::parseFromConfigCommand(
		BSON(kHostAndPort
			 << ConnectionString(HostAndPort("test0:1234")).toString() ));

	ASSERT_EQ(ErrorCodes::NoSuchKey, request.getStatus());
}

TEST(CfgRegisterShardsvrRequest, MissingConnectionErrors) {
	auto request = RegShardSvrRequest::parseFromConfigCommand(
		BSON(kConfigsvrRegShardSvr
			 << "TestRegisterShardSvr.TestConfigCommand"));

	ASSERT_EQ(ErrorCodes::NoSuchKey, request.getStatus());
}

TEST(CfgRegisterShardsvrRequest, WrongNameSpaceTypeErrors) {
	auto request = RegShardSvrRequest::parseFromConfigCommand(
		BSON(kConfigsvrRegShardSvr
			 << 1234
			 << kHostAndPort
			 << ConnectionString(HostAndPort("test0:1234")).toString() ));

	ASSERT_EQ(ErrorCodes::TypeMismatch, request.getStatus());
}

TEST(CfgRegisterShardsvrRequest, WrongConnectionTypeErrors) {
	auto request = RegShardSvrRequest::parseFromConfigCommand(
		BSON(kConfigsvrRegShardSvr
			 << "TestRegisterShardSvr.TestConfigCommand"
			 << kHostAndPort
			 << 1234));

	ASSERT_EQ(ErrorCodes::TypeMismatch, request.getStatus());
}

}//namespace
} //namespace mongo
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding
#include "mongo/s/client/shard.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/client/connection_string.h"
#include "mongo/client/remote_command_targeter_factory_mock.h"
#include "mongo/client/remote_command_targeter_mock.h"
#include "mongo/db/commands.h"
#include "mongo/db/query/query_request.h"
#include "mongo/db/repl/member_state.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/s/type_shard_identity.h"
#include "mongo/platform/basic.h"
#include "mongo/s/catalog/catalog_extend/sharding_catalog_shard_server_manager.h"
#include "mongo/s/catalog/config_server_version.h"
#include "mongo/s/catalog/sharding_catalog_manager.h"
#include "mongo/s/catalog/type_changelog.h"
#include "mongo/s/catalog/type_config_version.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/catalog/type_shard_server.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/cluster_identity_loader.h"
#include "mongo/s/config_server_test_fixture.h"
#include "mongo/s/write_ops/batched_command_request.h"
#include "mongo/s/write_ops/batched_command_response.h"
#include "mongo/s/write_ops/batched_insert_request.h"
#include "mongo/s/write_ops/batched_update_document.h"
#include "mongo/s/write_ops/batched_update_request.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"

#include <iomanip>
#include <list>
#include <string.h>
#include <string>
#include <unordered_map>
#include <vector>

namespace mongo {
namespace {

using unittest::assertGet;
using executor::RemoteCommandRequest;
using executor::RemoteCommandResponse;
using std::vector;

const std::string kAddShardServer = "addShardServer";
const std::string kShardServerToBeActive = "shardServerToBeActive";
const std::string kShardServerActive = "shardServerActive";
const std::string kRemoveShardServer = "removeShardServer";
const int kShardNameDigitWidth(4);

class ShardServerManagerTest : public ConfigServerTestFixture {
protected:
    /**
     * Performs the test setup steps from the parent class and then configures the config shard and
     * the client name.
    */
    void setUp() override {
        ConfigServerTestFixture::setUp();

        // Make sure clusterID is written to the config.version collection.
        ASSERT_OK(catalogManager()->initializeConfigDatabaseIfNeeded(operationContext()));
    }


    void assertShardExists(const ShardType& expectedShard) {
        auto foundShard = assertGet(getShardDoc(operationContext(), expectedShard.getName()));

        ASSERT_EQUALS(expectedShard.getName(), foundShard.getName());
        ASSERT_EQUALS(expectedShard.getHost(), foundShard.getHost());
        ASSERT_EQUALS(expectedShard.getMaxSizeMB(), foundShard.getMaxSizeMB());
        ASSERT_EQUALS(expectedShard.getDraining(), foundShard.getDraining());
        ASSERT_EQUALS((int)expectedShard.getState(), (int)foundShard.getState());
        ASSERT_TRUE(foundShard.getTags().empty());
    }

    int getElementNumInNodeMap(
        const std::unordered_map<std::string, std::list<ShardServerManager::ShardInfo>>& nodeMap) {
        int count{};
        for (auto itMap = nodeMap.begin(); itMap != nodeMap.end(); ++itMap) {
            for (auto itList = itMap->second.begin(); itList != itMap->second.end(); ++itList) {
                ++count;
            }
        }
        return count;
    }

    // void assertShardServerEquals(const ShardServerType& shardServerFromView, const
    // ShardServerType& shardServer) {
    //     BSONObj shardServerFromViewObj = shardServerFromView.toBSON();
    //     BSONObj shardServerObj = shardServer.toBSON();

    //     if (strcmp(shardServerFromViewObj.getStringField("shardName"), "") == 0) {
    //         shardServerFromViewObj = shardServerFromViewObj.removeField("shardName");
    //     }

    //     ASSERT_BSONOBJ_EQ(shardServerFromViewObj, shardServerObj);
    // }

    void assertChangeWasLogged(const ShardServerType& updatedShardServer,
                               const std::string& option) {
        auto response = assertGet(getConfigShard()->exhaustiveFindOnConfig(
            operationContext(),
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            repl::ReadConcernLevel::kLocalReadConcern,
            NamespaceString("config.changelog"),
            BSON("what" << option << "details.host" << updatedShardServer.getHost()),
            BSONObj(),
            1));
        ASSERT_EQ(1U, response.docs.size());
        auto logEntryBSON = response.docs.front();
        auto logEntry = assertGet(ChangeLogType::fromBSON(logEntryBSON));

        ASSERT_EQUALS(updatedShardServer.getHost(), logEntry.getDetails()["host"].String());
        if (option.compare(kRemoveShardServer) != 0) {
            ASSERT_EQUALS((int)updatedShardServer.getState(), logEntry.getDetails()["state"].Int());
        }
        if ((option.compare(kShardServerToBeActive) == 0) ||
            (option.compare(kShardServerActive) == 0)) {
            ASSERT_EQUALS(updatedShardServer.getShardName(),
                          logEntry.getDetails()["shardName"].String());
        }
    }
};

TEST_F(ShardServerManagerTest, InitWithEmptyShard) {
    auto future = launchAsync([this] {
        Client::initThreadIfNotAlready();
        auto shardServerManager = catalogManager()->getShardServerManager();

        ASSERT_EQUALS(
            assertGet(shardServerManager->getShardServerNodeMap(operationContext())).empty(), 1);
    });

    future.timed_get(kFutureTimeout);
}


TEST_F(ShardServerManagerTest, InitWithExsitingShard) {
    // The shard docs inserted into the config.shards collection on the config server.
    ShardType shard1;
    shard1.setName("shard0001");
    shard1.setHost("100.0.0.1:16002");
    shard1.setMaxSizeMB(100);
    shard1.setState(ShardType::ShardState::kShardActive);


    ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
                                                    ShardType::ConfigNS,
                                                    shard1.toBSON(),
                                                    ShardingCatalogClient::kMajorityWriteConcern));
    assertShardExists(shard1);

    ShardType shard2;
    shard2.setName("shard0002");
    shard2.setHost("101.0.0.1:16002");
    shard2.setMaxSizeMB(100);
    shard2.setState(ShardType::ShardState::kShardActive);


    ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
                                                    ShardType::ConfigNS,
                                                    shard2.toBSON(),
                                                    ShardingCatalogClient::kMajorityWriteConcern));
    assertShardExists(shard2);

    auto future = launchAsync([this] {
        Client::initThreadIfNotAlready();

        auto shardServerManager = catalogManager()->getShardServerManager();

        std::unordered_map<std::string, std::list<ShardServerManager::ShardInfo>> nodeMap =
            std::move(assertGet(shardServerManager->getShardServerNodeMap(operationContext())));

        ASSERT_EQUALS((int)nodeMap.size(), 2);
        ASSERT_EQUALS(getElementNumInNodeMap(nodeMap), 2);
    });

    future.timed_get(kFutureTimeout);
}


// TEST_F(ShardServerManagerTest, AddShardServerWrongConnectionStringTypeErrors) {

//     HostAndPort host1("host1:12345");
//     vector<HostAndPort> hosts;
//     hosts.push_back(std::move(host1));


//     ConnectionString expectedShardServerHost(ConnectionString::ConnectionType::INVALID,
//     std::move(hosts), "wrongTypeShardServer");

//     auto future = launchAsync([this, expectedShardServerHost] {
//         Client::initThreadIfNotAlready();
//         auto status =
//             catalogManager()->getShardServerManager()->addShardServer(operationContext(),
//                                                                       expectedShardServerHost);
//         ASSERT_EQUALS(ErrorCodes::BadValue, status.code());
//         ASSERT_EQUALS("Invalid connection string", status.reason());
//     });

//     future.timed_get(kFutureTimeout);
// }

// TEST_F(ShardServerManagerTest, AddShardServerMoreThanOneConnectionErrors) {

//     ConnectionString conn = assertGet(ConnectionString::parse("mySet/host1:12345,host2:12345"));
//     auto future = launchAsync([this, &conn] {
//         Client::initThreadIfNotAlready();
//         auto status =
//             catalogManager()->getShardServerManager()->addShardServer(operationContext(),
//                                                                       conn);
//         ASSERT_EQUALS(ErrorCodes::BadValue, status.code());
//         ASSERT_EQUALS("more than one connections for a shard server", status.reason());
//     });

//     future.timed_get(kFutureTimeout);
// }

// TEST_F(ShardServerManagerTest, AddShardServerOnExpansionNodeSuccessfully) {
//     // The shard server doc inserted into the config.shards collection on the config server.
//     ShardServerType shardServer;
//     shardServer.setHost("100.0.0.1:16001");
//     shardServer.setState(ShardServerType::ShardServerState::kStandby);

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     shardServer.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardServerExists(shardServer);

//     ConnectionString conn = assertGet(ConnectionString::parse("101.0.0.1:16001"));

//     ShardServerType expectedShardServer;
//     expectedShardServer.setHost(conn.toString());
//     expectedShardServer.setState(ShardServerType::ShardServerState::kStandby);

//     auto future = launchAsync([this, &conn, &expectedShardServer] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();
//         ASSERT_OK(shardServerManager->addShardServer(operationContext(), conn));

//         ASSERT_EQUALS(assertGet(shardServerManager->getShardCount(operationContext())), 0);

//         std::unordered_map<std::string, std::list<ShardServerManager::ShardServerInfo> > nodeMap
//         =
//             std::move(assertGet(shardServerManager->getShardServerNodeMap(operationContext())));
//         ASSERT_EQUALS(getElementNumInNodeMap(nodeMap), 2);

//         auto foundShardServerInfo =
//         assertGet(shardServerManager->getTargetShardServer(operationContext(), conn));
//         assertShardServerEquals(foundShardServerInfo.shardServer, expectedShardServer);
//     });

//     future.timed_get(kFutureTimeout);

//     assertShardServerExists(expectedShardServer);
//     assertChangeWasLogged(expectedShardServer, kAddShardServer);
// }

// TEST_F(ShardServerManagerTest, AddShardServerOnExistingNodeSuccessfully) {
//     // The shard server doc inserted into the config.shards collection on the config server.
//     ShardServerType shardServer;
//     shardServer.setHost("100.0.0.1:16001");
//     shardServer.setState(ShardServerType::ShardServerState::kActive);
//     shardServer.setShardName("shard0001");

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     shardServer.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardServerExists(shardServer);

//     // The shard doc inserted into the config.shards collection on the config server.
//     ShardType shard;
//     shard.setName("shard0001");
//     shard.setHost("100.0.0.1:16001");
//     shard.setMaxSizeMB(100);
//     shard.setState(ShardType::ShardState::kShardActive);

//     RootPlogList shardRootPlogList;
//     RootPlogItem shardRootPlogItem;
//     shardRootPlogItem.plogID.id = 5;
//     shardRootPlogItem.plogID.pool_id = 0;
//     shardRootPlogItem.chunkSize = 65536;
//     shardRootPlogItem.blockSize = 4096;
//     shardRootPlogList.add(shardRootPlogItem);
//     shard.setRootPlogList(shardRootPlogList.toBSON());

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardType::ConfigNS,
//                                                     shard.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardExists(shard);

//     ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16002"));

//     ShardServerType expectedShardServer;
//     expectedShardServer.setHost(conn.toString());
//     expectedShardServer.setState(ShardServerType::ShardServerState::kStandby);

//     auto future = launchAsync([this, &conn, &expectedShardServer] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();
//         ASSERT_OK(shardServerManager->addShardServer(operationContext(), conn));

//         ASSERT_EQUALS(assertGet(shardServerManager->getShardCount(operationContext())), 1);

//         std::unordered_map<std::string, std::list<ShardServerManager::ShardServerInfo> > nodeMap
//         =
//             std::move(assertGet(shardServerManager->getShardServerNodeMap(operationContext())));
//         ASSERT_EQUALS(getElementNumInNodeMap(nodeMap), 2);

//         auto foundShardServerInfo =
//         assertGet(shardServerManager->getTargetShardServer(operationContext(), conn));
//         assertShardServerEquals(foundShardServerInfo.shardServer, expectedShardServer);
//     });

//     future.timed_get(kFutureTimeout);

//     assertShardServerExists(expectedShardServer);
//     assertChangeWasLogged(expectedShardServer, kAddShardServer);
// }

// TEST_F(ShardServerManagerTest, AddShardServerStandbyExistsErrors) {
//     // The shard server doc inserted into the config.shards collection on the config server.
//     ShardServerType shardServer1;
//     shardServer1.setHost("100.0.0.1:16001");
//     shardServer1.setState(ShardServerType::ShardServerState::kActive);
//     shardServer1.setShardName("shard0001");

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     shardServer1.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardServerExists(shardServer1);

//     ShardServerType shardServer2;
//     shardServer2.setHost("100.0.0.1:16002");
//     shardServer2.setState(ShardServerType::ShardServerState::kStandby);

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     shardServer2.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardServerExists(shardServer2);

//     ShardType shard;
//     shard.setName("shard0001");
//     shard.setHost("100.0.0.1:16001");
//     shard.setMaxSizeMB(100);
//     shard.setState(ShardType::ShardState::kShardActive);

//     RootPlogList shardRootPlogList;
//     RootPlogItem shardRootPlogItem;
//     shardRootPlogItem.plogID.id = 5;
//     shardRootPlogItem.plogID.pool_id = 0;
//     shardRootPlogItem.chunkSize = 65536;
//     shardRootPlogItem.blockSize = 4096;
//     shardRootPlogList.add(shardRootPlogItem);
//     shard.setRootPlogList(shardRootPlogList.toBSON());

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardType::ConfigNS,
//                                                     shard.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardExists(shard);

//     ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16003"));

//     auto future = launchAsync([this, &conn] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();

//         ASSERT_EQUALS(assertGet(shardServerManager->getShardCount(operationContext())), 1);
//         std::unordered_map<std::string, std::list<ShardServerManager::ShardServerInfo> >
//         nodeMapBefore =
//             std::move(assertGet(shardServerManager->getShardServerNodeMap(operationContext())));
//         ASSERT_EQUALS(getElementNumInNodeMap(nodeMapBefore), 2);

//         auto status = shardServerManager->addShardServer(operationContext(), conn);
//         ASSERT_EQUALS(ErrorCodes::BadValue, status.code());
//         ASSERT_EQUALS("standby shard server already exists on the same node", status.reason());

//         std::unordered_map<std::string, std::list<ShardServerManager::ShardServerInfo> >
//         nodeMapAfter =
//             std::move(assertGet(shardServerManager->getShardServerNodeMap(operationContext())));
//         ASSERT_EQUALS(getElementNumInNodeMap(nodeMapAfter), 2);

//     });

//     future.timed_get(kFutureTimeout);
// }

// TEST_F(ShardServerManagerTest, AddShardServerWithActiveStateErrors) {
//     // The shard server doc inserted into the config.shards collection on the config server.
//     ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16001"));

//     ShardServerType shardServer;
//     shardServer.setHost(conn.toString());
//     shardServer.setState(ShardServerType::ShardServerState::kActive);
//     shardServer.setShardName("shard0001");

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     shardServer.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardServerExists(shardServer);

//     // The shard doc inserted into the config.shards collection on the config server.
//     ShardType shard;
//     shard.setName("shard0001");
//     shard.setHost(conn.toString());
//     shard.setMaxSizeMB(100);
//     shard.setState(ShardType::ShardState::kShardActive);

//     RootPlogList shardRootPlogList;
//     RootPlogItem shardRootPlogItem;
//     shardRootPlogItem.plogID.id = 5;
//     shardRootPlogItem.plogID.pool_id = 0;
//     shardRootPlogItem.chunkSize = 65536;
//     shardRootPlogItem.blockSize = 4096;
//     shardRootPlogList.add(shardRootPlogItem);
//     shard.setRootPlogList(shardRootPlogList.toBSON());

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardType::ConfigNS,
//                                                     shard.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardExists(shard);

//     auto future = launchAsync([this, &conn, &shardServer] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();
//         auto status = shardServerManager->addShardServer(operationContext(), conn);

//         ASSERT_EQUALS(ErrorCodes::BadValue, status.code());
//         ASSERT_EQUALS("try to reset active shard server to standby", status.reason());

//         ASSERT_EQUALS(assertGet(shardServerManager->getShardCount(operationContext())), 1);

//         std::unordered_map<std::string, std::list<ShardServerManager::ShardServerInfo> > nodeMap
//         =
//             std::move(assertGet(shardServerManager->getShardServerNodeMap(operationContext())));
//         ASSERT_EQUALS(getElementNumInNodeMap(nodeMap), 1);

//         auto foundShardServerInfo =
//         assertGet(shardServerManager->getTargetShardServer(operationContext(), conn));
//         assertShardServerEquals(foundShardServerInfo.shardServer, shardServer);

//     });

//     future.timed_get(kFutureTimeout);
//     assertShardServerExists(shardServer);
// }

// TEST_F(ShardServerManagerTest, AddShardServerUpdateFromBeActiveSuccessfully) {
//     // The shard server doc inserted into the config.shards collection on the config server.
//     ShardServerType shardServer;

//     ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16001"));

//     shardServer.setHost(conn.toString());
//     shardServer.setState(ShardServerType::ShardServerState::kToBeActive);
//     shardServer.setShardName("shard0001");

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     shardServer.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardServerExists(shardServer);

//     ShardType shard;
//     shard.setName("shard0001");
//     shard.setHost(conn.toString());
//     shard.setMaxSizeMB(100);
//     shard.setState(ShardType::ShardState::kShardRegistering);

//     RootPlogList shardRootPlogList;
//     RootPlogItem shardRootPlogItem;
//     shardRootPlogItem.plogID.id = 5;
//     shardRootPlogItem.plogID.pool_id = 0;
//     shardRootPlogItem.chunkSize = 65536;
//     shardRootPlogItem.blockSize = 4096;
//     shardRootPlogList.add(shardRootPlogItem);
//     shard.setRootPlogList(shardRootPlogList.toBSON());

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardType::ConfigNS,
//                                                     shard.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardExists(shard);

//     auto future = launchAsync([this, &conn, &shardServer] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();
//         ASSERT_EQUALS(assertGet(shardServerManager->getShardCount(operationContext())), 1);

//         ASSERT_OK(shardServerManager->addShardServer(operationContext(), conn));
//         ASSERT_EQUALS(assertGet(shardServerManager->getShardCount(operationContext())), 1);

//         std::unordered_map<std::string, std::list<ShardServerManager::ShardServerInfo> > nodeMap
//         =
//             std::move(assertGet(shardServerManager->getShardServerNodeMap(operationContext())));
//         ASSERT_EQUALS(getElementNumInNodeMap(nodeMap), 1);

//         auto foundShardServerInfo =
//         assertGet(shardServerManager->getTargetShardServer(operationContext(), conn));
//         shardServer.setState(ShardServerType::ShardServerState::kStandby);
//         assertShardServerEquals(foundShardServerInfo.shardServer, shardServer);
//     });

//     future.timed_get(kFutureTimeout);

//     assertShardServerExists(shardServer);
//     assertChangeWasLogged(shardServer, kAddShardServer);
// }

// TEST_F(ShardServerManagerTest, AddShardServerUpdateFromStandbySuccessfully) {
//     // The shard server doc inserted into the config.shards collection on the config server.
//     ShardServerType shardServer;

//     ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16001"));

//     shardServer.setHost(conn.toString());
//     shardServer.setState(ShardServerType::ShardServerState::kStandby);

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     shardServer.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardServerExists(shardServer);

//     auto future = launchAsync([this, &conn, &shardServer] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();
//         ASSERT_OK(shardServerManager->addShardServer(operationContext(), conn));

//         ASSERT_EQUALS(assertGet(shardServerManager->getShardCount(operationContext())), 0);

//         std::unordered_map<std::string, std::list<ShardServerManager::ShardServerInfo> > nodeMap
//         =
//             std::move(assertGet(shardServerManager->getShardServerNodeMap(operationContext())));
//         ASSERT_EQUALS(getElementNumInNodeMap(nodeMap), 1);

//         auto foundShardServerInfo =
//         assertGet(shardServerManager->getTargetShardServer(operationContext(), conn));
//         assertShardServerEquals(foundShardServerInfo.shardServer, shardServer);
//     });

//     future.timed_get(kFutureTimeout);

//     assertShardServerExists(shardServer);
//     assertChangeWasLogged(shardServer, kAddShardServer);
// }

// TEST_F(ShardServerManagerTest, AddShardServerFailedUpdateShardServer) {
//     // Add a pre-existing shard server
//     ShardServerType existingShardServer;
//     ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16001"));
//     existingShardServer.setHost(conn.toString());
//     existingShardServer.setState(ShardServerType::ShardServerState::kToBeActive);
//     existingShardServer.setShardName("shard0001");

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     existingShardServer.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));

//     assertShardServerExists(existingShardServer);

//     ShardType shard;
//     shard.setName("shard0001");
//     shard.setHost(conn.toString());
//     shard.setMaxSizeMB(100);
//     shard.setState(ShardType::ShardState::kShardRegistering);

//     RootPlogList shardRootPlogList;
//     RootPlogItem shardRootPlogItem;
//     shardRootPlogItem.plogID.id = 5;
//     shardRootPlogItem.plogID.pool_id = 0;
//     shardRootPlogItem.chunkSize = 65536;
//     shardRootPlogItem.blockSize = 4096;
//     shardRootPlogList.add(shardRootPlogItem);
//     shard.setRootPlogList(shardRootPlogList.toBSON());

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardType::ConfigNS,
//                                                     shard.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardExists(shard);

//     auto failPoint = getGlobalFailPointRegistry()->getFailPoint("failAllUpdates");
//     ASSERT(failPoint);
//     failPoint->setMode(FailPoint::alwaysOn);
//     ON_BLOCK_EXIT([&] { failPoint->setMode(FailPoint::off); });

//     auto future = launchAsync([this, &conn, &existingShardServer] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();
//         ASSERT_EQUALS(assertGet(shardServerManager->getShardCount(operationContext())), 1);

//         ASSERT_NOT_EQUALS(Status::OK(), shardServerManager->addShardServer(operationContext(),
//         conn));

//         ASSERT_EQUALS(assertGet(shardServerManager->getShardCount(operationContext())), 1);

//         std::unordered_map<std::string, std::list<ShardServerManager::ShardServerInfo> > nodeMap
//         =
//             std::move(assertGet(shardServerManager->getShardServerNodeMap(operationContext())));
//         ASSERT_EQUALS(getElementNumInNodeMap(nodeMap), 1);

//         auto foundShardServerInfo =
//         assertGet(shardServerManager->getTargetShardServer(operationContext(), conn));
//         assertShardServerEquals(foundShardServerInfo.shardServer, existingShardServer);
//     });

//     future.timed_get(kFutureTimeout);

//     assertShardServerExists(existingShardServer);
// }


// TEST_F(ShardServerManagerTest, RemoveNoSuchShardServer) {
//     // The shard server doc inserted into the config.shards collection on the config server.
//     ShardServerType shardServer;
//     shardServer.setHost("100.0.0.1:16001");
//     shardServer.setState(ShardServerType::ShardServerState::kStandby);

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     shardServer.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardServerExists(shardServer);

//     HostAndPort hpExpected = assertGet(HostAndPort::parse("100.0.0.1:16002"));

//     auto future = launchAsync([this, &hpExpected] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();

//         ASSERT_OK(shardServerManager->removeShardServer(operationContext(), hpExpected));

//         ASSERT_EQUALS(assertGet(shardServerManager->getShardCount(operationContext())), 0);
//         ASSERT_EQUALS(getElementNumInNodeMap(std::move(assertGet(shardServerManager->getShardServerNodeMap(operationContext())))),
//         1);
//     });

//     future.timed_get(kFutureTimeout);
// }

// TEST_F(ShardServerManagerTest, RemoveStandbySuccessfully) {
//     // The shard server doc inserted into the config.shards collection on the config server.
//     ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16001"));

//     ShardServerType shardServer;
//     shardServer.setHost(conn.toString());
//     shardServer.setState(ShardServerType::ShardServerState::kStandby);

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     shardServer.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardServerExists(shardServer);

//     HostAndPort hpExpected = assertGet(HostAndPort::parse(conn.toString()));

//     auto future = launchAsync([this, &conn, &hpExpected] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();

//         ASSERT_OK(shardServerManager->removeShardServer(operationContext(), hpExpected));

//         ASSERT_EQUALS(assertGet(shardServerManager->getShardCount(operationContext())), 0);
//         ASSERT_EQUALS(assertGet(shardServerManager->getShardServerNodeMap(operationContext())).empty(),
//         1);

//         auto status = shardServerManager->getTargetShardServer(operationContext(), conn);
//         ASSERT_EQUALS(ErrorCodes::ShardServerNotFound, status.getStatus().code());
//         ASSERT_EQUALS("no matching shard server", status.getStatus().reason());
//     });

//     future.timed_get(kFutureTimeout);
//     assertChangeWasLogged(shardServer, kRemoveShardServer);
// }

// TEST_F(ShardServerManagerTest, RemoveToBeActiveSuccessfully) {
//     // The shard server doc inserted into the config.shards collection on the config server.
//     ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16001"));

//     ShardServerType shardServer;
//     shardServer.setHost(conn.toString());
//     shardServer.setState(ShardServerType::ShardServerState::kToBeActive);
//     shardServer.setShardName("shard0001");
//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     shardServer.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardServerExists(shardServer);

//     // The shard doc inserted into the config.shards collection on the config server.
//     ShardType shard;
//     shard.setName("shard0001");
//     shard.setHost(conn.toString());
//     shard.setMaxSizeMB(100);
//     shard.setState(ShardType::ShardState::kShardRegistering);

//     RootPlogList shardRootPlogList;
//     RootPlogItem shardRootPlogItem;
//     shardRootPlogItem.plogID.id = 5;
//     shardRootPlogItem.plogID.pool_id = 0;
//     shardRootPlogItem.chunkSize = 65536;
//     shardRootPlogItem.blockSize = 4096;
//     shardRootPlogList.add(shardRootPlogItem);
//     shard.setRootPlogList(shardRootPlogList.toBSON());

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardType::ConfigNS,
//                                                     shard.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardExists(shard);

//     HostAndPort hpExpected = assertGet(HostAndPort::parse(conn.toString()));

//     auto future = launchAsync([this, &conn, &hpExpected] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();

//         ASSERT_OK(shardServerManager->removeShardServer(operationContext(), hpExpected));

//         ASSERT_EQUALS(assertGet(shardServerManager->getShardCount(operationContext())), 1);
//         ASSERT_EQUALS(assertGet(shardServerManager->getShardServerNodeMap(operationContext())).empty(),
//         1);

//         auto status = shardServerManager->getTargetShardServer(operationContext(), conn);
//         ASSERT_EQUALS(ErrorCodes::ShardServerNotFound, status.getStatus().code());
//         ASSERT_EQUALS("no matching shard server", status.getStatus().reason());
//     });

//     future.timed_get(kFutureTimeout);
//     assertChangeWasLogged(shardServer, kRemoveShardServer);
// }

// TEST_F(ShardServerManagerTest, RemoveActiveSuccessfully) {
//     // The shard server doc inserted into the config.shards collection on the config server.
//     ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16001"));

//     ShardServerType shardServer;
//     shardServer.setHost(conn.toString());
//     shardServer.setState(ShardServerType::ShardServerState::kActive);
//     shardServer.setShardName("shard0001");

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     shardServer.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardServerExists(shardServer);

//     // The shard doc inserted into the config.shards collection on the config server.
//     ShardType shard;
//     shard.setName("shard0001");
//     shard.setHost(conn.toString());
//     shard.setMaxSizeMB(100);
//     shard.setState(ShardType::ShardState::kShardActive);

//     RootPlogList shardRootPlogList;
//     RootPlogItem shardRootPlogItem;
//     shardRootPlogItem.plogID.id = 5;
//     shardRootPlogItem.plogID.pool_id = 0;
//     shardRootPlogItem.chunkSize = 65536;
//     shardRootPlogItem.blockSize = 4096;
//     shardRootPlogList.add(shardRootPlogItem);
//     shard.setRootPlogList(shardRootPlogList.toBSON());

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardType::ConfigNS,
//                                                     shard.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardExists(shard);

//     HostAndPort hpExpected = assertGet(HostAndPort::parse(conn.toString()));

//     auto future = launchAsync([this, &conn, &hpExpected] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();

//         ASSERT_OK(shardServerManager->removeShardServer(operationContext(), hpExpected));

//         ASSERT_EQUALS(assertGet(shardServerManager->getShardCount(operationContext())), 1);
//         ASSERT_EQUALS(assertGet(shardServerManager->getShardServerNodeMap(operationContext())).empty(),
//         1);

//         auto status = shardServerManager->getTargetShardServer(operationContext(), conn);
//         ASSERT_EQUALS(ErrorCodes::ShardServerNotFound, status.getStatus().code());
//         ASSERT_EQUALS("no matching shard server", status.getStatus().reason());
//     });

//     future.timed_get(kFutureTimeout);
//     assertChangeWasLogged(shardServer, kRemoveShardServer);
// }


TEST_F(ShardServerManagerTest, SetHeartbeatTimeSuccessfully) {
    ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16001"));

    ShardType shard;
    shard.setName("shard0001");
    shard.setHost(conn.toString());
    shard.setState(ShardType::ShardState::kShardRegistering);

    ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
                                                    ShardType::ConfigNS,
                                                    shard.toBSON(),
                                                    ShardingCatalogClient::kMajorityWriteConcern));
    assertShardExists(shard);

    auto future = launchAsync([this, &conn] {
        Client::initThreadIfNotAlready();

        auto shardServerManager = catalogManager()->getShardServerManager();

        Date_t lastTimeExpected = Date_t::now();
        HostAndPort hpExpected = assertGet(HostAndPort::parse(conn.toString()));
        ASSERT_OK(shardServerManager->setLastHeartbeatTime(
            operationContext(), hpExpected, lastTimeExpected));

        auto lastTime =
            assertGet(shardServerManager->getLastHeartbeatTime(operationContext(), hpExpected));
        ASSERT_EQUALS(lastTimeExpected, lastTime);
    });

    future.timed_get(kFutureTimeout);
}

// TEST_F(ShardServerManagerTest, GenerateNameSuccessfully) {
//     ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16001"))
//     std::stringstream ss;
//     ss << "shard" << std::setfill('0') << std::setw(kShardNameDigitWidth) << 4;


//     ShardType shard;

//     shard.setHost(conn.toString());
//     shard.setMaxSizeMB(100);
//     shard.setState(ShardType::ShardState::kShardActive);
//     shard.setName(ss.str());


//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardType::ConfigNS,
//                                                     shard.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardExists(shard);

//     auto future = launchAsync([this] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();
//         auto shardName = assertGet(shardServerManager->generateNewShardName(operationContext()));
//         std::stringstream expectedss;
//         expectedss << "shard" << std::setfill('0') << std::setw(kShardNameDigitWidth) << 5;
//         ASSERT_EQUALS(expectedss.str(), shardName);
//     });

//     future.timed_get(kFutureTimeout);
// }

// TEST_F(ShardServerManagerTest, ShardServerNotExistCheckIfShardServerHasShardName) {
//     ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16001"));

//     auto future = launchAsync([this, &conn] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();
//         std::string shardName;
//         auto hasShardName =
//         assertGet(shardServerManager->checkIfShardServerHasShardName(operationContext(), conn,
//         shardName));
//         ASSERT_FALSE(hasShardName);
//     });

//     future.timed_get(kFutureTimeout);
// }

// TEST_F(ShardServerManagerTest, ReturnFalseCheckIfShardServerHasShardName) {
//     ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16001"));

//     ShardServerType shardServer;
//     shardServer.setHost(conn.toString());
//     shardServer.setState(ShardServerType::ShardServerState::kStandby);

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     shardServer.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardServerExists(shardServer);

//     auto future = launchAsync([this, &conn] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();
//         std::string shardName;
//         auto hasShardName =
//         assertGet(shardServerManager->checkIfShardServerHasShardName(operationContext(), conn,
//         shardName));
//         ASSERT_FALSE(hasShardName);
//     });

//     future.timed_get(kFutureTimeout);
// }

// TEST_F(ShardServerManagerTest, ReturnTrueCheckIfShardServerHasShardName) {
//     ConnectionString conn = assertGet(ConnectionString::parse("100.0.0.1:16001"));

//     ShardServerType shardServer;
//     shardServer.setHost(conn.toString());
//     shardServer.setState(ShardServerType::ShardServerState::kActive);
//     std::stringstream ss;
//     ss << "shard" << std::setfill('0') << std::setw(kShardNameDigitWidth) << 4;
//     shardServer.setShardName(ss.str());

//     ASSERT_OK(catalogClient()->insertConfigDocument(operationContext(),
//                                                     ShardServerType::ConfigNS,
//                                                     shardServer.toBSON(),
//                                                     ShardingCatalogClient::kMajorityWriteConcern));
//     assertShardServerExists(shardServer);

//     auto future = launchAsync([this, &conn, &ss] {
//         Client::initThreadIfNotAlready();

//         auto shardServerManager = catalogManager()->getShardServerManager();
//         std::string shardName;
//         auto hasShardName =
//         assertGet(shardServerManager->checkIfShardServerHasShardName(operationContext(), conn,
//         shardName));
//         ASSERT_TRUE(hasShardName);
//         ASSERT_EQUALS(shardName, ss.str());

//     });

//     future.timed_get(kFutureTimeout);
// }

}  // namesapce
}  // namespace mongo

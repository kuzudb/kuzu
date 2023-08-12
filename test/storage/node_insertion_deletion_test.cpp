#include "graph_test/graph_test.h"
#include "storage/storage_manager.h"
#include "storage/wal_replayer.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

// Note: ID and nodeOffset in this test are equal for each node, so we use nodeID and nodeOffset
// interchangeably.
class NodeInsertionDeletionTests : public DBTest {

public:
    void SetUp() override {
        DBTest::SetUp();
        initDBAndConnection();
    }

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/node-insertion-deletion-tests/int64-pk/");
    }

    void initDBAndConnection() {
        createDBAndConn();
        readConn = std::make_unique<Connection>(database.get());
        conn->beginWriteTransaction();
    }

    void deleteNode(offset_t id) {
        auto res = conn->query("MATCH (a:person) WHERE a.ID = " + std::to_string(id) + " DELETE a");
        ASSERT_TRUE(res->isSuccess());
    }

    void addNode(offset_t id) {
        auto res = conn->query("CREATE (a:person {ID: " + std::to_string(id) + "})");
        ASSERT_TRUE(res->isSuccess());
    }

public:
    std::unique_ptr<Connection> readConn;
};

TEST_F(NodeInsertionDeletionTests, DeleteAddMixedTest) {
    for (offset_t nodeOffset = 10; nodeOffset < 90; ++nodeOffset) {
        deleteNode(nodeOffset);
    }
    for (offset_t i = 10; i < 90; ++i) {
        addNode(i);
    }
    // Add additional node offsets
    for (int i = 0; i < 10; ++i) {
        addNode(10000 + i);
    }

    std::string query = "MATCH (a:person) RETURN count(*)";
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10010);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10000);
    conn->commit();
    conn->beginWriteTransaction();
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10010);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10010);

    for (offset_t nodeOffset = 0; nodeOffset < 10; ++nodeOffset) {
        deleteNode(nodeOffset);
    }

    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10000);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10010);
    conn->commit();
    conn->beginWriteTransaction();
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10000);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10000);

    for (int i = 0; i < 5; ++i) {
        addNode(i);
    }

    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10005);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10000);
    conn->commit();
    conn->beginWriteTransaction();
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10005);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10005);
}

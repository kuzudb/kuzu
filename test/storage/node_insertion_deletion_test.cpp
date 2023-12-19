#include <fcntl.h>

#include "common/constants.h"
#include "common/file_system/local_file_system.h"
#include "graph_test/graph_test.h"

using namespace kuzu::common;
using namespace kuzu::main;
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
        conn->query("BEGIN TRANSACTION");
    }

    void deleteNode(offset_t id) {
        auto res = conn->query("MATCH (a:person) WHERE a.ID = " + std::to_string(id) + " DELETE a");
        ASSERT_TRUE(res->isSuccess()) << res->toString();
    }

    void addNode(offset_t id) {
        auto res = conn->query("CREATE (a:person {ID: " + std::to_string(id) + "})");
        ASSERT_TRUE(res->isSuccess()) << res->toString();
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
    conn->query("COMMIT");
    conn->query("BEGIN TRANSACTION");
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10010);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10010);

    for (offset_t nodeOffset = 0; nodeOffset < 10; ++nodeOffset) {
        deleteNode(nodeOffset);
    }

    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10000);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10010);
    conn->query("COMMIT");
    conn->query("BEGIN TRANSACTION");
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10000);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10000);

    for (int i = 0; i < 5; ++i) {
        addNode(i);
    }

    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10005);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10000);
    conn->query("COMMIT");
    conn->query("BEGIN TRANSACTION");
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10005);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->getValue<int64_t>(), 10005);
}

TEST_F(NodeInsertionDeletionTests, InsertManyNodesTest) {
    auto preparedStatement = conn->prepare("CREATE (:person {ID:$id});");
    for (int64_t i = 0; i < (int64_t)BufferPoolConstants::PAGE_4KB_SIZE; i++) {
        auto result =
            conn->execute(preparedStatement.get(), std::make_pair(std::string("id"), 10000 + i));
        ASSERT_TRUE(result->isSuccess()) << result->toString();
    }
    auto result = conn->query("MATCH (a:person) WHERE a.ID >= 10000 RETURN COUNT(*);");
    ASSERT_TRUE(result->hasNext());
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), BufferPoolConstants::PAGE_4KB_SIZE);
    ASSERT_FALSE(result->hasNext());
    result = conn->query("MATCH (a:person) WHERE a.ID=10000 RETURN a.ID;");
    ASSERT_TRUE(result->hasNext());
    tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 10000);
    result = conn->query("MATCH (a:person) WHERE a.ID>=10000 RETURN a.ID ORDER BY a.ID;");
    int64_t i = 0;
    while (result->hasNext()) {
        tuple = result->getNext();
        EXPECT_EQ(10000 + i++, tuple->getValue(0)->getValue<int64_t>());
    }
    ASSERT_EQ(i, BufferPoolConstants::PAGE_4KB_SIZE);
}

TEST_F(NodeInsertionDeletionTests, TruncatedWalTest) {
    auto preparedStatement = conn->prepare("CREATE (:person {ID:$id});");
    auto fs = LocalFileSystem();
    // Note: this test will fail if the transaction is small enough to fit the wal headers in a
    // single page, since we currently may fail to recover if the headers are intact but shadow
    // pages are missing. Pages are flushed before writing the commit record to make sure that
    // doesn't happen during a regular interruption.
    for (int64_t i = 0; i < 100; i++) {
        auto result =
            conn->execute(preparedStatement.get(), std::make_pair(std::string("id"), 10000 + i));
        ASSERT_TRUE(result->isSuccess()) << result->toString();
    }
    conn->query("COMMIT_SKIP_CHECKPOINT");
    auto databasePath = this->databasePath;
    auto walPath = fs.joinPath(databasePath, StorageConstants::WAL_FILE_SUFFIX);
    // Close database
    database.reset();
    {
        auto walFileInfo = fs.openFile(walPath, O_RDWR);
        ASSERT_GT(walFileInfo->getFileSize(), BufferPoolConstants::PAGE_4KB_SIZE)
            << "Test needs a wal file with more than one page";
        walFileInfo->truncate(BufferPoolConstants::PAGE_4KB_SIZE);
    }
    // Re-open database
    database = std::make_unique<Database>(databasePath);
}

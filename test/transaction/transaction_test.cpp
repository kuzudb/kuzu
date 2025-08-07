#include <fstream>

#include "api_test/api_test.h"
#include "api_test/private_api_test.h"
#include "storage/storage_utils.h"
#include "storage/wal/wal.h"

using namespace kuzu::common;
using namespace kuzu::testing;
using namespace kuzu::transaction;

TEST_F(PrivateApiTest, TransactionModes) {
    // Test initially connections are in AUTO_COMMIT mode.
    ASSERT_EQ(TransactionMode::AUTO, getTransactionMode(*conn));
    // Test beginning a transaction (first in read only mode) sets mode to MANUAL automatically.
    conn->query("BEGIN TRANSACTION READ ONLY;");
    ASSERT_EQ(TransactionMode::MANUAL, getTransactionMode(*conn));
    // Test commit automatically switches the mode to AUTO_COMMIT read transaction
    conn->query("COMMIT");
    ASSERT_EQ(TransactionMode::AUTO, getTransactionMode(*conn));

    conn->query("BEGIN TRANSACTION READ ONLY;");
    ASSERT_EQ(TransactionMode::MANUAL, getTransactionMode(*conn));
    // Test rollback automatically switches the mode to AUTO_COMMIT for read transaction
    conn->query("ROLLBACK;");
    ASSERT_EQ(TransactionMode::AUTO, getTransactionMode(*conn));

    // Test beginning a transaction (now in write mode) sets mode to MANUAL automatically.
    conn->query("BEGIN TRANSACTION;");
    ASSERT_EQ(TransactionMode::MANUAL, getTransactionMode(*conn));
    // Test commit automatically switches the mode to AUTO_COMMIT for write transaction
    conn->query("COMMIT;");
    ASSERT_EQ(TransactionMode::AUTO, getTransactionMode(*conn));

    // Test beginning a transaction (now in write mode) sets mode to MANUAL automatically.
    conn->query("BEGIN TRANSACTION;");
    ASSERT_EQ(TransactionMode::MANUAL, getTransactionMode(*conn));
    // Test rollback automatically switches the mode to AUTO_COMMIT write transaction
    conn->query("ROLLBACK;");
    ASSERT_EQ(TransactionMode::AUTO, getTransactionMode(*conn));
}

TEST_F(PrivateApiTest, MultipleCallsFromSameTransaction) {
    conn->query("BEGIN TRANSACTION READ ONLY;");
    auto activeTransactionID = getActiveTransactionID(*conn);
    conn->query("MATCH (a:person) RETURN COUNT(*)");
    ASSERT_EQ(activeTransactionID, getActiveTransactionID(*conn));
    conn->query("MATCH (a:person) RETURN COUNT(*)");
    ASSERT_EQ(activeTransactionID, getActiveTransactionID(*conn));
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)");
    conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), true));
    ASSERT_EQ(activeTransactionID, getActiveTransactionID(*conn));
    conn->query("COMMIT;");
    ASSERT_FALSE(hasActiveTransaction(*conn));
}

TEST_F(PrivateApiTest, CommitRollbackRemoveActiveTransaction) {
    conn->query("BEGIN TRANSACTION;");
    ASSERT_TRUE(hasActiveTransaction(*conn));
    conn->query("ROLLBACK;");
    ASSERT_FALSE(hasActiveTransaction(*conn));
    conn->query("BEGIN TRANSACTION READ ONLY;");
    ASSERT_TRUE(hasActiveTransaction(*conn));
    conn->query("COMMIT;");
    ASSERT_FALSE(hasActiveTransaction(*conn));
}

TEST_F(PrivateApiTest, CloseConnectionWithActiveTransaction) {
    conn->query("BEGIN TRANSACTION;");
    ASSERT_TRUE(hasActiveTransaction(*conn));
    conn->query("MATCH (a:person) SET a.age=10;");
    conn.reset();
    conn = std::make_unique<kuzu::main::Connection>(database.get());
    conn->query("BEGIN TRANSACTION;");
    auto res = conn->query("MATCH (a:person) WHERE a.age=10 RETURN COUNT(*) AS count;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto count = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(count, 0); // The previous transaction was rolled back.
}

TEST_F(PrivateApiTest, CloseDatabaseWithActiveTransaction) {
    if (inMemMode) {
        GTEST_SKIP();
    }
    conn->query("BEGIN TRANSACTION;");
    ASSERT_TRUE(hasActiveTransaction(*conn));
    conn->query("MATCH (a:person) SET a.age=10;");
    conn.reset();
    database.reset();
    createDBAndConn();
    conn->query("BEGIN TRANSACTION;");
    auto res = conn->query("MATCH (a:person) WHERE a.age=10 RETURN COUNT(*) AS count;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto count = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(count, 0); // The previous transaction was rolled back.
}

class EmptyDBTransactionTest : public EmptyDBTest {
protected:
    void SetUp() override {
        EmptyDBTest::SetUp();
        createDBAndConn();
    }

    void TearDown() override { EmptyDBTest::TearDown(); }
};

TEST_F(EmptyDBTransactionTest, DatabaseFilesAfterCheckpoint) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL auto_checkpoint=false;");
    ASSERT_FALSE(
        std::filesystem::exists(kuzu::storage::StorageUtils::getTmpFilePath(databasePath)));
    ASSERT_FALSE(
        std::filesystem::exists(kuzu::storage::StorageUtils::getShadowFilePath(databasePath)));
    ASSERT_FALSE(
        std::filesystem::exists(kuzu::storage::StorageUtils::getWALFilePath(databasePath)));
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    ASSERT_TRUE(std::filesystem::exists(kuzu::storage::StorageUtils::getWALFilePath(databasePath)));
    conn->query("CHECKPOINT;");
    ASSERT_FALSE(
        std::filesystem::exists(kuzu::storage::StorageUtils::getTmpFilePath(databasePath)));
    ASSERT_FALSE(
        std::filesystem::exists(kuzu::storage::StorageUtils::getShadowFilePath(databasePath)));
    ASSERT_FALSE(
        std::filesystem::exists(kuzu::storage::StorageUtils::getWALFilePath(databasePath)));
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
}

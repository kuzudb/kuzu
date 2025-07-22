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

TEST_F(PrivateApiTest, NoWALFile) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("BEGIN TRANSACTION;");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("COMMIT;");
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::file_size(walFilePath) > 0);
    std::filesystem::remove(walFilePath);
    // No WAL file, so no replay.
    createDBAndConn();
    auto res = conn->query("CALL show_tables() WHERE name='test' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 0);
}

TEST_F(PrivateApiTest, EmptyWALFile) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("BEGIN TRANSACTION;");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("COMMIT;");
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::file_size(walFilePath) > 0);
    std::filesystem::resize_file(walFilePath, 0);
    // Empty WAL file, so no replay.
    createDBAndConn();
    auto res = conn->query("CALL show_tables() WHERE name='test' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 0);
}

TEST_F(PrivateApiTest, NoWALAfterCheckpoint) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("BEGIN TRANSACTION;");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("COMMIT;");
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::file_size(walFilePath) > 0);
    // Checkpoint should remove the WAL file.
    conn->query("checkpoint;");
    ASSERT_FALSE(std::filesystem::exists(walFilePath));
}

TEST_F(PrivateApiTest, ShadowFileExistsWithoutWAL) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("BEGIN TRANSACTION;");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("COMMIT;");
    auto shadowFilePath = kuzu::storage::StorageUtils::getShadowFilePath(databasePath);
    // Create a shadow file that is corrupted.
    std::ofstream file(shadowFilePath);
    file << "This is not a valid Kuzu database file.";
    file.close();
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::file_size(walFilePath) > 0);
    std::filesystem::remove(walFilePath);
    // No WAL file, but the shadow file exists. Should not replay the shadow file, and remove wal
    // and shadow files.
    createDBAndConn();
    auto res = conn->query("CALL show_tables() WHERE name='test' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 0);
    ASSERT_FALSE(std::filesystem::exists(walFilePath));
    ASSERT_FALSE(std::filesystem::exists(shadowFilePath));
}

TEST_F(PrivateApiTest, ShadowFileExistsWithEmptyWAL) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("BEGIN TRANSACTION;");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("COMMIT;");
    auto shadowFilePath = kuzu::storage::StorageUtils::getShadowFilePath(databasePath);
    // Create a shadow file that is corrupted.
    std::ofstream file(shadowFilePath);
    file << "This is not a valid Kuzu database file.";
    file.close();
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::file_size(walFilePath) > 0);
    std::filesystem::resize_file(walFilePath, 0);
    // Empty WAL file, but the shadow file exists. Should not replay the shadow file, and remove wal
    // and shadow files.
    createDBAndConn();
    auto res = conn->query("CALL show_tables() WHERE name='test' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 0);
    ASSERT_FALSE(std::filesystem::exists(walFilePath));
    ASSERT_FALSE(std::filesystem::exists(shadowFilePath));
}

TEST_F(PrivateApiTest, CorruptedWALTailTruncated) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("BEGIN TRANSACTION;");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test2(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test3(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test4(id INT64 PRIMARY KEY, name STRING);");
    conn->query("COMMIT;");
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::file_size(walFilePath) > 10);
    // Truncate the last 10 bytes of the WAL file.
    std::filesystem::resize_file(walFilePath, std::filesystem::file_size(walFilePath) - 10);
    createDBAndConn();
    auto res = conn->query("CALL show_tables() WHERE name='test' OR name='test2' OR name='test3' "
                           "OR name='test4' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 0);
}

TEST_F(PrivateApiTest, CorruptedWALTailTruncated2) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test2(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test3(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test4(id INT64 PRIMARY KEY, name STRING);");
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::file_size(walFilePath) > 10);
    // Truncate the last 10 bytes of the WAL file.
    std::filesystem::resize_file(walFilePath, std::filesystem::file_size(walFilePath) - 10);
    createDBAndConn();
    auto res = conn->query("CALL show_tables() WHERE name='test' OR name='test2' OR name='test3' "
                           "OR name='test4' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 3);
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

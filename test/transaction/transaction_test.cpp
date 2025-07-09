#include "api_test/private_api_test.h"
#include "common/exception/io.h"
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
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
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
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
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

TEST_F(PrivateApiTest, MissingShadowFile) {
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
    // Simulate a checkpoint with a FlakyCheckpointer that fail right after logging CHECKPOINT.
    auto context = getClientContext(*conn);
    context->getWAL()->logAndFlushCheckpoint(context);
    auto shadowFilePath = kuzu::storage::StorageUtils::getShadowFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    // Remove the shadow file to simulate a missing shadow file.
    std::filesystem::remove(shadowFilePath);
    // No shadow file, so no replay.
    EXPECT_THROW(createDBAndConn(), Exception);
}

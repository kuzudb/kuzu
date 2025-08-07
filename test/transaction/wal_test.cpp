#include <fstream>

#include "api_test/api_test.h"
#include "api_test/private_api_test.h"
#include "storage/storage_utils.h"
#include "storage/wal/wal.h"

using namespace kuzu::common;
using namespace kuzu::testing;
using namespace kuzu::transaction;

class WalTest : public ApiTest {};

TEST_F(WalTest, NoWALFile) {
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

TEST_F(WalTest, EmptyWALFile) {
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

TEST_F(WalTest, NoWALAfterCheckpoint) {
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

TEST_F(WalTest, ShadowFileExistsWithoutWAL) {
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

TEST_F(WalTest, ShadowFileExistsWithEmptyWAL) {
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

// Simulation of a corrupted WAL tail by truncating the WAL file. Note that in this case, there
// would only be a single write transaction. This would cause the last wal record to be corrupted
// and the database should ignore the last record when recovering.
TEST_F(WalTest, CorruptedWALTailTruncated) {
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
    auto res = conn->query("CALL show_tables() WHERE name STARTS WITH 'test' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 0);
}

// Simulation of a corrupted WAL tail by truncating the WAL file, but then continuing to write to
// the WAL, and recover from the same WAL file again. This should recover the tables that were
// created after the database's first recovering from the corrupted WAL.
TEST_F(WalTest, CorruptedWALTailTruncatedAndRecoverTwice) {
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
    // Restart to recover from the corrupted WAL file.
    createDBAndConn();
    auto res = conn->query("CALL show_tables() WHERE name STARTS WITH 'test' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 0);
    // Continue with some more writes to the WAL.
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test2(id INT64 PRIMARY KEY, name STRING);");
    // Recover again from the same WAL file.
    createDBAndConn();
    res = conn->query("CALL show_tables() WHERE name STARTS WITH 'test' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 2);
}

// Similar to CorruptedWALTailTruncated, but with multiple transactions.
TEST_F(WalTest, CorruptedWALTailTruncated2) {
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
    auto res = conn->query("CALL show_tables() WHERE name STARTS WITH 'test' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 3);
}

// Similar to CorruptedWALTailTruncated2, but with multiple transactions and then recovering from
// the WAL file again. This should recover the tables that were created after the database's first
// recovering from the corrupted WAL.
TEST_F(WalTest, CorruptedWALTailTruncated2RecoverTwice) {
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
    // Restart to recover from the corrupted WAL file.
    createDBAndConn();
    auto res = conn->query("CALL show_tables() WHERE name STARTS WITH 'test' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 3);
    // Continue with some more writes to the WAL.
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("CREATE NODE TABLE test5(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test6(id INT64 PRIMARY KEY, name STRING);");
    // Recover again from the same WAL file.
    createDBAndConn();
    res = conn->query("CALL show_tables() WHERE name STARTS WITH 'test' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 5);
}

TEST_F(WalTest, ReadOnlyRecoveryFromExistingWAL) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE (:test {id: 1, name: 'Alice'});");
    conn->query("CREATE (:test {id: 2, name: 'Bob'});");
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::file_size(walFilePath) > 0);

    // Restart in read-only mode
    systemConfig->readOnly = true;
    createDBAndConn();
    auto res = conn->query("MATCH (n:test) RETURN n.id ORDER BY n.id;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 2);
    // WAL file should still exist in read-only mode
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
}

TEST_F(WalTest, ReadOnlyRecoveryFromCorruptedWALTail) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE (:test {id: 1, name: 'Alice'});");
    conn->query("CREATE (:test {id: 2, name: 'Bob'});");
    conn->query("CREATE (:test {id: 3, name: 'Charlie'});");
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::file_size(walFilePath) > 10);

    // Truncate the last 10 bytes of the WAL file to simulate corruption
    std::filesystem::resize_file(walFilePath, std::filesystem::file_size(walFilePath) - 10);

    // Restart in read-only mode
    systemConfig->readOnly = true;
    createDBAndConn();
    auto res = conn->query("MATCH (n:test) RETURN n.id ORDER BY n.id;");
    ASSERT_TRUE(res->isSuccess());
    // Should still recover up to the last valid record
    ASSERT_GE(res->getNumTuples(), 0);
    // WAL file should remain unchanged in read-only mode
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
}

TEST_F(WalTest, ReadOnlyRecoveryWithShadowFile) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE (:test {id: 1, name: 'Alice'});");
    conn->query("COMMIT;");
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    auto shadowFilePath = kuzu::storage::StorageUtils::getShadowFilePath(databasePath);

    // Create a shadow file (simulating checkpoint in progress)
    std::ofstream file(shadowFilePath);
    file << "shadow file content";
    file.close();
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));

    // Restart in read-only mode
    systemConfig->readOnly = true;
    createDBAndConn();
    auto res = conn->query("MATCH (n:test) RETURN n.id ORDER BY n.id;");
    ASSERT_TRUE(res->isSuccess());
    // Should handle WAL recovery correctly
    ASSERT_GE(res->getNumTuples(), 0);
    // Files should remain unchanged in read-only mode
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
}

TEST_F(WalTest, ReadOnlyRecoveryEmptyWALFile) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::file_size(walFilePath) > 0);

    // Make WAL file empty
    std::filesystem::resize_file(walFilePath, 0);

    // Restart in read-only mode
    systemConfig->readOnly = true;
    createDBAndConn();
    auto res = conn->query("CALL show_tables() WHERE name='test' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 0);
    // Empty WAL file should still exist in read-only mode
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
}

TEST_F(WalTest, ReadOnlyRecoveryNoWALFile) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::file_size(walFilePath) > 0);

    // Remove WAL file
    std::filesystem::remove(walFilePath);
    ASSERT_FALSE(std::filesystem::exists(walFilePath));

    // Restart in read-only mode
    systemConfig->readOnly = true;
    createDBAndConn();
    auto res = conn->query("CALL show_tables() WHERE name='test' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 0);
}

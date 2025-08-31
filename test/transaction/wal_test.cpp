#include <fstream>

#include "api_test/api_test.h"
#include "common/exception/runtime.h"
#include "common/exception/storage.h"
#include "gmock/gmock.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;
using namespace kuzu::testing;
using namespace kuzu::transaction;

class WalTest : public ApiTest {
protected:
    void SetUp() override {
        ApiTest::SetUp();
        // Most of these tests are checking if partial recovery works correctly
        systemConfig->throwOnWalReplayFailure = false;
    }

    void testStrayWALFile(const std::function<void()>& setupNewDBFunc);
    void setupChecksumMismatchTest(std::function<void(std::ofstream&)> corruptFunc);
};

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

void WalTest::setupChecksumMismatchTest(std::function<void(std::ofstream&)> corruptFunc) {
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("BEGIN TRANSACTION;");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test2(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test3(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test4(id INT64 PRIMARY KEY, name STRING);");
    conn->query("COMMIT;");
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    // rewrite part of the wal
    std::ofstream file(walFilePath, std::ios_base::in | std::ios_base::out | std::ios_base::ate);
    ASSERT_TRUE(std::filesystem::file_size(walFilePath) > 13);
    corruptFunc(file);
    file.close();
}

// Simulation of a corrupted WAL tail by changing some data, this should trigger a checksum failure
TEST_F(WalTest, CorruptedWALChecksumMismatchInHeader) {
    if (inMemMode || systemConfig->checkpointThreshold == 0 || !systemConfig->enableChecksums) {
        GTEST_SKIP();
    }
    systemConfig->throwOnWalReplayFailure = true;
    createDBAndConn();
    setupChecksumMismatchTest([](std::ofstream& walFileToCorrupt) {
        walFileToCorrupt.seekp(10);
        // 10 bytes in will be the database ID's checksum
        walFileToCorrupt << "abc";
    });
    EXPECT_THROW(createDBAndConn();, kuzu::common::StorageException);
}

TEST_F(WalTest, CorruptedWALChecksumMismatchInHeaderNoThrow) {
    if (inMemMode || systemConfig->checkpointThreshold == 0 || !systemConfig->enableChecksums) {
        GTEST_SKIP();
    }
    setupChecksumMismatchTest([](std::ofstream& walFileToCorrupt) {
        walFileToCorrupt.seekp(10);
        // 10 bytes in will be the database ID's checksum
        walFileToCorrupt << "abc";
    });

    // The replay shouldn't complete but shouldn't throw either
    createDBAndConn();
    auto result = conn->query("match (t:test) return count(*)");
    EXPECT_FALSE(result->isSuccess());
}

TEST_F(WalTest, CorruptedWALChecksumMismatchInBody) {
    if (inMemMode || systemConfig->checkpointThreshold == 0 || !systemConfig->enableChecksums) {
        GTEST_SKIP();
    }
    systemConfig->throwOnWalReplayFailure = true;
    createDBAndConn();
    setupChecksumMismatchTest([](std::ofstream& walFileToCorrupt) {
        walFileToCorrupt.seekp(30);
        walFileToCorrupt << "abc";
    });
    EXPECT_THROW(createDBAndConn();, kuzu::common::StorageException);
}

TEST_F(WalTest, CorruptedWALChecksumMismatchInBodyNoThrow) {
    if (inMemMode || systemConfig->checkpointThreshold == 0 || !systemConfig->enableChecksums) {
        GTEST_SKIP();
    }
    setupChecksumMismatchTest([](std::ofstream& walFileToCorrupt) {
        walFileToCorrupt.seekp(10);
        // 10 bytes in will be the database ID's checksum
        walFileToCorrupt << "abc";
    });
    // The replay shouldn't complete but shouldn't throw either
    createDBAndConn();
    auto result = conn->query("match (t:test) return count(*)");
    EXPECT_FALSE(result->isSuccess());
    EXPECT_STREQ("Binder exception: Table test does not exist.", result->getErrorMessage().c_str());
}

TEST_F(WalTest, WALChecksumConfigMismatch) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("call force_checkpoint_on_close=false")->isSuccess());
    ASSERT_TRUE(conn->query("call auto_checkpoint=false")->isSuccess());
    ASSERT_TRUE(conn->query("create node table test1(id int64 primary key)")->isSuccess());
    ASSERT_TRUE(conn->query("create node table test2(id int64 primary key)")->isSuccess());
    systemConfig->enableChecksums = !systemConfig->enableChecksums;
    systemConfig->throwOnWalReplayFailure = true;
    EXPECT_THROW(
        {
            try {
                createDBAndConn();
            } catch (std::exception& e) {
                EXPECT_THAT(e.what(),
                    testing::AnyOf(testing::StartsWith(
                                       "Runtime exception: The database you are trying to open "
                                       "was serialized with enableChecksums=True but you are "
                                       "trying to open it with enableChecksums=False."),
                        testing::StartsWith(
                            "Runtime exception: The database you are trying to open "
                            "was serialized with enableChecksums=False but you are "
                            "trying to open it with enableChecksums=True.")));
                throw;
            }
        },
        kuzu::common::RuntimeException);
}

TEST_F(WalTest, WALChecksumConfigMismatchNoThrow) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("call force_checkpoint_on_close=false")->isSuccess());
    ASSERT_TRUE(conn->query("call auto_checkpoint=false")->isSuccess());
    ASSERT_TRUE(conn->query("create node table test1(id int64 primary key)")->isSuccess());
    ASSERT_TRUE(conn->query("create node table test2(id int64 primary key)")->isSuccess());
    systemConfig->enableChecksums = !systemConfig->enableChecksums;
    systemConfig->throwOnWalReplayFailure = false;
    // We have throwOnWalReplayFailure=false so we essentially skip the replay
    createDBAndConn();
    auto res = conn->query("CALL show_tables() WHERE name STARTS WITH 'test' RETURN *;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 0);
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

TEST_F(WalTest, WALFileLeftoverFromPreviousDBNewReadOnlyDB) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test2(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test3(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test4(id INT64 PRIMARY KEY, name STRING);");

    // Delete the DB file but keep the WAL
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    conn.reset();
    database.reset();
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::exists(databasePath));
    std::filesystem::remove(databasePath);

    // Recreate the DB then close it
    ASSERT_FALSE(std::filesystem::exists(databasePath));
    auto createEmptyDB = [&]() {
        database = std::make_unique<kuzu::main::Database>(databasePath, *systemConfig);
    };
    // When opening an empty read-only DB we don't write the header
    // This shouldn't cause any crashes when deserializing
    systemConfig->readOnly = true;
    // Creating a new empty DB file bypasses the empty read-only DB check
    std::ofstream ofs(databasePath);
    ofs.close();

    // When loading the DB, replaying should fail
    EXPECT_THROW(createEmptyDB(), RuntimeException);
}

void WalTest::testStrayWALFile(const std::function<void()>& setupNewDBFunc) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }

    conn->query("CALL force_checkpoint_on_close=false");
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test2(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test3(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE test4(id INT64 PRIMARY KEY, name STRING);");

    // Delete the DB file but keep the WAL
    auto walFilePath = kuzu::storage::StorageUtils::getWALFilePath(databasePath);
    conn.reset();
    database.reset();
    ASSERT_TRUE(std::filesystem::exists(walFilePath));
    ASSERT_TRUE(std::filesystem::exists(databasePath));
    std::filesystem::remove(databasePath);

    // temporarily rename the WAL file so that replay doesn't immediately trigger
    auto tmpWALPath = walFilePath + "__";
    std::filesystem::rename(walFilePath, tmpWALPath);

    // Recreate the DB then close it
    createDBAndConn();
    setupNewDBFunc();
    conn.reset();
    database.reset();

    // Rename the WAL to the original
    ASSERT_FALSE(std::filesystem::exists(walFilePath));
    std::filesystem::rename(tmpWALPath, walFilePath);

    // When loading the DB, replaying should fail
    EXPECT_THROW(createDBAndConn(), RuntimeException);
}

TEST_F(WalTest, WALFileLeftoverFromPreviousDBExistingDB) {
    testStrayWALFile([]() {});
}

TEST_F(WalTest, WALFileLeftoverFromPreviousDBNewDBCOPYWithoutCheckpoint) {
    testStrayWALFile([this]() {
        conn->query("CALL force_checkpoint_on_close=false");
        conn->query("create node table Comment (id int64, creationDate INT64, locationIP STRING, "
                    "browserUsed STRING, content STRING, length INT32, PRIMARY KEY (id));");
        conn->query(stringFormat("COPY Comment FROM '{}/dataset/ldbc-sf01/Comment.csv'",
            KUZU_ROOT_DIRECTORY));
    });
}

TEST_F(WalTest, WALFileLeftoverFromPreviousDBNewDBCOPYWithoutCheckpointReadOnly) {
    testStrayWALFile([this]() {
        conn->query("CALL force_checkpoint_on_close=false");
        conn->query("create node table Comment (id int64, creationDate INT64, locationIP STRING, "
                    "browserUsed STRING, content STRING, length INT32, PRIMARY KEY (id));");
        conn->query(stringFormat("COPY Comment FROM '{}/dataset/ldbc-sf01/Comment.csv'",
            KUZU_ROOT_DIRECTORY));
        systemConfig->readOnly = true;
    });
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

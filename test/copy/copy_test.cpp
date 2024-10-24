#include "common/file_system/virtual_file_system.h"
#include "common/string_format.h"
#include "graph_test/base_graph_test.h"
#include "graph_test/graph_test.h"
#include "main/database.h"
#include "storage/buffer_manager/buffer_manager.h"

namespace kuzu {
namespace testing {

class FlakyBufferManager : public storage::BufferManager {
public:
    FlakyBufferManager(const std::string& databasePath, const std::string& spillToDiskPath,
        uint64_t bufferPoolSize, uint64_t maxDBSize, common::VirtualFileSystem* vfs, bool readOnly,
        uint64_t& failureFrequency)
        : storage::BufferManager(databasePath, spillToDiskPath, bufferPoolSize, maxDBSize, vfs,
              readOnly),
          failureFrequency(failureFrequency) {}

    bool reserve(uint64_t sizeToReserve) override {
        reserveCount = (reserveCount + 1) % failureFrequency;
        if (reserveCount == 0) {
            failureFrequency *= 2;
            return false;
        }
        return storage::BufferManager::reserve(sizeToReserve);
    }

    uint64_t& failureFrequency;
    uint64_t reserveCount = 0;
};

class CopyTest : public BaseGraphTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        failureFrequency = 32;
    }

    void resetDB(uint64_t bufferPoolSize) {
        systemConfig->bufferPoolSize = bufferPoolSize;
        conn.reset();
        database.reset();
        createDBAndConn();
    }
    void resetDBFlaky() {
        database.reset();
        conn.reset();
        systemConfig->bufferPoolSize = main::SystemConfig{}.bufferPoolSize;
        auto constructBMFunc = [&](const main::Database& db) {
            return std::unique_ptr<storage::BufferManager>(new FlakyBufferManager(databasePath,
                getFileSystem(db)->joinPath(databasePath, "copy.tmp"), systemConfig->bufferPoolSize,
                systemConfig->maxDBSize, getFileSystem(db), systemConfig->readOnly,
                failureFrequency));
        };
        database = BaseGraphTest::constructDB(databasePath, *systemConfig, constructBMFunc);
        conn = std::make_unique<main::Connection>(database.get());
    }
    std::string getInputDir() override { KU_UNREACHABLE; }
    uint64_t failureFrequency;
};

TEST_F(CopyTest, RelCopyOutOfMemoryRecovery) {
    if (inMemMode) {
        GTEST_SKIP();
    }
    // Needs to be small enough that we cannot successfully complete the rel table copy
    resetDB(64 * 1024 * 1024);
    conn->query("CREATE NODE TABLE account(ID INT64, PRIMARY KEY(ID))");
    conn->query("CREATE REL TABLE follows(FROM account TO account);");
    {
        auto result = conn->query(common::stringFormat(
            "COPY account FROM \"{}/dataset/snap/twitter/csv/twitter-nodes.csv\"",
            KUZU_ROOT_DIRECTORY));
        ASSERT_TRUE(result->isSuccess()) << result->toString();
        result = conn->query(common::stringFormat(
            "COPY follows FROM '{}/dataset/snap/twitter/csv/twitter-edges.csv' (DELIM=' ')",
            KUZU_ROOT_DIRECTORY));
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(),
            "Buffer manager exception: Unable to allocate memory! The buffer pool is full and no "
            "memory could be freed!");
    }
    // Try opening then closing the database
    resetDB(256 * 1024 * 1024);
    // Try again with a larger buffer pool size
    resetDB(256 * 1024 * 1024);
    {
        auto result = conn->query(common::stringFormat(
            "COPY follows FROM '{}/dataset/snap/twitter/csv/twitter-edges.csv' (DELIM=' ')",
            KUZU_ROOT_DIRECTORY));
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        // Test that the table copied as expected after the query
        result = conn->query("MATCH (a:account)-[:follows]->(b:account) RETURN COUNT(*)");
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        ASSERT_TRUE(result->hasNext());
        ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 2420766);
    }
}

TEST_F(CopyTest, RelCopyBMExceptionRecovery) {
    if (inMemMode) {
        GTEST_SKIP();
    }
    resetDB(64 * 1024 * 1024);
    conn->query("CREATE NODE TABLE account(ID INT64, PRIMARY KEY(ID))");
    conn->query("CREATE REL TABLE follows(FROM account TO account);");
    {
        auto result = conn->query(common::stringFormat(
            "COPY account FROM \"{}/dataset/snap/twitter/csv/twitter-nodes.csv\"",
            KUZU_ROOT_DIRECTORY));
        ASSERT_TRUE(result->isSuccess()) << result->toString();
    }
    for (int i = 0;; i++) {
        ASSERT_LT(i, 20);

        resetDBFlaky();
        resetDBFlaky();
        auto result = conn->query(common::stringFormat(
            "COPY follows FROM '{}/dataset/snap/twitter/csv/twitter-edges.csv' (DELIM=' ')",
            KUZU_ROOT_DIRECTORY));
        if (!result->isSuccess()) {
            ASSERT_EQ(result->getErrorMessage(), "Buffer manager exception: Unable to allocate "
                                                 "memory! The buffer pool is full and no "
                                                 "memory could be freed!");
        } else {
            // the copy shouldn't succeed first try
            ASSERT_GT(i, 0);
            break;
        }
    }
    // Try opening then closing the database
    resetDB(256 * 1024 * 1024);
    // Try again with a larger buffer pool size
    resetDB(256 * 1024 * 1024);
    {
        // If BM exception happens during checkpoint the values from the COPY actually persist in
        // the table So we just test to see that there are >0 tuples in the table and no
        // crashes/assertion failures occur
        auto result = conn->query("MATCH (a:account)-[:follows]->(b:account) RETURN COUNT(*)");
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        ASSERT_TRUE(result->hasNext());
        ASSERT_GT(result->getNext()->getValue(0)->getValue<int64_t>(), 0);
    }
}

TEST_F(CopyTest, NodeCopyBMExceptionRecoverySameConnection) {
    if (inMemMode) {
        GTEST_SKIP();
    }
    static constexpr uint64_t dbSize = 64 * 1024 * 1024;
    resetDB(dbSize);
    conn->query("CREATE NODE TABLE account(ID INT64, PRIMARY KEY(ID))");
    resetDBFlaky();
    bool copyCommitted = false;
    for (int i = 0;; i++) {
        ASSERT_LT(i, 20);

        const auto queryString =
            copyCommitted ?
                "CHECKPOINT" :
                common::stringFormat(
                    "COPY account FROM \"{}/dataset/snap/twitter/csv/twitter-nodes.csv\"",
                    KUZU_ROOT_DIRECTORY);

        auto result = conn->query(queryString);
        if (!result->isSuccess()) {
            if (result->getErrorMessage().starts_with(
                    "Copy exception: Found duplicated primary key")) {
                // previous BM exception was during checkpoint so the copy was already committed
                copyCommitted = true;
                continue;
            }
            ASSERT_EQ(result->getErrorMessage(), "Buffer manager exception: Unable to allocate "
                                                 "memory! The buffer pool is full and no "
                                                 "memory could be freed!");
        } else {
            // the copy shouldn't succeed first try
            ASSERT_GT(i, 0);
            break;
        }
    }

    // Reopen the DB so no spurious errors occur during the query
    resetDB(dbSize);
    {
        // Test that the table copied as expected after the query
        auto result = conn->query("MATCH (a:account) RETURN COUNT(*)");
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        ASSERT_TRUE(result->hasNext());
        ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 81306);
    }
}

TEST_F(CopyTest, NodeCopyBMExceptionRecoveryResetDB) {
    if (inMemMode) {
        GTEST_SKIP();
    }
    static constexpr uint64_t dbSize = 64 * 1024 * 1024;
    resetDB(dbSize);
    conn->query("CREATE NODE TABLE account(ID INT64, PRIMARY KEY(ID))");
    for (int i = 0;; i++) {
        ASSERT_LT(i, 20);

        resetDBFlaky();
        resetDBFlaky();
        auto result = conn->query(common::stringFormat(
            "COPY account FROM \"{}/dataset/snap/twitter/csv/twitter-nodes.csv\"",
            KUZU_ROOT_DIRECTORY));
        if (!result->isSuccess()) {
            ASSERT_EQ(result->getErrorMessage(), "Buffer manager exception: Unable to allocate "
                                                 "memory! The buffer pool is full and no "
                                                 "memory could be freed!");
        } else {
            // the copy shouldn't succeed first try
            ASSERT_GT(i, 0);
            break;
        }
    }
    // Try opening then closing the database
    resetDB(dbSize);
    // Try again with a larger buffer pool size
    resetDB(dbSize);

    {
        // Test that the table copied as expected after the query
        auto result = conn->query("MATCH (a:account) RETURN COUNT(*)");
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        ASSERT_TRUE(result->hasNext());
        ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 81306);
    }
}

TEST_F(CopyTest, OutOfMemoryRecoveryDropTable) {
    if (inMemMode) {
        GTEST_SKIP();
    }
    // Needs to be small enough that we cannot successfully complete the rel table copy
    resetDB(64 * 1024 * 1024);
    conn->query("CREATE NODE TABLE account(ID INT64, PRIMARY KEY(ID))");
    conn->query("CREATE REL TABLE follows(FROM account TO account);");
    {
        auto result = conn->query(common::stringFormat(
            "COPY account FROM \"{}/dataset/snap/twitter/csv/twitter-nodes.csv\"",
            KUZU_ROOT_DIRECTORY));
        ASSERT_TRUE(result->isSuccess()) << result->toString();
        result = conn->query(common::stringFormat(
            "COPY follows FROM '{}/dataset/snap/twitter/csv/twitter-edges.csv' (DELIM=' ')",
            KUZU_ROOT_DIRECTORY));
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(),
            "Buffer manager exception: Unable to allocate memory! The buffer pool is full and no "
            "memory could be freed!");
    }
    // Try dropping the table before trying again with a larger buffer pool size
    resetDB(256 * 1024 * 1024);
    {
        auto result = conn->query("DROP TABLE follows;");
        ASSERT_TRUE(result->isSuccess()) << result->toString();
        result = conn->query("CREATE REL TABLE follows(FROM account TO account);");
        ASSERT_TRUE(result->isSuccess()) << result->toString();
        result = conn->query(common::stringFormat(
            "COPY follows FROM '{}/dataset/snap/twitter/csv/twitter-edges.csv' (DELIM=' ')",
            KUZU_ROOT_DIRECTORY));
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        // Test that the table copied as expected after the query
        result = conn->query("MATCH (a:account)-[:follows]->(b:account) RETURN COUNT(*)");
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        ASSERT_TRUE(result->hasNext());
        ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 2420766);
    }
}
} // namespace testing
} // namespace kuzu

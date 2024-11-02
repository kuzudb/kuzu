#include "common/file_system/virtual_file_system.h"
#include "common/string_format.h"
#include "graph_test/base_graph_test.h"
#include "graph_test/graph_test.h"
#include "main/database.h"
#include "storage/buffer_manager/buffer_manager.h"

namespace kuzu {
namespace testing {

// TODO(Royi) add tests that use this once enough issues are fixed so that the tests can pass
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

TEST_F(CopyTest, DISABLED_OutOfMemoryRecovery) {
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

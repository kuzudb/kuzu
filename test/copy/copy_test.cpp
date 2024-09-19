#include "common/string_format.h"
#include "graph_test/base_graph_test.h"
#include "graph_test/graph_test.h"
#include "main/database.h"

namespace kuzu {
namespace testing {

// TODO: set buffer pool size
class CopyTest : public BaseGraphTest {
public:
    void resetDB(uint64_t bufferPoolSize) {
        systemConfig->bufferPoolSize = bufferPoolSize;
        conn.reset();
        database.reset();
        createDBAndConn();
    }
    std::string getInputDir() override { KU_UNREACHABLE; }
};

TEST_F(CopyTest, OutOfMemoryRecovery) {
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

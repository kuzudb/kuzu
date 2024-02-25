#include <thread>

#ifdef _WIN32
#include <windows.h>
#endif

#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::testing;

TEST_F(ApiTest, BasicConnect) {
    ApiTest::assertMatchPersonCountStar(conn.get());
}

static void parallel_query(Connection* conn) {
    for (auto i = 0u; i < 100; ++i) {
        ApiTest::assertMatchPersonCountStar(conn);
    }
}

TEST_F(ApiTest, ParallelQuerySingleConnect) {
    const auto numThreads = 20u;
    std::thread threads[numThreads];
    for (auto i = 0u; i < numThreads; ++i) {
        threads[i] = std::thread(parallel_query, conn.get());
    }
    for (auto i = 0u; i < numThreads; ++i) {
        threads[i].join();
    }
}

static void parallel_connect(Database* database) {
    auto conn = std::make_unique<Connection>(database);
    ApiTest::assertMatchPersonCountStar(conn.get());
}

TEST_F(ApiTest, ParallelConnect) {
    const auto numThreads = 5u;
    std::thread threads[numThreads];
    for (auto i = 0u; i < numThreads; ++i) {
        threads[i] = std::thread(parallel_connect, database.get());
    }
    for (auto i = 0u; i < numThreads; ++i) {
        threads[i].join();
    }
}

TEST_F(ApiTest, CommitRollbackRemoveActiveTransaction) {
    ASSERT_TRUE(conn->query("BEGIN TRANSACTION;")->isSuccess());
    ASSERT_TRUE(conn->query("ROLLBACK;")->isSuccess());
    ASSERT_TRUE(conn->query("BEGIN TRANSACTION READ ONLY;")->isSuccess());
    ASSERT_TRUE(conn->query("COMMIT;")->isSuccess());
}

TEST_F(ApiTest, BeginningMultipleTransactionErrors) {
    ASSERT_TRUE(conn->query("BEGIN TRANSACTION;")->isSuccess());
    ASSERT_FALSE(conn->query("BEGIN TRANSACTION")->isSuccess());
    ASSERT_TRUE(conn->query("BEGIN TRANSACTION READ ONLY")->isSuccess());
    ASSERT_FALSE(conn->query("BEGIN TRANSACTION READ ONLY")->isSuccess());
}

// These two tests are designed to make sure that the explain and profile statements don't create a
// segmentation fault.
TEST_F(ApiTest, Explain) {
    auto result = conn->query("EXPLAIN MATCH (a:person)-[:knows]->(b:person), "
                              "(b)-[:knows]->(a) RETURN a.fName, b.fName ORDER BY a.ID");
    ASSERT_TRUE(result->isSuccess());
}

TEST_F(ApiTest, Profile) {
    auto result =
        conn->query("EXPLAIN MATCH (a:person) WHERE EXISTS { MATCH (a)-[:knows]->(b:person) WHERE "
                    "b.fName='Farooq' } RETURN a.ID, min(a.age)");
    ASSERT_TRUE(result->isSuccess());
}

TEST_F(ApiTest, Interrupt) {
    std::thread longRunningQueryThread(executeLongRunningQuery, conn.get());
#ifdef _WIN32
    Sleep(1000);
#else
    sleep(1 /* sleep 1 second before interrupt the query */);
#endif
    conn->interrupt();
    longRunningQueryThread.join();
}

TEST_F(ApiTest, TimeOut) {
    conn->setQueryTimeOut(1000 /* timeoutInMS */);
    auto result = conn->query("MATCH (a:person)-[:knows*1..28]->(b:person) RETURN COUNT(*);");
    ASSERT_FALSE(result->isSuccess());
    ASSERT_EQ(result->getErrorMessage(), "Interrupted.");
}

TEST_F(ApiTest, MultipleQueryExplain) {
    auto result = conn->query("EXPLAIN MATCH (a:person)-[:knows]->(b:person), "
                              "(b)-[:knows]->(a) RETURN a.fName, b.fName ORDER BY a.ID; MATCH "
                              "(a:person) RETURN a.fName;");
    ASSERT_TRUE(result->isSuccess());
}

TEST_F(ApiTest, MultipleQuery) {
    auto result = conn->query("");
    ASSERT_EQ(result->getErrorMessage(), "Connection Exception: Query is empty.");

    result = conn->query("MATCH (a:A)\n"
                         "            MATCH (a)-[:LIKES..]->(c)\n"
                         "            RETURN c.name;");
    ASSERT_FALSE(result->isSuccess());

    result = conn->query(
        "MATCH (a:person) RETURN a.fName; MATCH (a:person)-[:knows]->(b:person) RETURN count(*);");
    ASSERT_TRUE(result->isSuccess());

    result =
        conn->query("CREATE NODE TABLE N(ID INT64, PRIMARY KEY(ID));CREATE REL TABLE E(FROM N TO "
                    "N, MANY_MANY);MATCH (a:N)-[:E]->(b:N) WHERE a.ID = 0 return b.ID;");
    ASSERT_TRUE(result->isSuccess());
}

TEST_F(ApiTest, Prepare) {
    auto result = conn->prepare("");
    ASSERT_EQ(result->getErrorMessage(), "Connection Exception: Query is empty.");

    result =
        conn->prepare("CREATE NODE TABLE N(ID INT64, PRIMARY KEY(ID));CREATE REL TABLE E(FROM N TO "
                      "N, MANY_MANY);MATCH (a:N)-[:E]->(b:N) WHERE a.ID = 0 return b.ID;");
    ASSERT_EQ(result->getErrorMessage(),
        "Connection Exception: We do not support prepare multiple statements.");
}
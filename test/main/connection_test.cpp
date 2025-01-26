#include <memory>
#include <thread>

#include "main/connection.h"
#include "main/database.h"

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

#ifndef __SINGLE_THREADED__
// The following two tests are disabled in single-threaded mode because they
// require multiple threads to run.
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

TEST_F(ApiTest, Interrupt) {
    std::thread longRunningQueryThread(executeLongRunningQuery, conn.get());
    std::atomic<bool> finished_executing{false};
    std::thread interruptingThread([&]() {
        while (!finished_executing) {
#ifdef _WIN32
            Sleep(1000);
#else
            sleep(1 /* sleep 1 second before interrupt the query */);
#endif
            conn->interrupt();
        }
    });
    longRunningQueryThread.join();
    finished_executing = true;
    interruptingThread.join();
}
#endif

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

TEST_F(ApiTest, TimeOut) {
    conn->setQueryTimeOut(1000 /* timeoutInMS */);
    auto result = conn->query(
        "UNWIND RANGE(1,100000) AS x UNWIND RANGE(1, 100000) AS y RETURN COUNT(x + y);");
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
    ASSERT_EQ(result->getErrorMessage(), "Connection exception: Query is empty.");

    result = conn->query("MATCH (a:A)\n"
                         "            MATCH (a)-[:LIKES..]->(c)\n"
                         "            RETURN c.name;");
    ASSERT_FALSE(result->isSuccess());

    result = conn->query(
        "MATCH (a:person) RETURN a.fName; MATCH (a:person)-[:knows]->(b:person) RETURN count(*);");
    ASSERT_TRUE(result->isSuccess());

    result = conn->query("CREATE NODE TABLE Test(name STRING, age INT64, PRIMARY KEY(name));CREATE "
                         "(:Test {name: 'Alice', age: 25});"
                         "MATCH (a:Test) where a.name='Alice' return a.age;");
    ASSERT_TRUE(result->isSuccess());

    result = conn->query("return 1; return 2; return 3;");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_EQ(result->toString(), "1\n1\n");
    ASSERT_TRUE(result->hasNextQueryResult());
    ASSERT_EQ(result->getNextQueryResult()->toString(), "2\n2\n");
    ASSERT_TRUE(result->hasNextQueryResult());
    ASSERT_EQ(result->getNextQueryResult()->toString(), "3\n3\n");
}

TEST_F(ApiTest, SingleQueryHasNextQueryResult) {
    auto result = conn->query("MATCH (a:person) RETURN a.fName;");
    ASSERT_TRUE(result->isSuccess());
    ASSERT_FALSE(result->hasNextQueryResult());
}

TEST_F(ApiTest, Prepare) {
    auto result = conn->prepare("");
    ASSERT_EQ(result->getErrorMessage(), "Connection exception: Query is empty.");

    result =
        conn->prepare("CREATE NODE TABLE N(ID INT64, PRIMARY KEY(ID));CREATE REL TABLE E(FROM N TO "
                      "N, MANY_MANY);MATCH (a:N)-[:E]->(b:N) WHERE a.ID = 0 return b.ID;");
    ASSERT_EQ(result->getErrorMessage(),
        "Connection Exception: We do not support prepare multiple statements.");
}

TEST_F(ApiTest, PrepareWithLimit) {
    auto prepared = conn->prepare("MATCH (p:person) RETURN p.ID limit $lm");
    auto result = conn->execute(prepared.get(), std::make_pair(std::string{"lm"}, 3));
    ASSERT_TRUE(result->isSuccess());
    std::vector<std::string> expectedResult = {"0", "2", "3"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), expectedResult);
    result = conn->execute(prepared.get(), std::make_pair(std::string{"lm"}, 5));
    ASSERT_TRUE(result->isSuccess());
    expectedResult = {"0", "2", "3", "5", "7"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), expectedResult);
}

TEST_F(ApiTest, PrepareWithSkip) {
    auto prepared = conn->prepare("MATCH (p:person) RETURN p.ID skip $sp");
    auto result = conn->execute(prepared.get(), std::make_pair(std::string{"sp"}, 2));
    ASSERT_TRUE(result->isSuccess());
    std::vector<std::string> expectedResult = {"10", "3", "5", "7", "8", "9"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), expectedResult);
    result = conn->execute(prepared.get(), std::make_pair(std::string{"sp"}, 4));
    ASSERT_TRUE(result->isSuccess());
    expectedResult = {"10", "7", "8", "9"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), expectedResult);
}

TEST_F(ApiTest, PrepareWithSkipAndLimit) {
    auto prepared = conn->prepare("MATCH (p:person) RETURN p.ID skip $sp limit $lm");
    auto result = conn->execute(prepared.get(), std::make_pair(std::string{"sp"}, 2),
        std::make_pair(std::string{"lm"}, 5));
    ASSERT_TRUE(result->isSuccess());
    std::vector<std::string> expectedResult = {"3", "5", "7", "8", "9"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), expectedResult);
    result = conn->execute(prepared.get(), std::make_pair(std::string{"sp"}, 4),
        std::make_pair(std::string{"lm"}, 2));
    ASSERT_TRUE(result->isSuccess());
    expectedResult = {"7", "8"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), expectedResult);
}

TEST_F(ApiTest, PrepareWithSkipLimitError) {
    auto prepared = conn->prepare("MATCH (p:person) RETURN p.ID skip $sp");
    auto result = conn->execute(prepared.get());
    ASSERT_FALSE(result->isSuccess());
    ASSERT_EQ(result->toString(),
        "Runtime exception: The number of rows to skip is a parameter. Please give a "
        "valid skip number.");

    prepared = conn->prepare("MATCH (p:person) RETURN p.ID limit $sp");
    result = conn->execute(prepared.get());
    ASSERT_FALSE(result->isSuccess());
    ASSERT_EQ(result->toString(),
        "Runtime exception: The number of rows to limit is a parameter. Please give a "
        "valid limit number.");

    prepared = conn->prepare("MATCH (p:person) RETURN p.ID skip $s limit $sp");
    result = conn->execute(prepared.get(), std::make_pair(std::string("s"), 3));
    ASSERT_FALSE(result->isSuccess());
    ASSERT_EQ(result->toString(), "Runtime exception: The number of rows to limit is a parameter. "
                                  "Please give a valid limit number.");

    prepared = conn->prepare("MATCH (p:person) RETURN p.ID skip $s");
    result = conn->execute(prepared.get(), std::make_pair(std::string("s"), 3.4));
    ASSERT_FALSE(result->isSuccess());
    ASSERT_EQ(result->toString(),
        "Runtime exception: The number of rows to skip/limit must be a non-negative integer.");
}

TEST_F(ApiTest, CreateTableAfterClosingDatabase) {
    database.reset();
    database = std::make_unique<Database>(databasePath, *systemConfig);
    conn = std::make_unique<Connection>(database.get());

    auto result = conn->query("CREATE NODE TABLE Test(name STRING, age INT64, PRIMARY KEY(name));");
    ASSERT_TRUE(result->isSuccess()) << result->toString();
    result = conn->query("CREATE (:Test {name: 'Alice', age: 25});"
                         "MATCH (a:Test) where a.name='Alice' return a.age;");
    ASSERT_TRUE(result->isSuccess()) << result->toString();
}

TEST_F(ApiTest, QueryWithHeadingNewline) {
    createDBAndConn();
    ASSERT_TRUE(conn->query("\n PROFILE RETURN 5; \n")->isSuccess());
}

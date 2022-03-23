#include <thread>

#include "test/test_utility/include/test_helper.h"

#include "src/main/include/graphflowdb.h"

using namespace graphflow::testing;
using namespace graphflow::main;

TEST_F(ApiTest, basic_connect) {
    auto result = conn->query("MATCH (a:person) RETURN COUNT(*)");
    ASSERT_TRUE(result->hasNext());
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->val.int64Val, 8);
    ASSERT_FALSE(result->hasNext());
}

static void parallel_query(Connection* conn) {
    for (auto i = 0u; i < 100; ++i) {
        auto result = conn->query("MATCH (a:person) RETURN COUNT(*)");
        auto tuple = result->getNext();
        ASSERT_EQ(tuple->getValue(0)->val.int64Val, 8);
    }
}

TEST_F(ApiTest, parallel_query_single_connect) {
    auto numThreads = 20u;
    thread threads[numThreads];
    for (auto i = 0u; i < numThreads; ++i) {
        threads[i] = thread(parallel_query, conn.get());
    }
    for (auto i = 0u; i < numThreads; ++i) {
        threads[i].join();
    }
}

static void parallel_connect(Database* database) {
    auto conn = make_unique<Connection>(database);
    auto result = conn->query("MATCH (a:person) RETURN COUNT(*)");
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->val.int64Val, 8);
}

TEST_F(ApiTest, parallel_connect) {
    auto numThreads = 5u;
    thread threads[numThreads];
    for (auto i = 0u; i < numThreads; ++i) {
        threads[i] = thread(parallel_connect, database.get());
    }
    for (auto i = 0u; i < numThreads; ++i) {
        threads[i].join();
    }
}

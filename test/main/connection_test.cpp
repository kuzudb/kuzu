#include <thread>

#include "include/main_test_helper.h"

TEST_F(ApiTest, BasicConnect) {
    ApiTest::assertMatchPersonCountStar(conn.get());
}

static void parallel_query(Connection* conn) {
    for (auto i = 0u; i < 100; ++i) {
        ApiTest::assertMatchPersonCountStar(conn);
    }
}

TEST_F(ApiTest, ParallelQuerySingleConnect) {
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
    ApiTest::assertMatchPersonCountStar(conn.get());
}

TEST_F(ApiTest, ParallelConnect) {
    auto numThreads = 5u;
    thread threads[numThreads];
    for (auto i = 0u; i < numThreads; ++i) {
        threads[i] = thread(parallel_connect, database.get());
    }
    for (auto i = 0u; i < numThreads; ++i) {
        threads[i].join();
    }
}

TEST_F(ApiTest, TransactionModes) {
    // Test initially connections are in AUTO_COMMIT mode.
    ASSERT_EQ(Connection::ConnectionTransactionMode::AUTO_COMMIT, conn->getTransactionMode());
    // Test beginning a transaction (first in read only mode) sets mode to MANUAL automatically.
    conn->beginReadOnlyTransaction();
    ASSERT_EQ(Connection::ConnectionTransactionMode::MANUAL, conn->getTransactionMode());
    // Test commit automatically switches the mode to AUTO_COMMIT read transaction
    conn->commit();
    ASSERT_EQ(Connection::ConnectionTransactionMode::AUTO_COMMIT, conn->getTransactionMode());

    conn->beginReadOnlyTransaction();
    ASSERT_EQ(Connection::ConnectionTransactionMode::MANUAL, conn->getTransactionMode());
    // Test rollback automatically switches the mode to AUTO_COMMIT for read transaction
    conn->rollback();
    ASSERT_EQ(Connection::ConnectionTransactionMode::AUTO_COMMIT, conn->getTransactionMode());

    // Test beginning a transaction (now in write mode) sets mode to MANUAL automatically.
    conn->beginWriteTransaction();
    ASSERT_EQ(Connection::ConnectionTransactionMode::MANUAL, conn->getTransactionMode());
    // Test commit automatically switches the mode to AUTO_COMMIT for write transaction
    conn->commit();
    ASSERT_EQ(Connection::ConnectionTransactionMode::AUTO_COMMIT, conn->getTransactionMode());

    // Test beginning a transaction (now in write mode) sets mode to MANUAL automatically.
    conn->beginWriteTransaction();
    ASSERT_EQ(Connection::ConnectionTransactionMode::MANUAL, conn->getTransactionMode());
    // Test rollback automatically switches the mode to AUTO_COMMIT write transaction
    conn->rollback();
    ASSERT_EQ(Connection::ConnectionTransactionMode::AUTO_COMMIT, conn->getTransactionMode());
}

TEST_F(ApiTest, MultipleCallsFromSameTransaction) {
    conn->beginReadOnlyTransaction();
    auto activeTransactionID = conn->getActiveTransactionID();
    conn->query("MATCH (a:person) RETURN COUNT(*)");
    ASSERT_EQ(activeTransactionID, conn->getActiveTransactionID());
    conn->query("MATCH (a:person) RETURN COUNT(*)");
    ASSERT_EQ(activeTransactionID, conn->getActiveTransactionID());
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)");
    conn->execute(preparedStatement.get(), make_pair(string("1"), true));
    ASSERT_EQ(activeTransactionID, conn->getActiveTransactionID());
    conn->commit();
    ASSERT_FALSE(conn->hasActiveTransaction());
}

TEST_F(ApiTest, CommitRollbackRemoveActiveTransaction) {
    conn->beginWriteTransaction();
    ASSERT_TRUE(conn->hasActiveTransaction());
    conn->rollback();
    ASSERT_FALSE(conn->hasActiveTransaction());
    conn->beginReadOnlyTransaction();
    ASSERT_TRUE(conn->hasActiveTransaction());
    conn->commit();
    ASSERT_FALSE(conn->hasActiveTransaction());
}

TEST_F(ApiTest, BeginningMultipleTransactionErrors) {
    conn->beginWriteTransaction();
    try {
        conn->beginWriteTransaction();
        FAIL();
    } catch (ConnectionException& e) {}
    try {
        conn->beginReadOnlyTransaction();
        FAIL();
    } catch (ConnectionException& e) {}

    conn->rollback();
    conn->beginReadOnlyTransaction();
    try {
        conn->beginWriteTransaction();
        FAIL();
    } catch (ConnectionException& e) {}

    try {
        conn->beginReadOnlyTransaction();
        FAIL();
    } catch (ConnectionException& e) {}
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

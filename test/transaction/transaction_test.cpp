#include <fstream>

#include "api_test/api_test.h"
#include "api_test/private_api_test.h"
#include "storage/storage_utils.h"
#include "storage/wal/wal.h"

using namespace kuzu::common;
using namespace kuzu::testing;
using namespace kuzu::transaction;

TEST_F(PrivateApiTest, TransactionModes) {
    // Test initially connections are in AUTO_COMMIT mode.
    ASSERT_EQ(TransactionMode::AUTO, getTransactionMode(*conn));
    // Test beginning a transaction (first in read-only mode) sets mode to MANUAL automatically.
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

TEST_F(PrivateApiTest, CloseConnectionWithActiveTransaction) {
    conn->query("BEGIN TRANSACTION;");
    ASSERT_TRUE(hasActiveTransaction(*conn));
    conn->query("MATCH (a:person) SET a.age=10;");
    conn.reset();
    conn = std::make_unique<kuzu::main::Connection>(database.get());
    conn->query("BEGIN TRANSACTION;");
    auto res = conn->query("MATCH (a:person) WHERE a.age=10 RETURN COUNT(*) AS count;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto count = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(count, 0); // The previous transaction was rolled back.
}

TEST_F(PrivateApiTest, CloseDatabaseWithActiveTransaction) {
    if (inMemMode) {
        GTEST_SKIP();
    }
    conn->query("BEGIN TRANSACTION;");
    ASSERT_TRUE(hasActiveTransaction(*conn));
    conn->query("MATCH (a:person) SET a.age=10;");
    conn.reset();
    database.reset();
    createDBAndConn();
    conn->query("BEGIN TRANSACTION;");
    auto res = conn->query("MATCH (a:person) WHERE a.age=10 RETURN COUNT(*) AS count;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto count = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(count, 0); // The previous transaction was rolled back.
}

class EmptyDBTransactionTest : public EmptyDBTest {
protected:
    void SetUp() override {
        EmptyDBTest::SetUp();
        systemConfig->maxDBSize = 1024ull * 1024 * 1024 * 1024;
        systemConfig->bufferPoolSize = 1024 * 1024 * 1024;
        createDBAndConn();
    }

    void TearDown() override { EmptyDBTest::TearDown(); }
};

TEST_F(EmptyDBTransactionTest, DatabaseFilesAfterCheckpoint) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL auto_checkpoint=false;");
    ASSERT_FALSE(
        std::filesystem::exists(kuzu::storage::StorageUtils::getTmpFilePath(databasePath)));
    ASSERT_FALSE(
        std::filesystem::exists(kuzu::storage::StorageUtils::getShadowFilePath(databasePath)));
    ASSERT_FALSE(
        std::filesystem::exists(kuzu::storage::StorageUtils::getWALFilePath(databasePath)));
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    ASSERT_TRUE(std::filesystem::exists(kuzu::storage::StorageUtils::getWALFilePath(databasePath)));
    conn->query("CHECKPOINT;");
    ASSERT_FALSE(
        std::filesystem::exists(kuzu::storage::StorageUtils::getTmpFilePath(databasePath)));
    ASSERT_FALSE(
        std::filesystem::exists(kuzu::storage::StorageUtils::getShadowFilePath(databasePath)));
    ASSERT_FALSE(
        std::filesystem::exists(kuzu::storage::StorageUtils::getWALFilePath(databasePath)));
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
}

#ifndef __SINGLE_THREADED__
static void insertNodes(uint64_t startID, uint64_t num, kuzu::main::Database& database) {
    auto conn = std::make_unique<kuzu::main::Connection>(&database);
    for (uint64_t i = 0; i < num; ++i) {
        auto id = startID + i;
        auto res = conn->query(stringFormat("CREATE (:test {id: {}, name: 'Person{}'});", id, id));
        ASSERT_TRUE(res->isSuccess())
            << "Failed to insert test" << id << ": " << res->getErrorMessage();
    }
}

TEST_F(EmptyDBTransactionTest, ConcurrentNodeInsertions) {
    if (systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL debug_enable_multi_writes=true;");
    auto numThreads = 4;
    auto numInsertsPerThread = 1000;
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
    std::vector<std::thread> threads;
    for (auto i = 0; i < numThreads; ++i) {
        threads.emplace_back(insertNodes, i * numInsertsPerThread, numInsertsPerThread,
            std::ref(*database));
    }
    for (auto& thread : threads) {
        thread.join();
    }
    auto numTotalInsertions = numThreads * numInsertsPerThread;
    auto res = conn->query("MATCH (a:test) RETURN COUNT(a) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto count = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(count, numTotalInsertions);
    res = conn->query("MATCH (a:test) RETURN SUM(a.id) AS SUM_ID;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto sumID = res->getNext()->getValue(0)->getValue<int128_t>();
    ASSERT_EQ(sumID, (numTotalInsertions * (numTotalInsertions - 1)) / 2);
}

static void insertNodesWithMixedTypes(uint64_t startID, uint64_t num,
    kuzu::main::Database& database) {
    auto conn = std::make_unique<kuzu::main::Connection>(&database);
    for (auto i = 0u; i < num; ++i) {
        auto id = startID + i;
        auto score = 95.5 + (id % 10);
        auto isActive = (id % 2 == 0) ? "true" : "false";
        auto res = conn->query(
            stringFormat("CREATE (:mixed_test {id: {}, score: {}, active: {}, name: 'User{}'});",
                id, score, isActive, id));
        ASSERT_TRUE(res->isSuccess())
            << "Failed to insert mixed_test" << id << ": " << res->getErrorMessage();
    }
}

TEST_F(EmptyDBTransactionTest, ConcurrentNodeInsertionsMixedTypes) {
    if (systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL debug_enable_multi_writes=true;");
    auto numThreads = 4;
    auto numInsertsPerThread = 1000;
    conn->query("CREATE NODE TABLE mixed_test(id INT64 PRIMARY KEY, score DOUBLE, active BOOLEAN, "
                "name STRING);");
    std::vector<std::thread> threads;
    for (auto i = 0; i < numThreads; ++i) {
        threads.emplace_back(insertNodesWithMixedTypes, i * numInsertsPerThread,
            numInsertsPerThread, std::ref(*database));
    }
    for (auto& thread : threads) {
        thread.join();
    }
    auto numTotalInsertions = numThreads * numInsertsPerThread;
    auto res = conn->query("MATCH (a:mixed_test) RETURN COUNT(a) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto count = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(count, numTotalInsertions);

    res =
        conn->query("MATCH (a:mixed_test) WHERE a.active = true RETURN COUNT(a) AS ACTIVE_COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto activeCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(activeCount, numTotalInsertions / 2);
}

static void insertRelationships(uint64_t startID, uint64_t num, kuzu::main::Database& database) {
    auto conn = std::make_unique<kuzu::main::Connection>(&database);
    for (auto i = 0u; i < num; ++i) {
        auto fromID = startID + i;
        auto toID = (startID + i + 1) % (num * 4);
        auto weight = 1.0 + (i % 10) * 0.1;
        auto res = conn->query(stringFormat("MATCH (a:person), (b:person) WHERE a.id = {} AND b.id "
                                            "= {} CREATE (a)-[:knows {weight: {}}]->(b);",
            fromID, toID, weight));
        ASSERT_TRUE(res->isSuccess()) << "Failed to insert relationship from " << fromID << " to "
                                      << toID << ": " << res->getErrorMessage();
    }
}

TEST_F(EmptyDBTransactionTest, ConcurrentRelationshipInsertions) {
    if (systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL debug_enable_multi_writes=true;");
    auto numThreads = 4;
    auto numInsertsPerThread = 2000;
    auto numTotalInsertions = numThreads * numInsertsPerThread;

    conn->query("CREATE NODE TABLE person(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);");

    conn->query("BEGIN TRANSACTION;");
    for (auto i = 0; i < numTotalInsertions; ++i) {
        auto res = conn->query(stringFormat("CREATE (:person {id: {}, name: 'Person{}'});", i, i));
        ASSERT_TRUE(res->isSuccess());
    }
    conn->query("COMMIT;");

    std::vector<std::thread> threads;
    for (auto i = 0; i < numThreads; ++i) {
        threads.emplace_back(insertRelationships, i * numInsertsPerThread, numInsertsPerThread,
            std::ref(*database));
    }
    for (auto& thread : threads) {
        thread.join();
    }

    auto res = conn->query("MATCH ()-[r:knows]->() RETURN COUNT(r) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto count = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(count, numTotalInsertions);

    res = conn->query("MATCH ()-[r:knows]->() RETURN AVG(r.weight) AS AVG_WEIGHT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto avgWeight = res->getNext()->getValue(0)->getValue<double>();
    ASSERT_GT(avgWeight, 1.0);
    ASSERT_LT(avgWeight, 2.0);
}

static void insertComplexRelationships(uint64_t startID, uint64_t num,
    kuzu::main::Database& database) {
    auto conn = std::make_unique<kuzu::main::Connection>(&database);
    for (auto i = 0u; i < num; ++i) {
        auto userID = startID + i;
        auto productID = (startID + i) % (num * 2);
        auto rating = 1 + (i % 5);
        auto isVerified = (i % 3 == 0) ? "true" : "false";
        auto res =
            conn->query(stringFormat("MATCH (u:user), (p:product) WHERE u.id = {} AND p.id = {} "
                                     "CREATE (u)-[:rates {rating: {}, verified: {}}]->(p);",
                userID, productID, rating, isVerified));
        ASSERT_TRUE(res->isSuccess())
            << "Failed to insert rating from user " << userID << " to product " << productID << ": "
            << res->getErrorMessage();
    }
}

TEST_F(EmptyDBTransactionTest, ConcurrentComplexRelationshipInsertions) {
    if (systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL debug_enable_multi_writes=true;");
    auto numThreads = 3;
    auto numInsertsPerThread = 1500;
    auto numTotalInsertions = numThreads * numInsertsPerThread;

    conn->query("CREATE NODE TABLE user(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE NODE TABLE product(id INT64 PRIMARY KEY, title STRING);");
    conn->query("CREATE REL TABLE rates(FROM user TO product, rating INT64, verified BOOLEAN);");

    conn->query("BEGIN TRANSACTION;");
    for (auto i = 0; i < numTotalInsertions; ++i) {
        auto res = conn->query(stringFormat("CREATE (:user {id: {}, name: 'User{}'});", i, i));
        ASSERT_TRUE(res->isSuccess());
    }
    for (auto i = 0; i < numTotalInsertions * 2; ++i) {
        auto res =
            conn->query(stringFormat("CREATE (:product {id: {}, title: 'Product{}'});", i, i));
        ASSERT_TRUE(res->isSuccess());
    }
    conn->query("COMMIT;");

    std::vector<std::thread> threads;
    for (auto i = 0; i < numThreads; ++i) {
        threads.emplace_back(insertComplexRelationships, i * numInsertsPerThread,
            numInsertsPerThread, std::ref(*database));
    }
    for (auto& thread : threads) {
        thread.join();
    }

    auto res = conn->query("MATCH ()-[r:rates]->() RETURN COUNT(r) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto count = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(count, numTotalInsertions);

    res = conn->query(
        "MATCH ()-[r:rates]->() WHERE r.verified = true RETURN COUNT(r) AS VERIFIED_COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto verifiedCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(verifiedCount, numTotalInsertions / 3);
}

static void updateNodes(uint64_t startID, uint64_t num, kuzu::main::Database& database) {
    auto conn = std::make_unique<kuzu::main::Connection>(&database);
    for (uint64_t i = 0; i < num; ++i) {
        auto id = startID + i;
        auto newName = stringFormat("UPerson{}", id);
        auto res = conn->query(
            stringFormat("MATCH (n:test) WHERE n.id = {} SET n.name = '{}';", id, newName));
        ASSERT_TRUE(res->isSuccess())
            << "Failed to update test" << id << ": " << res->getErrorMessage();
    }
}

TEST_F(EmptyDBTransactionTest, ConcurrentNodeUpdates) {
    if (systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL debug_enable_multi_writes=true;");
    auto numThreads = 4;
    auto numUpdatesPerThread = 3000;
    auto numTotalNodes = numThreads * numUpdatesPerThread;

    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");

    // First insert all nodes
    for (auto i = 0; i < numTotalNodes; ++i) {
        auto res = conn->query(stringFormat("CREATE (:test {id: {}, name: 'Person{}'});", i, i));
        ASSERT_TRUE(res->isSuccess());
    }

    // Verify initial state
    auto res = conn->query("MATCH (a:test) RETURN COUNT(a) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto initialCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(initialCount, numTotalNodes);

    // Update concurrently
    std::vector<std::thread> threads;
    for (auto i = 0; i < numThreads; ++i) {
        threads.emplace_back(updateNodes, i * numUpdatesPerThread, numUpdatesPerThread,
            std::ref(*database));
    }
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all nodes were updated
    res =
        conn->query("MATCH (a:test) WHERE a.name STARTS WITH 'UPerson' RETURN COUNT(a) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto updatedCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(updatedCount, numTotalNodes);
}

static void updateMixedTypeNodes(uint64_t startID, uint64_t num, kuzu::main::Database& database) {
    auto conn = std::make_unique<kuzu::main::Connection>(&database);
    for (auto i = 0u; i < num; ++i) {
        auto id = startID + i;
        auto newScore = 100.0 + (id % 20);
        auto newActive = (id % 3 == 0) ? "false" : "true";
        auto newName = stringFormat("UpdatedUser{}", id);
        auto res = conn->query(stringFormat(
            "MATCH (n:mixed_test) WHERE n.id = {} SET n.score = {}, n.active = {}, n.name = '{}';",
            id, newScore, newActive, newName));
        ASSERT_TRUE(res->isSuccess())
            << "Failed to update mixed_test" << id << ": " << res->getErrorMessage();
    }
}

TEST_F(EmptyDBTransactionTest, ConcurrentMixedTypeUpdates) {
    if (systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL debug_enable_multi_writes=true;");
    auto numThreads = 4;
    auto numUpdatesPerThread = 2500;
    auto numTotalNodes = numThreads * numUpdatesPerThread;

    conn->query("CREATE NODE TABLE mixed_test(id INT64 PRIMARY KEY, score DOUBLE, active BOOLEAN, "
                "name STRING);");

    // First insert all nodes with initial values
    for (auto i = 0; i < numTotalNodes; ++i) {
        auto score = 95.5 + (i % 10);
        auto isActive = (i % 2 == 0) ? "true" : "false";
        auto res = conn->query(
            stringFormat("CREATE (:mixed_test {id: {}, score: {}, active: {}, name: 'User{}'});", i,
                score, isActive, i));
        ASSERT_TRUE(res->isSuccess());
    }

    // Verify initial state
    auto res = conn->query("MATCH (a:mixed_test) RETURN COUNT(a) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto initialCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(initialCount, numTotalNodes);

    // Update concurrently
    std::vector<std::thread> threads;
    for (auto i = 0; i < numThreads; ++i) {
        threads.emplace_back(updateMixedTypeNodes, i * numUpdatesPerThread, numUpdatesPerThread,
            std::ref(*database));
    }
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all nodes were updated
    res = conn->query(
        "MATCH (a:mixed_test) WHERE a.name STARTS WITH 'UpdatedUser' RETURN COUNT(a) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto updatedCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(updatedCount, numTotalNodes);

    // Verify score updates (all should be >= 100.0)
    res = conn->query("MATCH (a:mixed_test) WHERE a.score >= 100.0 RETURN COUNT(a) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto scoreUpdatedCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(scoreUpdatedCount, numTotalNodes);

    // Verify boolean updates (distribution should be different from initial)
    res =
        conn->query("MATCH (a:mixed_test) WHERE a.active = false RETURN COUNT(a) AS FALSE_COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto falseCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(falseCount, 3334);
}

static void updateRelationships(uint64_t startID, uint64_t num, kuzu::main::Database& database) {
    auto conn = std::make_unique<kuzu::main::Connection>(&database);
    for (auto i = 0u; i < num; ++i) {
        auto fromID = startID + i;
        auto toID = (startID + i + 1) % (num * 4);
        auto newWeight = 10.0 + (i % 5) * 2.0;
        auto res = conn->query(stringFormat("MATCH (a:person)-[r:knows]->(b:person) WHERE a.id = "
                                            "{} AND b.id = {} SET r.weight = {};",
            fromID, toID, newWeight));
        ASSERT_TRUE(res->isSuccess()) << "Failed to update relationship from " << fromID << " to "
                                      << toID << ": " << res->getErrorMessage();
    }
}

TEST_F(EmptyDBTransactionTest, ConcurrentRelationshipUpdates) {
    if (systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL debug_enable_multi_writes=true;");
    auto numThreads = 4;
    auto numUpdatesPerThread = 1500;
    auto numTotalUpdates = numThreads * numUpdatesPerThread;

    conn->query("CREATE NODE TABLE person(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);");

    // Create nodes
    for (auto i = 0; i < numTotalUpdates; ++i) {
        auto res = conn->query(stringFormat("CREATE (:person {id: {}, name: 'Person{}'});", i, i));
        ASSERT_TRUE(res->isSuccess());
    }

    // Create relationships
    for (auto i = 0; i < numTotalUpdates; ++i) {
        auto fromID = i;
        auto toID = (i + 1) % numTotalUpdates;
        auto weight = 1.0 + (i % 10) * 0.1;
        auto res = conn->query(stringFormat("MATCH (a:person), (b:person) WHERE a.id = {} AND b.id "
                                            "= {} CREATE (a)-[:knows {weight: {}}]->(b);",
            fromID, toID, weight));
        ASSERT_TRUE(res->isSuccess());
    }

    // Verify initial relationships
    auto res = conn->query("MATCH ()-[r:knows]->() RETURN COUNT(r) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto initialCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(initialCount, numTotalUpdates);

    // Update relationships concurrently
    std::vector<std::thread> threads;
    for (auto i = 0; i < numThreads; ++i) {
        threads.emplace_back(updateRelationships, i * numUpdatesPerThread, numUpdatesPerThread,
            std::ref(*database));
    }
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all relationships were updated (all weights should be >= 10.0)
    res = conn->query("MATCH ()-[r:knows]->() WHERE r.weight >= 10.0 RETURN COUNT(r) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto updatedCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(updatedCount, numTotalUpdates);
}

static void updateNodesWithMixedTransactions(uint64_t startID, uint64_t num, bool shouldCommit,
    kuzu::main::Database& database) {
    auto conn = std::make_unique<kuzu::main::Connection>(&database);
    conn->query("BEGIN TRANSACTION;");
    for (uint64_t i = 0; i < num; ++i) {
        auto id = startID + i;
        auto newName = stringFormat("TxPerson{}", id);
        auto res = conn->query(
            stringFormat("MATCH (n:test) WHERE n.id = {} SET n.name = '{}';", id, newName));
        ASSERT_TRUE(res->isSuccess())
            << "Failed to update test" << id << ": " << res->getErrorMessage();
    }
    if (shouldCommit) {
        auto res = conn->query("COMMIT;");
        ASSERT_TRUE(res->isSuccess()) << "Failed to update commit:" << res->getErrorMessage();
    } else {
        auto res = conn->query("ROLLBACK;");
        ASSERT_TRUE(res->isSuccess()) << "Failed to update rollback:" << res->getErrorMessage();
    }
}

TEST_F(EmptyDBTransactionTest, ConcurrentNodeUpdatesWithMixedTransactions) {
    if (systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL debug_enable_multi_writes=true;");
    auto numThreads = 4;
    auto numUpdatesPerThread = 100;
    auto numTotalNodes = numThreads * numUpdatesPerThread;

    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");

    // Insert initial nodes
    for (auto i = 0; i < numTotalNodes; ++i) {
        auto res = conn->query(stringFormat("CREATE (:test {id: {}, name: 'Person{}'});", i, i));
        ASSERT_TRUE(res->isSuccess());
    }

    // Verify initial state
    auto res = conn->query("MATCH (a:test) RETURN COUNT(a) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto initialCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(initialCount, numTotalNodes);

    // Update concurrently with mixed transactions (half commit, half rollback)
    std::vector<std::thread> threads;
    for (auto i = 0; i < numThreads; ++i) {
        bool shouldCommit = (i % 2 == 0); // Even threads commit, odd threads rollback
        threads.emplace_back(updateNodesWithMixedTransactions, i * numUpdatesPerThread,
            numUpdatesPerThread, shouldCommit, std::ref(*database));
    }
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify only committed transactions persisted (half of the updates)
    res =
        conn->query("MATCH (a:test) WHERE a.name STARTS WITH 'TxPerson' RETURN COUNT(a) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto committedCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(committedCount, 200);

    // Verify rollback transactions didn't persist
    res = conn->query("MATCH (a:test) WHERE a.name STARTS WITH 'Person' RETURN COUNT(a) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto originalCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(originalCount, 200);
}

static void updateRelationshipsWithMixedTransactions(uint64_t startID, uint64_t num,
    bool shouldCommit, kuzu::main::Database& database) {
    auto conn = std::make_unique<kuzu::main::Connection>(&database);
    conn->query("BEGIN TRANSACTION;");
    for (auto i = 0u; i < num; ++i) {
        auto fromID = startID + i;
        auto toID = startID + i;
        auto newWeight = 200.0;
        auto res = conn->query(stringFormat("MATCH (a:person)-[r:knows]->(b:person) WHERE a.id = "
                                            "{} AND b.id = {} SET r.weight = {};",
            fromID, toID, newWeight));
        ASSERT_TRUE(res->isSuccess()) << "Failed to update relationship from " << fromID << " to "
                                      << toID << ": " << res->getErrorMessage();
    }
    if (shouldCommit) {
        conn->query("COMMIT;");
    } else {
        conn->query("ROLLBACK;");
    }
}

TEST_F(EmptyDBTransactionTest, ConcurrentRelationshipUpdatesWithMixedTransactions) {
    if (systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL debug_enable_multi_writes=true;");
    auto numThreads = 4;
    auto numUpdatesPerThread = 1000;
    auto numTotalUpdates = numThreads * numUpdatesPerThread;

    conn->query("CREATE NODE TABLE person(id INT64 PRIMARY KEY, name STRING);");
    conn->query("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);");

    // Create nodes
    for (auto i = 0; i < numTotalUpdates; ++i) {
        auto res = conn->query(stringFormat("CREATE (:person {id: {}, name: 'Person{}'});", i, i));
        ASSERT_TRUE(res->isSuccess());
    }

    // Create relationships with initial weights
    for (auto i = 0; i < numTotalUpdates; ++i) {
        auto fromID = i;
        auto toID = i;
        auto weight = 20.0;
        auto res = conn->query(stringFormat("MATCH (a:person), (b:person) WHERE a.id = {} AND b.id "
                                            "= {} CREATE (a)-[:knows {weight: {}}]->(b);",
            fromID, toID, weight));
        ASSERT_TRUE(res->isSuccess());
    }

    // Verify initial relationships
    auto res = conn->query("MATCH ()-[r:knows]->() RETURN COUNT(r) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto initialCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(initialCount, numTotalUpdates);

    // Update relationships with mixed transactions
    std::vector<std::thread> threads;
    for (auto i = 0; i < numThreads; ++i) {
        bool shouldCommit = (i % 3 != 0);
        threads.emplace_back(updateRelationshipsWithMixedTransactions, i * numUpdatesPerThread,
            numUpdatesPerThread, shouldCommit, std::ref(*database));
    }
    for (auto& thread : threads) {
        thread.join();
    }

    res = conn->query("MATCH ()-[r:knows]->() WHERE r.weight = 200.0 RETURN COUNT(r) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto committedCount = res->getNext()->getValue(0)->getValue<int64_t>();
    auto expectedCommitted = numTotalUpdates / 2;
    ASSERT_EQ(committedCount, expectedCommitted);

    res = conn->query("MATCH ()-[r:knows]->() WHERE r.weight = 20.0 RETURN COUNT(r) AS COUNT;");
    ASSERT_TRUE(res->isSuccess());
    ASSERT_EQ(res->getNumTuples(), 1);
    auto originalCount = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(originalCount, numTotalUpdates / 2);
}
#endif

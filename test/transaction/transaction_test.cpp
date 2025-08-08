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
    auto numInsertsPerThread = 10000;
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
    auto numInsertsPerThread = 5000;
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

    for (auto i = 0; i < numTotalInsertions; ++i) {
        auto res = conn->query(stringFormat("CREATE (:person {id: {}, name: 'Person{}'});", i, i));
        ASSERT_TRUE(res->isSuccess());
    }

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

    for (auto i = 0; i < numTotalInsertions; ++i) {
        auto res = conn->query(stringFormat("CREATE (:user {id: {}, name: 'User{}'});", i, i));
        ASSERT_TRUE(res->isSuccess());
    }
    for (auto i = 0; i < numTotalInsertions * 2; ++i) {
        auto res =
            conn->query(stringFormat("CREATE (:product {id: {}, title: 'Product{}'});", i, i));
        ASSERT_TRUE(res->isSuccess());
    }

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
#endif

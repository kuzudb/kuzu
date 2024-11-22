#include "common/file_system/virtual_file_system.h"
#include "common/string_format.h"
#include "graph_test/base_graph_test.h"
#include "graph_test/graph_test.h"
#include "main/database.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "transaction/transaction_manager.h"

namespace kuzu {
namespace testing {

class CopyTestHelper {
public:
    static std::vector<std::unique_ptr<storage::FileHandle>>& getBMFileHandles(
        storage::BufferManager* bm) {
        return bm->fileHandles;
    }
};

class FlakyBufferManager : public storage::BufferManager {
public:
    FlakyBufferManager(const std::string& databasePath, const std::string& spillToDiskPath,
        uint64_t bufferPoolSize, uint64_t maxDBSize, common::VirtualFileSystem* vfs, bool readOnly,
        uint64_t& failureFrequency, bool canFailDuringExecute, bool canFailDuringCheckpoint)
        : storage::BufferManager(databasePath, spillToDiskPath, bufferPoolSize, maxDBSize, vfs,
              readOnly),
          failureFrequency(failureFrequency), canFailDuringCheckpoint(canFailDuringCheckpoint),
          canFailDuringExecute(canFailDuringExecute) {}

    bool reserve(uint64_t sizeToReserve) override {
        const bool inCheckpoint =
            ctx && !ctx->getTransactionManagerUnsafe()->hasActiveWriteTransactionNoLock();
        const bool inCommit =
            !inCheckpoint && ctx && ctx->getTx()->getCommitTS() != common::INVALID_TRANSACTION;
        const bool inExecute = (!inCommit && !inCheckpoint);
        reserveCount = (reserveCount + 1) % failureFrequency;
        if ((canFailDuringCheckpoint || !inCheckpoint) && (canFailDuringExecute || !inExecute) &&
            reserveCount == 0) {
            failureFrequency *= 2;
            return false;
        }
        return storage::BufferManager::reserve(sizeToReserve);
    }

    void setClientContext(main::ClientContext* newCtx) { ctx = newCtx; }

    uint64_t& failureFrequency;
    main::ClientContext* ctx{nullptr};
    bool canFailDuringCheckpoint;
    bool canFailDuringExecute;
    uint64_t reserveCount = 0;
};

struct BMExceptionRecoveryTestConfig {
    bool canFailDuringExecute;
    bool canFailDuringCheckpoint;
    std::function<void(main::Connection*)> initFunc;
    std::function<std::unique_ptr<main::QueryResult>(main::Connection*, int)> executeFunc;
    std::function<bool(main::QueryResult*)> earlyExitOnFailureFunc;
    std::function<std::unique_ptr<main::QueryResult>(main::Connection*)> checkFunc;
    uint64_t checkResult;
};

class CopyTest : public BaseGraphTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        failureFrequency = 32;
    }

    void resetDB(uint64_t bufferPoolSize) {
        systemConfig->bufferPoolSize = bufferPoolSize;
        database.reset();
        conn.reset();
        createDBAndConn();
    }
    void resetDBFlaky(bool canFailDuringExecute = true, bool canFailDuringCheckpoint = true) {
        database.reset();
        conn.reset();
        systemConfig->bufferPoolSize = main::SystemConfig{}.bufferPoolSize;
        auto constructBMFunc = [&](const main::Database& db) {
            auto bm = std::unique_ptr<FlakyBufferManager>(new FlakyBufferManager(databasePath,
                getFileSystem(db)->joinPath(databasePath, "copy.tmp"), systemConfig->bufferPoolSize,
                systemConfig->maxDBSize, getFileSystem(db), systemConfig->readOnly,
                failureFrequency, canFailDuringExecute, canFailDuringCheckpoint));
            currentBM = bm.get();
            return bm;
        };
        database = BaseGraphTest::constructDB(databasePath, *systemConfig, constructBMFunc);
        conn = std::make_unique<main::Connection>(database.get());
        currentBM->setClientContext(conn->getClientContext());
    }
    std::string getInputDir() override { KU_UNREACHABLE; }
    void BMExceptionRecoveryTest(BMExceptionRecoveryTestConfig cfg);
    uint64_t failureFrequency;
    FlakyBufferManager* currentBM;
};

void CopyTest::BMExceptionRecoveryTest(BMExceptionRecoveryTestConfig cfg) {
    if (inMemMode) {
        GTEST_SKIP();
    }
    static constexpr uint64_t dbSize = 64 * 1024 * 1024;
    resetDB(dbSize);
    cfg.initFunc(conn.get());

    // this test only checks robustness during the transaction
    // we don't want to trigger BM exceptions during checkpoint
    // TODO(Royi) fix checkpointing so this test passes even if BM fails during checkpoint
    resetDBFlaky(cfg.canFailDuringExecute, cfg.canFailDuringCheckpoint);

    for (int i = 0;; i++) {
        ASSERT_LT(i, 20);
        auto result = cfg.executeFunc(conn.get(), i);
        if (!result->isSuccess()) {
            if (cfg.earlyExitOnFailureFunc(result.get())) {
                break;
            }
            ASSERT_EQ(result->getErrorMessage(), "Buffer manager exception: Unable to allocate "
                                                 "memory! The buffer pool is full and no "
                                                 "memory could be freed!");
        } else {
            break;
        }
    }

    // Reopen the DB so no spurious errors occur during the query
    resetDB(dbSize);
    {
        // Test that the table copied as expected after the query
        auto result = cfg.checkFunc(conn.get());
        ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
        ASSERT_TRUE(result->hasNext());
        ASSERT_EQ(cfg.checkResult, result->getNext()->getValue(0)->getValue<int64_t>());
    }
}

TEST_F(CopyTest, NodeCopyBMExceptionRecoverySameConnection) {
    BMExceptionRecoveryTestConfig cfg{.canFailDuringExecute = true,
        .canFailDuringCheckpoint = false,
        .initFunc =
            [](main::Connection* conn) {
                conn->query("CREATE NODE TABLE account(ID INT64, PRIMARY KEY(ID))");
            },
        .executeFunc =
            [](main::Connection* conn, int) {
                const auto queryString = common::stringFormat(
                    "COPY account FROM \"{}/dataset/snap/twitter/csv/twitter-nodes.csv\"",
                    KUZU_ROOT_DIRECTORY);

                return conn->query(queryString);
            },
        .earlyExitOnFailureFunc = [](main::QueryResult*) { return false; },
        .checkFunc =
            [](main::Connection* conn) { return conn->query("MATCH (a:account) RETURN COUNT(*)"); },
        .checkResult = 81306};
    BMExceptionRecoveryTest(cfg);
}

TEST_F(CopyTest, RelCopyBMExceptionRecoverySameConnection) {
    BMExceptionRecoveryTestConfig cfg{.canFailDuringExecute = true,
        .canFailDuringCheckpoint = false,
        .initFunc =
            [](main::Connection* conn) {
                conn->query("CREATE NODE TABLE account(ID INT64, PRIMARY KEY(ID))");
                conn->query("CREATE REL TABLE follows(FROM account TO account);");
                ASSERT_TRUE(conn->query(common::stringFormat(
                    "COPY account FROM \"{}/dataset/snap/twitter/csv/twitter-nodes.csv\"",
                    KUZU_ROOT_DIRECTORY)));
            },
        .executeFunc =
            [this](main::Connection* conn, int i) {
                // there are many allocations in the partitioning phase
                // we scale the failure frequency linearly so that we trigger at least one
                // allocation failure in the batch insert phase
                failureFrequency = 512 * (i + 15);

                return conn->query(common::stringFormat(
                    "COPY follows FROM '{}/dataset/snap/twitter/csv/twitter-edges.csv' (DELIM=' ')",
                    KUZU_ROOT_DIRECTORY));
            },
        .earlyExitOnFailureFunc =
            [this](main::QueryResult*) {
                // clear the BM so that the failure frequency isn't messed with by cached pages
                for (auto& fh : CopyTestHelper::getBMFileHandles(currentBM)) {
                    currentBM->removeFilePagesFromFrames(*fh);
                }
                return false;
            },
        .checkFunc =
            [](main::Connection* conn) {
                return conn->query("MATCH (a:account)-[:follows]->(b:account) RETURN COUNT(*)");
            },
        .checkResult = 2420766};
    BMExceptionRecoveryTest(cfg);
}

TEST_F(CopyTest, NodeInsertBMExceptionDuringCommitRecovery) {
    static constexpr uint64_t numValues = 200000;
    BMExceptionRecoveryTestConfig cfg{.canFailDuringExecute = false,
        .canFailDuringCheckpoint = false,
        .initFunc =
            [this](main::Connection* conn) {
                failureFrequency = 128;
                conn->query("CREATE NODE TABLE account(ID INT64, PRIMARY KEY(ID))");
            },
        .executeFunc =
            [](main::Connection* conn, int) {
                const auto queryString = common::stringFormat(
                    "UNWIND RANGE(1,{}) AS i CREATE (a:account {ID:i})", numValues);

                return conn->query(queryString);
            },
        .earlyExitOnFailureFunc = [](main::QueryResult*) { return false; },
        .checkFunc =
            [](main::Connection* conn) { return conn->query("MATCH (a:account) RETURN COUNT(*)"); },
        .checkResult = numValues};
    BMExceptionRecoveryTest(cfg);
}

TEST_F(CopyTest, RelInsertBMExceptionDuringCommitRecovery) {
    static constexpr auto numNodes = 10000;
    BMExceptionRecoveryTestConfig cfg{.canFailDuringExecute = false,
        .canFailDuringCheckpoint = false,
        .initFunc =
            [this](main::Connection* conn) {
                failureFrequency = 32;
                conn->query("CREATE NODE TABLE account(ID INT64, PRIMARY KEY(ID))");
                conn->query("CREATE REL TABLE follows(FROM account TO account);");
                const auto queryString = common::stringFormat(
                    "UNWIND RANGE(1,{}) AS i CREATE (a:account {ID:i})", numNodes);
                ASSERT_TRUE(conn->query(queryString)->isSuccess());
            },
        .executeFunc =
            [](main::Connection* conn, int) {
                return conn->query(common::stringFormat(
                    "UNWIND RANGE(1,{}) AS i MATCH (a:account), (b:account) WHERE a.ID = i AND "
                    "b.ID = i + 1 CREATE (a)-[f:follows]->(b)",
                    numNodes));
            },
        .earlyExitOnFailureFunc = [](main::QueryResult*) { return false; },
        .checkFunc =
            [](main::Connection* conn) {
                return conn->query("MATCH (a)-[f:follows]->(b) RETURN COUNT(*)");
            },
        .checkResult = numNodes - 1};
    BMExceptionRecoveryTest(cfg);
}

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

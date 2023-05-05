#pragma once

#include <cstring>

#include "common/file_utils.h"
#include "gtest/gtest.h"
#include "main/kuzu.h"
#include "parser/parser.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/planner.h"
#include "test_helper/test_helper.h"

using ::testing::Test;

namespace kuzu {
namespace testing {

enum class TransactionTestType : uint8_t {
    NORMAL_EXECUTION = 0,
    RECOVERY = 1,
};

class BaseGraphTest : public Test {
public:
    void SetUp() override {
        systemConfig = std::make_unique<main::SystemConfig>(
            common::BufferPoolConstants::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        if (common::FileUtils::fileOrPathExists(TestHelper::getTmpTestDir())) {
            common::FileUtils::removeDir(TestHelper::getTmpTestDir());
        }
        databasePath = TestHelper::getTmpTestDir();
    }

    virtual std::string getInputDir() = 0;

    void TearDown() override { common::FileUtils::removeDir(TestHelper::getTmpTestDir()); }

    void createDBAndConn();

    void initGraph();

    void commitOrRollbackConnection(bool isCommit, TransactionTestType transactionTestType) const;

protected:
    // Static functions to access Database's non-public properties/interfaces.
    static inline catalog::Catalog* getCatalog(main::Database& database) {
        return database.catalog.get();
    }
    static inline storage::StorageManager* getStorageManager(main::Database& database) {
        return database.storageManager.get();
    }
    static inline storage::BufferManager* getBufferManager(main::Database& database) {
        return database.bufferManager.get();
    }
    static inline storage::MemoryManager* getMemoryManager(main::Database& database) {
        return database.memoryManager.get();
    }
    static inline transaction::TransactionManager* getTransactionManager(main::Database& database) {
        return database.transactionManager.get();
    }
    static inline uint64_t getBMSize(main::Database& database) {
        return database.systemConfig.bufferPoolSize;
    }
    static inline storage::WAL* getWAL(main::Database& database) { return database.wal.get(); }
    static inline void commitAndCheckpointOrRollback(main::Database& database,
        transaction::Transaction* writeTransaction, bool isCommit,
        bool skipCheckpointForTestingRecovery = false) {
        database.commitAndCheckpointOrRollback(
            writeTransaction, isCommit, skipCheckpointForTestingRecovery);
    }
    static inline processor::QueryProcessor* getQueryProcessor(main::Database& database) {
        return database.queryProcessor.get();
    }

    // Static functions to access Connection's non-public properties/interfaces.
    static inline main::Connection::ConnectionTransactionMode getTransactionMode(
        main::Connection& connection) {
        return connection.getTransactionMode();
    }
    static inline void setTransactionModeNoLock(main::Connection& connection,
        main::Connection::ConnectionTransactionMode newTransactionMode) {
        connection.setTransactionModeNoLock(newTransactionMode);
    }
    static inline void commitButSkipCheckpointingForTestingRecovery(main::Connection& connection) {
        connection.commitButSkipCheckpointingForTestingRecovery();
    }
    static inline void rollbackButSkipCheckpointingForTestingRecovery(
        main::Connection& connection) {
        connection.rollbackButSkipCheckpointingForTestingRecovery();
    }
    static inline transaction::Transaction* getActiveTransaction(main::Connection& connection) {
        return connection.getActiveTransaction();
    }
    static inline uint64_t getMaxNumThreadForExec(main::Connection& connection) {
        return connection.getMaxNumThreadForExec();
    }
    static inline uint64_t getActiveTransactionID(main::Connection& connection) {
        return connection.getActiveTransactionID();
    }
    static inline bool hasActiveTransaction(main::Connection& connection) {
        return connection.hasActiveTransaction();
    }
    static inline void commitNoLock(main::Connection& connection) { connection.commitNoLock(); }
    static inline void rollbackIfNecessaryNoLock(main::Connection& connection) {
        connection.rollbackIfNecessaryNoLock();
    }
    static inline void sortAndCheckTestResults(
        std::vector<std::string>& actualResult, std::vector<std::string>& expectedResult) {
        sort(expectedResult.begin(), expectedResult.end());
        ASSERT_EQ(actualResult, expectedResult);
    }
    static inline bool containsOverflowFile(common::DataTypeID typeID) {
        return typeID == common::STRING || typeID == common::VAR_LIST;
    }

    void validateColumnFilesExistence(std::string fileName, bool existence, bool hasOverflow);

    void validateListFilesExistence(
        std::string fileName, bool existence, bool hasOverflow, bool hasHeader);

    void validateNodeColumnFilesExistence(
        catalog::NodeTableSchema* nodeTableSchema, common::DBFileType dbFileType, bool existence);

    void validateRelColumnAndListFilesExistence(
        catalog::RelTableSchema* relTableSchema, common::DBFileType dbFileType, bool existence);

    void validateQueryBestPlanJoinOrder(std::string query, std::string expectedJoinOrder);

    void commitOrRollbackConnectionAndInitDBIfNecessary(
        bool isCommit, TransactionTestType transactionTestType);

private:
    void validateRelPropertyFiles(catalog::RelTableSchema* relTableSchema,
        common::RelDirection relDirection, bool isColumnProperty, common::DBFileType dbFileType,
        bool existence);

public:
    std::string databasePath;
    std::unique_ptr<main::SystemConfig> systemConfig;
    std::unique_ptr<main::Database> database;
    std::unique_ptr<main::Connection> conn;
};

// This class starts database without initializing graph.
class EmptyDBTest : public BaseGraphTest {
    std::string getInputDir() override { throw common::NotImplementedException("getInputDir()"); }
};

// This class starts database in on-disk mode.
class DBTest : public BaseGraphTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        createDBAndConn();
        initGraph();
    }

    inline void runTest(const std::string& queryFile) {
        auto queryConfigs = TestHelper::parseTestFile(queryFile);
        ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
    }

    inline void runTestAndCheckOrder(const std::string& queryFile) {
        auto queryConfigs = TestHelper::parseTestFile(queryFile, true /* checkOutputOrder */);
        for (auto& queryConfig : queryConfigs) {
            queryConfig->checkOutputOrder = true;
        }
        ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
    }
};

} // namespace testing
} // namespace kuzu

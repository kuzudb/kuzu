#include "graph_test/graph_test.h"

#include "binder/binder.h"
#include "graph_test/base_graph_test.h"
#include "spdlog/spdlog.h"
#include "storage/storage_manager.h"
#include "test_runner/insert_by_row.h"
#include "test_runner/multi_copy_split.h"
#include "test_runner/test_runner.h"
#include "transaction/transaction_manager.h"

using ::testing::Test;
using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace testing {

void DBTest::createDB(uint64_t checkpointWaitTimeout) {
    if (database != nullptr) {
        database.reset();
    }
    database = std::make_unique<main::Database>(databasePath, *systemConfig);
    getTransactionManager(*database)->setCheckPointWaitTimeoutForTransactionsToLeaveInMicros(
        checkpointWaitTimeout /* 10ms */);
    spdlog::set_level(spdlog::level::info);
}

void DBTest::createNewDB() {
    database.reset();
    conn.reset();
    removeDir(databasePath);
    createDBAndConn();
}

void DBTest::runTest(const std::vector<std::unique_ptr<TestStatement>>& statements,
    uint64_t checkpointWaitTimeout, std::set<std::string> connNames) {
    for (const auto& connName : connNames) {
        concurrentTests.try_emplace(connName, connectionsPaused, *connMap[connName], databasePath);
    }
    for (auto& statement : statements) {
        // special for testing import and export test cases
        if (statement->removeFileFlag) {
            auto filePath = statement->removeFilePath;
            filePath.erase(std::remove(filePath.begin(), filePath.end(), '\"'), filePath.end());
            removeDir(filePath);
            continue;
        }
        if (statement->importDBFlag) {
            auto filePath = statement->importFilePath;
            filePath.erase(std::remove(filePath.begin(), filePath.end(), '\"'), filePath.end());
            createNewDB();
            BaseGraphTest::setIEDatabasePath(filePath);
            continue;
        }
        if (statement->reloadDBFlag) {
            // For in-mem mode, we skip reload.
            if (!inMemMode) {
                createDB(checkpointWaitTimeout);
                createConns(connNames);
            }
            continue;
        }
        if (statement->connectionsStatusFlag == ConcurrentStatusFlag::BEGIN) {
            for (auto& concurrentTest : concurrentTests) {
                concurrentTest.second.reset();
            }
            isConcurrent = true;
            connectionsPaused = true;
            continue;
        }
        if (statement->connectionsStatusFlag == ConcurrentStatusFlag::END) {
            for (auto& concurrentTest : concurrentTests) {
                concurrentTest.second.execute();
            }
            isConcurrent = false;
            connectionsPaused = false;
            for (auto& concurrentTest : concurrentTests) {
                concurrentTest.second.join();
            }
            continue;
        }
        if (statement->manualUseDataset == ManualUseDatasetFlag::SCHEMA) {
            auto dataset = TestHelper::appendKuzuRootPath("dataset/" + statement->dataset);
            if (conn) {
                TestHelper::executeScript(dataset + "/" + TestHelper::SCHEMA_FILE_NAME, *conn);
            } else {
                TestHelper::executeScript(dataset + "/" + TestHelper::SCHEMA_FILE_NAME,
                    *(connMap.begin()->second));
            }
            continue;
        }
        if (statement->manualUseDataset == ManualUseDatasetFlag::INSERT) {
            auto& connection = conn ? *conn : *(connMap.begin()->second);
            InsertDatasetByRow insert(statement->dataset, connection);
            insert.init();
            insert.run();
            continue;
        }
        if (statement->multiCopySplits > 0) {
            auto& connection = conn ? *conn : *(connMap.begin()->second);
            SplitMultiCopyRandom split(statement->multiCopySplits, statement->multiCopyTable,
                statement->multiCopySource, connection);
            if (statement->seed.size() == 2) {
                split.setSeed(statement->seed);
            }
            split.init();
            split.run();
            continue;
        }
        if (conn) {
            TestRunner::runTest(statement.get(), *conn, databasePath);
        } else {
            auto connName = *statement->connName;
            if (isConcurrent) {
                concurrentTests.at(connName).addStatement(statement.get());
            } else {
                TestRunner::runTest(statement.get(), *connMap[connName], databasePath);
            }
        }
    }
}

void ConcurrentTestExecutor::runStatements() {
    while (connectionPaused) {}
    for (auto statement : statements) {
        TestRunner::runTest(statement, connection, databasePath);
    }
}

} // namespace testing
} // namespace kuzu

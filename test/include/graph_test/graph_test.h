#include <string>
#include <thread>
#include <vector>
#include "graph_test/base_graph_test.h"
#include "test_runner/test_group.h"
#include "test_runner/test_runner.h"
#include "transaction/transaction_context.h"

namespace kuzu {
namespace testing {

// Graph test which uses private APIs
class PrivateGraphTest : public BaseGraphTest {
protected:
    static inline transaction::TransactionManager* getTransactionManager(main::Database& database) {
        return database.transactionManager.get();
    }
    static inline transaction::TransactionMode getTransactionMode(main::Connection& connection) {
        return connection.clientContext->getTransactionContext()->getTransactionMode();
    }
    static inline transaction::Transaction* getActiveTransaction(main::Connection& connection) {
        return connection.clientContext->getTransactionContext()->getActiveTransaction();
    }
    static inline uint64_t getActiveTransactionID(main::Connection& connection) {
        return connection.clientContext->getTransactionContext()->getActiveTransaction()->getID();
    }
    static inline bool hasActiveTransaction(main::Connection& connection) {
        return connection.clientContext->getTransactionContext()->hasActiveTransaction();
    }

    void validateQueryBestPlanJoinOrder(std::string query, std::string expectedJoinOrder);
};

// This class starts database without initializing graph.
class EmptyDBTest : public PrivateGraphTest {
    std::string getInputDir() override { KU_UNREACHABLE; }
};

// This class starts database in on-disk mode.
class DBTest : public PrivateGraphTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        createDBAndConn();
        initGraph();
    }
    void createDB(uint64_t checkpointWaitTimeout);
    void createNewDB();
    void createConns(const std::set<std::string>& connNames) override;

    void runConnStatements(const std::string& name);

    inline void runTest(const std::vector<std::unique_ptr<TestStatement>>& statements,
        uint64_t checkpointWaitTimeout = common::DEFAULT_CHECKPOINT_WAIT_TIMEOUT_IN_MICROS,
        std::set<std::string> connNames = std::set<std::string>()) {
        closeThreads = false;
        for (const auto& connName : connNames) {
            connThreads.emplace_back(&DBTest::runConnStatements, this, connName);
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
                createDB(checkpointWaitTimeout);
                createConns(connNames);
                continue;
            }
            if (statement->connectionsStatusFlag != ConnectionsStatusFlag::NONE) {
                connectionsPaused = statement->connectionsStatusFlag == ConnectionsStatusFlag::WAIT;
                continue;
            }
            if (conn) {
                TestRunner::runTest(statement.get(), *conn, databasePath);
            } else {
                auto connName = *statement->connName;
                CheckConn(connName);
                if (connectionsPaused) {
                    statementQueue[connName].push_back(statement.get());
                } else {
                    TestRunner::runTest(statement.get(), *connMap[connName], databasePath);
                }
            }
        }
        closeThreads = true;
        for (auto& thread: connThreads) {
            thread.join();
        }
    }

    inline void CheckConn(std::string connName) {
        if (connMap[connName] == nullptr) {
            connMap[connName] = std::make_unique<main::Connection>(database.get());
        }
    }

protected:
    bool closeThreads = false;
    bool connectionsPaused = false;
    std::vector<std::thread> connThreads;
    std::unordered_map<std::string, std::vector<TestStatement*>> statementQueue;
};
} // namespace testing
} // namespace kuzu

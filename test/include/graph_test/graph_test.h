#pragma once

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "graph_test/base_graph_test.h"
#include "test_runner/test_group.h"
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
};

// This class starts database without initializing graph.
class EmptyDBTest : public PrivateGraphTest {
    std::string getInputDir() override { KU_UNREACHABLE; }
};

// ConcurrentTestExecutor is not thread safe
class ConcurrentTestExecutor {
public:
    ConcurrentTestExecutor(bool& connectionsPaused, main::Connection& connection,
        std::string databasePath)
        : connectionPaused{connectionsPaused}, connection{connection}, databasePath{databasePath} {}

    void execute() { connThread = std::thread(&ConcurrentTestExecutor::runStatements, this); }
    void join() {
        if (connThread.joinable()) {
            connThread.join();
        }
    }
    void reset() { statements.clear(); }
    void addStatement(TestStatement* statement) { statements.emplace_back(statement); }

private:
    void runStatements();

    bool& connectionPaused;
    main::Connection& connection;
    std::string databasePath;
    std::thread connThread;
    std::vector<TestStatement*> statements;
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

    void runTest(const std::vector<std::unique_ptr<TestStatement>>& statements,
        uint64_t checkpointWaitTimeout = common::DEFAULT_CHECKPOINT_WAIT_TIMEOUT_IN_MICROS,
        std::set<std::string> connNames = std::set<std::string>());

protected:
    bool isConcurrent = false;
    bool connectionsPaused = false;
    std::unordered_map<std::string, ConcurrentTestExecutor> concurrentTests;
};

} // namespace testing
} // namespace kuzu

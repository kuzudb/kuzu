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
    static transaction::TransactionManager* getTransactionManager(main::Database& database) {
        return database.getTransactionManager();
    }
    static transaction::TransactionMode getTransactionMode(const main::Connection& connection) {
        return transaction::TransactionContext::Get(*connection.clientContext)
            ->getTransactionMode();
    }
    static transaction::Transaction* getActiveTransaction(const main::Connection& connection) {
        return transaction::TransactionContext::Get(*connection.clientContext)
            ->getActiveTransaction();
    }
    static uint64_t getActiveTransactionID(const main::Connection& connection) {
        return transaction::TransactionContext::Get(*connection.clientContext)
            ->getActiveTransaction()
            ->getID();
    }
    static bool hasActiveTransaction(const main::Connection& connection) {
        return transaction::TransactionContext::Get(*connection.clientContext)
            ->hasActiveTransaction();
    }
};

// This class starts database without initializing graph.
class EmptyDBTest : public PrivateGraphTest {
    std::string getInputDir() override { KU_UNREACHABLE; }
};

// ConcurrentTestExecutor is not thread safe
class ConcurrentTestExecutor {
public:
    ConcurrentTestExecutor(std::atomic<bool>& connectionsPaused, main::Connection& connection,
        std::string databasePath)
        : connectionPaused{connectionsPaused}, connection{connection},
          databasePath{std::move(databasePath)} {}

    void execute() { connThread = std::thread(&ConcurrentTestExecutor::runStatements, this); }
    void join() {
        if (connThread.joinable()) {
            connThread.join();
        }
    }
    void reset() { statements.clear(); }
    void addStatement(TestStatement& statement) { statements.emplace_back(statement); }

private:
    void runStatements() const;

    std::atomic<bool>& connectionPaused;
    main::Connection& connection;
    std::string databasePath;
    std::thread connThread;
    std::vector<std::reference_wrapper<TestStatement>> statements;
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

    void runTest(std::vector<TestStatement>& statements,
        uint64_t checkpointWaitTimeout = common::DEFAULT_CHECKPOINT_WAIT_TIMEOUT_IN_MICROS,
        std::set<std::string> connNames = std::set<std::string>());

protected:
    bool isConcurrent = false;
    std::atomic<bool> connectionsPaused = false;
    std::unordered_map<std::string, ConcurrentTestExecutor> concurrentTests;
};

} // namespace testing
} // namespace kuzu

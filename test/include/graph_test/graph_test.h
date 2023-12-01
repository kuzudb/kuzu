#include "graph_test/base_graph_test.h"
#include "main/kuzu.h"
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

    inline void runTest(const std::vector<std::unique_ptr<TestStatement>>& statements,
        uint64_t checkpointWaitTimeout =
            common::DEFAULT_CHECKPOINT_WAIT_TIMEOUT_FOR_TRANSACTIONS_TO_LEAVE_IN_MICROS,
        std::set<std::string> connNames = std::set<std::string>()) {
        for (auto& statement : statements) {
            if (statement->reloadDBFlag) {
                createDB(checkpointWaitTimeout);
                createConns(connNames);
                continue;
            }
            if (conn) {
                TestRunner::runTest(statement.get(), *conn, databasePath);
            } else {
                auto connName = *statement->connName;
                CheckConn(connName);
                TestRunner::runTest(statement.get(), *connMap[connName], databasePath);
            }
        }
    }

    inline void CheckConn(std::string connName) {
        if (connMap[connName] == nullptr) {
            connMap[connName] = std::make_unique<main::Connection>(database.get());
        }
    }
};
} // namespace testing
} // namespace kuzu

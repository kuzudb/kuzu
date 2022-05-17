#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

// Test manual transaction. Auto transaction is tested in update test.
class TinySnbTransactionTest : public BaseGraphLoadingTest {

public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }

    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        systemConfig->largePageBufferPoolSize = (1ull << 22);
        createConn();
        readConn = make_unique<Connection>(database.get());
    }

    void checkResult(vector<string>& result, vector<string>& groundTruth) {
        ASSERT_EQ(result.size(), groundTruth.size());
        for (auto i = 0u; i < result.size(); ++i) {
            ASSERT_STREQ(result[i].c_str(), groundTruth[i].c_str());
        }
    }

    void readAndAssertNodeProperty(
        Connection* conn, uint64_t nodeOffset, string propertyName, vector<string> groundTruth) {
        auto readQuery =
            "MATCH (a:person) WHERE a.ID=" + to_string(nodeOffset) + " RETURN a." + propertyName;
        auto result = conn->query(readQuery);
        auto resultStr = TestHelper::convertResultToString(*result);
        checkResult(resultStr, groundTruth);
    }

    void assertReadBehaviorForBeforeRollbackAndCommitForConcurrent1Write1ReadTransactionTest() {
        conn->beginWriteTransaction();
        readConn->beginReadOnlyTransaction();
        // read before update
        readAndAssertNodeProperty(conn.get(), 0 /* node offset */, "age", vector<string>{"35"});
        readAndAssertNodeProperty(readConn.get(), 0 /* node offset */, "age", vector<string>{"35"});
        // update
        conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.age = 70;");
        // read after update but before commit
        readAndAssertNodeProperty(conn.get(), 0 /* node offset */, "age", vector<string>{"70"});
        readAndAssertNodeProperty(readConn.get(), 0 /* node offset */, "age", vector<string>{"35"});
    }

public:
    unique_ptr<Connection> readConn;
};

TEST_F(TinySnbTransactionTest, SingleTransactionReadWriteToStructuredNodePropertyNonNullTest) {
    conn->beginWriteTransaction();
    readAndAssertNodeProperty(conn.get(), 0 /* node offset */, "age", vector<string>{"35"});
    conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.age = 70;");
    readAndAssertNodeProperty(conn.get(), 0 /* node offset */, "age", vector<string>{"70"});
}

TEST_F(TinySnbTransactionTest, SingleTransactionReadWriteToStructuredNodePropertyNullTest) {
    conn->beginWriteTransaction();
    readAndAssertNodeProperty(conn.get(), 0 /* node offset */, "age", vector<string>{"35"});
    conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.age = a.unstrNumericProp;");
    readAndAssertNodeProperty(conn.get(), 0 /* node offset */, "age", vector<string>{""});
}

TEST_F(TinySnbTransactionTest, Concurrent1Write1ReadTransactionInTheMiddleOfTransaction) {
    assertReadBehaviorForBeforeRollbackAndCommitForConcurrent1Write1ReadTransactionTest();
}

TEST_F(TinySnbTransactionTest, Concurrent1Write1ReadTransactionCommitAndCheckpoint) {
    assertReadBehaviorForBeforeRollbackAndCommitForConcurrent1Write1ReadTransactionTest();
    readConn->commit();
    conn->commit();
    // read after commit
    conn->beginWriteTransaction();
    readConn->beginReadOnlyTransaction();
    readAndAssertNodeProperty(conn.get(), 0 /* node offset */, "age", vector<string>{"70"});
    readAndAssertNodeProperty(readConn.get(), 0 /* node offset */, "age", vector<string>{"70"});
}

TEST_F(TinySnbTransactionTest, Concurrent1Write1ReadTransactionRollback) {
    assertReadBehaviorForBeforeRollbackAndCommitForConcurrent1Write1ReadTransactionTest();
    readConn->commit();
    conn->rollback();
    // read after rollback
    conn->beginWriteTransaction();
    readConn->beginReadOnlyTransaction();
    readAndAssertNodeProperty(conn.get(), 0 /* node offset */, "age", vector<string>{"35"});
    readAndAssertNodeProperty(readConn.get(), 0 /* node offset */, "age", vector<string>{"35"});
}

TEST_F(TinySnbTransactionTest, OpenReadOnlyTransactionTriggersTimeoutErrorForWriteTransaction) {
    database->getTransactionManager()->setCheckPointWaitTimeoutForTransactionsToLeaveInMicros(
        10000 /* 10ms */);
    readConn->beginReadOnlyTransaction();
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.age = 70;");
    try {
        conn->commit();
        FAIL();
    } catch (TransactionManagerException& e) {
    } catch (Exception& e) { FAIL(); }
    readAndAssertNodeProperty(readConn.get(), 0 /* node offset */, "age", vector<string>{"35"});
}

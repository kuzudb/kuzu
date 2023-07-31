#include "graph_test/graph_test.h"
#include "transaction/transaction_manager.h"

using namespace kuzu::common;
using namespace kuzu::testing;

// Test manual transaction. Auto transaction is tested in update test.
class BaseSetNodePropTransactionTest : public DBTest {
public:
    void SetUp() override {
        DBTest::SetUp();
        readConn = std::make_unique<Connection>(database.get());
    }

    static void readAndAssertNodeProperty(Connection* conn, uint64_t nodeOffset,
        std::string propertyName, std::vector<std::string> groundTruth) {
        auto readQuery = "MATCH (a:person) WHERE a.ID=" + std::to_string(nodeOffset) +
                         " RETURN a." + propertyName;
        auto result = conn->query(readQuery);
        auto resultStr = TestHelper::convertResultToString(*result);
        checkResult(resultStr, groundTruth);
    }

private:
    static void checkResult(
        std::vector<std::string>& result, std::vector<std::string>& groundTruth) {
        ASSERT_EQ(result.size(), groundTruth.size());
        for (auto i = 0u; i < result.size(); ++i) {
            ASSERT_STREQ(result[i].c_str(), groundTruth[i].c_str());
        }
    }

protected:
    std::unique_ptr<Connection> readConn;
};

class SetNodeStructuredPropTransactionTest : public BaseSetNodePropTransactionTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }

    void insertLongStrings1000TimesAndVerify(Connection* connection) {
        int numWriteQueries = 1000;
        for (int i = 0; i < numWriteQueries; ++i) {
            connection->query("MATCH (a:person) WHERE a.ID < 100 SET a.fName = "
                              "concat('abcdefghijklmnopqrstuvwxyz', string(a.ID+" +
                              std::to_string(numWriteQueries) + "))");
        }
        auto result = connection->query("MATCH (a:person) WHERE a.ID=2 RETURN a.fName");
        ASSERT_EQ(result->getNext()->getValue(0)->getValue<std::string>(),
            "abcdefghijklmnopqrstuvwxyz" + std::to_string(numWriteQueries + 2));
    }
};

TEST_F(SetNodeStructuredPropTransactionTest,
    SingleTransactionReadWriteToFixedLengthStructuredNodePropertyNonNullTest) {
    conn->beginWriteTransaction();
    readAndAssertNodeProperty(
        conn.get(), 0 /* node offset */, "age", std::vector<std::string>{"35"});
    conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.age = 70;");
    readAndAssertNodeProperty(
        conn.get(), 0 /* node offset */, "age", std::vector<std::string>{"70"});
}

// TODO(Guodong): FIXME: Fix all disabled tests.
// TEST_F(SetNodeStructuredPropTransactionTest,
//    SingleTransactionReadWriteToStringStructuredNodePropertyNonNullTest) {
//    conn->beginWriteTransaction();
//    readAndAssertNodeProperty(
//        conn.get(), 0 /* node offset */, "fName", std::vector<std::string>{"Alice"});
//    conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.fName = 'abcdefghijklmnopqrstuvwxyz';");
//    readAndAssertNodeProperty(conn.get(), 0 /* node offset */, "fName",
//        std::vector<std::string>{"abcdefghijklmnopqrstuvwxyz"});
//}

TEST_F(SetNodeStructuredPropTransactionTest,
    SingleTransactionReadWriteToFixedLengthStructuredNodePropertyNullTest) {
    conn->beginWriteTransaction();
    readAndAssertNodeProperty(
        conn.get(), 0 /* node offset */, "age", std::vector<std::string>{"35"});
    conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.age = null;");
    readAndAssertNodeProperty(conn.get(), 0 /* node offset */, "age", std::vector<std::string>{""});
}

// TEST_F(SetNodeStructuredPropTransactionTest,
//    SingleTransactionReadWriteToStringStructuredNodePropertyNullTest) {
//    conn->beginWriteTransaction();
//    readAndAssertNodeProperty(
//        conn.get(), 0 /* node offset */, "fName", std::vector<std::string>{"Alice"});
//    auto result = conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.fName = null;");
//    readAndAssertNodeProperty(
//        conn.get(), 0 /* node offset */, "fName", std::vector<std::string>{""});
//}

// TEST_F(SetNodeStructuredPropTransactionTest,
//    Concurrent1Write1ReadTransactionInTheMiddleOfTransaction) {
//    conn->beginWriteTransaction();
//    readConn->beginReadOnlyTransaction();
//    // read before update
//    readAndAssertNodeProperty(
//        conn.get(), 0 /* node offset */, "age", std::vector<std::string>{"35"});
//    readAndAssertNodeProperty(
//        readConn.get(), 0 /* node offset */, "age", std::vector<std::string>{"35"});
//    readAndAssertNodeProperty(
//        conn.get(), 0 /* node offset */, "fName", std::vector<std::string>{"Alice"});
//    readAndAssertNodeProperty(
//        readConn.get(), 0 /* nodeoffset */, "fName", std::vector<std::string>{"Alice"});
//    // update
//    conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.age = 70;");
//    conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.fName = 'abcdefghijklmnopqrstuvwxyz'");
//    // read after update but before commit
//    readAndAssertNodeProperty(
//        conn.get(), 0 /* node offset */, "age", std::vector<std::string>{"70"});
//    readAndAssertNodeProperty(
//        readConn.get(), 0 /* node offset */, "age", std::vector<std::string>{"35"});
//    readAndAssertNodeProperty(conn.get(), 0 /* node offset */, "fName",
//        std::vector<std::string>{"abcdefghijklmnopqrstuvwxyz"});
//    readAndAssertNodeProperty(
//        readConn.get(), 0 /* node offset */, "fName", std::vector<std::string>{"Alice"});
//}

// TEST_F(SetNodeStructuredPropTransactionTest, Concurrent1Write1ReadTransactionCommitAndCheckpoint)
// {
//    conn->beginWriteTransaction();
//    readConn->beginReadOnlyTransaction();
//    conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.age = 70;");
//    conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.fName = 'abcdefghijklmnopqrstuvwxyz'");
//    readConn->commit();
//    conn->commit();
//    // read after commit
//    readAndAssertNodeProperty(
//        conn.get(), 0 /* node offset */, "age", std::vector<std::string>{"70"});
//    readAndAssertNodeProperty(
//        readConn.get(), 0 /* node offset */, "age", std::vector<std::string>{"70"});
//    readAndAssertNodeProperty(conn.get(), 0 /* node offset */, "fName",
//        std::vector<std::string>{"abcdefghijklmnopqrstuvwxyz"});
//    readAndAssertNodeProperty(readConn.get(), 0 /* node offset */, "fName",
//        std::vector<std::string>{"abcdefghijklmnopqrstuvwxyz"});
//}

// TEST_F(SetNodeStructuredPropTransactionTest, Concurrent1Write1ReadTransactionRollback) {
//    conn->beginWriteTransaction();
//    readConn->beginReadOnlyTransaction();
//    conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.age = 70;");
//    conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.fName = 'abcdefghijklmnopqrstuvwxyz'");
//    readConn->commit();
//    conn->rollback();
//    // read after rollback
//    readAndAssertNodeProperty(
//        conn.get(), 0 /* node offset */, "age", std::vector<std::string>{"35"});
//    readAndAssertNodeProperty(
//        readConn.get(), 0 /* node offset */, "age", std::vector<std::string>{"35"});
//    readAndAssertNodeProperty(
//        conn.get(), 0 /* node offset */, "fName", std::vector<std::string>{"Alice"});
//    readAndAssertNodeProperty(
//        readConn.get(), 0 /* node offset */, "fName", std::vector<std::string>{"Alice"});
//}

TEST_F(SetNodeStructuredPropTransactionTest,
    OpenReadOnlyTransactionTriggersTimeoutErrorForWriteTransaction) {
    getTransactionManager(*database)->setCheckPointWaitTimeoutForTransactionsToLeaveInMicros(
        10000 /* 10ms */);
    readConn->beginReadOnlyTransaction();
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) WHERE a.ID = 0 SET a.age = 70;");
    try {
        conn->commit();
        FAIL();
    } catch (TransactionManagerException& e) {
    } catch (Exception& e) { FAIL(); }
    readAndAssertNodeProperty(
        readConn.get(), 0 /* node offset */, "age", std::vector<std::string>{"35"});
}

TEST_F(SetNodeStructuredPropTransactionTest, SetNodeLongStringPropRollbackTest) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) WHERE a.ID=0 SET a.fName='abcdefghijklmnopqrstuvwxyz'");
    conn->rollback();
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.fName");
    ASSERT_EQ(result->getNext()->getValue(0)->getValue<std::string>(), "Alice");
}

TEST_F(SetNodeStructuredPropTransactionTest, SetVeryLongStringErrorsTest) {
    conn->beginWriteTransaction();
    std::string veryLongStr = "";
    for (auto i = 0u; i < BufferPoolConstants::PAGE_4KB_SIZE + 1; ++i) {
        veryLongStr += "a";
    }
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 SET a.fName='" + veryLongStr + "'");
    ASSERT_FALSE(result->isSuccess());
}

// TEST_F(SetNodeStructuredPropTransactionTest, SetManyNodeLongStringPropCommitTest) {
//    conn->beginWriteTransaction();
//    insertLongStrings1000TimesAndVerify(conn.get());
//    conn->commit();
//    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.fName");
//    ASSERT_EQ(result->getNext()->getValue(0)->getValue<std::string>(),
//        "abcdefghijklmnopqrstuvwxyz" + std::to_string(1000));
//}

// TEST_F(SetNodeStructuredPropTransactionTest, SetManyNodeLongStringPropRollbackTest) {
//    conn->beginWriteTransaction();
//    insertLongStrings1000TimesAndVerify(conn.get());
//    conn->rollback();
//    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.fName");
//    ASSERT_EQ(result->getNext()->getValue(0)->getValue<std::string>(), "Alice");
//}

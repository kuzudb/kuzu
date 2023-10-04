#include "main_test_helper/private_main_test_helper.h"

using namespace kuzu::common;
using namespace kuzu::testing;
using namespace kuzu::transaction;

TEST_F(PrivateApiTest, TransactionModes) {
    // Test initially connections are in AUTO_COMMIT mode.
    ASSERT_EQ(TransactionMode::AUTO, getTransactionMode(*conn));
    // Test beginning a transaction (first in read only mode) sets mode to MANUAL automatically.
    conn->beginReadOnlyTransaction();
    ASSERT_EQ(TransactionMode::MANUAL, getTransactionMode(*conn));
    // Test commit automatically switches the mode to AUTO_COMMIT read transaction
    conn->commit();
    ASSERT_EQ(TransactionMode::AUTO, getTransactionMode(*conn));

    conn->beginReadOnlyTransaction();
    ASSERT_EQ(TransactionMode::MANUAL, getTransactionMode(*conn));
    // Test rollback automatically switches the mode to AUTO_COMMIT for read transaction
    conn->rollback();
    ASSERT_EQ(TransactionMode::AUTO, getTransactionMode(*conn));

    // Test beginning a transaction (now in write mode) sets mode to MANUAL automatically.
    conn->beginWriteTransaction();
    ASSERT_EQ(TransactionMode::MANUAL, getTransactionMode(*conn));
    // Test commit automatically switches the mode to AUTO_COMMIT for write transaction
    conn->commit();
    ASSERT_EQ(TransactionMode::AUTO, getTransactionMode(*conn));

    // Test beginning a transaction (now in write mode) sets mode to MANUAL automatically.
    conn->beginWriteTransaction();
    ASSERT_EQ(TransactionMode::MANUAL, getTransactionMode(*conn));
    // Test rollback automatically switches the mode to AUTO_COMMIT write transaction
    conn->rollback();
    ASSERT_EQ(TransactionMode::AUTO, getTransactionMode(*conn));
}

TEST_F(PrivateApiTest, MultipleCallsFromSameTransaction) {
    conn->beginReadOnlyTransaction();
    auto activeTransactionID = getActiveTransactionID(*conn);
    conn->query("MATCH (a:person) RETURN COUNT(*)");
    ASSERT_EQ(activeTransactionID, getActiveTransactionID(*conn));
    conn->query("MATCH (a:person) RETURN COUNT(*)");
    ASSERT_EQ(activeTransactionID, getActiveTransactionID(*conn));
    auto preparedStatement =
        conn->prepare("MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)");
    conn->execute(preparedStatement.get(), std::make_pair(std::string("1"), true));
    ASSERT_EQ(activeTransactionID, getActiveTransactionID(*conn));
    conn->commit();
    ASSERT_FALSE(hasActiveTransaction(*conn));
}

TEST_F(PrivateApiTest, CommitRollbackRemoveActiveTransaction) {
    conn->beginWriteTransaction();
    ASSERT_TRUE(hasActiveTransaction(*conn));
    conn->rollback();
    ASSERT_FALSE(hasActiveTransaction(*conn));
    conn->beginReadOnlyTransaction();
    ASSERT_TRUE(hasActiveTransaction(*conn));
    conn->commit();
    ASSERT_FALSE(hasActiveTransaction(*conn));
}

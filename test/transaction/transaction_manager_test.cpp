#include "common/exception.h"
#include "graph_test/graph_test.h"
#include "transaction/transaction_manager.h"

using namespace kuzu::common;
using namespace kuzu::testing;
using namespace kuzu::transaction;
using namespace kuzu::storage;
using ::testing::Test;

class TransactionManagerTest : public Test {

protected:
    void SetUp() override {
        FileUtils::createDir(TestHelper::getTmpTestDir());
        bufferManager =
            std::make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        wal = std::make_unique<WAL>(TestHelper::getTmpTestDir(), *bufferManager);
        transactionManager = std::make_unique<TransactionManager>(*wal);
    }

    void TearDown() override { FileUtils::removeDir(TestHelper::getTmpTestDir()); }

public:
    void runTwoCommitRollback(TransactionType type, bool firstIsCommit, bool secondIsCommit) {
        std::unique_ptr<Transaction> trx = WRITE == type ?
                                               transactionManager->beginWriteTransaction() :
                                               transactionManager->beginReadOnlyTransaction();
        if (firstIsCommit) {
            transactionManager->commit(trx.get());
        } else {
            transactionManager->rollback(trx.get());
        }
        if (secondIsCommit) {
            transactionManager->commit(trx.get());
        } else {
            transactionManager->rollback(trx.get());
        }
    }

public:
    std::unique_ptr<TransactionManager> transactionManager;

private:
    std::unique_ptr<BufferManager> bufferManager;
    std::unique_ptr<WAL> wal;
};

TEST_F(TransactionManagerTest, MultipleWriteTransactionsErrors) {
    std::unique_ptr<Transaction> trx1 = transactionManager->beginWriteTransaction();
    try {
        transactionManager->beginWriteTransaction();
        FAIL();
    } catch (TransactionManagerException& e) {}
}

TEST_F(TransactionManagerTest, MultipleCommitsAndRollbacks) {
    // At TransactionManager level, we disallow multiple commit/rollbacks on a write transaction.
    try {
        runTwoCommitRollback(WRITE, true /* firstIsCommit */, true /* secondIsCommit */);
        FAIL();
    } catch (TransactionManagerException& e) {}
    try {
        runTwoCommitRollback(WRITE, true /* firstIsCommit */, false /* secondIsRollback */);
        FAIL();
    } catch (TransactionManagerException& e) {}
    try {
        runTwoCommitRollback(WRITE, false /* firstIsRollback */, true /* secondIsCommit */);
        FAIL();
    } catch (TransactionManagerException& e) {}
    try {
        runTwoCommitRollback(WRITE, false /* firstIsRollback */, false /* secondIsRollback */);
        FAIL();
    } catch (TransactionManagerException& e) {}
    // At TransactionManager level, we allow multiple commit/rollbacks on a read-only transaction.
    runTwoCommitRollback(READ_ONLY, true /* firstIsCommit */, true /* secondIsCommit */);
    runTwoCommitRollback(READ_ONLY, true /* firstIsCommit */, false /* secondIsRollback */);
    runTwoCommitRollback(READ_ONLY, false /* firstIsRollback */, true /* secondIsCommit */);
    runTwoCommitRollback(READ_ONLY, false /* firstIsRollback */, false /* secondIsRollback */);
}

TEST_F(TransactionManagerTest, BasicOneWriteMultipleReadOnlyTransactions) {
    // Tests the internal states of the transaction manager at different points in time, e.g.,
    // before and after commits or rollbacks under concurrent transactions. Specifically we test:
    // that transaction IDs increase incrementally, the states of activeReadOnlyTransactionIDs set,
    // and activeWriteTransactionID.
    std::unique_ptr<Transaction> trx1 = transactionManager->beginReadOnlyTransaction();
    std::unique_ptr<Transaction> trx2 = transactionManager->beginWriteTransaction();
    std::unique_ptr<Transaction> trx3 = transactionManager->beginReadOnlyTransaction();
    ASSERT_EQ(READ_ONLY, trx1->getType());
    ASSERT_EQ(WRITE, trx2->getType());
    ASSERT_EQ(READ_ONLY, trx3->getType());
    ASSERT_EQ(trx1->getID() + 1, trx2->getID());
    ASSERT_EQ(trx2->getID() + 1, trx3->getID());
    ASSERT_EQ(trx2->getID(), transactionManager->getActiveWriteTransactionID());
    std::unordered_set<uint64_t> expectedReadOnlyTransactionSet({trx1->getID(), trx3->getID()});
    ASSERT_EQ(
        expectedReadOnlyTransactionSet, transactionManager->getActiveReadOnlyTransactionIDs());

    transactionManager->commit(trx2.get());
    ASSERT_FALSE(transactionManager->hasActiveWriteTransactionID());
    transactionManager->rollback(trx1.get());
    expectedReadOnlyTransactionSet.erase(trx1->getID());
    ASSERT_EQ(
        expectedReadOnlyTransactionSet, transactionManager->getActiveReadOnlyTransactionIDs());
    transactionManager->commit(trx3.get());
    expectedReadOnlyTransactionSet.erase(trx3->getID());
    ASSERT_EQ(
        expectedReadOnlyTransactionSet, transactionManager->getActiveReadOnlyTransactionIDs());

    std::unique_ptr<Transaction> trx4 = transactionManager->beginWriteTransaction();
    std::unique_ptr<Transaction> trx5 = transactionManager->beginReadOnlyTransaction();
    ASSERT_EQ(trx3->getID() + 1, trx4->getID());
    ASSERT_EQ(trx4->getID() + 1, trx5->getID());
    ASSERT_EQ(trx4->getID(), transactionManager->getActiveWriteTransactionID());
    expectedReadOnlyTransactionSet.insert(trx5->getID());
    ASSERT_EQ(
        expectedReadOnlyTransactionSet, transactionManager->getActiveReadOnlyTransactionIDs());
}

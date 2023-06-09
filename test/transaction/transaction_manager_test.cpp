#include "common/exception.h"
#include "graph_test/graph_test.h"
#include "transaction/transaction_manager.h"

using namespace kuzu::common;
using namespace kuzu::testing;
using namespace kuzu::transaction;
using namespace kuzu::storage;
using ::testing::Test;

class TransactionManagerTest : public EmptyDBTest {

protected:
    void SetUp() override {
        EmptyDBTest::SetUp();
        FileUtils::createDir(databasePath);
        LoggerUtils::createLogger(LoggerConstants::LoggerEnum::BUFFER_MANAGER);
        LoggerUtils::createLogger(LoggerConstants::LoggerEnum::WAL);
        LoggerUtils::createLogger(LoggerConstants::LoggerEnum::TRANSACTION_MANAGER);
        LoggerUtils::createLogger(LoggerConstants::LoggerEnum::STORAGE);
        bufferManager = std::make_unique<BufferManager>(
            BufferPoolConstants::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        wal = std::make_unique<WAL>(databasePath, *bufferManager);
        transactionManager = std::make_unique<TransactionManager>(*wal);
    }

    void TearDown() override {
        EmptyDBTest::TearDown();
        LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::BUFFER_MANAGER);
        LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::WAL);
        LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::TRANSACTION_MANAGER);
        LoggerUtils::dropLogger(LoggerConstants::LoggerEnum::STORAGE);
    }

public:
    void runTwoCommitRollback(TransactionType type, bool firstIsCommit, bool secondIsCommit) {
        std::unique_ptr<Transaction> trx = TransactionType::WRITE == type ?
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
        runTwoCommitRollback(
            TransactionType::WRITE, true /* firstIsCommit */, true /* secondIsCommit */);
        FAIL();
    } catch (TransactionManagerException& e) {}
    try {
        runTwoCommitRollback(
            TransactionType::WRITE, true /* firstIsCommit */, false /* secondIsRollback */);
        FAIL();
    } catch (TransactionManagerException& e) {}
    try {
        runTwoCommitRollback(
            TransactionType::WRITE, false /* firstIsRollback */, true /* secondIsCommit */);
        FAIL();
    } catch (TransactionManagerException& e) {}
    try {
        runTwoCommitRollback(
            TransactionType::WRITE, false /* firstIsRollback */, false /* secondIsRollback */);
        FAIL();
    } catch (TransactionManagerException& e) {}
    // At TransactionManager level, we allow multiple commit/rollbacks on a read-only transaction.
    runTwoCommitRollback(
        TransactionType::READ_ONLY, true /* firstIsCommit */, true /* secondIsCommit */);
    runTwoCommitRollback(
        TransactionType::READ_ONLY, true /* firstIsCommit */, false /* secondIsRollback */);
    runTwoCommitRollback(
        TransactionType::READ_ONLY, false /* firstIsRollback */, true /* secondIsCommit */);
    runTwoCommitRollback(
        TransactionType::READ_ONLY, false /* firstIsRollback */, false /* secondIsRollback */);
}

TEST_F(TransactionManagerTest, BasicOneWriteMultipleReadOnlyTransactions) {
    // Tests the internal states of the transaction manager at different points in time, e.g.,
    // before and after commits or rollbacks under concurrent transactions. Specifically we test:
    // that transaction IDs increase incrementally, the states of activeReadOnlyTransactionIDs set,
    // and activeWriteTransactionID.
    std::unique_ptr<Transaction> trx1 = transactionManager->beginReadOnlyTransaction();
    std::unique_ptr<Transaction> trx2 = transactionManager->beginWriteTransaction();
    std::unique_ptr<Transaction> trx3 = transactionManager->beginReadOnlyTransaction();
    ASSERT_EQ(TransactionType::READ_ONLY, trx1->getType());
    ASSERT_EQ(TransactionType::WRITE, trx2->getType());
    ASSERT_EQ(TransactionType::READ_ONLY, trx3->getType());
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

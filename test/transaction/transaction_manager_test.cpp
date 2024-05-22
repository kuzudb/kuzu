#include "common/exception/transaction_manager.h"
#include "graph_test/graph_test.h"
#include "main/client_context.h"
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
        std::filesystem::create_directory(databasePath);
        createDBAndConn();
        context = std::make_unique<kuzu::main::ClientContext>(database.get());
        bufferManager = getBufferManager(*database);
        std::make_unique<BufferManager>(BufferPoolConstants::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING,
            BufferPoolConstants::DEFAULT_VM_REGION_MAX_SIZE);
        wal = std::make_unique<WAL>(databasePath, false /* readOnly */, *bufferManager,
            getFileSystem(*database), context.get());
        transactionManager = std::make_unique<TransactionManager>(*wal);
    }

    void TearDown() override { EmptyDBTest::TearDown(); }

public:
    void runTwoCommitRollback(TransactionType type, bool firstIsCommit, bool secondIsCommit) {
        std::unique_ptr<Transaction> trx =
            TransactionType::WRITE == type ?
                transactionManager->beginTransaction(*getClientContext(*conn),
                    TransactionType::WRITE) :
                transactionManager->beginTransaction(*getClientContext(*conn),
                    TransactionType::READ_ONLY);
        if (firstIsCommit) {
            transactionManager->commit(*getClientContext(*conn));
        } else {
            transactionManager->rollback(*getClientContext(*conn), getActiveTransaction(*conn));
        }
        if (secondIsCommit) {
            transactionManager->commit(*getClientContext(*conn));
        } else {
            transactionManager->rollback(*getClientContext(*conn), getActiveTransaction(*conn));
        }
    }

public:
    std::unique_ptr<TransactionManager> transactionManager;

private:
    BufferManager* bufferManager;
    std::unique_ptr<WAL> wal;
    std::unique_ptr<kuzu::main::ClientContext> context;
};

// TODO: Rewrite to end to end test.
// TEST_F(TransactionManagerTest, MultipleCommitsAndRollbacks) {
//    // At TransactionManager level, we disallow multiple commit/rollbacks on a write transaction.
//    try {
//        runTwoCommitRollback(TransactionType::WRITE, true /* firstIsCommit */,
//            true /* secondIsCommit */);
//        FAIL();
//    } catch (TransactionManagerException& e) {}
//    try {
//        runTwoCommitRollback(TransactionType::WRITE, true /* firstIsCommit */,
//            false /* secondIsRollback */);
//        FAIL();
//    } catch (TransactionManagerException& e) {}
//    try {
//        runTwoCommitRollback(TransactionType::WRITE, false /* firstIsRollback */,
//            true /* secondIsCommit */);
//        FAIL();
//    } catch (TransactionManagerException& e) {}
//    try {
//        runTwoCommitRollback(TransactionType::WRITE, false /* firstIsRollback */,
//            false /* secondIsRollback */);
//        FAIL();
//    } catch (TransactionManagerException& e) {}
//    // At TransactionManager level, we allow multiple commit/rollbacks on a read-only transaction.
//    runTwoCommitRollback(TransactionType::READ_ONLY, true /* firstIsCommit */,
//        true /* secondIsCommit */);
//    runTwoCommitRollback(TransactionType::READ_ONLY, true /* firstIsCommit */,
//        false /* secondIsRollback */);
//    runTwoCommitRollback(TransactionType::READ_ONLY, false /* firstIsRollback */,
//        true /* secondIsCommit */);
//    runTwoCommitRollback(TransactionType::READ_ONLY, false /* firstIsRollback */,
//        false /* secondIsRollback */);
//}

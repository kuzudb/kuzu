#include "transaction/transaction_manager.h"

#include <thread>

#include "common/exception/transaction_manager.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "storage/storage_manager.h"
#include "storage/wal_replayer.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace transaction {

std::unique_ptr<Transaction> TransactionManager::beginTransaction(
    main::ClientContext& clientContext, TransactionType type) {
    // We obtain the lock for starting new transactions. In case this cannot be obtained this
    // ensures calls to other public functions is not restricted.
    lock_t newTransactionLck{mtxForStartingNewTransactions};
    lock_t publicFunctionLck{mtxForSerializingPublicFunctionCalls};
    if (type == TransactionType::WRITE) {
        if (!clientContext.getDBConfig()->enableMultiWrites && hasActiveWriteTransactionNoLock()) {
            throw TransactionManagerException(
                "Cannot start a new write transaction in the system. "
                "Only one write transaction at a time is allowed in the system.");
        }
    }
    auto transaction =
        std::make_unique<Transaction>(clientContext, type, ++lastTransactionID, lastTimestamp);
    type == TransactionType::WRITE ? activeWriteTransactionID.insert(transaction->getID()) :
                                     activeReadOnlyTransactionIDs.insert(transaction->getID());
    return transaction;
}

void TransactionManager::commit(main::ClientContext& clientContext, bool skipCheckPointing) {
    lock_t lck{mtxForSerializingPublicFunctionCalls};
    clientContext.cleanUP();
    auto transaction = clientContext.getTx();
    if (transaction->isReadOnly()) {
        activeReadOnlyTransactionIDs.erase(transaction->getID());
        return;
    }
    lastTimestamp++;
    transaction->commitTS = lastTimestamp;
    transaction->commit(&wal);
    clientContext.getStorageManager()->prepareCommit(transaction, clientContext.getVFSUnsafe());
    wal.flushAllPages();
    clearActiveWriteTransactionIfWriteTransactionNoLock(transaction);
    if (!skipCheckPointing) {
        checkpointNoLock(clientContext);
    }
}

// Note: We take in additional `transaction` here is due to that `transactionContext` might be
// destructed when a transaction throws exception, while we need to rollback the active transaction
// still.
void TransactionManager::rollback(main::ClientContext& clientContext, Transaction* transaction,
    bool skipCheckPointing) {
    lock_t lck{mtxForSerializingPublicFunctionCalls};
    clientContext.cleanUP();
    if (transaction->isReadOnly()) {
        activeReadOnlyTransactionIDs.erase(transaction->getID());
        return;
    }
    clientContext.getStorageManager()->prepareRollback();
    transaction->rollback();
    clearActiveWriteTransactionIfWriteTransactionNoLock(transaction);
    wal.flushAllPages();
    if (!skipCheckPointing) {
        auto walReplayer = std::make_unique<WALReplayer>(clientContext, wal.getShadowingFH(),
            WALReplayMode::ROLLBACK);
        walReplayer->replay();
        // We next perform an in-memory rolling back of node/relTables.
        clientContext.getStorageManager()->rollbackInMemory();
        wal.clearWAL();
    }
}

void TransactionManager::checkpoint(main::ClientContext& clientContext) {
    lock_t lck{mtxForSerializingPublicFunctionCalls};
    checkpointNoLock(clientContext);
}

void TransactionManager::stopNewTransactionsAndWaitUntilAllTransactionsLeave() {
    mtxForStartingNewTransactions.lock();
    uint64_t numTimesWaited = 0;
    while (true) {
        if (!canCheckpointNoLock()) {
            numTimesWaited++;
            if (numTimesWaited * THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS >
                checkpointWaitTimeoutInMicros) {
                mtxForStartingNewTransactions.unlock();
                throw TransactionManagerException(
                    "Timeout waiting for active transactions to leave the system before "
                    "checkpointing. If you have an open transaction, please close it and try "
                    "again.");
            }
            std::this_thread::sleep_for(
                std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
        } else {
            break;
        }
    }
}

void TransactionManager::allowReceivingNewTransactions() {
    mtxForStartingNewTransactions.unlock();
}

bool TransactionManager::canCheckpointNoLock() {
    return activeWriteTransactionID.empty() && activeReadOnlyTransactionIDs.empty();
}

void TransactionManager::checkpointNoLock(main::ClientContext& clientContext) {
    // Note: It is enough to stop and wait transactions to leave the system instead of
    // for example checking on the query processor's task scheduler. This is because the
    // first and last steps that a connection performs when executing a query is to
    // start and commit/rollback transaction. The query processor also ensures that it
    // will only return results or error after all threads working on the tasks of a
    // query stop working on the tasks of the query and these tasks are removed from the
    // query.
    stopNewTransactionsAndWaitUntilAllTransactionsLeave();
    KU_ASSERT(canCheckpointNoLock());
    clientContext.getCatalog()->prepareCheckpoint(clientContext.getDatabasePath(), &wal,
        clientContext.getVFSUnsafe());
    wal.flushAllPages();
    // Replay the WAL to commit page updates/inserts, and table statistics.
    auto walReplayer = std::make_unique<WALReplayer>(clientContext, wal.getShadowingFH(),
        WALReplayMode::COMMIT_CHECKPOINT);
    walReplayer->replay();
    // We next perform an in-memory checkpointing of node/relTables.
    clientContext.getStorageManager()->checkpointInMemory();
    // Resume receiving new transactions.
    allowReceivingNewTransactions();
    // Clear the wal.
    wal.clearWAL();
}

void TransactionManager::clearActiveWriteTransactionIfWriteTransactionNoLock(
    Transaction* transaction) {
    if (transaction->isWriteTransaction()) {
        for (auto& id : activeWriteTransactionID) {
            if (id == transaction->getID()) {
                activeWriteTransactionID.erase(id);
                return;
            }
        }
    }
}

} // namespace transaction
} // namespace kuzu

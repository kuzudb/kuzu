#include "transaction/transaction_manager.h"

#include <thread>

#include "common/exception/transaction_manager.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace transaction {

std::unique_ptr<Transaction> TransactionManager::beginTransaction(
    main::ClientContext& clientContext, TransactionType type) {
    // We obtain the lock for starting new transactions. In case this cannot be obtained this
    // ensures calls to other public functions is not restricted.
    std::unique_lock<std::mutex> newTransactionLck{mtxForStartingNewTransactions};
    std::unique_lock<std::mutex> publicFunctionLck{mtxForSerializingPublicFunctionCalls};
    std::unique_ptr<Transaction> transaction;
    switch (type) {
    case TransactionType::READ_ONLY: {
        transaction =
            std::make_unique<Transaction>(clientContext, type, ++lastTransactionID, lastTimestamp);
        activeReadOnlyTransactions.insert(transaction->getID());
    } break;
    case TransactionType::RECOVERY:
    case TransactionType::WRITE: {
        if (!clientContext.getDBConfig()->enableMultiWrites && hasActiveWriteTransactionNoLock()) {
            throw TransactionManagerException(
                "Cannot start a new write transaction in the system. "
                "Only one write transaction at a time is allowed in the system.");
        }
        transaction =
            std::make_unique<Transaction>(clientContext, type, ++lastTransactionID, lastTimestamp);
        activeWriteTransactions.insert(transaction->getID());
        KU_ASSERT(clientContext.getStorageManager());
        if (transaction->shouldLogToWAL()) {
            clientContext.getStorageManager()->getWAL().logBeginTransaction();
        }
    } break;
    default: {
        throw TransactionManagerException("Invalid transaction type to begin transaction.");
    }
    }
    return transaction;
}

void TransactionManager::commit(main::ClientContext& clientContext) {
    std::unique_lock<std::mutex> lck{mtxForSerializingPublicFunctionCalls};
    clientContext.cleanUP();
    const auto transaction = clientContext.getTx();
    switch (transaction->getType()) {
    case TransactionType::READ_ONLY: {
        activeReadOnlyTransactions.erase(transaction->getID());
    } break;
    case TransactionType::RECOVERY:
    case TransactionType::WRITE: {
        lastTimestamp++;
        transaction->commitTS = lastTimestamp;
        transaction->commit(&wal);
        activeWriteTransactions.erase(transaction->getID());
        if (transaction->shouldForceCheckpoint() || canAutoCheckpoint(clientContext)) {
            checkpointNoLock(clientContext);
        }
    } break;
    default: {
        throw TransactionManagerException("Invalid transaction type to commit.");
    }
    }
}

// Note: We take in additional `transaction` here is due to that `transactionContext` might be
// destructed when a transaction throws exception, while we need to rollback the active transaction
// still.
void TransactionManager::rollback(main::ClientContext& clientContext,
    const Transaction* transaction) {
    std::unique_lock<std::mutex> lck{mtxForSerializingPublicFunctionCalls};
    clientContext.cleanUP();
    switch (transaction->getType()) {
    case TransactionType::READ_ONLY: {
        activeReadOnlyTransactions.erase(transaction->getID());
    } break;
    case TransactionType::RECOVERY:
    case TransactionType::WRITE: {
        transaction->rollback(&wal);
        activeWriteTransactions.erase(transaction->getID());
    } break;
    default: {
        throw TransactionManagerException("Invalid transaction type to rollback.");
    }
    }
}

void TransactionManager::checkpoint(main::ClientContext& clientContext) {
    std::unique_lock<std::mutex> lck{mtxForSerializingPublicFunctionCalls};
    if (main::DBConfig::isDBPathInMemory(clientContext.getDatabasePath())) {
        return;
    }
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

bool TransactionManager::canAutoCheckpoint(const main::ClientContext& clientContext) const {
    if (main::DBConfig::isDBPathInMemory(clientContext.getDatabasePath())) {
        return false;
    }
    if (!clientContext.getDBConfig()->autoCheckpoint) {
        return false;
    }
    if (clientContext.getTx()->isRecovery()) {
        // Recovery transactions are not allowed to trigger auto checkpoint.
        return false;
    }
    const auto expectedSize = clientContext.getTx()->getEstimatedMemUsage() + wal.getFileSize();
    return expectedSize > clientContext.getDBConfig()->checkpointThreshold;
}

bool TransactionManager::canCheckpointNoLock() const {
    return activeWriteTransactions.empty() && activeReadOnlyTransactions.empty();
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
    // Checkpoint node/relTables, which writes the updated/newly-inserted pages and metadata to
    // disk.
    clientContext.getStorageManager()->checkpoint(clientContext);
    // Checkpoint catalog, which serializes a snapshot of the catalog to disk.
    clientContext.getCatalog()->checkpoint(clientContext.getDatabasePath(),
        clientContext.getVFSUnsafe());
    // Log the checkpoint to the WAL and flush WAL. This indicates that all shadow pages and files(
    // snapshots of catalog and metadata) have been written to disk. The part is not done is replace
    // them with the original pages or catalog and metadata files.
    // If the system crashes before this point, the WAL can still be used to recover the system to a
    // state where the checkpoint can be redo.
    wal.logAndFlushCheckpoint();
    // Replace the original pages and catalog and metadata files with the updated/newly-created
    // ones.
    StorageUtils::overwriteWALVersionFiles(clientContext.getDatabasePath(),
        clientContext.getVFSUnsafe());
    clientContext.getStorageManager()->getShadowFile().replayShadowPageRecords(clientContext);
    // Clear the wal, and also shadowing files.
    wal.clearWAL();
    clientContext.getStorageManager()->getShadowFile().clearAll(clientContext);
    StorageUtils::removeWALVersionFiles(clientContext.getDatabasePath(),
        clientContext.getVFSUnsafe());
    // Resume receiving new transactions.
    allowReceivingNewTransactions();
}

} // namespace transaction
} // namespace kuzu

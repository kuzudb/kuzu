#include "transaction/transaction_manager.h"

#include <thread>

#include "common/exception/checkpoint.h"
#include "common/exception/transaction_manager.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "storage/checkpointer.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace transaction {

std::unique_ptr<Transaction> TransactionManager::beginTransaction(
    main::ClientContext& clientContext, TransactionType type) {
    // We acquire the lock for starting new transactions. In case this cannot be acquired, this
    // ensures calls to other public functions are not restricted.
    std::unique_lock publicFunctionLck{mtxForSerializingPublicFunctionCalls};
    std::unique_lock newTransactionLck{mtxForStartingNewTransactions};
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
    std::unique_lock lck{mtxForSerializingPublicFunctionCalls};
    clientContext.cleanUp();
    const auto transaction = clientContext.getTransaction();
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
        if (transaction->shouldForceCheckpoint() ||
            Checkpointer::canAutoCheckpoint(clientContext)) {
            checkpointNoLock(clientContext);
        }
    } break;
    default: {
        throw TransactionManagerException("Invalid transaction type to commit.");
    }
    }
}

// Note: We take in additional `transaction` here is due to that `transactionContext` might be
// destructed when a transaction throws an exception, while we need to roll back the active
// transaction still.
void TransactionManager::rollback(main::ClientContext& clientContext, Transaction* transaction) {
    std::unique_lock lck{mtxForSerializingPublicFunctionCalls};
    clientContext.cleanUp();
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
    UniqLock lck{mtxForSerializingPublicFunctionCalls};
    if (clientContext.isInMemory()) {
        return;
    }
    checkpointNoLock(clientContext);
}

UniqLock TransactionManager::stopNewTransactionsAndWaitUntilAllTransactionsLeave() {
    UniqLock startTransactionLock{mtxForStartingNewTransactions};
    uint64_t numTimesWaited = 0;
    while (true) {
        if (hasNoActiveTransactions()) {
            break;
        }
        numTimesWaited++;
        if (numTimesWaited * THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS >
            checkpointWaitTimeoutInMicros) {
            throw TransactionManagerException(
                "Timeout waiting for active transactions to leave the system before "
                "checkpointing. If you have an open transaction, please close it and try "
                "again.");
        }
        std::this_thread::sleep_for(
            std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
    }
    return startTransactionLock;
}

bool TransactionManager::hasNoActiveTransactions() const {
    return activeWriteTransactions.empty() && activeReadOnlyTransactions.empty();
}

void TransactionManager::checkpointNoLock(main::ClientContext& clientContext) {
    // Note: It is enough to stop and wait for transactions to leave the system instead of, for
    // example, checking on the query processor's task scheduler. This is because the
    // first and last steps that a connection performs when executing a query are to
    // start and commit/rollback transaction. The query processor also ensures that it
    // will only return results or error after all threads working on the tasks of a
    // query stop working on the tasks of the query and these tasks are removed from the
    // query.
    auto lockForStartingTransaction = stopNewTransactionsAndWaitUntilAllTransactionsLeave();
    Checkpointer checkpointer(clientContext);
    try {
        checkpointer.writeCheckpoint();
    } catch (std::exception& e) {
        checkpointer.rollback();
        throw CheckpointException{e};
    }
}

} // namespace transaction
} // namespace kuzu

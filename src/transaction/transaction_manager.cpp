#include "include/transaction_manager.h"

#include "src/common/include/exception.h"

using namespace graphflow::common;

namespace graphflow {
namespace transaction {

unique_ptr<Transaction> TransactionManager::beginWriteTransaction() {
    lock_t lck{mtx};
    if (hasActiveWriteTransactionNoLock()) {
        throw TransactionManagerException(
            "Cannot start a new write transaction in the system. Only one write transaction at a "
            "time is allowed in the system.");
    }
    auto transaction = unique_ptr<Transaction>(new Transaction(WRITE, ++lastTransactionID));
    activeWriteTransactionID = lastTransactionID;
    return transaction;
}

unique_ptr<Transaction> TransactionManager::beginReadOnlyTransaction() {
    lock_t lck{mtx};
    auto transaction = unique_ptr<Transaction>(new Transaction(READ_ONLY, ++lastTransactionID));
    activeReadOnlyTransactionIDs.insert(transaction->getID());
    return transaction;
}

void TransactionManager::commitOrRollback(Transaction* transaction, bool isCommit) {
    lock_t lck{mtx};
    if (transaction->isReadOnly()) {
        activeReadOnlyTransactionIDs.erase(transaction->getID());
        return;
    }
    if (activeWriteTransactionID != transaction->getID()) {
        throw TransactionManagerException(
            "The ID of the committing write transaction " + to_string(transaction->getID()) +
            " is not equal to the ID of the activeWriteTransaction: " +
            to_string(activeWriteTransactionID));
    }
    if (isCommit) {
        lastCommitID++;
    }
    clearActiveWriteTransactionNoLock();
}

void TransactionManager::commit(Transaction* transaction) {
    commitOrRollback(transaction, true /* is commit */);
}

void TransactionManager::rollback(Transaction* transaction) {
    commitOrRollback(transaction, false /* is rollback */);
}

} // namespace transaction
} // namespace graphflow

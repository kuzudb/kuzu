#include "src/transaction/include/transaction_manager.h"

namespace graphflow {
namespace transaction {

Transaction* TransactionManager::startTransaction() {
    lock_guard lck(transaction_manager_lock);
    auto newTransaction = make_unique<Transaction>(logicalTransactionClock++);
    auto transactionPtr = newTransaction.get();
    activeTransactions.push_back(move(newTransaction));
    return transactionPtr;
}

TransactionStatus TransactionManager::commit(Transaction* transaction) {
    uint64_t commitId;
    {
        lock_guard lck(transaction_manager_lock);
        commitId = logicalTransactionClock++;
    }
    TransactionStatus status;
    status = transaction->commit(logicalTransactionClock++);
    if (FAILED == status.statusType) {
        // commit unsuccessful: rollback the transaction instead
        auto rollbackStatus = transaction->rollback();
        rollbackStatus.msg = status.msg;
        status = rollbackStatus;
    }
    removeTransaction(transaction);
    return status;
}

TransactionStatus TransactionManager::rollback(Transaction* transaction) {
    auto status = transaction->rollback();
    removeTransaction(transaction);
    return status;
}

void TransactionManager::removeTransaction(Transaction* transaction) {
    lock_guard lck(transaction_manager_lock);
    auto idx = activeTransactions.size();
    for (auto i = 0u; i < activeTransactions.size(); i++) {
        if (activeTransactions[i].get() == transaction) {
            idx = i;
            break;
        }
    }
    assert(idx != activeTransactions.size());
    activeTransactions.erase(activeTransactions.begin() + idx);
}

} // namespace transaction
} // namespace graphflow

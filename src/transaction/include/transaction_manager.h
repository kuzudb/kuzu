#pragma once

#include <cassert>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "src/transaction/include/transaction.h"

using namespace std;

namespace graphflow {
namespace transaction {

class TransactionManager {

public:
    Transaction* startTransaction();

    TransactionStatus commit(Transaction* transaction);
    TransactionStatus rollback(Transaction* transaction);

private:
    void removeTransaction(Transaction* transaction);

private:
    uint64_t logicalTransactionClock = 0;
    vector<unique_ptr<Transaction>> activeTransactions;
    mutex transaction_manager_lock;
};

} // namespace transaction
} // namespace graphflow

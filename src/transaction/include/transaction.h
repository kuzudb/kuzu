#pragma once

#include "src/transaction/include/local_storage.h"

namespace graphflow {
namespace transaction {

class TransactionManager;

enum TransactionStatusType { COMMITTED, ROLLBACKED, FAILED };

struct TransactionStatus {
    TransactionStatusType statusType;
    string msg;
};

class Transaction {
    friend class TransactionManager;

public:
    Transaction(uint64_t transactionId) : transactionId{transactionId}, commitId{INVALID_TXN_ID} {};

    const uint64_t getId() { return transactionId; }

    void setIsReadOnly(bool val) { isReadOnly = val; }
    const bool getIsReadOnly() { return isReadOnly; }

private:
    TransactionStatus commit(uint64_t commitId);
    TransactionStatus rollback();

public:
    LocalStorage localStorage;

private:
    static constexpr uint64_t INVALID_TXN_ID = -1ul;

    uint64_t transactionId;
    uint64_t commitId;
    bool isReadOnly;
};

} // namespace transaction
} // namespace graphflow

#pragma once

namespace graphflow {
namespace transaction {

class TransactionManager;

enum TransactionType : uint8_t { READ_ONLY, WRITE };

class Transaction {
    friend class TransactionManager;

public:
    inline TransactionType getType() { return type; }
    inline bool isReadOnly() { return READ_ONLY == type; }
    inline bool isWriteTransaction() { return WRITE == type; }
    inline uint64_t getID() { return ID; }

private:
    Transaction(TransactionType transactionType, uint64_t transactionID)
        : type{transactionType}, ID{transactionID} {}

private:
    TransactionType type;
    uint64_t ID;
};

} // namespace transaction
} // namespace graphflow

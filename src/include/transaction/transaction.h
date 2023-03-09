#pragma once

#include <memory>

namespace kuzu {
namespace transaction {

class TransactionManager;

enum class TransactionType : uint8_t { READ_ONLY, WRITE };

class Transaction {
    friend class TransactionManager;

public:
    Transaction(TransactionType transactionType, uint64_t transactionID)
        : type{transactionType}, ID{transactionID} {}

public:
    inline TransactionType getType() const { return type; }
    inline bool isReadOnly() const { return TransactionType::READ_ONLY == type; }
    inline bool isWriteTransaction() const { return TransactionType::WRITE == type; }
    inline uint64_t getID() const { return ID; }
    static inline std::unique_ptr<Transaction> getDummyWriteTrx() {
        return std::make_unique<Transaction>(TransactionType::WRITE, UINT64_MAX);
    }
    static inline std::unique_ptr<Transaction> getDummyReadOnlyTrx() {
        return std::make_unique<Transaction>(TransactionType::READ_ONLY, UINT64_MAX);
    }

private:
    TransactionType type;
    uint64_t ID;
};

} // namespace transaction
} // namespace kuzu

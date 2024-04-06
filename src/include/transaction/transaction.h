#pragma once

#include <memory>

#include "storage/local_storage/local_storage.h"

namespace kuzu {
namespace main {
class ClientContext;
} // namespace main
namespace storage {
class LocalStorage;
} // namespace storage
namespace transaction {
class TransactionManager;

enum class TransactionType : uint8_t { READ_ONLY, WRITE };
constexpr uint64_t INVALID_TRANSACTION_ID = UINT64_MAX;

class Transaction {
    friend class TransactionManager;

public:
    Transaction(main::ClientContext& clientContext, TransactionType transactionType,
        uint64_t transactionID)
        : type{transactionType}, ID{transactionID} {
        localStorage = std::make_unique<storage::LocalStorage>(clientContext);
    }

    constexpr explicit Transaction(TransactionType transactionType) noexcept
        : type{transactionType}, ID{INVALID_TRANSACTION_ID} {}

public:
    inline TransactionType getType() const { return type; }
    inline bool isReadOnly() const { return TransactionType::READ_ONLY == type; }
    inline bool isWriteTransaction() const { return TransactionType::WRITE == type; }
    inline uint64_t getID() const { return ID; }
    inline storage::LocalStorage* getLocalStorage() { return localStorage.get(); }

    static inline std::unique_ptr<Transaction> getDummyWriteTrx() {
        return std::make_unique<Transaction>(TransactionType::WRITE);
    }
    static inline std::unique_ptr<Transaction> getDummyReadOnlyTrx() {
        return std::make_unique<Transaction>(TransactionType::READ_ONLY);
    }

private:
    TransactionType type;
    // TODO(Guodong): add type transaction_id_t.
    uint64_t ID;
    std::unique_ptr<storage::LocalStorage> localStorage;
};

static Transaction DUMMY_READ_TRANSACTION = Transaction(TransactionType::READ_ONLY);
static Transaction DUMMY_WRITE_TRANSACTION = Transaction(TransactionType::WRITE);

} // namespace transaction
} // namespace kuzu

#pragma once

#include <memory>

#include "common/types/timestamp_t.h"
#include "storage/local_storage/local_storage.h"
#include "storage/undo_buffer.h"

namespace kuzu {
namespace catalog {
class CatalogEntry;
class CatalogSet;
} // namespace catalog
namespace main {
class ClientContext;
} // namespace main
namespace storage {
class LocalStorage;
class UndoBuffer;
class WAL;
} // namespace storage
namespace transaction {
class TransactionManager;

enum class TransactionType : uint8_t { READ_ONLY, WRITE };
constexpr uint64_t INVALID_TRANSACTION_ID = UINT64_MAX;

class Transaction {
    friend class TransactionManager;

public:
    static constexpr common::transaction_t START_TRANSACTION_ID = (common::transaction_t)1 << 63;

    Transaction(main::ClientContext& clientContext, TransactionType transactionType,
        common::transaction_t transactionID, common::transaction_t startTS)
        : type{transactionType}, ID{transactionID}, startTS{startTS} {
        localStorage = std::make_unique<storage::LocalStorage>(clientContext);
        undoBuffer = std::make_unique<storage::UndoBuffer>(clientContext);
        currentTS = common::Timestamp::getCurrentTimestamp().value;
    }

    constexpr explicit Transaction(TransactionType transactionType) noexcept
        : type{transactionType}, ID{0}, startTS{0} {}

public:
    TransactionType getType() const { return type; }
    bool isReadOnly() const { return TransactionType::READ_ONLY == type; }
    bool isWriteTransaction() const { return TransactionType::WRITE == type; }
    common::transaction_t getID() const { return ID; }
    common::transaction_t getStartTS() const { return startTS; }
    common::transaction_t getCommitTS() const { return commitTS; }
    int64_t getCurrentTS() const { return currentTS; }

    void commit(storage::WAL* wal);
    void rollback();

    storage::LocalStorage* getLocalStorage() { return localStorage.get(); }

    void addCatalogEntry(catalog::CatalogSet* catalogSet, catalog::CatalogEntry* catalogEntry);

    static std::unique_ptr<Transaction> getDummyWriteTrx() {
        return std::make_unique<Transaction>(TransactionType::WRITE);
    }
    static std::unique_ptr<Transaction> getDummyReadOnlyTrx() {
        return std::make_unique<Transaction>(TransactionType::READ_ONLY);
    }

private:
    TransactionType type;
    common::transaction_t ID;
    common::transaction_t startTS;
    common::transaction_t commitTS;
    int64_t currentTS;
    std::unique_ptr<storage::LocalStorage> localStorage;
    std::unique_ptr<storage::UndoBuffer> undoBuffer;
};

static Transaction DUMMY_READ_TRANSACTION = Transaction(TransactionType::READ_ONLY);
static Transaction DUMMY_WRITE_TRANSACTION = Transaction(TransactionType::WRITE);

} // namespace transaction
} // namespace kuzu

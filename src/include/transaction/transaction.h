#pragma once

#include "common/types/timestamp_t.h"
#include "storage/local_storage/local_storage.h"
#include "storage/undo_buffer.h"

namespace kuzu {
namespace catalog {
class CatalogEntry;
class CatalogSet;
class SequenceCatalogEntry;
struct SequenceData;
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

enum class TransactionType : uint8_t { READ_ONLY, WRITE, CHECKPOINT, DUMMY };

class Transaction {
    friend class TransactionManager;

public:
    static constexpr common::transaction_t DUMMY_TRANSACTION_ID = 0;
    static constexpr common::transaction_t DUMMY_START_TIMESTAMP = 0;
    static constexpr common::transaction_t START_TRANSACTION_ID =
        static_cast<common::transaction_t>(1) << 63;

    Transaction(main::ClientContext& clientContext, TransactionType transactionType,
        common::transaction_t transactionID, common::transaction_t startTS)
        : type{transactionType}, ID{transactionID}, startTS{startTS},
          commitTS{common::INVALID_TRANSACTION} {
        localStorage = std::make_unique<storage::LocalStorage>(clientContext);
        undoBuffer = std::make_unique<storage::UndoBuffer>(clientContext);
        currentTS = common::Timestamp::getCurrentTimestamp().value;
    }

    explicit Transaction(TransactionType transactionType) noexcept
        : type{transactionType}, ID{DUMMY_TRANSACTION_ID}, startTS{DUMMY_START_TIMESTAMP},
          commitTS{common::INVALID_TRANSACTION} {
        currentTS = common::Timestamp::getCurrentTimestamp().value;
    }
    explicit Transaction(TransactionType transactionType, common::transaction_t ID,
        common::transaction_t startTS) noexcept
        : type{transactionType}, ID{ID}, startTS{startTS}, commitTS{common::INVALID_TRANSACTION} {
        currentTS = common::Timestamp::getCurrentTimestamp().value;
    }

    TransactionType getType() const { return type; }
    bool isDummy() const { return TransactionType::DUMMY == type; }
    bool isReadOnly() const { return TransactionType::READ_ONLY == type; }
    bool isWriteTransaction() const { return TransactionType::WRITE == type; }
    common::transaction_t getID() const { return ID; }
    common::transaction_t getStartTS() const { return startTS; }
    common::transaction_t getCommitTS() const { return commitTS; }
    int64_t getCurrentTS() const { return currentTS; }

    void commit(storage::WAL* wal) const;
    void rollback() const;

    storage::LocalStorage* getLocalStorage() const { return localStorage.get(); }
    bool hasNewlyInsertedNodes(common::table_id_t tableID) const {
        return maxCommittedNodeOffsets.contains(tableID);
    }
    void setMaxCommittedNodeOffset(common::table_id_t tableID, common::offset_t offset) {
        maxCommittedNodeOffsets[tableID] = offset;
    }
    common::offset_t getMaxNodeOffsetBeforeCommit(common::table_id_t tableID) const {
        KU_ASSERT(maxCommittedNodeOffsets.contains(tableID));
        return maxCommittedNodeOffsets.at(tableID);
    }

    void pushCatalogEntry(catalog::CatalogSet& catalogSet,
        catalog::CatalogEntry& catalogEntry) const;
    void pushSequenceChange(catalog::SequenceCatalogEntry* sequenceEntry,
        const catalog::SequenceData& data, int64_t prevVal) const;
    void pushNodeBatchInsert(common::table_id_t tableID) const;
    void pushVectorInsertInfo(storage::VersionInfo& versionInfo, common::idx_t vectorIdx,
        storage::VectorVersionInfo& vectorVersionInfo,
        const std::vector<common::row_idx_t>& rowsInVector) const;
    void pushVectorDeleteInfo(storage::VersionInfo& versionInfo, common::idx_t vectorIdx,
        storage::VectorVersionInfo& vectorVersionInfo,
        const std::vector<common::row_idx_t>& rowsInVector) const;
    void pushVectorUpdateInfo(storage::UpdateInfo& updateInfo, common::idx_t vectorIdx,
        storage::VectorUpdateInfo& vectorUpdateInfo) const;

private:
    TransactionType type;
    common::transaction_t ID;
    common::transaction_t startTS;
    common::transaction_t commitTS;
    int64_t currentTS;
    std::unique_ptr<storage::LocalStorage> localStorage;
    std::unique_ptr<storage::UndoBuffer> undoBuffer;

    std::unordered_map<common::table_id_t, common::offset_t> maxCommittedNodeOffsets;
};

static auto DUMMY_TRANSACTION = Transaction(TransactionType::DUMMY);
static auto DUMMY_CHECKPOINT_TRANSACTION = Transaction(TransactionType::CHECKPOINT,
    Transaction::DUMMY_TRANSACTION_ID, Transaction::START_TRANSACTION_ID - 1);

} // namespace transaction
} // namespace kuzu

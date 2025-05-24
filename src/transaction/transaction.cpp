#include "transaction/transaction.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/runtime.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "storage/local_storage/local_storage.h"
#include "storage/storage_manager.h"
#include "storage/undo_buffer.h"
#include "storage/wal/wal.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace transaction {

bool LocalCacheManager::put(std::unique_ptr<LocalCacheObject> object) {
    std::unique_lock lck{mtx};
    auto key = object->getKey();
    if (cachedObjects.contains(key)) {
        return false;
    }
    cachedObjects[object->getKey()] = std::move(object);
    return true;
}

Transaction::Transaction(main::ClientContext& clientContext, TransactionType transactionType,
    common::transaction_t transactionID, common::transaction_t startTS)
    : type{transactionType}, ID{transactionID}, startTS{startTS},
      commitTS{common::INVALID_TRANSACTION}, forceCheckpoint{false}, hasCatalogChanges{false} {
    this->clientContext = &clientContext;
    localStorage = std::make_unique<storage::LocalStorage>(clientContext);
    undoBuffer = std::make_unique<storage::UndoBuffer>(clientContext.getMemoryManager());
    currentTS = common::Timestamp::getCurrentTimestamp().value;
    // Note that the use of `this` should be safe here as there is no inheritance.
    for (auto entry : clientContext.getCatalog()->getNodeTableEntries(this)) {
        auto id = entry->getTableID();
        minUncommittedNodeOffsets[id] =
            clientContext.getStorageManager()->getTable(id)->getNumTotalRows(this);
    }
}

Transaction::Transaction(TransactionType transactionType) noexcept
    : type{transactionType}, ID{DUMMY_TRANSACTION_ID}, startTS{DUMMY_START_TIMESTAMP},
      commitTS{common::INVALID_TRANSACTION}, clientContext{nullptr}, undoBuffer{nullptr},
      forceCheckpoint{false}, hasCatalogChanges{false} {
    currentTS = common::Timestamp::getCurrentTimestamp().value;
}

Transaction::Transaction(TransactionType transactionType, common::transaction_t ID,
    common::transaction_t startTS) noexcept
    : type{transactionType}, ID{ID}, startTS{startTS}, commitTS{common::INVALID_TRANSACTION},
      clientContext{nullptr}, undoBuffer{nullptr}, forceCheckpoint{false},
      hasCatalogChanges{false} {
    currentTS = common::Timestamp::getCurrentTimestamp().value;
}

bool Transaction::shouldLogToWAL() const {
    // When we are in recovery mode, we don't log to WAL.
    return !isRecovery() && !main::DBConfig::isDBPathInMemory(clientContext->getDatabasePath());
}

bool Transaction::shouldForceCheckpoint() const {
    return !main::DBConfig::isDBPathInMemory(clientContext->getDatabasePath()) && forceCheckpoint;
}

void Transaction::commit(storage::WAL* wal) {
    localStorage->commit();
    undoBuffer->commit(commitTS);
    if (isWriteTransaction() && shouldLogToWAL()) {
        KU_ASSERT(wal);
        wal->logAndFlushCommit();
    }
    if (hasCatalogChanges) {
        clientContext->getCatalog()->incrementVersion();
        hasCatalogChanges = false;
    }
}

void Transaction::rollback(storage::WAL* wal) {
    localStorage->rollback();
    undoBuffer->rollback(this);
    if (isWriteTransaction() && shouldLogToWAL()) {
        KU_ASSERT(wal);
        wal->logRollback();
    }
    hasCatalogChanges = false;
}

uint64_t Transaction::getEstimatedMemUsage() const {
    return localStorage->getEstimatedMemUsage() + undoBuffer->getMemUsage();
}

void Transaction::pushCreateDropCatalogEntry(CatalogSet& catalogSet, CatalogEntry& catalogEntry,
    bool isInternal, bool skipLoggingToWAL) {
    undoBuffer->createCatalogEntry(catalogSet, catalogEntry);
    hasCatalogChanges = true;
    if (!shouldLogToWAL() || skipLoggingToWAL) {
        return;
    }
    const auto wal = clientContext->getWAL();
    KU_ASSERT(wal);
    const auto newCatalogEntry = catalogEntry.getNext();
    switch (newCatalogEntry->getType()) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY: {
        if (catalogEntry.getType() == CatalogEntryType::DUMMY_ENTRY) {
            auto& entry = newCatalogEntry->constCast<TableCatalogEntry>();
            KU_ASSERT(catalogEntry.isDeleted());
            if (entry.hasParent()) { // TODO(Guodong): I don't think this is still needed
                return;
            }
            wal->logCreateCatalogEntryRecord(newCatalogEntry, isInternal);
        } else {
            throw common::RuntimeException("This shouldn't happen. Alter table is not supported.");
        }
    } break;
    case CatalogEntryType::SEQUENCE_ENTRY: {
        KU_ASSERT(
            catalogEntry.getType() == CatalogEntryType::DUMMY_ENTRY && catalogEntry.isDeleted());
        if (newCatalogEntry->hasParent()) {
            // We don't log SERIAL catalog entry creation as it is implicit
            return;
        }
        wal->logCreateCatalogEntryRecord(newCatalogEntry, isInternal);
    } break;
    case CatalogEntryType::SCALAR_MACRO_ENTRY:
    case CatalogEntryType::TYPE_ENTRY: {
        KU_ASSERT(
            catalogEntry.getType() == CatalogEntryType::DUMMY_ENTRY && catalogEntry.isDeleted());
        wal->logCreateCatalogEntryRecord(newCatalogEntry, isInternal);
    } break;
    case CatalogEntryType::DUMMY_ENTRY: {
        KU_ASSERT(newCatalogEntry->isDeleted());
        if (catalogEntry.hasParent()) {
            return;
        }
        switch (catalogEntry.getType()) {
        // Eventually we probably want to merge these
        case CatalogEntryType::NODE_TABLE_ENTRY:
        case CatalogEntryType::REL_GROUP_ENTRY:
        case CatalogEntryType::SEQUENCE_ENTRY: {
            wal->logDropCatalogEntryRecord(catalogEntry.getOID(), catalogEntry.getType());
        } break;
        case CatalogEntryType::INDEX_ENTRY:
        case CatalogEntryType::SCALAR_FUNCTION_ENTRY: {
            // DO NOTHING. We don't persistent index/function entries.
        } break;
        case CatalogEntryType::SCALAR_MACRO_ENTRY:
        case CatalogEntryType::TYPE_ENTRY:
        default: {
            throw common::RuntimeException(
                common::stringFormat("Not supported catalog entry type {} yet.",
                    CatalogEntryTypeUtils::toString(catalogEntry.getType())));
        }
        }
    } break;
    case CatalogEntryType::SCALAR_FUNCTION_ENTRY:
    case CatalogEntryType::INDEX_ENTRY: {
        // DO NOTHING. We don't persistent function/index entries.
    } break;
    default: {
        throw common::RuntimeException(
            common::stringFormat("Not supported catalog entry type {} yet.",
                CatalogEntryTypeUtils::toString(catalogEntry.getType())));
    }
    }
}

void Transaction::pushAlterCatalogEntry(CatalogSet& catalogSet, CatalogEntry& catalogEntry,
    const binder::BoundAlterInfo& alterInfo) {
    undoBuffer->createCatalogEntry(catalogSet, catalogEntry);
    hasCatalogChanges = true;
    if (!shouldLogToWAL()) {
        return;
    }
    const auto wal = clientContext->getWAL();
    KU_ASSERT(wal);
    wal->logAlterCatalogEntryRecord(&alterInfo);
}

void Transaction::pushSequenceChange(SequenceCatalogEntry* sequenceEntry, int64_t kCount,
    const SequenceRollbackData& data) {
    undoBuffer->createSequenceChange(*sequenceEntry, data);
    hasCatalogChanges = true;
    if (clientContext->getTransaction()->shouldLogToWAL()) {
        clientContext->getWAL()->logUpdateSequenceRecord(sequenceEntry->getOID(), kCount);
    }
}

void Transaction::pushInsertInfo(common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
    common::row_idx_t numRows, const storage::VersionRecordHandler* versionRecordHandler) const {
    undoBuffer->createInsertInfo(nodeGroupIdx, startRow, numRows, versionRecordHandler);
}

void Transaction::pushDeleteInfo(common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
    common::row_idx_t numRows, const storage::VersionRecordHandler* versionRecordHandler) const {
    undoBuffer->createDeleteInfo(nodeGroupIdx, startRow, numRows, versionRecordHandler);
}

void Transaction::pushVectorUpdateInfo(storage::UpdateInfo& updateInfo,
    const common::idx_t vectorIdx, storage::VectorUpdateInfo& vectorUpdateInfo) const {
    undoBuffer->createVectorUpdateInfo(&updateInfo, vectorIdx, &vectorUpdateInfo);
}

Transaction::~Transaction() = default;

Transaction::Transaction(TransactionType transactionType, common::transaction_t ID,
    common::transaction_t startTS,
    std::unordered_map<common::table_id_t, common::offset_t> minUncommittedNodeOffsets)
    : type{transactionType}, ID{ID}, startTS{startTS}, commitTS{common::INVALID_TRANSACTION},
      currentTS{INT64_MAX}, clientContext{nullptr}, undoBuffer{nullptr}, forceCheckpoint{false},
      hasCatalogChanges{false}, minUncommittedNodeOffsets{std::move(minUncommittedNodeOffsets)} {}

Transaction Transaction::getDummyTransactionFromExistingOne(const Transaction& other) {
    return Transaction(TransactionType::DUMMY, DUMMY_TRANSACTION_ID, DUMMY_START_TIMESTAMP,
        other.minUncommittedNodeOffsets);
}

Transaction DUMMY_TRANSACTION = Transaction(TransactionType::DUMMY);
Transaction DUMMY_CHECKPOINT_TRANSACTION = Transaction(TransactionType::CHECKPOINT,
    Transaction::DUMMY_TRANSACTION_ID, Transaction::START_TRANSACTION_ID - 1);

} // namespace transaction
} // namespace kuzu

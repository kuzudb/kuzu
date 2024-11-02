#include "transaction/transaction.h"

#include "catalog/catalog_entry/table_catalog_entry.h"
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

Transaction::Transaction(main::ClientContext& clientContext, TransactionType transactionType,
    common::transaction_t transactionID, common::transaction_t startTS)
    : type{transactionType}, ID{transactionID}, startTS{startTS},
      commitTS{common::INVALID_TRANSACTION}, forceCheckpoint{false} {
    this->clientContext = &clientContext;
    localStorage = std::make_unique<storage::LocalStorage>(clientContext);
    undoBuffer = std::make_unique<storage::UndoBuffer>(this);
    currentTS = common::Timestamp::getCurrentTimestamp().value;
    // Note that the use of `this` should be safe here as there is no inheritance.
    for (auto tableID : clientContext.getCatalog()->getNodeTableIDs(this)) {
        minUncommittedNodeOffsets[tableID] =
            clientContext.getStorageManager()->getTable(tableID)->getNumTotalRows(this);
    }
}

Transaction::Transaction(TransactionType transactionType) noexcept
    : type{transactionType}, ID{DUMMY_TRANSACTION_ID}, startTS{DUMMY_START_TIMESTAMP},
      commitTS{common::INVALID_TRANSACTION}, clientContext{nullptr}, undoBuffer{nullptr},
      forceCheckpoint{false} {
    currentTS = common::Timestamp::getCurrentTimestamp().value;
}

Transaction::Transaction(TransactionType transactionType, common::transaction_t ID,
    common::transaction_t startTS) noexcept
    : type{transactionType}, ID{ID}, startTS{startTS}, commitTS{common::INVALID_TRANSACTION},
      clientContext{nullptr}, undoBuffer{nullptr}, forceCheckpoint{false} {
    currentTS = common::Timestamp::getCurrentTimestamp().value;
}

bool Transaction::shouldLogToWAL() const {
    // When we are in recovery mode, we don't log to WAL.
    return !isRecovery() && !main::DBConfig::isDBPathInMemory(clientContext->getDatabasePath());
}

bool Transaction::shouldForceCheckpoint() const {
    return !main::DBConfig::isDBPathInMemory(clientContext->getDatabasePath()) && forceCheckpoint;
}

void Transaction::commit(storage::WAL* wal) const {
    localStorage->commit();
    undoBuffer->commit(commitTS);
    if (isWriteTransaction() && shouldLogToWAL()) {
        KU_ASSERT(wal);
        wal->logAndFlushCommit();
    }
}

void Transaction::rollback(storage::WAL* wal) const {
    localStorage->rollback();
    undoBuffer->rollback();
    if (isWriteTransaction() && shouldLogToWAL()) {
        KU_ASSERT(wal);
        wal->logRollback();
    }
}

uint64_t Transaction::getEstimatedMemUsage() const {
    return localStorage->getEstimatedMemUsage() + undoBuffer->getMemUsage();
}

void Transaction::pushCatalogEntry(CatalogSet& catalogSet, CatalogEntry& catalogEntry,
    bool skipLoggingToWAL) const {
    undoBuffer->createCatalogEntry(catalogSet, catalogEntry);
    if (!shouldLogToWAL() || skipLoggingToWAL) {
        return;
    }
    const auto wal = clientContext->getWAL();
    KU_ASSERT(wal);
    const auto newCatalogEntry = catalogEntry.getNext();
    switch (newCatalogEntry->getType()) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
    case CatalogEntryType::REL_TABLE_ENTRY:
    case CatalogEntryType::REL_GROUP_ENTRY: {
        if (catalogEntry.getType() == CatalogEntryType::DUMMY_ENTRY) {
            KU_ASSERT(catalogEntry.isDeleted());
            auto& tableEntry = newCatalogEntry->constCast<TableCatalogEntry>();
            if (tableEntry.hasParent()) {
                return;
            }
            wal->logCreateTableEntryRecord(
                tableEntry.getBoundCreateTableInfo(clientContext->getTx()));
        } else {
            // Must be alter.
            KU_ASSERT(catalogEntry.getType() == newCatalogEntry->getType());
            const auto& tableEntry = catalogEntry.constCast<TableCatalogEntry>();
            wal->logAlterTableEntryRecord(tableEntry.getAlterInfo());
        }
    } break;
    case CatalogEntryType::SEQUENCE_ENTRY: {
        KU_ASSERT(
            catalogEntry.getType() == CatalogEntryType::DUMMY_ENTRY && catalogEntry.isDeleted());
        if (newCatalogEntry->hasParent()) {
            // We don't log SERIAL catalog entry creation as it is implicit
            return;
        }
        wal->logCreateCatalogEntryRecord(newCatalogEntry);
    } break;
    case CatalogEntryType::SCALAR_MACRO_ENTRY:
    case CatalogEntryType::TYPE_ENTRY: {
        KU_ASSERT(
            catalogEntry.getType() == CatalogEntryType::DUMMY_ENTRY && catalogEntry.isDeleted());
        wal->logCreateCatalogEntryRecord(newCatalogEntry);
    } break;
    case CatalogEntryType::DUMMY_ENTRY: {
        KU_ASSERT(newCatalogEntry->isDeleted());
        if (catalogEntry.hasParent()) {
            return;
        }
        switch (catalogEntry.getType()) {
        // Eventually we probably want to merge these
        case CatalogEntryType::NODE_TABLE_ENTRY:
        case CatalogEntryType::REL_TABLE_ENTRY:
        case CatalogEntryType::REL_GROUP_ENTRY: {
            const auto tableCatalogEntry = catalogEntry.constPtrCast<TableCatalogEntry>();
            if (const auto alterInfo = tableCatalogEntry->getAlterInfo()) {
                // Must be rename table
                wal->logAlterTableEntryRecord(alterInfo);
            } else {
                wal->logDropCatalogEntryRecord(tableCatalogEntry->getTableID(),
                    catalogEntry.getType());
            }
        } break;
        case CatalogEntryType::SEQUENCE_ENTRY: {
            const auto sequenceCatalogEntry = catalogEntry.constPtrCast<SequenceCatalogEntry>();
            wal->logDropCatalogEntryRecord(sequenceCatalogEntry->getOID(), catalogEntry.getType());
        } break;
        case CatalogEntryType::SCALAR_FUNCTION_ENTRY: {
            // DO NOTHING. We don't persistent function entries.
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
    case CatalogEntryType::SCALAR_FUNCTION_ENTRY: {
        // DO NOTHING. We don't persistent function entries.
    } break;
    default: {
        throw common::RuntimeException(
            common::stringFormat("Not supported catalog entry type {} yet.",
                CatalogEntryTypeUtils::toString(catalogEntry.getType())));
    }
    }
}

void Transaction::pushSequenceChange(SequenceCatalogEntry* sequenceEntry, int64_t kCount,
    const SequenceRollbackData& data) const {
    undoBuffer->createSequenceChange(*sequenceEntry, data);
    if (clientContext->getTx()->shouldLogToWAL()) {
        clientContext->getWAL()->logUpdateSequenceRecord(sequenceEntry->getOID(), kCount);
    }
}

void Transaction::pushInsertInfo(storage::ChunkedNodeGroup* chunkedNodeGroup,
    common::row_idx_t startRow, common::row_idx_t numRows) const {
    undoBuffer->createInsertInfo(chunkedNodeGroup, startRow, numRows);
}

void Transaction::pushDeleteInfo(storage::ChunkedNodeGroup* chunkedNodeGroup,
    common::row_idx_t startRow, common::row_idx_t numRows) const {
    undoBuffer->createDeleteInfo(chunkedNodeGroup, startRow, numRows);
}

void Transaction::pushVectorUpdateInfo(storage::UpdateInfo& updateInfo,
    const common::idx_t vectorIdx, storage::VectorUpdateInfo& vectorUpdateInfo) const {
    undoBuffer->createVectorUpdateInfo(&updateInfo, vectorIdx, &vectorUpdateInfo);
}

Transaction::~Transaction() = default;

Transaction::Transaction(TransactionType transactionType, common::transaction_t ID,
    common::transaction_t startTS,
    std::unordered_map<common::table_id_t, common::offset_t> minUncommittedNodeOffsets,
    std::unordered_map<common::table_id_t, common::offset_t> maxCommittedNodeOffsets)
    : type{transactionType}, ID{ID}, startTS{startTS}, commitTS{common::INVALID_TRANSACTION},
      currentTS{INT64_MAX}, clientContext{nullptr}, undoBuffer{nullptr}, forceCheckpoint{false},
      minUncommittedNodeOffsets{minUncommittedNodeOffsets},
      maxCommittedNodeOffsets{maxCommittedNodeOffsets} {}

Transaction Transaction::getDummyTransactionFromExistingOne(const Transaction& other) {
    return Transaction(TransactionType::DUMMY, DUMMY_TRANSACTION_ID, DUMMY_START_TIMESTAMP,
        other.minUncommittedNodeOffsets, other.maxCommittedNodeOffsets);
}

Transaction DUMMY_TRANSACTION = Transaction(TransactionType::DUMMY);
Transaction DUMMY_CHECKPOINT_TRANSACTION = Transaction(TransactionType::CHECKPOINT,
    Transaction::DUMMY_TRANSACTION_ID, Transaction::START_TRANSACTION_ID - 1);

} // namespace transaction
} // namespace kuzu

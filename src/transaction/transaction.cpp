#include "transaction/transaction.h"

#include "main/client_context.h"
#include "storage/local_storage/local_storage.h"
#include "storage/store/version_info.h"
#include "storage/undo_buffer.h"
#include "storage/wal/wal.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace transaction {

Transaction::Transaction(main::ClientContext& clientContext, TransactionType transactionType,
    common::transaction_t transactionID, common::transaction_t startTS)
    : type{transactionType}, ID{transactionID}, startTS{startTS},
      commitTS{common::INVALID_TRANSACTION} {
    this->clientContext = &clientContext;
    localStorage = std::make_unique<storage::LocalStorage>(clientContext);
    undoBuffer = std::make_unique<storage::UndoBuffer>(clientContext, this);
    currentTS = common::Timestamp::getCurrentTimestamp().value;
}

void Transaction::commit(storage::WAL* wal) const {
    if (!isRecovery()) {
        KU_ASSERT(wal);
        wal->logBeginTransaction();
    }
    localStorage->commit(wal);
    undoBuffer->commit(commitTS);
    // During recovery, we don't have a WAL.
    if (!isRecovery()) {
        KU_ASSERT(wal);
        undoBuffer->writeWAL(wal);
        wal->logAndFlushCommit(ID);
        // wal->flushAllPages();
    }
}

void Transaction::rollback() const {
    localStorage->rollback();
    undoBuffer->rollback();
}

void Transaction::pushNodeBatchInsert(common::table_id_t tableID) const {
    undoBuffer->createNodeBatchInsert(tableID);
}

void Transaction::pushCatalogEntry(CatalogSet& catalogSet, CatalogEntry& catalogEntry) const {
    undoBuffer->createCatalogEntry(catalogSet, catalogEntry);
}

void Transaction::pushSequenceChange(SequenceCatalogEntry* sequenceEntry, const SequenceData& data,
    int64_t prevVal) const {
    undoBuffer->createSequenceChange(*sequenceEntry, data, prevVal);
}

void Transaction::pushVectorInsertInfo(storage::VersionInfo& versionInfo,
    const common::idx_t vectorIdx, storage::VectorVersionInfo& vectorVersionInfo,
    common::row_idx_t startRowInVector, common::row_idx_t numRows) const {
    undoBuffer->createVectorInsertInfo(&versionInfo, vectorIdx, &vectorVersionInfo,
        startRowInVector, numRows);
}

void Transaction::pushVectorDeleteInfo(storage::VersionInfo& versionInfo,
    const common::idx_t vectorIdx, storage::VectorVersionInfo& vectorVersionInfo,
    common::row_idx_t startRowInVector, common::row_idx_t numRows) const {
    undoBuffer->createVectorDeleteInfo(&versionInfo, vectorIdx, &vectorVersionInfo,
        startRowInVector, numRows);
}

void Transaction::pushVectorUpdateInfo(storage::UpdateInfo& updateInfo,
    const common::idx_t vectorIdx, storage::VectorUpdateInfo& vectorUpdateInfo) const {
    undoBuffer->createVectorUpdateInfo(&updateInfo, vectorIdx, &vectorUpdateInfo);
}

} // namespace transaction
} // namespace kuzu

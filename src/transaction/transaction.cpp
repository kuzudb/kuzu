#include "transaction/transaction.h"

#include "storage/wal/wal.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace transaction {

void Transaction::commit(storage::WAL* wal) const {
    localStorage->prepareCommit();
    undoBuffer->commit(commitTS);
    wal->logCommit(ID);
    wal->flushAllPages();
}

void Transaction::rollback() const {
    localStorage->prepareRollback();
    undoBuffer->rollback();
}

void Transaction::pushCatalogEntry(CatalogSet& catalogSet, CatalogEntry& catalogEntry) const {
    undoBuffer->createCatalogEntry(&catalogSet, &catalogEntry);
}

void Transaction::pushVectorUpdateInfo(storage::UpdateInfo& updateInfo,
    common::vector_idx_t vectorIdx, storage::VectorUpdateInfo& vectorUpdateInfo) const {
    undoBuffer->createVectorUpdateInfo(&updateInfo, vectorIdx, &vectorUpdateInfo);
}

} // namespace transaction
} // namespace kuzu

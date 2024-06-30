#include "transaction/transaction.h"

#include "storage/store/version_info.h"
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
    const std::vector<common::row_idx_t>& rowsInVector) const {
    undoBuffer->createVectorInsertInfo(&versionInfo, vectorIdx, &vectorVersionInfo, rowsInVector);
}

void Transaction::pushVectorDeleteInfo(storage::VersionInfo& versionInfo,
    const common::idx_t vectorIdx, storage::VectorVersionInfo& vectorVersionInfo,
    const std::vector<common::row_idx_t>& rowsInVector) const {
    undoBuffer->createVectorDeleteInfo(&versionInfo, vectorIdx, &vectorVersionInfo, rowsInVector);
}

void Transaction::pushVectorUpdateInfo(storage::UpdateInfo& updateInfo,
    const common::idx_t vectorIdx, storage::VectorUpdateInfo& vectorUpdateInfo) const {
    undoBuffer->createVectorUpdateInfo(&updateInfo, vectorIdx, &vectorUpdateInfo);
}

} // namespace transaction
} // namespace kuzu

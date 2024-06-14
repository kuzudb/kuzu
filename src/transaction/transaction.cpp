#include "transaction/transaction.h"

#include "storage/wal/wal.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace transaction {

void Transaction::commit(storage::WAL* wal) {
    localStorage->prepareCommit();
    undoBuffer->commit(commitTS);
    wal->logCommit(ID);
    wal->flushAllPages();
}

void Transaction::rollback() {
    localStorage->prepareRollback();
    undoBuffer->rollback();
}

void Transaction::addCatalogEntry(CatalogSet* catalogSet, CatalogEntry* catalogEntry) {
    undoBuffer->createCatalogEntry(*catalogSet, *catalogEntry);
}

void Transaction::addSequenceChange(SequenceCatalogEntry* sequenceEntry, const SequenceData& data,
    int64_t prevVal) {
    undoBuffer->createSequenceChange(*sequenceEntry, data, prevVal);
}

} // namespace transaction
} // namespace kuzu

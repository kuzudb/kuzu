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

} // namespace transaction
} // namespace kuzu

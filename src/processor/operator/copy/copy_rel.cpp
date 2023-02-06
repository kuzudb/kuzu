#include "processor/operator/copy/copy_rel.h"

#include "storage/copy_arrow/copy_rel_arrow.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

uint64_t CopyRel::executeInternal(
    kuzu::common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    auto relCSVCopier = make_unique<CopyRelArrow>(copyDescription, wal->getDirectory(),
        *taskScheduler, *catalog, nodesStatistics->getMaxNodeOffsetPerTable(),
        executionContext->bufferManager, tableID, relsStatistics);
    auto numRelsCopied = relCSVCopier->copy();
    wal->logCopyRelRecord(tableID);
    return numRelsCopied;
}

uint64_t CopyRel::getNumTuplesInTable() {
    return relsStatistics->getReadOnlyVersion()->tableStatisticPerTable[tableID]->getNumTuples();
}

} // namespace processor
} // namespace kuzu

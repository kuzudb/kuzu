#include "processor/operator/copy_csv/copy_rel_csv.h"

#include "storage/in_mem_csv_copier/in_mem_rel_csv_copier.h"

namespace kuzu {
namespace processor {

uint64_t CopyRelCSV::executeInternal(
    kuzu::common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    auto relCSVCopier = make_unique<InMemRelCSVCopier>(csvDescription, wal->getDirectory(),
        *taskScheduler, *catalog, nodesStatistics->getMaxNodeOffsetPerTable(),
        executionContext->bufferManager, tableID, relsStatistics);
    auto numRelsCopied = relCSVCopier->copy();
    wal->logCopyRelCSVRecord(tableID);
    return numRelsCopied;
}

uint64_t CopyRelCSV::getNumTuplesInTable() {
    return relsStatistics->getReadOnlyVersion()->tableStatisticPerTable[tableID]->getNumTuples();
}

} // namespace processor
} // namespace kuzu

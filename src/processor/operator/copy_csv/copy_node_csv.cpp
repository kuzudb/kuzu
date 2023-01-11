#include "processor/operator/copy_csv/copy_node_csv.h"

#include "storage/in_mem_csv_copier/in_mem_arrow_node_copier.h"

namespace kuzu {
namespace processor {

uint64_t CopyNodeCSV::executeInternal(
    common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    auto nodeCSVCopier = make_unique<InMemArrowNodeCopier>(
        csvDescription, wal->getDirectory(), *taskScheduler, *catalog, tableID, nodesStatistics);
    auto numNodesCopied = nodeCSVCopier->copy();
    wal->logCopyNodeCSVRecord(tableID);
    return numNodesCopied;
}

uint64_t CopyNodeCSV::getNumTuplesInTable() {
    // TODO(Ziyi): this chains looks weird. Fix when refactoring table statistics. Ditto in
    // CopyRelCSV.
    return nodesStatistics->getReadOnlyVersion()->tableStatisticPerTable[tableID]->getNumTuples();
}

} // namespace processor
} // namespace kuzu

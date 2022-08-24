#include "src/processor/include/physical_plan/operator/copy_csv/copy_rel_csv.h"

#include "src/storage/in_mem_csv_copier/include/in_mem_rel_csv_copier.h"

namespace graphflow {
namespace processor {

void CopyRelCSV::execute(TaskScheduler& taskScheduler, ExecutionContext* executionContext) {
    auto relCSVCopier = make_unique<InMemRelCSVCopier>(csvDescription, wal->getDirectory(),
        taskScheduler, *catalog, nodesMetadata->getMaxNodeOffsetPerTable(),
        executionContext->bufferManager, tableSchema.tableID);
    // Note: This copy function will update the numRelsPerDirectionBoundTablID and numRels
    // information in Catalog for this relTable.
    relCSVCopier->copy();
    wal->logCopyRelCSVRecord(tableSchema.tableID);
}

} // namespace processor
} // namespace graphflow

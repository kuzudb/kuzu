#include "include/copy_rel_csv.h"

#include "src/storage/in_mem_csv_copier/include/in_mem_rel_csv_copier.h"

namespace graphflow {
namespace processor {

void CopyRelCSV::execute(TaskScheduler& taskScheduler, ExecutionContext* executionContext) {
    auto relCSVCopier = make_unique<InMemRelCSVCopier>(csvDescription, wal->getDirectory(),
        taskScheduler, *catalog, nodesStatisticsAndDeletedIDs->getMaxNodeOffsetPerTable(),
        executionContext->bufferManager, tableSchema.tableID, relsStatistics);
    // Note: This copy function will update the numRelsPerDirectionBoundTable and numRels
    // information in relsStatistics for this relTable.
    relCSVCopier->copy();
    wal->logCopyRelCSVRecord(tableSchema.tableID);
}

} // namespace processor
} // namespace graphflow

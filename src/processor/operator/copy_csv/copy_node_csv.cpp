#include "include/copy_node_csv.h"

#include "src/storage/in_mem_csv_copier/include/in_mem_node_csv_copier.h"

namespace graphflow {
namespace processor {

void CopyNodeCSV::execute(TaskScheduler& taskScheduler, ExecutionContext* executionContext) {
    auto nodeCSVCopier =
        make_unique<InMemNodeCSVCopier>(csvDescription, wal->getDirectory(), taskScheduler,
            *catalog, tableSchema.tableID, &nodesStore->getNodesStatisticsAndDeletedIDs());
    errorIfTableIsNonEmpty(&nodesStore->getNodesStatisticsAndDeletedIDs());
    // Note: This copy function will update the unstructured properties of the nodeTable and the
    // maxNodeOffset in nodesStatisticsAndDeletedIDs.
    nodeCSVCopier->copy();
    wal->logCopyNodeCSVRecord(tableSchema.tableID);
}

} // namespace processor
} // namespace graphflow

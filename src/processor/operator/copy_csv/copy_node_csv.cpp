#include "processor/operator/copy_csv/copy_node_csv.h"

#include "storage/in_mem_csv_copier/in_mem_node_csv_copier.h"

namespace kuzu {
namespace processor {

string CopyNodeCSV::execute(TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    auto nodeCSVCopier =
        make_unique<InMemNodeCSVCopier>(csvDescription, wal->getDirectory(), *taskScheduler,
            *catalog, tableSchema.tableID, &nodesStore->getNodesStatisticsAndDeletedIDs());
    errorIfTableIsNonEmpty(&nodesStore->getNodesStatisticsAndDeletedIDs());
    // Note: This copy function will update the maxNodeOffset in nodesStatisticsAndDeletedIDs.
    auto numNodesCopied = nodeCSVCopier->copy();
    wal->logCopyNodeCSVRecord(tableSchema.tableID);
    return StringUtils::string_format("%d number of nodes has been copied to nodeTable: %s.",
        numNodesCopied, tableSchema.tableName.c_str());
}

} // namespace processor
} // namespace kuzu

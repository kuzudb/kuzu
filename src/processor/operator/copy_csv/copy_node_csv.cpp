#include "processor/operator/copy_csv/copy_node_csv.h"

<<<<<<< HEAD
#include "src/storage/in_mem_csv_copier/include/in_mem_arrow_node_csv_copier.h"
=======
#include "storage/in_mem_csv_copier/in_mem_node_csv_copier.h"
>>>>>>> arrow_cmake

namespace kuzu {
namespace processor {

string CopyNodeCSV::execute(TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    auto nodeCSVCopier =
//        make_unique<InMemNodeCSVCopier>(csvDescription, wal->getDirectory(), *taskScheduler,
//            *catalog, tableSchema.tableID, &nodesStore->getNodesStatisticsAndDeletedIDs());
            make_unique<InMemArrowNodeCSVCopier>(csvDescription, wal->getDirectory(), *taskScheduler,
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

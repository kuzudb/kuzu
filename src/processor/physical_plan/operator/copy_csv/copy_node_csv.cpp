#include "src/processor/include/physical_plan/operator/copy_csv/copy_node_csv.h"

#include "src/storage/in_mem_csv_copier/include/in_mem_node_csv_copier.h"

namespace graphflow {
namespace processor {

void CopyNodeCSV::execute(TaskScheduler& taskScheduler, ExecutionContext* executionContext) {
    auto nodeCSVCopier = make_unique<InMemNodeCSVCopier>(csvDescription, wal->getDirectory(),
        taskScheduler, *catalog, label.labelID, &nodesStore->getNodesMetadata());
    // Note: This copy function will update the unstructured properties of the nodeLabel and the
    // maxNodeOffset in nodesMetadata.
    nodeCSVCopier->copy();
    wal->logCopyNodeCSVRecord(label.labelID);
}

} // namespace processor
} // namespace graphflow

#include "src/processor/include/physical_plan/operator/copy_csv/copy_rel_csv.h"

#include "src/storage/in_mem_csv_copier/include/in_mem_rel_csv_copier.h"

namespace graphflow {
namespace processor {

void CopyRelCSV::execute(TaskScheduler& taskScheduler, ExecutionContext* executionContext) {
    auto relCSVCopier = make_unique<InMemRelCSVCopier>(csvDescription, wal->getDirectory(),
        taskScheduler, *catalog, nodesMetadata->getMaxNodeOffsetPerLabel(),
        executionContext->bufferManager, label.labelID);
    // Note: This copy function will update the numRelsPerDirectionBoundLabel and numRels
    // information in Catalog for this relLabel.
    relCSVCopier->copy();
    wal->logCopyRelCSVRecord(label.labelID);
}

} // namespace processor
} // namespace graphflow

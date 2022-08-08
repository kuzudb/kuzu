#include "src/processor/include/physical_plan/operator/copy_csv/copy_rel_csv.h"

#include "src/storage/in_mem_csv_copier/include/in_mem_rel_csv_copier.h"

namespace graphflow {
namespace processor {

void CopyRelCSV::execute(TaskScheduler& taskScheduler, ExecutionContext* executionContext) {
    auto relCSVCopier = make_unique<InMemRelCSVCopier>(csvDescription, outputDirectory,
        taskScheduler, *catalog, nodesMetadata->getMaxNodeOffsetPerLabel(),
        executionContext->bufferManager, label.labelID);
    relCSVCopier->copy();
    // Todo(Ziyi): these function calls can be removed once we implement the transaction for
    // CopyCSV.
    relsStore->getRel(label.labelID)
        ->loadColumnsAndListsFromDisk(
            *catalog, nodesMetadata->getMaxNodeOffsetPerLabel(), *executionContext->bufferManager);
    nodesMetadata->setAdjListsAndColumns(relsStore);
    catalog->getReadOnlyVersion()->saveToFile(outputDirectory, false /* isForWALRecord */);
}

} // namespace processor
} // namespace graphflow

#include "src/processor/include/physical_plan/operator/copy_csv/copy_node_csv.h"

#include "src/storage/in_mem_csv_copier/include/in_mem_node_csv_copier.h"

namespace graphflow {
namespace processor {

void CopyNodeCSV::execute(TaskScheduler& taskScheduler, ExecutionContext* executionContext) {
    auto nodeCSVCopier = make_unique<InMemNodeCSVCopier>(
        csvDescription, outputDirectory, taskScheduler, *catalog, label.labelID);
    auto numNodesCopied = nodeCSVCopier->copy();
    // Todo(Ziyi): these function calls can be removed once we implement the transaction for
    // CopyCSV.
    nodesStore->getNodesMetadata()
        .getNodeMetadata(label.labelID)
        ->getNodeOffsetInfo()
        ->setMaxNodeOffset(numNodesCopied - 1);
    auto maxNodeOffsets = nodesStore->getNodesMetadata().getMaxNodeOffsetPerLabel();
    nodesStore->getNodesMetadata().saveToFile(
        outputDirectory, false /* isForWALRecord */, TransactionType::READ_ONLY);
    catalog->getReadOnlyVersion()->saveToFile(outputDirectory, false /* isForWALRecord */);
    nodesStore->getNode(label.labelID)
        ->loadColumnsAndListsFromDisk(catalog->getReadOnlyVersion()->getNodeLabel(label.labelID));
}

} // namespace processor
} // namespace graphflow

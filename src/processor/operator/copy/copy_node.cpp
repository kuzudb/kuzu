#include "processor/operator/copy/copy_node.h"

#include "storage/copy_arrow/copy_node_arrow.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

uint64_t CopyNode::executeInternal(
    common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    auto nodeCSVCopier = make_unique<CopyNodeArrow>(
        copyDescription, wal->getDirectory(), *taskScheduler, *catalog, tableID, nodesStatistics);
    auto numNodesCopied = nodeCSVCopier->copy();
    for (auto& relTableSchema : catalog->getAllRelTableSchemasContainBoundTable(tableID)) {
        relsStore.getRelTable(relTableSchema->tableID)
            ->batchInitEmptyRelsForNewNodes(relTableSchema, numNodesCopied);
    }
    wal->logCopyNodeRecord(tableID);
    return numNodesCopied;
}

uint64_t CopyNode::getNumTuplesInTable() {
    // TODO(Ziyi): this chains looks weird. Fix when refactoring table statistics. Ditto in
    // CopyRel.
    return nodesStatistics->getReadOnlyVersion()->tableStatisticPerTable[tableID]->getNumTuples();
}

} // namespace processor
} // namespace kuzu

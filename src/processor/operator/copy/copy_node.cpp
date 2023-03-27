#include "processor/operator/copy/copy_node.h"

#include "storage/copy_arrow/copy_node_arrow.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

uint64_t CopyNode::executeInternal(
    common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    auto nodeCSVCopier = make_unique<CopyNodeArrow>(copyDescription, wal->getDirectory(),
        *taskScheduler, *catalog, tableID, nodesStatistics, executionContext->bufferManager);
    auto numNodesCopied = nodeCSVCopier->copy();
    for (auto& relTableSchema : catalog->getAllRelTableSchemasContainBoundTable(tableID)) {
        relsStore.getRelTable(relTableSchema->tableID)
            ->batchInitEmptyRelsForNewNodes(relTableSchema, numNodesCopied);
    }
    wal->logCopyNodeRecord(tableID);
    return numNodesCopied;
}

} // namespace processor
} // namespace kuzu

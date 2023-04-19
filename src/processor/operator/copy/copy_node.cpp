#include "processor/operator/copy/copy_node.h"

#include "storage/copier/node_copy_executor.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

uint64_t CopyNode::executeInternal(
    common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    auto nodeCopier = std::make_unique<NodeCopyExecutor>(
        copyDescription, wal->getDirectory(), *taskScheduler, *catalog, tableID, nodesStatistics);
    auto numNodesCopied = nodeCopier->copy(executionContext);
    for (auto& relTableSchema : catalog->getAllRelTableSchemasContainBoundTable(tableID)) {
        relsStore.getRelTable(relTableSchema->tableID)
            ->batchInitEmptyRelsForNewNodes(relTableSchema, numNodesCopied);
    }
    wal->logCopyNodeRecord(tableID);
    return numNodesCopied;
}

} // namespace processor
} // namespace kuzu

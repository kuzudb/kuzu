#include "processor/operator/copy/copy_node.h"

#include "storage/copier/node_copier.h"
#include "storage/copier/npy_node_copier.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

uint64_t CopyNode::executeInternal(
    common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    size_t numNodesCopied;

    if (copyDescription.fileType == common::CopyDescription::FileType::NPY) {
        auto nodeCopier = std::make_unique<NpyNodeCopier>(copyDescription, wal->getDirectory(),
            *taskScheduler, *catalog, tableID, nodesStatistics);
        numNodesCopied = nodeCopier->copy();
    } else {
        auto nodeCopier = std::make_unique<NodeCopier>(copyDescription, wal->getDirectory(),
            *taskScheduler, *catalog, tableID, nodesStatistics);
        numNodesCopied = nodeCopier->copy();
    }
    for (auto& relTableSchema : catalog->getAllRelTableSchemasContainBoundTable(tableID)) {
        relsStore.getRelTable(relTableSchema->tableID)
            ->batchInitEmptyRelsForNewNodes(relTableSchema, numNodesCopied);
    }
    wal->logCopyNodeRecord(tableID);
    return numNodesCopied;
}

} // namespace processor
} // namespace kuzu

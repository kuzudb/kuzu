#include "processor/operator/copy/copy_node.h"

#include "storage/copy_arrow/copy_node_arrow.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

uint64_t CopyNode::copy(common::CopyDescription& copyDescription, uint64_t numThreads) {
    auto nodeCSVCopier = make_unique<CopyNodeArrow>(
        copyDescription, wal->getDirectory(), *catalog, tableID, nodesStatistics, numThreads);
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

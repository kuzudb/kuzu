#include "storage/store/node_group_collection.h"

#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/store/column.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeGroupCollection::NodeGroupCollection(const std::vector<LogicalType>& types,
    const bool enableCompression, const row_idx_t startRowIdx, BMFileHandle* dataFH,
    Deserializer* deSer)
    : enableCompression{enableCompression}, startRowIdx{startRowIdx}, numRows{0},
      types{LogicalType::copy(types)}, dataFH{dataFH} {
    if (deSer) {
        KU_ASSERT(dataFH);
        nodeGroups.loadGroups(*deSer);
    }
    const auto lock = nodeGroups.lock();
    for (auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        numRows += nodeGroup->getNumRows();
    }
}

void NodeGroupCollection::append(const Transaction* transaction,
    const std::vector<ValueVector*>& vectors) {
    const auto numRowsToAppend = vectors[0]->state->getSelVector().getSelSize();
    KU_ASSERT(numRowsToAppend == vectors[0]->state->getSelVector().getSelSize());
    for (auto i = 1u; i < vectors.size(); i++) {
        KU_ASSERT(vectors[i]->state->getSelVector().getSelSize() == numRowsToAppend);
    }
    // TODO(Guodong): Optimize the lock contention here. We should first lock to reserve a set of
    // rows to append, then append in parallel without locking.
    const auto lock = nodeGroups.lock();
    if (nodeGroups.isEmpty(lock)) {
        auto newGroup = std::make_unique<NodeGroup>(0, enableCompression, LogicalType::copy(types));
        nodeGroups.appendGroup(lock, std::move(newGroup));
    }
    row_idx_t numRowsAppended = 0u;
    while (numRowsAppended < numRowsToAppend) {
        if (nodeGroups.getLastGroup(lock)->isFull()) {
            auto newGroup = std::make_unique<NodeGroup>(nodeGroups.getNumGroups(lock),
                enableCompression, LogicalType::copy(types));
            nodeGroups.appendGroup(lock, std::move(newGroup));
        }
        const auto& lastNodeGroup = nodeGroups.getLastGroup(lock);
        const auto numToAppendInNodeGroup =
            std::min(numRowsToAppend - numRowsAppended, StorageConstants::NODE_GROUP_SIZE);
        lastNodeGroup->moveNextRowToAppend(numToAppendInNodeGroup);
        lastNodeGroup->append(transaction, vectors, numRowsAppended, numToAppendInNodeGroup);
        numRowsAppended += numToAppendInNodeGroup;
    }
    numRows += numRowsAppended;
}

std::pair<offset_t, offset_t> NodeGroupCollection::appendToLastNodeGroup(Transaction* transaction,
    ChunkedNodeGroup& chunkedGroup) {
    NodeGroup* lastNodeGroup;
    offset_t startOffset = 0;
    offset_t numToAppend = 0;
    bool directFlushWhenAppend;
    {
        const auto lock = nodeGroups.lock();
        startOffset = numRows.load();
        if (nodeGroups.isEmpty(lock)) {
            nodeGroups.appendGroup(lock, std::make_unique<NodeGroup>(nodeGroups.getNumGroups(lock),
                                             enableCompression, LogicalType::copy(types)));
        }
        lastNodeGroup = nodeGroups.getLastGroup(lock);
        auto numRowsLeftInLastNodeGroup = lastNodeGroup->getNumRowsLeftToAppend();
        if (numRowsLeftInLastNodeGroup == 0) {
            nodeGroups.appendGroup(lock, std::make_unique<NodeGroup>(nodeGroups.getNumGroups(lock),
                                             enableCompression, LogicalType::copy(types)));
            lastNodeGroup = nodeGroups.getLastGroup(lock);
            numRowsLeftInLastNodeGroup = lastNodeGroup->getNumRowsLeftToAppend();
        }
        numToAppend = std::min(chunkedGroup.getNumRows(), numRowsLeftInLastNodeGroup);
        lastNodeGroup->moveNextRowToAppend(numToAppend);
        // If the node group is empty now and the chunked group is full, we can directly flush it.
        directFlushWhenAppend =
            numToAppend == numRowsLeftInLastNodeGroup && lastNodeGroup->getNumRows() == 0;
        if (!directFlushWhenAppend) {
            // TODO(Guodong): Furthur optimize on this. Should directly figure out startRowIdx to
            // start appending into the node group and pass in as param.
            lastNodeGroup->append(transaction, chunkedGroup, numToAppend);
        }
    }
    if (directFlushWhenAppend) {
        chunkedGroup.finalize();
        auto flushedGroup = chunkedGroup.flushAsNewChunkedNodeGroup(*dataFH);
        KU_ASSERT(lastNodeGroup->getNumChunkedGroups() == 0);
        lastNodeGroup->merge(transaction, std::move(flushedGroup));
    }
    numRows += numToAppend;
    return {startOffset, numToAppend};
}

row_idx_t NodeGroupCollection::getNumRows() const {
    return numRows.load();
}

void NodeGroupCollection::addColumn(Transaction* transaction, TableAddColumnState& addColumnState) {
    const auto lock = nodeGroups.lock();
    for (const auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        nodeGroup->addColumn(transaction, addColumnState, dataFH);
    }
}

uint64_t NodeGroupCollection::getEstimatedMemoryUsage() {
    auto estimatedMemUsage = 0u;
    const auto lock = nodeGroups.lock();
    for (const auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        estimatedMemUsage += nodeGroup->getEstimatedMemoryUsage();
    }
    return estimatedMemUsage;
}

void NodeGroupCollection::checkpoint(const NodeGroupCheckpointState& state) {
    KU_ASSERT(dataFH);
    const auto lock = nodeGroups.lock();
    for (const auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        nodeGroup->checkpoint(state);
    }
}

void NodeGroupCollection::serialize(Serializer& ser) {
    nodeGroups.serializeGroups(ser);
}

} // namespace storage
} // namespace kuzu

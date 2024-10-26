#include "storage/store/node_group_collection.h"

#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/store/csr_node_group.h"
#include "storage/store/table.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeGroupCollection::NodeGroupCollection(MemoryManager& memoryManager,
    const std::vector<LogicalType>& types, const bool enableCompression, FileHandle* dataFH,
    Deserializer* deSer)
    : enableCompression{enableCompression}, numTotalRows{0}, types{LogicalType::copy(types)},
      dataFH{dataFH} {
    if (deSer) {
        deserialize(*deSer, memoryManager);
    }
    const auto lock = nodeGroups.lock();
    for (auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        numTotalRows += nodeGroup->getNumRows();
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
        auto lastNodeGroup = nodeGroups.getLastGroup(lock);
        if (!lastNodeGroup || lastNodeGroup->isFull()) {
            auto newGroup = std::make_unique<NodeGroup>(nodeGroups.getNumGroups(lock),
                enableCompression, LogicalType::copy(types));
            nodeGroups.appendGroup(lock, std::move(newGroup));
        }
        lastNodeGroup = nodeGroups.getLastGroup(lock);
        const auto numToAppendInNodeGroup =
            std::min(numRowsToAppend - numRowsAppended, lastNodeGroup->getNumRowsLeftToAppend());
        lastNodeGroup->moveNextRowToAppend(numToAppendInNodeGroup);
        lastNodeGroup->append(transaction, vectors, numRowsAppended, numToAppendInNodeGroup);
        numRowsAppended += numToAppendInNodeGroup;
    }
    numTotalRows += numRowsAppended;
    stats.incrementCardinality(numRowsAppended);
}

void NodeGroupCollection::append(const Transaction* transaction, NodeGroupCollection& other) {
    const auto otherLock = other.nodeGroups.lock();
    for (auto& nodeGroup : other.nodeGroups.getAllGroups(otherLock)) {
        appned(transaction, *nodeGroup);
    }
}

void NodeGroupCollection::appned(const Transaction* transaction, NodeGroup& nodeGroup) {
    const auto numRowsToAppend = nodeGroup.getNumRows();
    KU_ASSERT(nodeGroup.getDataTypes().size() == types.size());
    const auto lock = nodeGroups.lock();
    if (nodeGroups.isEmpty(lock)) {
        auto newGroup = std::make_unique<NodeGroup>(0, enableCompression, LogicalType::copy(types));
        nodeGroups.appendGroup(lock, std::move(newGroup));
    }
    const auto numChunkedGroupsToAppend = nodeGroup.getNumChunkedGroups();
    node_group_idx_t numChunkedGroupsAppended = 0;
    while (numChunkedGroupsAppended < numChunkedGroupsToAppend) {
        const auto chunkedGrouoToAppend = nodeGroup.getChunkedNodeGroup(numChunkedGroupsAppended);
        const auto numRowsToAppendInChunkedGroup = chunkedGrouoToAppend->getNumRows();
        row_idx_t numRowsAppendedInChunkedGroup = 0;
        while (numRowsAppendedInChunkedGroup < numRowsToAppendInChunkedGroup) {
            auto lastNodeGroup = nodeGroups.getLastGroup(lock);
            if (!lastNodeGroup || lastNodeGroup->isFull()) {
                auto newGroup = std::make_unique<NodeGroup>(nodeGroups.getNumGroups(lock),
                    enableCompression, LogicalType::copy(types));
                nodeGroups.appendGroup(lock, std::move(newGroup));
            }
            lastNodeGroup = nodeGroups.getLastGroup(lock);
            const auto numToAppendInBatch =
                std::min(numRowsToAppendInChunkedGroup - numRowsAppendedInChunkedGroup,
                    lastNodeGroup->getNumRowsLeftToAppend());
            lastNodeGroup->moveNextRowToAppend(numToAppendInBatch);
            lastNodeGroup->append(transaction, *chunkedGrouoToAppend, numRowsAppendedInChunkedGroup,
                numToAppendInBatch);
            numRowsAppendedInChunkedGroup += numToAppendInBatch;
        }
        numChunkedGroupsAppended++;
    }
    numTotalRows += numRowsToAppend;
    stats.incrementCardinality(numRowsToAppend);
}

std::pair<offset_t, offset_t> NodeGroupCollection::appendToLastNodeGroupAndFlushWhenFull(
    Transaction* transaction, ChunkedNodeGroup& chunkedGroup) {
    NodeGroup* lastNodeGroup = nullptr;
    offset_t startOffset = 0;
    offset_t numToAppend = 0;
    bool directFlushWhenAppend = false;
    {
        const auto lock = nodeGroups.lock();
        startOffset = numTotalRows;
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
            lastNodeGroup->append(transaction, chunkedGroup, 0, numToAppend);
        }
        numTotalRows += numToAppend;
    }
    if (directFlushWhenAppend) {
        chunkedGroup.finalize();
        auto flushedGroup = chunkedGroup.flushAsNewChunkedNodeGroup(transaction, *dataFH);
        KU_ASSERT(lastNodeGroup->getNumChunkedGroups() == 0);
        lastNodeGroup->merge(transaction, std::move(flushedGroup));
    }
    stats.incrementCardinality(numToAppend);
    return {startOffset, numToAppend};
}

row_idx_t NodeGroupCollection::getNumTotalRows() {
    const auto lock = nodeGroups.lock();
    return numTotalRows;
}

NodeGroup* NodeGroupCollection::getOrCreateNodeGroup(node_group_idx_t groupIdx,
    NodeGroupDataFormat format) {
    const auto lock = nodeGroups.lock();
    while (groupIdx >= nodeGroups.getNumGroups(lock)) {
        const auto currentGroupIdx = nodeGroups.getNumGroups(lock);
        nodeGroups.appendGroup(lock, format == NodeGroupDataFormat::REGULAR ?
                                         std::make_unique<NodeGroup>(currentGroupIdx,
                                             enableCompression, LogicalType::copy(types)) :
                                         std::make_unique<CSRNodeGroup>(currentGroupIdx,
                                             enableCompression, LogicalType::copy(types)));
    }
    KU_ASSERT(groupIdx < nodeGroups.getNumGroups(lock));
    return nodeGroups.getGroup(lock, groupIdx);
}

void NodeGroupCollection::addColumn(Transaction* transaction, TableAddColumnState& addColumnState) {
    const auto lock = nodeGroups.lock();
    for (const auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        nodeGroup->addColumn(transaction, addColumnState, dataFH);
    }
    types.push_back(addColumnState.propertyDefinition.getType().copy());
}

uint64_t NodeGroupCollection::getEstimatedMemoryUsage() {
    auto estimatedMemUsage = 0u;
    const auto lock = nodeGroups.lock();
    for (const auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        estimatedMemUsage += nodeGroup->getEstimatedMemoryUsage();
    }
    return estimatedMemUsage;
}

void NodeGroupCollection::checkpoint(MemoryManager& memoryManager,
    NodeGroupCheckpointState& state) {
    KU_ASSERT(dataFH);
    const auto lock = nodeGroups.lock();
    for (const auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        nodeGroup->checkpoint(memoryManager, state);
    }
}

void NodeGroupCollection::serialize(Serializer& ser) {
    ser.writeDebuggingInfo("node_groups");
    nodeGroups.serializeGroups(ser);
    ser.writeDebuggingInfo("stats");
    stats.serialize(ser);
}

void NodeGroupCollection::deserialize(Deserializer& deSer, MemoryManager& memoryManager) {
    std::string key;
    deSer.validateDebuggingInfo(key, "node_groups");
    KU_ASSERT(dataFH);
    nodeGroups.deserializeGroups(memoryManager, deSer);
    deSer.validateDebuggingInfo(key, "stats");
    stats.deserialize(deSer);
}

} // namespace storage
} // namespace kuzu

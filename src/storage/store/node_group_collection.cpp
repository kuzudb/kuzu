#include "storage/store/node_group_collection.h"

#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/store/column.h"
#include "storage/store/table_data.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeGroupCollection::NodeGroupCollection(const std::vector<LogicalType>& types,
    const bool enableCompression, const offset_t startNodeOffset)
    : enableCompression{enableCompression}, startNodeOffset{startNodeOffset}, numRows{0},
      types{types}, dataFH{nullptr} {}

NodeGroupCollection::NodeGroupCollection(const std::vector<LogicalType>& types,
    bool enableCompression, BMFileHandle* dataFH, Deserializer* deSer)
    : enableCompression{enableCompression}, startNodeOffset{0}, numRows{0}, types{types},
      dataFH{dataFH} {
    if (deSer) {
        nodeGroups.loadGroups(*deSer);
    }
    auto lock = nodeGroups.lock();
    for (auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        numRows += nodeGroup->getNumRows();
    }
}

// NodeGroup& NodeGroupCollection::findNodeGroupFromOffset(const offset_t offset) {
//     KU_ASSERT(offset - startNodeOffset < getNumRows());
//     for (const auto& nodeGroup : nodeGroups) {
//         if (nodeGroup->getStartNodeOffset() <= offset &&
//             nodeGroup->getStartNodeOffset() + nodeGroup->getNumRows() > offset) {
//             return *nodeGroup;
//         }
//     }
//     KU_UNREACHABLE;
// }

// void NodeGroupCollection::initializeAppend(AppendState& appendState) {
//     std::unique_lock xLck{mtx};
//     if (nodeGroups.empty()) {
//         nodeGroups.push_back(std::make_unique<NodeGroup>(nodeGroups.size(), enableCompression,
//             ResidencyState::IN_MEMORY, types, startNodeOffset + getNumRows()));
//     }
//     row_idx_t numAppended = 0;
//     appendState.startRowIdx =
//         nodeGroups.back()->getStartNodeOffset() + nodeGroups.back()->getNumRows();
//     while (numAppended < appendState.numRowsToAppend) {
//         auto& lastNodeGroup = nodeGroups.back();
//         const auto numToAppendInNodeGroup = std::min(appendState.numRowsToAppend - numAppended,
//             StorageConstants::NODE_GROUP_SIZE - lastNodeGroup->getNumRows());
//         if (numToAppendInNodeGroup == 0) {
//             nodeGroups.push_back(std::make_unique<NodeGroup>(nodeGroups.size(),
//             enableCompression,
//                 ResidencyState::IN_MEMORY, types, startNodeOffset + getNumRows()));
//         } else {
//             NodeGroupAppendState nodeGroupAppendState{lastNodeGroup.get(),
//                 lastNodeGroup->getNumRows(), numToAppendInNodeGroup};
//             appendState.nodeGroupAppendState.push_back(nodeGroupAppendState);
//             numAppended += numToAppendInNodeGroup;
//             lastNodeGroup->incrementNumRows(numToAppendInNodeGroup);
//         }
//     }
// }

void NodeGroupCollection::append(const Transaction* transaction,
    const std::vector<ValueVector*>& vectors) {
    const auto numRowsToAppend = vectors[0]->state->getSelVector().getSelSize();
    KU_ASSERT(numRowsToAppend == vectors[0]->state->getSelVector().getSelSize());
    for (auto i = 1u; i < vectors.size(); i++) {
        KU_ASSERT(vectors[i]->state->getSelVector().getSelSize() == numRowsToAppend);
    }
    auto lock = nodeGroups.lock();
    if (nodeGroups.isEmpty(lock)) {
        auto newGroup = std::make_unique<NodeGroup>(0, enableCompression, ResidencyState::IN_MEMORY,
            types, startNodeOffset + getNumRows());
        nodeGroups.appendGroup(lock, std::move(newGroup));
    }
    row_idx_t numRowsAppended = 0u;
    while (numRowsAppended < numRowsToAppend) {
        if (nodeGroups.getLastGroup(lock)->isFull()) {
            auto newGroup = std::make_unique<NodeGroup>(0, enableCompression,
                ResidencyState::IN_MEMORY, types, startNodeOffset + getNumRows());
            nodeGroups.appendGroup(lock, std::move(newGroup));
        }
        const auto& lastNodeGroup = nodeGroups.getLastGroup(lock);
        const auto numToAppendInNodeGroup =
            std::min(numRowsToAppend - numRowsAppended, StorageConstants::NODE_GROUP_SIZE);
        lastNodeGroup->append(transaction, vectors, numRowsAppended, numToAppendInNodeGroup);
        numRowsAppended += numToAppendInNodeGroup;
    }
    numRows += numRowsAppended;
}

void NodeGroupCollection::append(const Transaction* transaction,
    const ChunkedNodeGroupCollection& chunkedGroupCollection) {
    const auto numRowsToAppend = chunkedGroupCollection.getNumRows();
    row_idx_t numRowsAppended = 0u;
    auto lock = nodeGroups.lock();
    if (nodeGroups.isEmpty(lock)) {
        nodeGroups.appendGroup(lock,
            std::make_unique<NodeGroup>(0, enableCompression, ResidencyState::IN_MEMORY, types,
                startNodeOffset + getNumRows()));
    }
    while (numRowsAppended < numRowsToAppend) {
        if (nodeGroups.getLastGroup(lock)->isFull()) {
            nodeGroups.appendGroup(lock,
                std::make_unique<NodeGroup>(nodeGroups.getNumGroups(lock), enableCompression,
                    ResidencyState::IN_MEMORY, types, startNodeOffset + getNumRows()));
        }
        const auto& lastNodeGroup = nodeGroups.getLastGroup(lock);
        const auto numToAppendInNodeGroup =
            std::min(numRowsToAppend - numRowsAppended, StorageConstants::NODE_GROUP_SIZE);
        lastNodeGroup->append(transaction, chunkedGroupCollection, numRowsAppended,
            numToAppendInNodeGroup);
        numRowsAppended += numToAppendInNodeGroup;
    }
    numRows += numRowsAppended;
}

// void NodeGroupCollection::append(const Transaction* transaction, const NodeGroupCollection&
// other) { for (auto& nodeGroup : other.nodeGroups) { append(transaction,
// nodeGroup->getChunkedGroups());
// }
// }

std::pair<offset_t, offset_t> NodeGroupCollection::appendPartially(Transaction* transaction,
    ChunkedNodeGroup& chunkedGroup) {
    NodeGroup* nodeGroupToAppend;
    offset_t startOffset = 0;
    offset_t numToAppend = 0;
    {
        auto lock = nodeGroups.lock();
        if (nodeGroups.isEmpty(lock)) {
            nodeGroups.appendGroup(lock,
                std::make_unique<NodeGroup>(0, enableCompression, ResidencyState::IN_MEMORY, types,
                    startNodeOffset + getNumRows()));
        }
        if (nodeGroups.getLastGroup(lock)->isFull()) {
            nodeGroups.appendGroup(lock,
                std::make_unique<NodeGroup>(nodeGroups.getNumGroups(lock), enableCompression,
                    ResidencyState::IN_MEMORY, types, startNodeOffset + getNumRows()));
        }
        nodeGroupToAppend = nodeGroups.getLastGroup(lock);
        numToAppend = std::min(chunkedGroup.getNumRows(),
            StorageConstants::NODE_GROUP_SIZE - nodeGroupToAppend->getNumRows());
        nodeGroupToAppend->incrementNumRows(numToAppend);
    }
    if (chunkedGroup.isFullOrOnDisk() && nodeGroupToAppend->getNumRows() == 0) {
        chunkedGroup.finalize();
        auto flushedGroup = chunkedGroup.flush(*dataFH);
        nodeGroupToAppend->merge(transaction, std::move(flushedGroup));
        startOffset = nodeGroupToAppend->getStartNodeOffset();
        numToAppend = chunkedGroup.getNumRows();
    } else {
        numToAppend = std::min(chunkedGroup.getNumRows(),
            StorageConstants::NODE_GROUP_SIZE - nodeGroupToAppend->getNumRows());
        startOffset = nodeGroupToAppend->append(transaction, chunkedGroup, numToAppend);
    }
    numRows += numToAppend;
    return {startOffset, numToAppend};
}

row_idx_t NodeGroupCollection::getNumRows() const {
    return numRows.load();
}

// void NodeGroupCollection::merge(Transaction* transaction, node_group_idx_t nodeGroupIdx,
//     const ChunkedNodeGroup& chunkedGroup) {
//     KU_ASSERT(chunkedGroup.getResidencyState() == ResidencyState::TEMPORARY ||
//               chunkedGroup.getResidencyState() == ResidencyState::IN_MEMORY);
//     {
//         std::unique_lock xLck{mtx};
//         if (nodeGroupIdx >= nodeGroups.size()) {
//             auto numRows = getNumRows();
//             nodeGroups.resize(nodeGroupIdx + 1);
//             nodeGroups[nodeGroupIdx] = std::make_unique<NodeGroup>(nodeGroupIdx,
//             enableCompression,
//                 ResidencyState::IN_MEMORY, types, numRows);
//         }
//     }
//     if (dataFH && chunkedGroup.isFullOrOnDisk()) {
//         // Flush chunks to disk.
//         auto flushedChunkedGroup = chunkedGroup.flush(*dataFH);
//         auto flushedNodeGroup = std::make_unique<NodeGroup>(nodeGroupIdx, enableCompression,
//             ResidencyState::ON_DISK, types, startNodeOffset + getNumRows());
//         flushedNodeGroup->merge(transaction, std::move(flushedChunkedGroup));
//         {
//             std::unique_lock xLck{mtx};
//             KU_ASSERT(nodeGroups[nodeGroupIdx]->getNumRows() == 0);
//             nodeGroups[nodeGroupIdx] = std::move(flushedNodeGroup);
//         }
//     } else {
//         auto& nodeGroup = *nodeGroups[nodeGroupIdx];
//         // TODO: Should grad a lock on the node group.
//         nodeGroup.append(transaction, chunkedGroup, chunkedGroup.getNumRows());
//         if (dataFH && nodeGroup.isFull()) {
//             nodeGroup.flush(*dataFH);
//         }
//     }
// }

uint64_t NodeGroupCollection::getEstimatedMemoryUsage() {
    auto estimatedMemUsage = 0u;
    auto lock = nodeGroups.lock();
    for (const auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        estimatedMemUsage += nodeGroup->getEstimatedMemoryUsage();
    }
    return estimatedMemUsage;
}

void NodeGroupCollection::checkpoint(const NodeGroupCheckpointState& state) {
    KU_ASSERT(dataFH);
    auto lock = nodeGroups.lock();
    for (const auto& nodeGroup : nodeGroups.getAllGroups(lock)) {
        nodeGroup->checkpoint(state);
    }
}

void NodeGroupCollection::serialize(Serializer& ser) {
    nodeGroups.serializeGroups(ser);
}

} // namespace storage
} // namespace kuzu

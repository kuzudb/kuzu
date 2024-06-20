#include "storage/store/node_group_collection.h"

#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/store/column.h"
#include "storage/store/table_data.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeGroupCollection::NodeGroupCollection(const std::vector<LogicalType>& types,
    const bool enableCompression, const row_idx_t startRowIdx, BMFileHandle* dataFH,
    Deserializer* deSer)
    : enableCompression{enableCompression}, startRowIdx{startRowIdx}, numRows{0}, types{types},
      dataFH{dataFH} {
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
        auto newGroup = std::make_unique<NodeGroup>(0, enableCompression, types);
        nodeGroups.appendGroup(lock, std::move(newGroup));
    }
    row_idx_t numRowsAppended = 0u;
    while (numRowsAppended < numRowsToAppend) {
        if (nodeGroups.getLastGroup(lock)->isFull()) {
            auto newGroup = std::make_unique<NodeGroup>(nodeGroups.getNumGroups(lock),
                enableCompression, types);
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

// void NodeGroupCollection::append(const Transaction* transaction,
//     const ChunkedNodeGroupCollection& chunkedGroupCollection) {
//     const auto numRowsToAppend = chunkedGroupCollection.getNumRows();
//     row_idx_t numRowsAppended = 0u;
//     const auto lock = nodeGroups.lock();
//     if (nodeGroups.isEmpty(lock)) {
//         nodeGroups.appendGroup(lock,
//             std::make_unique<NodeGroup>(0, enableCompression, ResidencyState::IN_MEMORY, types,
//                 startNodeOffset + getNumRows()));
//     }
//     while (numRowsAppended < numRowsToAppend) {
//         if (nodeGroups.getLastGroup(lock)->isFull()) {
//             nodeGroups.appendGroup(lock,
//                 std::make_unique<NodeGroup>(nodeGroups.getNumGroups(lock), enableCompression,
//                     ResidencyState::IN_MEMORY, types, startNodeOffset + getNumRows()));
//         }
//         const auto& lastNodeGroup = nodeGroups.getLastGroup(lock);
//         const auto numToAppendInNodeGroup =
//             std::min(numRowsToAppend - numRowsAppended, StorageConstants::NODE_GROUP_SIZE);
//         lastNodeGroup->append(transaction, chunkedGroupCollection, numRowsAppended,
//             numToAppendInNodeGroup);
//         numRowsAppended += numToAppendInNodeGroup;
//     }
//     numRows += numRowsAppended;
// }

// void NodeGroupCollection::append(const Transaction* transaction, const NodeGroupCollection&
// other) { for (auto& nodeGroup : other.nodeGroups) { append(transaction,
// nodeGroup->getChunkedGroups());
// }
// }

std::pair<offset_t, offset_t> NodeGroupCollection::appendToLastNodeGroup(Transaction* transaction,
    const ChunkedNodeGroup& chunkedGroup) {
    NodeGroup* lastNodeGroup;
    offset_t startOffset = 0;
    offset_t numToAppend = 0;
    bool directFlushWhenAppend;
    {
        const auto lock = nodeGroups.lock();
        if (nodeGroups.isEmpty(lock) || nodeGroups.getLastGroup(lock)->isFull()) {
            nodeGroups.appendGroup(lock, std::make_unique<NodeGroup>(nodeGroups.getNumGroups(lock),
                                             enableCompression, types));
        }
        lastNodeGroup = nodeGroups.getLastGroup(lock);
        const auto numRowsLeftInLastNodeGroup = lastNodeGroup->getNumRowsLeftToAppend();
        numToAppend = std::min(chunkedGroup.getNumRows(), numRowsLeftInLastNodeGroup);
        lastNodeGroup->moveNextRowToAppend(numToAppend);
        // If the node group is empty now and the chunked group is full, we can directly flush it.
        directFlushWhenAppend =
            numToAppend == numRowsLeftInLastNodeGroup && lastNodeGroup->getNumRows() == 0;
    }
    if (directFlushWhenAppend) {
        chunkedGroup.finalize();
        auto flushedGroup = chunkedGroup.flush(*dataFH);
        KU_ASSERT(lastNodeGroup->getNumChunkedGroups() == 0);
        lastNodeGroup->merge(transaction, std::move(flushedGroup));
        startOffset = lastNodeGroup->getStartNodeOffset();
    } else {
        // TODO(Guodong): Furthur optimize on this. Should directly figure out startRowIdx to start
        // appending into the node group and pass in as param.
        startOffset = lastNodeGroup->append(transaction, chunkedGroup, numToAppend);
    }
    numRows += numToAppend;
    return {startOffset, numToAppend};
}

row_idx_t NodeGroupCollection::getNumRows() const {
    return numRows.load();
}

NodeGroup& NodeGroupCollection::findNodeGroupFromOffset(offset_t offset) {
    const auto rowIdx = offset - startRowIdx;
    KU_ASSERT(rowIdx < getNumRows());
    const auto lock = nodeGroups.lock();
    for (auto& chunkedGroup : nodeGroups.getAllGroups(lock)) {
        if (chunkedGroup->getStartNodeOffset() <= rowIdx &&
            chunkedGroup->getStartNodeOffset() + chunkedGroup->getNumRows() >= rowIdx) {
            return *chunkedGroup;
        }
    }
    KU_UNREACHABLE;
}

// void NodeGroupCollection::merge(Transaction* transaction, node_group_idx_t nodeGroupIdx,
//     const ChunkedNodeGroup& chunkedGroup) {
//     KU_ASSERT(
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

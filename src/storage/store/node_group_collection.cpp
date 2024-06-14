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
      types{types}, dataFH{nullptr}, tableData{nullptr} {}

NodeGroupCollection::NodeGroupCollection(const std::vector<LogicalType>& types,
    BMFileHandle* dataFH, const TableData& tableData, Deserializer* deSer)
    : enableCompression{tableData.isCompressionEnabled()}, startNodeOffset{0}, numRows{0},
      types{types}, dataFH{dataFH}, tableData{&tableData} {
    if (deSer) {
        deSer->deserializeVectorOfPtrs<NodeGroup>(nodeGroups);
    }
    for (const auto& nodeGroup : nodeGroups) {
        numRows += nodeGroup->getNumRows();
    }
}

NodeGroup& NodeGroupCollection::findNodeGroupFromOffset(const offset_t offset) {
    std::shared_lock sLck{mtx};
    KU_ASSERT(offset - startNodeOffset < getNumRows());
    for (const auto& nodeGroup : nodeGroups) {
        if (nodeGroup->getStartNodeOffset() <= offset &&
            nodeGroup->getStartNodeOffset() + nodeGroup->getNumRows() > offset) {
            return *nodeGroup;
        }
    }
    KU_UNREACHABLE;
}

void NodeGroupCollection::append(const Transaction* transaction,
    const std::vector<ValueVector*>& vectors) {
    std::unique_lock xLck{mtx};
    const auto numRowsToAppend = vectors[0]->state->getSelVector().getSelSize();
    for (auto i = 1u; i < vectors.size(); i++) {
        KU_ASSERT(vectors[i]->state->getSelVector().getSelSize() == numRowsToAppend);
    }
    if (nodeGroups.empty()) {
        nodeGroups.push_back(std::make_unique<NodeGroup>(nodeGroups.size(), enableCompression,
            ResidencyState::IN_MEMORY, types, startNodeOffset + getNumRows()));
    }
    row_idx_t numRowsAppended = 0u;
    while (numRowsAppended < numRowsToAppend) {
        if (nodeGroups.back()->isFull()) {
            // if (dataFH) {
            // nodeGroups.back()->flush(*dataFH);
            // }
            nodeGroups.push_back(std::make_unique<NodeGroup>(nodeGroups.size(), enableCompression,
                ResidencyState::IN_MEMORY, types, startNodeOffset + getNumRows()));
        }
        const auto& lastNodeGroup = nodeGroups.back();
        const auto numToAppendInNodeGroup =
            std::min(numRowsToAppend - numRowsAppended, StorageConstants::NODE_GROUP_SIZE);
        lastNodeGroup->append(transaction, vectors, numRowsAppended, numToAppendInNodeGroup);
        numRowsAppended += numToAppendInNodeGroup;
    }
}

void NodeGroupCollection::append(const Transaction* transaction,
    const ChunkedNodeGroupCollection& chunkedGroupCollection) {
    std::unique_lock xLck{mtx};
    const auto numRowsToAppend = chunkedGroupCollection.getNumRows();
    row_idx_t numRowsAppended = 0u;
    if (nodeGroups.empty()) {
        nodeGroups.push_back(std::make_unique<NodeGroup>(nodeGroups.size(), enableCompression,
            ResidencyState::IN_MEMORY, types, startNodeOffset + getNumRows()));
    }
    while (numRowsAppended < numRowsToAppend) {
        if (nodeGroups.back()->isFull()) {
            if (dataFH) {
                nodeGroups.back()->flush(*dataFH);
            }
            nodeGroups.push_back(std::make_unique<NodeGroup>(nodeGroups.size(), enableCompression,
                ResidencyState::IN_MEMORY, types, startNodeOffset + getNumRows()));
        }
        const auto& lastNodeGroup = nodeGroups.back();
        const auto numToAppendInNodeGroup =
            std::min(numRowsToAppend - numRowsAppended, StorageConstants::NODE_GROUP_SIZE);
        lastNodeGroup->append(transaction, chunkedGroupCollection, numRowsAppended,
            numToAppendInNodeGroup);
        numRowsAppended += numToAppendInNodeGroup;
    }
}

void NodeGroupCollection::append(const Transaction* transaction, const NodeGroupCollection& other) {
    for (auto& nodeGroup : other.nodeGroups) {
        append(transaction, nodeGroup->getChunkedGroups());
    }
}

std::pair<offset_t, offset_t> NodeGroupCollection::appendPartially(Transaction* transaction,
    ChunkedNodeGroup& chunkedGroup) {
    NodeGroup* nodeGroupToAppend;
    // UniqLock nodeGroupLock;
    offset_t startOffset = 0;
    offset_t numAppended = 0;
    {
        std::unique_lock xLck{mtx};
        if (nodeGroups.empty()) {
            nodeGroups.push_back(std::make_unique<NodeGroup>(nodeGroups.size(), enableCompression,
                ResidencyState::IN_MEMORY, types, startNodeOffset + getNumRows()));
        }
        if (nodeGroups.back()->isFull()) {
            nodeGroups.push_back(std::make_unique<NodeGroup>(nodeGroups.size(), enableCompression,
                ResidencyState::IN_MEMORY, types, startNodeOffset + getNumRows()));
        }
        nodeGroupToAppend = nodeGroups.back().get();
        // nodeGroupLock = nodeGroupToAppend->lock();
    }
    // (void)nodeGroupLock;
    if (chunkedGroup.isFull() && nodeGroupToAppend->getNumRows() == 0) {
        // should call `finalize`.
        chunkedGroup.finalize(INVALID_NODE_GROUP_IDX);
        auto flushedGroup = chunkedGroup.flush(*dataFH);
        nodeGroupToAppend->merge(transaction, std::move(flushedGroup));
        startOffset = nodeGroupToAppend->getStartNodeOffset();
        numAppended = chunkedGroup.getNumRows();
    } else {
        const auto numToAppend = std::min(chunkedGroup.getNumRows(),
            StorageConstants::NODE_GROUP_SIZE - nodeGroupToAppend->getNumRows());
        startOffset = nodeGroupToAppend->append(transaction, chunkedGroup, numToAppend);
        numAppended = numToAppend;
    }
    return {startOffset, numAppended};
}

row_idx_t NodeGroupCollection::getNumRows() const {
    return numRows.load();
}

void NodeGroupCollection::merge(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    const ChunkedNodeGroup& chunkedGroup) {
    KU_ASSERT(chunkedGroup.getResidencyState() == ResidencyState::TEMPORARY ||
              chunkedGroup.getResidencyState() == ResidencyState::IN_MEMORY);
    {
        std::unique_lock xLck{mtx};
        if (nodeGroupIdx >= nodeGroups.size()) {
            auto numRows = getNumRows();
            nodeGroups.resize(nodeGroupIdx + 1);
            nodeGroups[nodeGroupIdx] = std::make_unique<NodeGroup>(nodeGroupIdx, enableCompression,
                ResidencyState::IN_MEMORY, types, numRows);
        }
    }
    if (dataFH && chunkedGroup.isFull()) {
        // Flush chunks to disk.
        auto flushedChunkedGroup = chunkedGroup.flush(*dataFH);
        auto flushedNodeGroup = std::make_unique<NodeGroup>(nodeGroupIdx, enableCompression,
            ResidencyState::ON_DISK, types, startNodeOffset + getNumRows());
        flushedNodeGroup->merge(transaction, std::move(flushedChunkedGroup));
        {
            std::unique_lock xLck{mtx};
            KU_ASSERT(nodeGroups[nodeGroupIdx]->getNumRows() == 0);
            nodeGroups[nodeGroupIdx] = std::move(flushedNodeGroup);
        }
    } else {
        auto& nodeGroup = *nodeGroups[nodeGroupIdx];
        // TODO: Should grad a lock on the node group.
        nodeGroup.append(transaction, chunkedGroup, chunkedGroup.getNumRows());
        if (dataFH && nodeGroup.isFull()) {
            nodeGroup.flush(*dataFH);
        }
    }
}

uint64_t NodeGroupCollection::getEstimatedMemoryUsage() const {
    auto estimatedMemUsage = 0u;
    for (const auto& nodeGroup : nodeGroups) {
        estimatedMemUsage += nodeGroup->getEstimatedMemoryUsage();
    }
    return estimatedMemUsage;
}

void NodeGroupCollection::checkpoint(TableData& tableData) {
    KU_ASSERT(dataFH);
    std::unique_lock xLck{mtx};
    for (const auto& nodeGroup : nodeGroups) {
        nodeGroup->checkpoint(tableData, *dataFH);
    }
}

void NodeGroupCollection::serialize(Serializer& ser) const {
    ser.serializeVectorOfPtrs(nodeGroups);
}

} // namespace storage
} // namespace kuzu

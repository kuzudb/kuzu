#include "storage/store/node_group_collection.h"

#include <storage/buffer_manager/bm_file_handle.h>
#include <storage/store/column.h>
#include <storage/store/table_data.h>

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeGroupCollection::NodeGroupCollection(const std::vector<LogicalType>& types,
    BMFileHandle* dataFH, const TableData& tableData)
    : types{types}, dataFH{dataFH} {
    auto numNodeGroups = tableData.getNumCommittedNodeGroups();
    nodeGroups.reserve(numNodeGroups);
    for (auto nodeGroupIdx = 0u; nodeGroupIdx < numNodeGroups; nodeGroupIdx++) {
        auto nodeGroup = std::make_unique<NodeGroup>(nodeGroupIdx, types);
        auto chunkedGroup = tableData.getCommittedNodeGroup(nodeGroupIdx);
        nodeGroup->merge(&DUMMY_WRITE_TRANSACTION, std::move(chunkedGroup));
        nodeGroups.push_back(std::move(nodeGroup));
    }
}

void NodeGroupCollection::append(const ChunkedNodeGroupCollection& chunkedGroupCollection) {
    std::unique_lock xLck{mtx};
    const auto numRowsToAppend = chunkedGroupCollection.getNumRows();
    row_idx_t numRowsAppended = 0u;
    if (nodeGroups.empty()) {
        nodeGroups.push_back(std::make_unique<NodeGroup>(nodeGroups.size(), types));
    }
    while (numRowsAppended < numRowsToAppend) {
        if (nodeGroups.back()->isFull()) {
            // TODO: Should flush the node group to disk.
            if (dataFH) {
                nodeGroups.back()->flush(*dataFH);
            }
            nodeGroups.push_back(std::make_unique<NodeGroup>(nodeGroups.size(), types));
        }
        const auto& lastNodeGroup = nodeGroups.back();
        const auto numToAppendInNodeGroup =
            std::min(numRowsToAppend - numRowsAppended, StorageConstants::NODE_GROUP_SIZE);
        lastNodeGroup->append(chunkedGroupCollection, numRowsAppended, numToAppendInNodeGroup);
        numRowsAppended += numToAppendInNodeGroup;
    }
}

row_idx_t NodeGroupCollection::getNumRows() {
    std::shared_lock sLck{mtx};
    row_idx_t numRows = 0;
    for (auto& nodeGroup : nodeGroups) {
        numRows += nodeGroup->getNumRows();
    }
    return numRows;
}

void NodeGroupCollection::merge(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    const ChunkedNodeGroup& chunkedGroup) {
    {
        std::unique_lock xLck{mtx};
        if (nodeGroupIdx >= nodeGroups.size()) {
            nodeGroups.resize(nodeGroupIdx + 1);
            nodeGroups[nodeGroupIdx] = std::make_unique<NodeGroup>(nodeGroupIdx, types);
        }
    }
    if (chunkedGroup.isFull()) {
        // Flush chunks to disk.
        auto flushedChunkedGroup = chunkedGroup.flush(*dataFH);
        auto flushedNodeGroup = std::make_unique<NodeGroup>(nodeGroupIdx, types);
        flushedNodeGroup->merge(transaction, std::move(flushedChunkedGroup));
        {
            std::unique_lock xLck{mtx};
            KU_ASSERT(nodeGroups[nodeGroupIdx]->getNumRows() == 0);
            nodeGroups[nodeGroupIdx] = std::move(flushedNodeGroup);
        }
    } else {
        auto& nodeGroup = *nodeGroups[nodeGroupIdx];
        // TODO: Should grad a lock on the node group.
        nodeGroup.append(transaction, chunkedGroup);
        if (nodeGroup.isFull()) {
            nodeGroup.flush(*dataFH);
        }
    }
}

void NodeGroupCollection::checkpoint() {
    KU_ASSERT(dataFH);
    std::unique_lock xLck{mtx};
    for (auto& nodeGroup : nodeGroups) {
        if (nodeGroup->getType() == NodeGroupType::IN_MEMORY)
            nodeGroup->flush(*dataFH);
    }
}

} // namespace storage
} // namespace kuzu

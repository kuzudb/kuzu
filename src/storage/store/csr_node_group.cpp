#include "storage/store/csr_node_group.h"

#include "storage/store/rel_table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void CSRNodeGroup::appendChunkedCSRGroup(const Transaction* transaction,
    ChunkedCSRNodeGroup& chunkedGroup) {
    const auto& csrHeader = chunkedGroup.getCSRHeader();
    std::vector<ColumnChunk*> chunkedGroupForProperties(chunkedGroup.getNumColumns());
    for (auto i = 0u; i < chunkedGroup.getNumColumns(); i++) {
        chunkedGroupForProperties[i] = &chunkedGroup.getColumnChunk(i);
    }
    auto startRow =
        NodeGroup::append(transaction, chunkedGroupForProperties, chunkedGroup.getNumRows());
    if (!csrIndex) {
        csrIndex = std::make_unique<CSRIndex>();
    }
    for (auto i = 0u; i < csrHeader.offset->getNumValues(); i++) {
        const auto length = csrHeader.length->getData().getValue<length_t>(i);
        updateCSRIndex(i, startRow, length);
        startRow += length;
    }
}

void CSRNodeGroup::append(const Transaction* transaction, offset_t boundOffsetInGroup,
    const std::vector<ColumnChunk*>& chunks, row_idx_t rowInChunks) {
    const auto startRow = NodeGroup::append(transaction, chunks, rowInChunks);
    if (!csrIndex) {
        csrIndex = std::make_unique<CSRIndex>();
    }
    updateCSRIndex(boundOffsetInGroup, startRow, 1 /*length*/);
}

void CSRNodeGroup::updateCSRIndex(offset_t boundNodeOffsetInGroup, row_idx_t startRow,
    length_t length) const {
    auto& nodeCSRIndex = csrIndex->indices[boundNodeOffsetInGroup];
    const auto isEmptyCSR = nodeCSRIndex.rowIndices.empty();
    const auto appendToEndOfCSR =
        !isEmptyCSR && nodeCSRIndex.isSequential &&
        (nodeCSRIndex.rowIndices[0] + nodeCSRIndex.rowIndices[1] == startRow);
    const bool sequential = isEmptyCSR || appendToEndOfCSR;
    if (nodeCSRIndex.isSequential && !sequential) {
        // Expand rowIndices for the node.
        const auto csrListStartRow = nodeCSRIndex.rowIndices[0];
        const auto csrListLength = nodeCSRIndex.rowIndices[1];
        nodeCSRIndex.rowIndices.clear();
        nodeCSRIndex.rowIndices.reserve(csrListLength + length);
        for (auto j = 0u; j < csrListLength; j++) {
            nodeCSRIndex.rowIndices.push_back(csrListStartRow + j);
        }
    }
    if (sequential) {
        nodeCSRIndex.isSequential = true;
        if (!nodeCSRIndex.rowIndices.empty()) {
            KU_ASSERT(appendToEndOfCSR);
            nodeCSRIndex.rowIndices[1] += length;
        } else {
            nodeCSRIndex.rowIndices.resize(2);
            nodeCSRIndex.rowIndices[0] = startRow;
            nodeCSRIndex.rowIndices[1] = length;
        }
    } else {
        for (auto j = 0u; j < length; j++) {
            nodeCSRIndex.rowIndices.push_back(startRow + j);
        }
        std::sort(nodeCSRIndex.rowIndices.begin(), nodeCSRIndex.rowIndices.end());
    }
}

void CSRNodeGroup::initializeScanState(Transaction* transaction, TableScanState& state) {
    auto& relScanState = state.cast<RelTableScanState>();
    KU_ASSERT(nodeGroupIdx == StorageUtils::getNodeGroupIdx(relScanState.boundNodeOffset));
    KU_ASSERT(relScanState.nodeGroupScanState);
    auto& nodeGroupScanState = relScanState.nodeGroupScanState->cast<CSRNodeGroupScanState>();
    if (relScanState.nodeGroupIdx != nodeGroupIdx) {
        // Initialize the scan state for checkpointed data in the node group.
        relScanState.nodeGroupScanState->resetState();
        relScanState.nodeGroupIdx = nodeGroupIdx;
        // Scan the csr header chunks from disk.
        ChunkState offsetState, lengthState;
        auto& csrChunkGroup = persistentChunkGroup->cast<ChunkedCSRNodeGroup>();
        const auto& csrHeader = csrChunkGroup.getCSRHeader();
        csrHeader.offset->initializeScanState(offsetState);
        csrHeader.length->initializeScanState(lengthState);
        offsetState.column = relScanState.csrOffsetColumn;
        lengthState.column = relScanState.csrLengthColumn;
        csrHeader.offset->scanCommitted<ResidencyState::ON_DISK>(transaction, offsetState,
            *nodeGroupScanState.csrHeader->offset);
        csrHeader.length->scanCommitted<ResidencyState::ON_DISK>(transaction, lengthState,
            *nodeGroupScanState.csrHeader->length);
        // Initialize the scan state for the persisted data in the node group.
        relScanState.zoneMapResult = ZoneMapCheckResult::ALWAYS_SCAN;
        for (auto i = 0u; i < relScanState.columnIDs.size(); i++) {
            if (relScanState.columnIDs[i] == INVALID_COLUMN_ID) {
                continue;
            }
            auto& chunk = persistentChunkGroup->getColumnChunk(relScanState.columnIDs[i]);
            chunk.initializeScanState(nodeGroupScanState.chunkStates[i]);
            // TODO: Not a good way to initialize column for chunkState here.
            nodeGroupScanState.chunkStates[i].column = state.columns[i];
        }
    }
    // Switch to a new bound node (i.e., new csr list) in the node group.
    nodeGroupScanState.nextRowToScan = 0;
    nodeGroupScanState.persistentCSRList.startRow = nodeGroupScanState.csrHeader->getStartCSROffset(
        relScanState.boundNodeOffset - startNodeOffset);
    nodeGroupScanState.persistentCSRList.length =
        nodeGroupScanState.csrHeader->getCSRLength(relScanState.boundNodeOffset - startNodeOffset);
    nodeGroupScanState.source = CSRNodeGroupScanSource::COMMITTED_PERSISTENT;
    if (csrIndex) {
        nodeGroupScanState.inMemCSRList =
            csrIndex->indices[relScanState.boundNodeOffset - startNodeOffset];
        if (!nodeGroupScanState.inMemCSRList.isSequential) {
            KU_ASSERT(std::is_sorted(nodeGroupScanState.inMemCSRList.rowIndices.begin(),
                nodeGroupScanState.inMemCSRList.rowIndices.end()));
        }
    }
}

NodeGroupScanResult CSRNodeGroup::scan(Transaction* transaction, TableScanState& state) {
    const auto& relScanState = state.cast<RelTableScanState>();
    auto& nodeGroupScanState = relScanState.nodeGroupScanState->cast<CSRNodeGroupScanState>();
    while (true) {
        switch (nodeGroupScanState.source) {
        case CSRNodeGroupScanSource::COMMITTED_PERSISTENT: {
            auto result = scanCommittedPersistent(transaction, relScanState, nodeGroupScanState);
            if (result == NODE_GROUP_SCAN_EMMPTY_RESULT) {
                continue;
            }
            return result;
        }
        case CSRNodeGroupScanSource::COMMITTED_IN_MEMORY: {
            auto result =
                nodeGroupScanState.inMemCSRList.isSequential ?
                    scanCommittedInMemSequential(transaction, relScanState, nodeGroupScanState) :
                    scanCommittedInMemRandom(transaction, relScanState, nodeGroupScanState);
            if (result == NODE_GROUP_SCAN_EMMPTY_RESULT) {
                nodeGroupScanState.source = CSRNodeGroupScanSource::NONE;
                continue;
            }
            return result;
        }
        case CSRNodeGroupScanSource::NONE: {
            relScanState.IDVector->state->getSelVectorUnsafe().setSelSize(0);
            return NODE_GROUP_SCAN_EMMPTY_RESULT;
        }
        default: {
            KU_UNREACHABLE;
        }
        }
    }
}

NodeGroupScanResult CSRNodeGroup::scanCommittedPersistent(Transaction* transaction,
    const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) {
    if (nodeGroupScanState.nextRowToScan == nodeGroupScanState.persistentCSRList.length) {
        nodeGroupScanState.source = CSRNodeGroupScanSource::COMMITTED_IN_MEMORY;
        nodeGroupScanState.nextRowToScan = 0;
        return NODE_GROUP_SCAN_EMMPTY_RESULT;
    }
    const auto startRow =
        nodeGroupScanState.persistentCSRList.startRow + nodeGroupScanState.nextRowToScan;
    const auto numToScan =
        std::min(nodeGroupScanState.persistentCSRList.length - nodeGroupScanState.nextRowToScan,
            DEFAULT_VECTOR_CAPACITY);
    const auto endRow = startRow + numToScan;
    for (auto i = 0u; i < tableState.columnIDs.size(); i++) {
        const auto columnID = tableState.columnIDs[i];
        if (columnID == INVALID_COLUMN_ID) {
            tableState.outputVectors[i]->setAllNull();
            continue;
        }
        tableState.outputVectors[i]->state->getSelVectorUnsafe().setSelSize(numToScan);
        tableState.columns[i]->scan(transaction, nodeGroupScanState.chunkStates[i], startRow,
            endRow, tableState.outputVectors[i], static_cast<uint64_t>(0));
    }
    nodeGroupScanState.nextRowToScan += numToScan;
    return NodeGroupScanResult{startRow, numToScan};
}

NodeGroupScanResult CSRNodeGroup::scanCommittedInMemSequential(const Transaction* transaction,
    const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) {
    const auto startRow =
        nodeGroupScanState.inMemCSRList.rowIndices[0] + nodeGroupScanState.nextRowToScan;
    const auto numRows =
        std::min(nodeGroupScanState.inMemCSRList.rowIndices[1] - nodeGroupScanState.nextRowToScan,
            DEFAULT_VECTOR_CAPACITY);
    if (numRows == 0) {
        return NODE_GROUP_SCAN_EMMPTY_RESULT;
    }
    const auto endRow = startRow + numRows;
    auto [startChunkIdx, startRowInChunk] =
        StorageUtils::getQuotientRemainder(startRow, DEFAULT_VECTOR_CAPACITY);
    auto [endChunkIdx, endRowInChunk] =
        StorageUtils::getQuotientRemainder(endRow, DEFAULT_VECTOR_CAPACITY);
    auto chunkIdx = startChunkIdx;
    ChunkedNodeGroup* chunkedGroup;
    while (chunkIdx <= endChunkIdx) {
        {
            const auto lock = chunkedGroups.lock();
            chunkedGroup = chunkedGroups.getGroup(lock, chunkIdx);
        }
        const auto startRowInChunkToScan = chunkIdx == startChunkIdx ? startRowInChunk : 0;
        const auto endRowInChunkToScan =
            chunkIdx == endChunkIdx ? endRowInChunk : DEFAULT_VECTOR_CAPACITY;
        const auto numToScan = std::min(endRowInChunkToScan - startRowInChunkToScan,
            DEFAULT_VECTOR_CAPACITY - startRowInChunkToScan);
        chunkedGroup->scan(transaction, tableState, nodeGroupScanState, startRowInChunkToScan,
            numToScan);
        chunkIdx++;
    }
    nodeGroupScanState.nextRowToScan += numRows;
    return NodeGroupScanResult{startRow, numRows};
}

NodeGroupScanResult CSRNodeGroup::scanCommittedInMemRandom(Transaction* transaction,
    const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) {
    const auto numRows = std::min(nodeGroupScanState.inMemCSRList.rowIndices.size() -
                                      nodeGroupScanState.nextRowToScan,
        DEFAULT_VECTOR_CAPACITY);
    if (numRows == 0) {
        return NODE_GROUP_SCAN_EMMPTY_RESULT;
    }
    row_idx_t nextRow = 0;
    ChunkedNodeGroup* chunkedGroup = nullptr;
    node_group_idx_t currentChunkIdx = INVALID_NODE_GROUP_IDX;
    while (nextRow < numRows) {
        const auto rowIdx =
            nodeGroupScanState.inMemCSRList.rowIndices[nextRow + nodeGroupScanState.nextRowToScan];
        auto [chunkIdx, rowInChunk] =
            StorageUtils::getQuotientRemainder(rowIdx, DEFAULT_VECTOR_CAPACITY);
        if (chunkIdx != currentChunkIdx) {
            currentChunkIdx = chunkIdx;
            const auto lock = chunkedGroups.lock();
            chunkedGroup = chunkedGroups.getGroup(lock, chunkIdx);
        }
        KU_ASSERT(chunkedGroup);
        chunkedGroup->lookup(transaction, tableState, nodeGroupScanState, rowInChunk, nextRow);
        nextRow++;
    }
    nodeGroupScanState.nextRowToScan += numRows;
    tableState.IDVector->state->getSelVectorUnsafe().setSelSize(numRows);
    return NodeGroupScanResult{0, numRows};
}

} // namespace storage
} // namespace kuzu

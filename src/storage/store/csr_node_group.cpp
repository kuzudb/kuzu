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
        csrIndex->indices[i].isSequential = true;
        csrIndex->indices[i].rowIndices.resize(2);
        const auto length = csrHeader.length->getData().getValue<length_t>(i);
        csrIndex->indices[i].rowIndices[0] = startRow;
        csrIndex->indices[i].rowIndices[1] = length;
        startRow += length;
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
            auto& rowIndices = nodeGroupScanState.inMemCSRList.rowIndices;
            // TODO(Guodong): Should sort when write. not during read.
            std::sort(rowIndices.begin(), rowIndices.end());
        }
    }
}

bool CSRNodeGroup::scan(Transaction* transaction, TableScanState& state) {
    const auto& relScanState = state.cast<RelTableScanState>();
    auto& nodeGroupScanState = relScanState.nodeGroupScanState->cast<CSRNodeGroupScanState>();
    while (true) {
        switch (nodeGroupScanState.source) {
        case CSRNodeGroupScanSource::COMMITTED_PERSISTENT: {
            if (!scanCommittedPersistent(transaction, relScanState, nodeGroupScanState)) {
                continue;
            }
            return true;
        }
        case CSRNodeGroupScanSource::COMMITTED_IN_MEMORY: {
            const bool hasMoreRows =
                nodeGroupScanState.inMemCSRList.isSequential ?
                    scanCommittedInMemSequential(transaction, relScanState, nodeGroupScanState) :
                    scanCommittedInMemRandom(transaction, relScanState, nodeGroupScanState);
            if (!hasMoreRows) {
                nodeGroupScanState.source = CSRNodeGroupScanSource::NONE;
                continue;
            }
            return true;
        }
        case CSRNodeGroupScanSource::NONE: {
            relScanState.relIDVector->state->getSelVectorUnsafe().setSelSize(0);
            return false;
        }
        default: {
            KU_UNREACHABLE;
        }
        }
    }
}

bool CSRNodeGroup::scanCommittedPersistent(Transaction* transaction,
    const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) {
    if (nodeGroupScanState.nextRowToScan == nodeGroupScanState.persistentCSRList.length) {
        nodeGroupScanState.source = CSRNodeGroupScanSource::COMMITTED_IN_MEMORY;
        nodeGroupScanState.nextRowToScan = 0;
        return false;
    }
    const auto startOffset =
        nodeGroupScanState.persistentCSRList.startRow + nodeGroupScanState.nextRowToScan;
    const auto numToScan =
        std::min(nodeGroupScanState.persistentCSRList.length - nodeGroupScanState.nextRowToScan,
            DEFAULT_VECTOR_CAPACITY);
    const auto endOffset = startOffset + numToScan;
    for (auto i = 0u; i < tableState.columnIDs.size(); i++) {
        const auto columnID = tableState.columnIDs[i];
        if (columnID == INVALID_COLUMN_ID) {
            tableState.outputVectors[i]->setAllNull();
            tableState.outputVectors[i]->state->getSelVectorUnsafe().setSelSize(numToScan);
            continue;
        }
        tableState.columns[i]->scan(transaction, nodeGroupScanState.chunkStates[i], startOffset,
            endOffset, tableState.outputVectors[i], static_cast<uint64_t>(0));
        tableState.outputVectors[i]->state->getSelVectorUnsafe().setSelSize(numToScan);
    }
    nodeGroupScanState.nextRowToScan += numToScan;
    return true;
}

bool CSRNodeGroup::scanCommittedInMemSequential(Transaction* transaction,
    const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) {
    const auto startRow =
        nodeGroupScanState.inMemCSRList.rowIndices[0] + nodeGroupScanState.nextRowToScan;
    const auto numRows =
        std::min(nodeGroupScanState.inMemCSRList.rowIndices[1] - nodeGroupScanState.nextRowToScan,
            DEFAULT_VECTOR_CAPACITY);
    if (numRows == 0) {
        return false;
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
    tableState.relIDVector->state->getSelVectorUnsafe().setSelSize(numRows);
    return true;
}

bool CSRNodeGroup::scanCommittedInMemRandom(Transaction* transaction,
    const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) {
    const auto numRows = std::min(nodeGroupScanState.inMemCSRList.rowIndices.size() -
                                      nodeGroupScanState.nextRowToScan,
        DEFAULT_VECTOR_CAPACITY);
    if (numRows == 0) {
        return false;
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
    return true;
}

// template<ResidencyState SCAN_RESIDENCY_STATE>
// std::unique_ptr<CSRNodeGroup> CSRNodeGroup::scanCommitted(
//     const std::vector<common::column_id_t>& columnIDs, const std::vector<Column*>& columns) {}
//
// template std::unique_ptr<CSRNodeGroup> CSRNodeGroup::scanCommitted<ResidencyState::ON_DISK>(
//     const std::vector<column_id_t>& columnIDs, const std::vector<Column*>& columns);
// template std::unique_ptr<CSRNodeGroup> CSRNodeGroup::scanCommitted<ResidencyState::IN_MEMORY>(
//     const std::vector<column_id_t>& columnIDs, const std::vector<Column*>& columns);

} // namespace storage
} // namespace kuzu

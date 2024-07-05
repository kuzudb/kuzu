#include "storage/store/csr_node_group.h"

#include "storage/store/rel_table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

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
        if (persistentChunkGroup) {
            initializePersistentCSRHeader(transaction, relScanState, nodeGroupScanState);
        }
    }
    // Switch to a new bound node (i.e., new csr list) in the node group.
    nodeGroupScanState.nextRowToScan = 0;
    const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    const auto offsetInGroup = relScanState.boundNodeOffset - startNodeOffset;
    if (persistentChunkGroup) {
        nodeGroupScanState.persistentCSRList.startRow =
            nodeGroupScanState.csrHeader->getStartCSROffset(offsetInGroup);
        nodeGroupScanState.persistentCSRList.length =
            nodeGroupScanState.csrHeader->getCSRLength(offsetInGroup);
    }
    if (csrIndex) {
        nodeGroupScanState.inMemCSRList = csrIndex->indices[offsetInGroup];
        if (!nodeGroupScanState.inMemCSRList.isSequential) {
            KU_ASSERT(std::is_sorted(nodeGroupScanState.inMemCSRList.rowIndices.begin(),
                nodeGroupScanState.inMemCSRList.rowIndices.end()));
        }
    }
    if (persistentChunkGroup && nodeGroupScanState.persistentCSRList.length > 0) {
        nodeGroupScanState.source = CSRNodeGroupScanSource::COMMITTED_PERSISTENT;
    } else if (csrIndex && nodeGroupScanState.inMemCSRList.rowIndices.size() > 0) {
        nodeGroupScanState.source = CSRNodeGroupScanSource::COMMITTED_IN_MEMORY;
    } else {
        nodeGroupScanState.source = CSRNodeGroupScanSource::NONE;
    }
}

void CSRNodeGroup::initializePersistentCSRHeader(Transaction* transaction,
    RelTableScanState& relScanState, CSRNodeGroupScanState& nodeGroupScanState) const {
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
        if (relScanState.columnIDs[i] == INVALID_COLUMN_ID ||
            relScanState.columnIDs[i] == ROW_IDX_COLUMN_ID) {
            continue;
        }
        auto& chunk = persistentChunkGroup->getColumnChunk(relScanState.columnIDs[i]);
        chunk.initializeScanState(nodeGroupScanState.chunkStates[i]);
        // TODO: Not a good way to initialize column for chunkState here.
        nodeGroupScanState.chunkStates[i].column = relScanState.columns[i];
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
                relScanState.IDVector->state->getSelVectorUnsafe().setSelSize(0);
                return NODE_GROUP_SCAN_EMMPTY_RESULT;
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

NodeGroupScanResult CSRNodeGroup::scanCommittedPersistent(const Transaction* transaction,
    const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) const {
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
    persistentChunkGroup->scan(transaction, tableState, nodeGroupScanState, startRow, numToScan);
    nodeGroupScanState.nextRowToScan += numToScan;
    return NodeGroupScanResult{startRow, numToScan};
}

NodeGroupScanResult CSRNodeGroup::scanCommittedInMemSequential(const Transaction* transaction,
    const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) {
    const auto startRow =
        nodeGroupScanState.inMemCSRList.rowIndices[0] + nodeGroupScanState.nextRowToScan;
    auto numRows =
        std::min(nodeGroupScanState.inMemCSRList.rowIndices[1] - nodeGroupScanState.nextRowToScan,
            DEFAULT_VECTOR_CAPACITY);
    auto [chunkIdx, startRowInChunk] =
        StorageUtils::getQuotientRemainder(startRow, ChunkedNodeGroup::CHUNK_CAPACITY);
    numRows = std::min(numRows, ChunkedNodeGroup::CHUNK_CAPACITY - startRowInChunk);
    if (numRows == 0) {
        return NODE_GROUP_SCAN_EMMPTY_RESULT;
    }
    ChunkedNodeGroup* chunkedGroup;
    {
        const auto lock = chunkedGroups.lock();
        chunkedGroup = chunkedGroups.getGroup(lock, chunkIdx);
    }
    chunkedGroup->scan(transaction, tableState, nodeGroupScanState, startRowInChunk, numRows);
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

void CSRNodeGroup::appendChunkedCSRGroup(const Transaction* transaction,
    ChunkedCSRNodeGroup& chunkedGroup) {
    const auto& csrHeader = chunkedGroup.getCSRHeader();
    std::vector<ColumnChunk*> chunkedGroupForProperties(chunkedGroup.getNumColumns());
    for (auto i = 0u; i < chunkedGroup.getNumColumns(); i++) {
        chunkedGroupForProperties[i] = &chunkedGroup.getColumnChunk(i);
    }
    auto startRow =
        NodeGroup::append(transaction, chunkedGroupForProperties, 0, chunkedGroup.getNumRows());
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
    const std::vector<ColumnChunk*>& chunks, row_idx_t startRowInChunks, row_idx_t numRows) {
    const auto startRow = NodeGroup::append(transaction, chunks, startRowInChunks, numRows);
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
        nodeCSRIndex.isSequential = false;
        for (auto j = 0u; j < length; j++) {
            nodeCSRIndex.rowIndices.push_back(startRow + j);
        }
        std::sort(nodeCSRIndex.rowIndices.begin(), nodeCSRIndex.rowIndices.end());
    }
}

void CSRNodeGroup::update(Transaction* transaction, CSRNodeGroupScanSource source,
    row_idx_t rowIdxInGroup, column_id_t columnID, const ::ValueVector& propertyVector) {
    switch (source) {
    case CSRNodeGroupScanSource::COMMITTED_PERSISTENT: {
        KU_ASSERT(persistentChunkGroup);
        return persistentChunkGroup->update(transaction, rowIdxInGroup, columnID, propertyVector);
    }
    case CSRNodeGroupScanSource::COMMITTED_IN_MEMORY: {
        KU_ASSERT(csrIndex);
        auto [chunkIdx, rowInChunk] =
            StorageUtils::getQuotientRemainder(rowIdxInGroup, ChunkedNodeGroup::CHUNK_CAPACITY);
        const auto lock = chunkedGroups.lock();
        const auto chunkedGroup = chunkedGroups.getGroup(lock, chunkIdx);
        return chunkedGroup->update(transaction, rowInChunk, columnID, propertyVector);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

bool CSRNodeGroup::delete_(const Transaction* transaction, CSRNodeGroupScanSource source,
    row_idx_t rowIdxInGroup) {
    switch (source) {
    case CSRNodeGroupScanSource::COMMITTED_PERSISTENT: {
        KU_ASSERT(persistentChunkGroup);
        return persistentChunkGroup->delete_(transaction, rowIdxInGroup);
    }
    case CSRNodeGroupScanSource::COMMITTED_IN_MEMORY: {
        KU_ASSERT(csrIndex);
        auto [chunkIdx, rowInChunk] =
            StorageUtils::getQuotientRemainder(rowIdxInGroup, ChunkedNodeGroup::CHUNK_CAPACITY);
        const auto lock = chunkedGroups.lock();
        const auto chunkedGroup = chunkedGroups.getGroup(lock, chunkIdx);
        return chunkedGroup->delete_(transaction, rowInChunk);
    }
    default: {
        return false;
    }
    }
}

void CSRNodeGroup::addColumn(Transaction* transaction, TableAddColumnState& addColumnState, 
    BMFileHandle* dataFH) {
    if (persistentChunkGroup) {
        persistentChunkGroup->addColumn(transaction, addColumnState, enableCompression, dataFH);
    }
    NodeGroup::addColumn(transaction, addColumnState, dataFH);
}

} // namespace storage
} // namespace kuzu

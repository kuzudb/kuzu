#include "storage/store/csr_node_group.h"

#include "storage/storage_utils.h"
#include "storage/store/rel_table.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

CSRNodeGroup::CSRRegion::CSRRegion(idx_t regionIdx, idx_t level)
    : regionIdx{regionIdx}, level{level} {
    const auto leftLeafRegion = regionIdx << level;
    leftNodeOffset = leftLeafRegion << StorageConstants::CSR_LEAF_REGION_SIZE_LOG2;
    rightNodeOffset = leftNodeOffset + (StorageConstants::CSR_LEAF_REGION_SIZE << level) - 1;
}

bool CSRNodeGroup::CSRRegion::isWithin(const CSRRegion& other) const {
    if (other.level <= level) {
        return false;
    }
    const auto leftRegionIdx = regionIdx << level;
    const auto rightRegionIdx = leftRegionIdx + (1 << level) - 1;
    const auto otherLeftRegionIdx = other.regionIdx << other.level;
    const auto otherRightRegionIdx = otherLeftRegionIdx + (1 << other.level) - 1;
    return leftRegionIdx >= otherLeftRegionIdx && rightRegionIdx <= otherRightRegionIdx;
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
                nodeGroupScanState.source = nodeGroupScanState.inMemCSRList.rowIndices.empty() ?
                                                CSRNodeGroupScanSource::NONE :
                                                CSRNodeGroupScanSource::COMMITTED_IN_MEMORY;
                nodeGroupScanState.nextRowToScan = 0;
                continue;
            }
            return result;
        }
        case CSRNodeGroupScanSource::COMMITTED_IN_MEMORY: {
            const auto result =
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
    sel_t numSelected = 0;
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
        numSelected += chunkedGroup->lookup(transaction, tableState, nodeGroupScanState, rowInChunk,
            numSelected);
        nextRow++;
    }
    nodeGroupScanState.nextRowToScan += numRows;
    tableState.IDVector->state->getSelVectorUnsafe().setSelSize(numSelected);
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

void CSRNodeGroup::serialize(Serializer& serializer) {
    serializer.writeDebuggingInfo("node_group_idx");
    serializer.write<node_group_idx_t>(nodeGroupIdx);
    serializer.writeDebuggingInfo("enable_compression");
    serializer.write<bool>(enableCompression);
    serializer.writeDebuggingInfo("format");
    serializer.write<NodeGroupDataFormat>(format);
    serializer.writeDebuggingInfo("has_checkpointed_data");
    serializer.write<bool>(persistentChunkGroup != nullptr);
    if (persistentChunkGroup) {
        serializer.writeDebuggingInfo("checkpointed_data");
        persistentChunkGroup->serialize(serializer);
    }
}

void CSRNodeGroup::checkpoint(NodeGroupCheckpointState& state) {
    const auto lock = chunkedGroups.lock();
    if (!persistentChunkGroup) {
        // No persistent data in the node group.
        checkpointInMemOnly(lock, state);
        return;
    }
    auto& csrState = state.cast<CSRNodeGroupCheckpointState>();
    // Scan old csr header from disk and construct new csr header.
    persistentChunkGroup->cast<ChunkedCSRNodeGroup>().scanCSRHeader(csrState);
    csrState.newHeader = std::make_unique<ChunkedCSRHeader>(false,
        StorageConstants::NODE_GROUP_SIZE, ResidencyState::IN_MEMORY);
    csrState.newHeader->setNumValues(StorageConstants::NODE_GROUP_SIZE);
    csrState.newHeader->copyFrom(*csrState.oldHeader);
    // Gather all csr leaf regions that have any updates/deletions/insertions.
    constexpr auto numLeafRegions =
        StorageConstants::NODE_GROUP_SIZE / StorageConstants::CSR_LEAF_REGION_SIZE;
    std::vector<CSRRegion> leafRegions;
    leafRegions.reserve(numLeafRegions);
    for (auto leafRegionIdx = 0u; leafRegionIdx < numLeafRegions; leafRegionIdx++) {
        CSRRegion region(leafRegionIdx, 0 /*level*/);
        collectRegionChangesAndUpdateHeaderLength(lock, region, csrState);
        leafRegions.push_back(std::move(region));
    }
    // Figure out regions we need to re-write.
    const auto mergedRegions = mergeRegionsToCheckpoint(csrState, leafRegions);
    if (mergedRegions.empty()) {
        // No csr regions need to be checkpointed.
        return;
    }
    csrState.newHeader->populateCSROffsets();
    KU_ASSERT(csrState.newHeader->sanityCheck());
    // Checkpoint columns.
    const auto numColumns = dataTypes.size();
    for (auto columnID = 0u; columnID < numColumns; columnID++) {
        checkpointColumn(lock, columnID, csrState, mergedRegions);
    }
    // Checkpoint csr headers.
    std::vector<ChunkCheckpointState> csrOffsetChunkCheckpointStates;
    const auto numNodes = csrState.newHeader->offset->getNumValues();
    KU_ASSERT(numNodes == csrState.newHeader->length->getNumValues());
    csrOffsetChunkCheckpointStates.push_back(
        ChunkCheckpointState{csrState.newHeader->offset->moveData(), 0, numNodes});
    ColumnCheckpointState csrOffsetCheckpointState(
        persistentChunkGroup->cast<ChunkedCSRNodeGroup>().getCSRHeader().offset->getData(),
        std::move(csrOffsetChunkCheckpointStates));
    csrState.csrOffsetColumn->checkpointColumnChunk(csrOffsetCheckpointState);
    std::vector<ChunkCheckpointState> csrLengthChunkCheckpointStates;
    csrLengthChunkCheckpointStates.push_back(
        ChunkCheckpointState{csrState.newHeader->length->moveData(), 0, numNodes});
    ColumnCheckpointState csrLengthCheckpointState(
        persistentChunkGroup->cast<ChunkedCSRNodeGroup>().getCSRHeader().length->getData(),
        std::move(csrLengthChunkCheckpointStates));
    csrState.csrLengthColumn->checkpointColumnChunk(csrLengthCheckpointState);
    // Clean up versions and in mem chunked groups.
    persistentChunkGroup->resetNumRowsFromChunks();
    chunkedGroups.clear(lock);
    // Set `numRows` back to 0 is to reflect that the in mem part of the node group is empty.
    numRows = 0;
    csrIndex.reset();
}

void CSRNodeGroup::checkpointColumn(const UniqLock& lock, column_id_t columnID,
    const CSRNodeGroupCheckpointState& csrState, const std::vector<CSRRegion>& regions) {
    std::vector<ChunkCheckpointState> chunkCheckpointStates;
    chunkCheckpointStates.reserve(regions.size());
    for (auto& region : regions) {
        const auto leftCSROffset = csrState.oldHeader->getStartCSROffset(region.leftNodeOffset);
        const auto rightCSROffset = csrState.oldHeader->getEndCSROffset(region.rightNodeOffset);
        const auto numOldRowsInRegion = rightCSROffset - leftCSROffset;
        auto oldChunkWithUpdates = std::make_unique<ColumnChunk>(dataTypes[columnID].copy(),
            numOldRowsInRegion, false, ResidencyState::IN_MEMORY);
        ChunkState chunkState;
        auto& persistentChunk = persistentChunkGroup->getColumnChunk(columnID);
        chunkState.column = csrState.columns[columnID];
        persistentChunk.initializeScanState(chunkState);
        persistentChunk.scanCommitted<ResidencyState::ON_DISK>(&DUMMY_CHECKPOINT_TRANSACTION,
            chunkState, *oldChunkWithUpdates);
        const auto numRowsInRegion =
            csrState.newHeader->getEndCSROffset(region.rightNodeOffset) - leftCSROffset;
        auto newChunk = std::make_unique<ColumnChunk>(dataTypes[columnID].copy(), numRowsInRegion,
            false, ResidencyState::IN_MEMORY);
        const auto dummyChunkForNulls = std::make_unique<ColumnChunk>(dataTypes[columnID].copy(),
            DEFAULT_VECTOR_CAPACITY, false, ResidencyState::IN_MEMORY);
        dummyChunkForNulls->getData().resetToAllNull();
        // Copy per csr list from old chunk and merge with new insertions into the newChunkData.
        for (auto nodeOffset = region.leftNodeOffset; nodeOffset <= region.rightNodeOffset;
             nodeOffset++) {
            const auto oldCSRLength = csrState.oldHeader->getCSRLength(nodeOffset);
            KU_ASSERT(csrState.oldHeader->getStartCSROffset(nodeOffset) >= leftCSROffset);
            const auto oldStartRow =
                csrState.oldHeader->getStartCSROffset(nodeOffset) - leftCSROffset;
            const auto newStartRow =
                csrState.newHeader->getStartCSROffset(nodeOffset) - leftCSROffset;
            KU_ASSERT(newStartRow == newChunk->getData().getNumValues());
            // Copy old csr list with updates into the new chunk.
            newChunk->getData().append(&oldChunkWithUpdates->getData(), oldStartRow, oldCSRLength);
            // Merge in-memory insertions into the new chunk.
            auto rows = csrIndex->indices[nodeOffset].getRows();
            // TODO(Guodong): Optimize here. if no deletions and has sequential rows, scan in range.
            for (const auto row : rows) {
                auto [chunkIdx, rowInChunk] =
                    StorageUtils::getQuotientRemainder(row, ChunkedNodeGroup::CHUNK_CAPACITY);
                const auto chunkedGroup = chunkedGroups.getGroup(lock, chunkIdx);
                if (chunkedGroup->isDeleted(&DUMMY_CHECKPOINT_TRANSACTION, rowInChunk)) {
                    continue;
                }
                chunkedGroup->getColumnChunk(columnID).scanCommitted<ResidencyState::IN_MEMORY>(
                    &DUMMY_CHECKPOINT_TRANSACTION, chunkState, *newChunk, rowInChunk, 1);
            }
            // Fill gaps if any.
            const auto newCSRLength = csrState.newHeader->getCSRLength(nodeOffset);
            const auto newEndRow = csrState.newHeader->getEndCSROffset(nodeOffset) - leftCSROffset;
            KU_ASSERT(newEndRow >= newStartRow + newCSRLength);
            int64_t numGaps = newEndRow - (newCSRLength + newStartRow);
            while (numGaps > 0) {
                const auto numGapsToFill =
                    std::min(numGaps, static_cast<int64_t>(DEFAULT_VECTOR_CAPACITY));
                dummyChunkForNulls->getData().setNumValues(numGapsToFill);
                newChunk->getData().append(&dummyChunkForNulls->getData(), 0, numGapsToFill);
                numGaps -= numGapsToFill;
            }
            KU_ASSERT(newChunk->getData().getNumValues() == newEndRow);
        }
        KU_ASSERT(newChunk->getData().getNumValues() == numRowsInRegion);
        ChunkCheckpointState chunkCheckpointState(newChunk->moveData(), leftCSROffset,
            numRowsInRegion);
        chunkCheckpointStates.push_back(std::move(chunkCheckpointState));
    }
    ColumnCheckpointState checkpointState(persistentChunkGroup->getColumnChunk(columnID).getData(),
        std::move(chunkCheckpointStates));
    csrState.columns[columnID]->checkpointColumnChunk(checkpointState);
}

void CSRNodeGroup::collectRegionChangesAndUpdateHeaderLength(const UniqLock& lock,
    CSRRegion& region, const CSRNodeGroupCheckpointState& csrState) {
    const auto leftCSROffset = csrState.oldHeader->getStartCSROffset(region.leftNodeOffset);
    const auto rightCSROffset = csrState.oldHeader->getEndCSROffset(region.rightNodeOffset);
    findUpdatesInRegion(region, leftCSROffset, rightCSROffset);
    row_idx_t numDeletionsInRegion = 0u;
    row_idx_t numInsertionsInRegion = 0u;
    if (csrIndex) {
        for (auto nodeOffset = region.leftNodeOffset; nodeOffset <= region.rightNodeOffset;
             nodeOffset++) {
            const auto numDeletedRows =
                getNumDeletionsForNodeInPersistentData(nodeOffset, csrState);
            auto rows = csrIndex->indices[nodeOffset].getRows();
            row_idx_t numInsertedRows = rows.size();
            row_idx_t numInMemDeletionsInCSR = 0;
            for (const auto row : rows) {
                auto [chunkIdx, rowInChunk] =
                    StorageUtils::getQuotientRemainder(row, ChunkedNodeGroup::CHUNK_CAPACITY);
                const auto chunkedGroup = chunkedGroups.getGroup(lock, chunkIdx);
                numInMemDeletionsInCSR +=
                    chunkedGroup->isDeleted(&DUMMY_CHECKPOINT_TRANSACTION, rowInChunk);
            }
            KU_ASSERT(numInMemDeletionsInCSR <= numInsertedRows);
            numInsertedRows -= numInMemDeletionsInCSR;
            const auto oldLength = csrState.oldHeader->getCSRLength(nodeOffset);
            KU_ASSERT(numInsertedRows + oldLength >= numDeletedRows);
            const auto newLength = oldLength + numInsertedRows - numDeletedRows;
            csrState.newHeader->length->getData().setValue<length_t>(newLength, nodeOffset);

            numDeletionsInRegion += numDeletedRows;
            numInsertionsInRegion += numInsertedRows;
        }
    }
    region.hasDeletions = numDeletionsInRegion > 0;
    region.hasInsertions = numInsertionsInRegion > 0;
    region.sizeChange =
        static_cast<int64_t>(numInsertionsInRegion) - static_cast<int64_t>(numDeletionsInRegion);
}

void CSRNodeGroup::findUpdatesInRegion(CSRRegion& region, offset_t leftCSROffset,
    offset_t rightCSROffset) const {
    const auto numColumns = dataTypes.size();
    region.hasUpdates.resize(numColumns, false);
    for (auto columnID = 0u; columnID < numColumns; columnID++) {
        if (persistentChunkGroup->hasAnyUpdates(&DUMMY_CHECKPOINT_TRANSACTION, columnID,
                leftCSROffset, rightCSROffset - leftCSROffset + 1)) {
            region.hasUpdates[columnID] = true;
        }
    }
}

row_idx_t CSRNodeGroup::getNumDeletionsForNodeInPersistentData(offset_t nodeOffset,
    const CSRNodeGroupCheckpointState& csrState) const {
    const auto length = csrState.oldHeader->getCSRLength(nodeOffset);
    const auto startRow = csrState.oldHeader->getStartCSROffset(nodeOffset);
    return persistentChunkGroup->getNumDeletions(&DUMMY_CHECKPOINT_TRANSACTION, startRow, length);
}

void CSRNodeGroup::checkpointInMemOnly(const UniqLock& lock, NodeGroupCheckpointState& state) {
    auto numRels = 0u;
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        numRels += chunkedGroup->getNumRows();
    }
    if (numRels == 0) {
        return;
    }
    // Construct in-mem csr header chunks.
    auto& csrState = state.cast<CSRNodeGroupCheckpointState>();
    csrState.newHeader = std::make_unique<ChunkedCSRHeader>(false /*enableCompression*/,
        StorageConstants::NODE_GROUP_SIZE, ResidencyState::IN_MEMORY);
    const auto numNodes = csrIndex->indices.size();
    csrState.newHeader->setNumValues(numNodes);
    for (auto offset = 0u; offset < numNodes; offset++) {
        auto rows = csrIndex->indices[offset].getRows();
        const length_t length = rows.size();
        auto lengthAfterDelete = length;
        for (const auto row : rows) {
            auto [chunkIdx, rowInChunk] =
                StorageUtils::getQuotientRemainder(row, ChunkedNodeGroup::CHUNK_CAPACITY);
            const auto chunkedGroup = chunkedGroups.getGroup(lock, chunkIdx);
            lengthAfterDelete -= chunkedGroup->isDeleted(&DUMMY_CHECKPOINT_TRANSACTION, rowInChunk);
        }
        KU_ASSERT(lengthAfterDelete <= length);
        csrState.newHeader->length->getData().setValue<length_t>(lengthAfterDelete, offset);
    }
    const auto gaps = csrState.newHeader->populateStartCSROffsetsAndGaps(true);
    csrState.newHeader->populateEndCSROffsets(gaps);

    // Construct scan state for reading from in-mem column chunks.
    const auto numColumns = dataTypes.size();
    std::vector<column_id_t> columnIDs;
    columnIDs.reserve(numColumns);
    for (auto i = 0u; i < numColumns; i++) {
        columnIDs.push_back(i);
    }
    const auto scanChunkState = std::make_shared<DataChunkState>();
    DataChunk scanChunk(numColumns, scanChunkState);
    for (auto i = 0u; i < numColumns; i++) {
        const auto valueVector = std::make_shared<ValueVector>(dataTypes[i].copy(), state.mm);
        scanChunk.insert(i, valueVector);
    }
    auto scanState = std::make_unique<TableScanState>(columnIDs);
    scanState->rowIdxVector->setState(scanChunkState);
    scanState->nodeGroupScanState = std::make_unique<NodeGroupScanState>(numColumns);
    for (auto i = 0u; i < numColumns; i++) {
        scanState->outputVectors.push_back(scanChunk.valueVectors[i].get());
    }

    // Scan in-mem column chunks and append into new column chunks to be flushed.
    auto numGaps = 0u;
    for (const auto gap : gaps) {
        numGaps += gap;
    }
    std::vector<std::unique_ptr<ColumnChunk>> dataChunksToFlush(numColumns);
    for (auto i = 0u; i < numColumns; i++) {
        dataChunksToFlush[i] = std::make_unique<ColumnChunk>(dataTypes[i], numRels + numGaps, false,
            ResidencyState::IN_MEMORY);
    }
    // TODO(Guodong): Should optimize this to 2048 rows at a time instead of per node.
    for (auto offset = 0u; offset < numNodes; offset++) {
        const auto numRows = csrIndex->getNumRows(offset);
        if (numRows > 0) {
            auto rows = csrIndex->indices[offset].getRows();
            auto numRowsAppended = 0u;
            while (numRowsAppended < numRows) {
                const auto numRowsToAppend =
                    std::min(numRows - numRowsAppended, DEFAULT_VECTOR_CAPACITY);
                scanChunkState->getSelVectorUnsafe().setSelSize(numRowsToAppend);
                for (auto i = 0u; i < numRowsToAppend; i++) {
                    scanState->rowIdxVector->setValue<row_idx_t>(i, rows[numRowsAppended + i]);
                }
                NodeGroup::lookup(lock, &DUMMY_CHECKPOINT_TRANSACTION, *scanState);
                for (auto columnID = 0u; columnID < numColumns; columnID++) {
                    dataChunksToFlush[columnID]->getData().append(
                        scanChunk.valueVectors[columnID].get(), scanChunkState->getSelVector());
                }
                numRowsAppended += numRowsToAppend;
            }
        }
        auto numGapsAppended = 0u;
        const auto gapSize = gaps[offset];
        while (numGapsAppended < gapSize) {
            const auto numGapsToAppend =
                std::min(gapSize - numGapsAppended, DEFAULT_VECTOR_CAPACITY);
            for (auto columnID = 0u; columnID < numColumns; columnID++) {
                scanChunk.valueVectors[columnID]->setAllNull();
            }
            scanChunkState->getSelVectorUnsafe().setSelSize(numGapsToAppend);
            for (auto columnID = 0u; columnID < numColumns; columnID++) {
                dataChunksToFlush[columnID]->getData().append(
                    scanChunk.valueVectors[columnID].get(), scanChunkState->getSelVector());
            }
            numGapsAppended += numGapsToAppend;
        }
    }

    for (const auto& chunk : dataChunksToFlush) {
        chunk->getData().flush(csrState.dataFH);
    }
    csrState.newHeader->offset->getData().flush(csrState.dataFH);
    csrState.newHeader->length->getData().flush(csrState.dataFH);
    persistentChunkGroup = std::make_unique<ChunkedCSRNodeGroup>(std::move(*csrState.newHeader),
        std::move(dataChunksToFlush), 0);
}

static CSRNodeGroup::CSRRegion upgradeLevel(const std::vector<CSRNodeGroup::CSRRegion>& leafRegions,
    const CSRNodeGroup::CSRRegion& region) {
    const auto regionIdx = region.regionIdx >> 1;
    CSRNodeGroup::CSRRegion newRegion{regionIdx, region.level + 1};
    newRegion.hasUpdates.resize(region.hasUpdates.size(), false);
    const idx_t leftLeafRegionIdx = newRegion.getLeftLeafRegionIdx();
    const idx_t rightLeafRegionIdx = newRegion.getRightLeafRegionIdx();
    for (auto leafRegionIdx = leftLeafRegionIdx; leafRegionIdx <= rightLeafRegionIdx;
         leafRegionIdx++) {
        newRegion.sizeChange += leafRegions[leafRegionIdx].sizeChange;
        newRegion.hasDeletions |= leafRegions[leafRegionIdx].hasDeletions;
        newRegion.hasInsertions |= leafRegions[leafRegionIdx].hasInsertions;
        for (auto columnID = 0u; columnID < leafRegions[leafRegionIdx].hasUpdates.size();
             columnID++) {
            newRegion.hasUpdates[columnID] =
                static_cast<bool>(newRegion.hasUpdates[columnID]) ||
                static_cast<bool>(leafRegions[leafRegionIdx].hasUpdates[columnID]);
        }
    }
    return newRegion;
}

std::vector<CSRNodeGroup::CSRRegion> CSRNodeGroup::mergeRegionsToCheckpoint(
    const CSRNodeGroupCheckpointState& csrState, std::vector<CSRRegion>& leafRegions) {
    KU_ASSERT(std::all_of(leafRegions.begin(), leafRegions.end(),
        [](const CSRRegion& region) { return region.level == 0; }));
    KU_ASSERT(std::is_sorted(leafRegions.begin(), leafRegions.end(),
        [](const CSRRegion& a, const CSRRegion& b) { return a.regionIdx < b.regionIdx; }));
    constexpr auto numLeafRegions =
        StorageConstants::NODE_GROUP_SIZE / StorageConstants::CSR_LEAF_REGION_SIZE;
    KU_ASSERT(leafRegions.size() == numLeafRegions);
    std::vector<CSRRegion> mergedRegions;
    idx_t leafRegionIdx = 0u;
    while (leafRegionIdx < numLeafRegions) {
        auto& region = leafRegions[leafRegionIdx];
        if (!region.needCheckpoint()) {
            leafRegionIdx++;
            continue;
        }
        while (!isWithinDensityBound(*csrState.oldHeader, leafRegions, region)) {
            region = upgradeLevel(leafRegions, region);
            if (region.level >= DEFAULT_PACKED_CSR_INFO.calibratorTreeHeight) {
                // Already hit the top level. Skip any other segments and directly return here.
                return {region};
            }
        }
        // Skip to the next right leaf region of the found region.
        leafRegionIdx = region.getRightLeafRegionIdx() + 1;
        // Loop through found regions and eliminate the ones that are under the realm of the
        // currently found region.
        std::erase_if(mergedRegions, [&](const CSRRegion& r) { return r.isWithin(region); });
        mergedRegions.push_back(region);
    }
    return mergedRegions;
}

static double getHighDensity(uint64_t level) {
    KU_ASSERT(level <= CSRNodeGroup::DEFAULT_PACKED_CSR_INFO.calibratorTreeHeight);
    if (level == 0) {
        return StorageConstants::LEAF_HIGH_CSR_DENSITY;
    }
    return StorageConstants::PACKED_CSR_DENSITY +
           CSRNodeGroup::DEFAULT_PACKED_CSR_INFO.highDensityStep *
               static_cast<double>(
                   CSRNodeGroup::DEFAULT_PACKED_CSR_INFO.calibratorTreeHeight - level);
}

bool CSRNodeGroup::isWithinDensityBound(const ChunkedCSRHeader& header,
    const std::vector<CSRRegion>& leafRegions, const CSRRegion& region) {
    int64_t oldSize = 0;
    for (auto offset = region.leftNodeOffset; offset <= region.rightNodeOffset; offset++) {
        oldSize += header.getCSRLength(offset);
    }
    int64_t sizeChange = 0;
    const idx_t leftRegionIdx = region.getLeftLeafRegionIdx();
    const idx_t rightRegionIdx = region.getRightLeafRegionIdx();
    for (auto regionIdx = leftRegionIdx; regionIdx <= rightRegionIdx; regionIdx++) {
        sizeChange += leafRegions[regionIdx].sizeChange;
    }
    KU_ASSERT(sizeChange >= 0 || sizeChange < oldSize);
    const auto newSize = oldSize + sizeChange;
    const auto capacity = header.getEndCSROffset(region.rightNodeOffset) -
                          header.getStartCSROffset(region.leftNodeOffset);
    const double ratio = static_cast<double>(newSize) / static_cast<double>(capacity);
    return ratio <= getHighDensity(region.level);
}

void CSRNodeGroup::resetVersionAndUpdateInfo() {
    NodeGroup::resetVersionAndUpdateInfo();
    persistentChunkGroup->resetVersionAndUpdateInfo();
}

} // namespace storage
} // namespace kuzu

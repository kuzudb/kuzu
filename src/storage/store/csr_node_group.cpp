#include "storage/store/csr_node_group.h"

#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_utils.h"
#include "storage/store/rel_table.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

bool CSRNodeGroupScanState::tryScanCachedTuples(RelTableScanState& tableScanState) {
    if (numCachedRows == 0 ||
        tableScanState.currBoundNodeIdx >= tableScanState.cachedBoundNodeSelVector.getSelSize()) {
        return false;
    }
    const auto boundNodeOffset = tableScanState.nodeIDVector->readNodeOffset(
        tableScanState.cachedBoundNodeSelVector[tableScanState.currBoundNodeIdx]);
    const auto boundNodeOffsetInGroup = boundNodeOffset % StorageConstants::NODE_GROUP_SIZE;
    const auto startCSROffset = header->getStartCSROffset(boundNodeOffsetInGroup);
    const auto csrLength = header->getCSRLength(boundNodeOffsetInGroup);
    nextCachedRowToScan = std::max(nextCachedRowToScan, startCSROffset);
    if (nextCachedRowToScan >= numScannedRows ||
        nextCachedRowToScan < numScannedRows - numCachedRows) {
        // Out of the bound of cached rows.
        return false;
    }
    KU_ASSERT(nextCachedRowToScan >= numScannedRows - numCachedRows);
    const auto numRowsToScan =
        std::min(numScannedRows, startCSROffset + csrLength) - nextCachedRowToScan;
    const auto startCachedRow = nextCachedRowToScan - (numScannedRows - numCachedRows);
    auto numSelected = 0u;
    tableScanState.outState->getSelVectorUnsafe().setToFiltered();
    for (auto i = 0u; i < numRowsToScan; i++) {
        const auto rowIdx = startCachedRow + i;
        tableScanState.outState->getSelVectorUnsafe()[numSelected] = rowIdx;
        numSelected += cachedScannedVectorsSelBitset[rowIdx];
    }
    tableScanState.outState->getSelVectorUnsafe().setSelSize(numSelected);
    tableScanState.setNodeIDVectorToFlat(
        tableScanState.cachedBoundNodeSelVector[tableScanState.currBoundNodeIdx]);
    nextCachedRowToScan += numRowsToScan;
    if ((startCSROffset + csrLength) == nextCachedRowToScan) {
        tableScanState.currBoundNodeIdx++;
        nextCachedRowToScan = 0;
    }
    return true;
}

void CSRNodeGroup::initializeScanState(Transaction* transaction, TableScanState& state) const {
    auto& relScanState = state.cast<RelTableScanState>();
    KU_ASSERT(relScanState.nodeGroupScanState);
    auto& nodeGroupScanState = relScanState.nodeGroupScanState->cast<CSRNodeGroupScanState>();
    if (relScanState.nodeGroupIdx != nodeGroupIdx) {
        relScanState.nodeGroupScanState->resetState();
        relScanState.nodeGroupIdx = nodeGroupIdx;
        if (persistentChunkGroup) {
            initScanForCommittedPersistent(transaction, relScanState, nodeGroupScanState);
        }
    }
    // Switch to a new Vector of bound nodes (i.e., new csr lists) in the node group.
    if (persistentChunkGroup) {
        nodeGroupScanState.numScannedRows = 0;
        nodeGroupScanState.numCachedRows = 0;
        nodeGroupScanState.nextRowToScan = 0;
        nodeGroupScanState.source = CSRNodeGroupScanSource::COMMITTED_PERSISTENT;
    } else if (csrIndex) {
        initScanForCommittedInMem(relScanState, nodeGroupScanState);
    } else {
        nodeGroupScanState.source = CSRNodeGroupScanSource::NONE;
        nodeGroupScanState.numScannedRows = 0;
    }
}

void CSRNodeGroup::initScanForCommittedPersistent(Transaction* transaction,
    RelTableScanState& relScanState, CSRNodeGroupScanState& nodeGroupScanState) const {
    // Scan the csr header chunks from disk.
    ChunkState offsetState, lengthState;
    auto& csrChunkGroup = persistentChunkGroup->cast<ChunkedCSRNodeGroup>();
    const auto& csrHeader = csrChunkGroup.getCSRHeader();
    // We are switching to a new node group.
    // Initialize the scan states of a new node group for the csr header.
    csrHeader.offset->initializeScanState(offsetState, relScanState.csrOffsetColumn);
    csrHeader.length->initializeScanState(lengthState, relScanState.csrLengthColumn);
    // Initialize the scan states of a new node group for data columns.
    for (auto i = 0u; i < relScanState.columnIDs.size(); i++) {
        if (relScanState.columnIDs[i] == INVALID_COLUMN_ID ||
            relScanState.columnIDs[i] == ROW_IDX_COLUMN_ID) {
            continue;
        }
        auto& chunk = persistentChunkGroup->getColumnChunk(relScanState.columnIDs[i]);
        chunk.initializeScanState(nodeGroupScanState.chunkStates[i], relScanState.columns[i]);
    }
    KU_ASSERT(csrHeader.offset->getNumValues() == csrHeader.length->getNumValues());
    auto numBoundNodes = csrHeader.offset->getNumValues();
    csrHeader.offset->scanCommitted<ResidencyState::ON_DISK>(transaction, offsetState,
        *nodeGroupScanState.header->offset);
    csrHeader.length->scanCommitted<ResidencyState::ON_DISK>(transaction, lengthState,
        *nodeGroupScanState.header->length);
    nodeGroupScanState.numTotalRows = nodeGroupScanState.header->getStartCSROffset(numBoundNodes);
}

void CSRNodeGroup::initScanForCommittedInMem(RelTableScanState& relScanState,
    CSRNodeGroupScanState& nodeGroupScanState) const {
    relScanState.currBoundNodeIdx = 0;
    nodeGroupScanState.source = CSRNodeGroupScanSource::COMMITTED_IN_MEMORY;
    nodeGroupScanState.numScannedRows = 0;
    nodeGroupScanState.numCachedRows = 0;
    nodeGroupScanState.nextRowToScan = 0;
    nodeGroupScanState.inMemCSRList.clear();
}

NodeGroupScanResult CSRNodeGroup::scan(Transaction* transaction, TableScanState& state) const {
    auto& relScanState = state.cast<RelTableScanState>();
    auto& nodeGroupScanState = relScanState.nodeGroupScanState->cast<CSRNodeGroupScanState>();
    while (true) {
        switch (nodeGroupScanState.source) {
        case CSRNodeGroupScanSource::COMMITTED_PERSISTENT: {
            auto result = scanCommittedPersistent(transaction, relScanState, nodeGroupScanState);
            if (result == NODE_GROUP_SCAN_EMMPTY_RESULT && csrIndex) {
                initScanForCommittedInMem(relScanState, nodeGroupScanState);
                continue;
            }
            return result;
        }
        case CSRNodeGroupScanSource::COMMITTED_IN_MEMORY: {
            relScanState.resetOutVectors();
            const auto result = scanCommittedInMem(transaction, relScanState, nodeGroupScanState);
            if (result == NODE_GROUP_SCAN_EMMPTY_RESULT) {
                relScanState.outState->getSelVectorUnsafe().setSelSize(0);
                return NODE_GROUP_SCAN_EMMPTY_RESULT;
            }
            return result;
        }
        case CSRNodeGroupScanSource::NONE: {
            relScanState.outState->getSelVectorUnsafe().setSelSize(0);
            return NODE_GROUP_SCAN_EMMPTY_RESULT;
        }
        default: {
            KU_UNREACHABLE;
        }
        }
    }
}

NodeGroupScanResult CSRNodeGroup::scanCommittedPersistent(const Transaction* transaction,
    RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) const {
    if (tableState.cachedBoundNodeSelVector.getSelSize() == 1) {
        // Note that we don't apply cache when there is only one bound node.
        return scanCommittedPersistentWtihoutCache(transaction, tableState, nodeGroupScanState);
    }
    return scanCommittedPersistentWithCache(transaction, tableState, nodeGroupScanState);
}

NodeGroupScanResult CSRNodeGroup::scanCommittedPersistentWithCache(const Transaction* transaction,
    RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) const {
    while (true) {
        while (nodeGroupScanState.tryScanCachedTuples(tableState)) {
            if (tableState.outState->getSelVector().getSelSize() > 0) {
                // Note: This is a dummy return value.
                return NodeGroupScanResult{nodeGroupScanState.numScannedRows,
                    tableState.outState->getSelVector().getSelSize()};
            }
        }
        if (nodeGroupScanState.numScannedRows == nodeGroupScanState.numTotalRows ||
            tableState.currBoundNodeIdx >= tableState.cachedBoundNodeSelVector.getSelSize()) {
            return NODE_GROUP_SCAN_EMMPTY_RESULT;
        }
        const auto currNodeOffset = tableState.nodeIDVector->readNodeOffset(
            tableState.cachedBoundNodeSelVector[tableState.currBoundNodeIdx]);
        const auto offsetInGroup = currNodeOffset % StorageConstants::NODE_GROUP_SIZE;
        const auto startCSROffset = nodeGroupScanState.header->getStartCSROffset(offsetInGroup);
        if (startCSROffset > nodeGroupScanState.numScannedRows) {
            nodeGroupScanState.numScannedRows = startCSROffset;
        }
        KU_ASSERT(nodeGroupScanState.numScannedRows <= nodeGroupScanState.numTotalRows);
        const auto numToScan =
            std::min(nodeGroupScanState.numTotalRows - nodeGroupScanState.numScannedRows,
                DEFAULT_VECTOR_CAPACITY);
        persistentChunkGroup->scan(transaction, tableState, nodeGroupScanState,
            nodeGroupScanState.numScannedRows, numToScan);
        nodeGroupScanState.numCachedRows = numToScan;
        nodeGroupScanState.numScannedRows += numToScan;
        if (tableState.outState->getSelVector().isUnfiltered()) {
            nodeGroupScanState.cachedScannedVectorsSelBitset.set();
        } else {
            nodeGroupScanState.cachedScannedVectorsSelBitset.reset();
            for (auto i = 0u; i < tableState.outState->getSelVector().getSelSize(); i++) {
                nodeGroupScanState.cachedScannedVectorsSelBitset.set(
                    tableState.outState->getSelVector()[i], true);
            }
        }
    }
}

NodeGroupScanResult CSRNodeGroup::scanCommittedPersistentWtihoutCache(
    const Transaction* transaction, RelTableScanState& tableState,
    CSRNodeGroupScanState& nodeGroupScanState) const {
    const auto currNodeOffset = tableState.nodeIDVector->readNodeOffset(
        tableState.cachedBoundNodeSelVector[tableState.currBoundNodeIdx]);
    const auto offsetInGroup = currNodeOffset % StorageConstants::NODE_GROUP_SIZE;
    const auto csrListLength = nodeGroupScanState.header->getCSRLength(offsetInGroup);
    if (nodeGroupScanState.nextRowToScan == csrListLength) {
        return NODE_GROUP_SCAN_EMMPTY_RESULT;
    }
    const auto startRow = nodeGroupScanState.header->getStartCSROffset(offsetInGroup) +
                          nodeGroupScanState.nextRowToScan;
    const auto numToScan =
        std::min(csrListLength - nodeGroupScanState.nextRowToScan, DEFAULT_VECTOR_CAPACITY);
    persistentChunkGroup->scan(transaction, tableState, nodeGroupScanState, startRow, numToScan);
    nodeGroupScanState.nextRowToScan += numToScan;
    tableState.setNodeIDVectorToFlat(
        tableState.cachedBoundNodeSelVector[tableState.currBoundNodeIdx]);
    return NodeGroupScanResult{startRow, numToScan};
}

NodeGroupScanResult CSRNodeGroup::scanCommittedInMem(Transaction* transaction,
    RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) const {
    while (true) {
        if (tableState.currBoundNodeIdx >= tableState.cachedBoundNodeSelVector.getSelSize()) {
            return NODE_GROUP_SCAN_EMMPTY_RESULT;
        }
        if (nodeGroupScanState.inMemCSRList.rowIndices.empty()) {
            const auto boundNodePos =
                tableState.cachedBoundNodeSelVector[tableState.currBoundNodeIdx];
            const auto boundNodeOffset = tableState.nodeIDVector->readNodeOffset(boundNodePos);
            const auto offsetInGroup = boundNodeOffset % StorageConstants::NODE_GROUP_SIZE;
            nodeGroupScanState.inMemCSRList = csrIndex->indices[offsetInGroup];
        }
        if (!nodeGroupScanState.inMemCSRList.isSequential) {
            KU_ASSERT(std::is_sorted(nodeGroupScanState.inMemCSRList.rowIndices.begin(),
                nodeGroupScanState.inMemCSRList.rowIndices.end()));
        }
        auto scanResult =
            nodeGroupScanState.inMemCSRList.isSequential ?
                scanCommittedInMemSequential(transaction, tableState, nodeGroupScanState) :
                scanCommittedInMemRandom(transaction, tableState, nodeGroupScanState);
        if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
            tableState.currBoundNodeIdx++;
            nodeGroupScanState.nextRowToScan = 0;
            nodeGroupScanState.inMemCSRList.clear();
        } else {
            tableState.setNodeIDVectorToFlat(
                tableState.cachedBoundNodeSelVector[tableState.currBoundNodeIdx]);
            return scanResult;
        }
    }
}

NodeGroupScanResult CSRNodeGroup::scanCommittedInMemSequential(const Transaction* transaction,
    const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) const {
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
    const ChunkedNodeGroup* chunkedGroup = nullptr;
    {
        const auto lock = chunkedGroups.lock();
        chunkedGroup = chunkedGroups.getGroup(lock, chunkIdx);
    }
    chunkedGroup->scan(transaction, tableState, nodeGroupScanState, startRowInChunk, numRows);
    nodeGroupScanState.nextRowToScan += numRows;
    return NodeGroupScanResult{startRow, numRows};
}

NodeGroupScanResult CSRNodeGroup::scanCommittedInMemRandom(Transaction* transaction,
    const RelTableScanState& tableState, CSRNodeGroupScanState& nodeGroupScanState) const {
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
            StorageUtils::getQuotientRemainder(rowIdx, ChunkedNodeGroup::CHUNK_CAPACITY);
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
    tableState.outState->getSelVectorUnsafe().setSelSize(numSelected);
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
    row_idx_t rowIdxInGroup, column_id_t columnID, const ValueVector& propertyVector) {
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
    FileHandle* dataFH) {
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

void CSRNodeGroup::checkpoint(MemoryManager&, NodeGroupCheckpointState& state) {
    const auto lock = chunkedGroups.lock();
    if (!persistentChunkGroup) {
        // No persistent data in the node group.
        checkpointInMemOnly(lock, state);
        return;
    }
    checkpointInMemAndOnDisk(lock, state);
}

void CSRNodeGroup::checkpointInMemAndOnDisk(const UniqLock& lock, NodeGroupCheckpointState& state) {
    // TODO(Guodong): Should skip early here if no changes in the node group, so we avoid scanning
    // the csr header. Case: No insertions/deletions in persistent chunk and no in-mem chunks.
    auto& csrState = state.cast<CSRNodeGroupCheckpointState>();
    // Scan old csr header from disk and construct new csr header.
    persistentChunkGroup->cast<ChunkedCSRNodeGroup>().scanCSRHeader(*state.mm, csrState);
    csrState.newHeader = std::make_unique<ChunkedCSRHeader>(*state.mm, false,
        StorageConstants::NODE_GROUP_SIZE, ResidencyState::IN_MEMORY);
    // TODO(Guodong): Find max node offset in the node group.
    csrState.newHeader->setNumValues(StorageConstants::NODE_GROUP_SIZE);
    csrState.newHeader->copyFrom(*csrState.oldHeader);
    auto leafRegions = collectLeafRegionsAndCSRLength(lock, csrState);
    KU_ASSERT(std::is_sorted(leafRegions.begin(), leafRegions.end(),
        [](const auto& a, const auto& b) { return a.regionIdx < b.regionIdx; }));
    const auto regionsToCheckpoint = mergeRegionsToCheckpoint(csrState, leafRegions);
    if (regionsToCheckpoint.empty()) {
        // No csr regions need to be checkpointed, meaning nothing is updated or deleted.
        // We should reset the version and update info of the persistent chunked group.
        persistentChunkGroup->resetVersionAndUpdateInfo();
        if (csrState.columnIDs.size() != persistentChunkGroup->getNumColumns()) {
            // The column set of the node group has changed. We need to re-create the persistent
            // chunked group.
            persistentChunkGroup = std::make_unique<ChunkedCSRNodeGroup>(
                persistentChunkGroup->cast<ChunkedCSRNodeGroup>(), csrState.columnIDs);
        }
        return;
    }
    if (regionsToCheckpoint.size() == 1 &&
        regionsToCheckpoint[0].level > DEFAULT_PACKED_CSR_INFO.calibratorTreeHeight) {
        // Need to re-distribute all CSR regions in the node group.
        redistributeCSRRegions(csrState, leafRegions);
    } else {
        for (auto& region : regionsToCheckpoint) {
            csrState.newHeader->populateRegionCSROffsets(region, *csrState.oldHeader);
            // The left node offset of a region should always maintain stable across length and
            // offset changes.
            KU_ASSERT(csrState.oldHeader->getStartCSROffset(region.leftNodeOffset) ==
                      csrState.newHeader->getStartCSROffset(region.leftNodeOffset));
        }
    }
    KU_ASSERT(csrState.newHeader->sanityCheck());
    for (const auto columnID : csrState.columnIDs) {
        checkpointColumn(lock, columnID, csrState, regionsToCheckpoint);
    }
    checkpointCSRHeaderColumns(csrState);
    persistentChunkGroup = std::make_unique<ChunkedCSRNodeGroup>(
        persistentChunkGroup->cast<ChunkedCSRNodeGroup>(), csrState.columnIDs);
    finalizeCheckpoint(lock);
}

std::vector<CSRRegion> CSRNodeGroup::collectLeafRegionsAndCSRLength(const UniqLock& lock,
    const CSRNodeGroupCheckpointState& csrState) {
    std::vector<CSRRegion> leafRegions;
    constexpr auto numLeafRegions =
        StorageConstants::NODE_GROUP_SIZE / StorageConstants::CSR_LEAF_REGION_SIZE;
    leafRegions.reserve(numLeafRegions);
    for (auto leafRegionIdx = 0u; leafRegionIdx < numLeafRegions; leafRegionIdx++) {
        CSRRegion region(leafRegionIdx, 0 /*level*/);
        collectRegionChangesAndUpdateHeaderLength(lock, region, csrState);
        leafRegions.push_back(std::move(region));
    }
    return leafRegions;
}

void CSRNodeGroup::redistributeCSRRegions(const CSRNodeGroupCheckpointState& csrState,
    const std::vector<CSRRegion>& leafRegions) {
    KU_ASSERT(std::is_sorted(leafRegions.begin(), leafRegions.end(),
        [](const auto& a, const auto& b) { return a.regionIdx < b.regionIdx; }));
    KU_ASSERT(std::all_of(leafRegions.begin(), leafRegions.end(),
        [](const CSRRegion& region) { return region.level == 0; }));
    KU_UNUSED(leafRegions);
    const auto rightCSROffsetOfRegions =
        csrState.newHeader->populateStartCSROffsetsFromLength(true /* leaveGaps */);
    csrState.newHeader->populateEndCSROffsetFromStartAndLength();
    csrState.newHeader->finalizeCSRRegionEndOffsets(rightCSROffsetOfRegions);
}

void CSRNodeGroup::checkpointColumn(const UniqLock& lock, column_id_t columnID,
    const CSRNodeGroupCheckpointState& csrState, const std::vector<CSRRegion>& regions) {
    std::vector<ChunkCheckpointState> chunkCheckpointStates;
    chunkCheckpointStates.reserve(regions.size());
    for (auto& region : regions) {
        if (!region.needCheckpointColumn(columnID)) {
            // Skip checkpoint for the column if it has no changes in the region.
            continue;
        }
        auto regionCheckpointState = checkpointColumnInRegion(lock, columnID, csrState, region);
        if (regionCheckpointState.numRows == 0) {
            // Skip the case when we have no rows to write for the region. This can happen when all
            // rows are deleted within the region. We don't aggressively reclaim the space in the
            // region, but keep deleted rows aa gaps.
            continue;
        }
        chunkCheckpointStates.push_back(std::move(regionCheckpointState));
    }
    ColumnCheckpointState checkpointState(persistentChunkGroup->getColumnChunk(columnID).getData(),
        std::move(chunkCheckpointStates));
    csrState.columns[columnID]->checkpointColumnChunk(checkpointState);
}

ChunkCheckpointState CSRNodeGroup::checkpointColumnInRegion(const UniqLock& lock,
    column_id_t columnID, const CSRNodeGroupCheckpointState& csrState, const CSRRegion& region) {
    const auto leftCSROffset = csrState.oldHeader->getStartCSROffset(region.leftNodeOffset);
    const auto rightCSROffset = csrState.oldHeader->getEndCSROffset(region.rightNodeOffset);
    const auto numOldRowsInRegion = rightCSROffset - leftCSROffset;
    const auto oldChunkWithUpdates = std::make_unique<ColumnChunk>(*csrState.mm,
        dataTypes[columnID].copy(), numOldRowsInRegion, false, ResidencyState::IN_MEMORY);
    ChunkState chunkState;
    const auto& persistentChunk = persistentChunkGroup->getColumnChunk(columnID);
    persistentChunk.initializeScanState(chunkState, csrState.columns[columnID]);
    persistentChunk.scanCommitted<ResidencyState::ON_DISK>(&DUMMY_CHECKPOINT_TRANSACTION,
        chunkState, *oldChunkWithUpdates, leftCSROffset, numOldRowsInRegion);
    KU_ASSERT(leftCSROffset == csrState.newHeader->getStartCSROffset(region.leftNodeOffset));
    const auto numRowsInRegion =
        csrState.newHeader->getEndCSROffset(region.rightNodeOffset) - leftCSROffset;
    const auto newChunk = std::make_unique<ColumnChunk>(*csrState.mm, dataTypes[columnID].copy(),
        numRowsInRegion, false, ResidencyState::IN_MEMORY);
    const auto dummyChunkForNulls = std::make_unique<ColumnChunk>(*csrState.mm,
        dataTypes[columnID].copy(), DEFAULT_VECTOR_CAPACITY, false, ResidencyState::IN_MEMORY);
    dummyChunkForNulls->getData().resetToAllNull();
    // Copy per csr list from old chunk and merge with new insertions into the newChunkData.
    for (auto nodeOffset = region.leftNodeOffset; nodeOffset <= region.rightNodeOffset;
         nodeOffset++) {
        const auto oldCSRLength = csrState.oldHeader->getCSRLength(nodeOffset);
        KU_ASSERT(csrState.oldHeader->getStartCSROffset(nodeOffset) >= leftCSROffset);
        const auto oldStartRow = csrState.oldHeader->getStartCSROffset(nodeOffset) - leftCSROffset;
        const auto newStartRow = csrState.newHeader->getStartCSROffset(nodeOffset) - leftCSROffset;
        KU_ASSERT(newStartRow == newChunk->getData().getNumValues());
        KU_UNUSED(newStartRow);
        // Copy old csr list with updates into the new chunk.
        if (!region.hasPersistentDeletions) {
            newChunk->getData().append(&oldChunkWithUpdates->getData(), oldStartRow, oldCSRLength);
        } else {
            // TODO(Guodong): Optimize the for loop away by appending in batch
            for (auto i = 0u; i < oldCSRLength; i++) {
                const auto csrOffsetInPersistentChunk = oldStartRow + i + leftCSROffset;
                if (persistentChunkGroup->isDeleted(&DUMMY_CHECKPOINT_TRANSACTION,
                        csrOffsetInPersistentChunk)) {
                    continue;
                }
                newChunk->getData().append(&oldChunkWithUpdates->getData(), oldStartRow + i, 1);
            }
        }
        // Merge in-memory insertions into the new chunk.
        if (csrIndex) {
            auto rows = csrIndex->indices[nodeOffset].getRows();
            // TODO(Guodong): Optimize here. if no deletions and has sequential rows, scan in
            // range.
            for (const auto row : rows) {
                if (row == INVALID_ROW_IDX) {
                    continue;
                }
                auto [chunkIdx, rowInChunk] =
                    StorageUtils::getQuotientRemainder(row, ChunkedNodeGroup::CHUNK_CAPACITY);
                const auto chunkedGroup = chunkedGroups.getGroup(lock, chunkIdx);
                KU_ASSERT(!chunkedGroup->isDeleted(&DUMMY_CHECKPOINT_TRANSACTION, rowInChunk));
                chunkedGroup->getColumnChunk(columnID).scanCommitted<ResidencyState::IN_MEMORY>(
                    &DUMMY_CHECKPOINT_TRANSACTION, chunkState, *newChunk, rowInChunk, 1);
            }
        }
        // Fill gaps if any.
        int64_t numGaps = csrState.newHeader->getGapSize(nodeOffset);
        while (numGaps > 0) {
            // Gaps should only happen at the end of the CSR region.
            KU_ASSERT((nodeOffset == region.rightNodeOffset - 1) ||
                      (nodeOffset + 1) % StorageConstants::CSR_LEAF_REGION_SIZE == 0);
            const auto numGapsToFill =
                std::min(numGaps, static_cast<int64_t>(DEFAULT_VECTOR_CAPACITY));
            dummyChunkForNulls->getData().setNumValues(numGapsToFill);
            newChunk->getData().append(&dummyChunkForNulls->getData(), 0, numGapsToFill);
            numGaps -= numGapsToFill;
        }
    }
    KU_ASSERT(newChunk->getData().getNumValues() == numRowsInRegion);
    return ChunkCheckpointState(newChunk->moveData(), leftCSROffset, numRowsInRegion);
}

void CSRNodeGroup::checkpointCSRHeaderColumns(const CSRNodeGroupCheckpointState& csrState) const {
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
}

void CSRNodeGroup::collectRegionChangesAndUpdateHeaderLength(const UniqLock& lock,
    CSRRegion& region, const CSRNodeGroupCheckpointState& csrState) {
    collectInMemRegionChangesAndUpdateHeaderLength(lock, region, csrState);
    collectOnDiskRegionChangesAndUpdateHeaderLength(lock, region, csrState);
}

void CSRNodeGroup::collectInMemRegionChangesAndUpdateHeaderLength(const UniqLock& lock,
    CSRRegion& region, const CSRNodeGroupCheckpointState& csrState) {
    row_idx_t numInsertionsInRegion = 0u;
    if (csrIndex) {
        for (auto nodeOffset = region.leftNodeOffset; nodeOffset <= region.rightNodeOffset;
             nodeOffset++) {
            auto rows = csrIndex->indices[nodeOffset].getRows();
            row_idx_t numInsertedRows = rows.size();
            row_idx_t numInMemDeletionsInCSR = 0;
            for (auto i = 0u; i < rows.size(); i++) {
                const auto row = rows[i];
                auto [chunkIdx, rowInChunk] =
                    StorageUtils::getQuotientRemainder(row, ChunkedNodeGroup::CHUNK_CAPACITY);
                const auto chunkedGroup = chunkedGroups.getGroup(lock, chunkIdx);
                if (chunkedGroup->isDeleted(&DUMMY_CHECKPOINT_TRANSACTION, rowInChunk)) {
                    csrIndex->indices[nodeOffset].turnToNonSequential();
                    csrIndex->indices[nodeOffset].setInvalid(i);
                    numInMemDeletionsInCSR++;
                }
            }
            KU_ASSERT(numInMemDeletionsInCSR <= numInsertedRows);
            numInsertedRows -= numInMemDeletionsInCSR;
            const auto oldLength = csrState.oldHeader->getCSRLength(nodeOffset);
            const auto newLength = oldLength + numInsertedRows;
            csrState.newHeader->length->getData().setValue<length_t>(newLength, nodeOffset);
            numInsertionsInRegion += numInsertedRows;
        }
    }
    region.hasInsertions = numInsertionsInRegion > 0;
    region.sizeChange += static_cast<int64_t>(numInsertionsInRegion);
}

void CSRNodeGroup::collectOnDiskRegionChangesAndUpdateHeaderLength(const UniqLock&,
    CSRRegion& region, const CSRNodeGroupCheckpointState& csrState) const {
    const auto leftCSROffset = csrState.oldHeader->getStartCSROffset(region.leftNodeOffset);
    const auto rightCSROffset = csrState.oldHeader->getEndCSROffset(region.rightNodeOffset);
    collectPersistentUpdatesInRegion(region, leftCSROffset, rightCSROffset);
    int64_t numDeletionsInRegion = 0u;
    if (persistentChunkGroup) {
        for (auto nodeOffset = region.leftNodeOffset; nodeOffset <= region.rightNodeOffset;
             nodeOffset++) {
            const auto numDeletedRows =
                getNumDeletionsForNodeInPersistentData(nodeOffset, csrState);
            if (numDeletedRows == 0) {
                continue;
            }
            numDeletionsInRegion += numDeletedRows;
            const auto currentLength = csrState.newHeader->getCSRLength(nodeOffset);
            KU_ASSERT(currentLength >= numDeletedRows);
            csrState.newHeader->length->getData().setValue<length_t>(currentLength - numDeletedRows,
                nodeOffset);
        }
    }
    region.hasPersistentDeletions = numDeletionsInRegion > 0;
    region.sizeChange -= numDeletionsInRegion;
}

void CSRNodeGroup::collectPersistentUpdatesInRegion(CSRRegion& region, offset_t leftCSROffset,
    offset_t rightCSROffset) const {
    // TODO(Guodong): Should take columnIDs from csrState here.
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

static DataChunk initScanDataChunk(const CSRNodeGroupCheckpointState& csrState,
    const std::vector<LogicalType>& dataTypes) {
    const auto scanChunkState = std::make_shared<DataChunkState>();
    DataChunk dataChunk(csrState.columnIDs.size(), scanChunkState);
    for (auto i = 0u; i < csrState.columnIDs.size(); i++) {
        const auto columnID = csrState.columnIDs[i];
        KU_ASSERT(columnID < dataTypes.size());
        const auto valueVector =
            std::make_shared<ValueVector>(dataTypes[columnID].copy(), csrState.mm);
        dataChunk.insert(i, valueVector);
    }
    return dataChunk;
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
    csrState.newHeader = std::make_unique<ChunkedCSRHeader>(*state.mm, false /*enableCompression*/,
        StorageConstants::NODE_GROUP_SIZE, ResidencyState::IN_MEMORY);
    const auto numNodes = csrIndex->getMaxOffsetWithRels() + 1;
    csrState.newHeader->setNumValues(numNodes);
    populateCSRLengthInMemOnly(lock, numNodes, csrState);
    const auto rightCSROffsetsOfRegions =
        csrState.newHeader->populateStartCSROffsetsFromLength(true /* leaveGap */);
    csrState.newHeader->populateEndCSROffsetFromStartAndLength();
    csrState.newHeader->finalizeCSRRegionEndOffsets(rightCSROffsetsOfRegions);

    // Init scan chunk and scan state.
    const auto numColumnsToCheckpoint = csrState.columnIDs.size();
    auto scanChunk = initScanDataChunk(csrState, dataTypes);
    const auto scanState = std::make_unique<TableScanState>(INVALID_TABLE_ID, csrState.columnIDs);
    initScanStateFromScanChunk(csrState, scanChunk, *scanState);
    auto dummyChunk = initScanDataChunk(csrState, dataTypes);
    for (auto i = 0u; i < dummyChunk.getNumValueVectors(); i++) {
        dummyChunk.getValueVectorMutable(i).setAllNull();
    }

    // Init data chunks to be appended and flushed.
    auto chunkCapacity = rightCSROffsetsOfRegions.back() + 1;
    std::vector<std::unique_ptr<ColumnChunk>> dataChunksToFlush(numColumnsToCheckpoint);
    for (auto i = 0u; i < numColumnsToCheckpoint; i++) {
        const auto columnID = csrState.columnIDs[i];
        KU_ASSERT(columnID < dataTypes.size());
        dataChunksToFlush[i] = std::make_unique<ColumnChunk>(*state.mm, dataTypes[columnID],
            chunkCapacity, enableCompression, ResidencyState::IN_MEMORY);
    }

    // Scan tuples from in mem node groups and append to data chunks to flush.
    for (auto offset = 0u; offset < numNodes; offset++) {
        const auto numRows = csrIndex->getNumRows(offset);
        auto rows = csrIndex->indices[offset].getRows();
        auto numRowsTryAppended = 0u;
        while (numRowsTryAppended < numRows) {
            const auto maxNumRowsToAppend =
                std::min(numRows - numRowsTryAppended, DEFAULT_VECTOR_CAPACITY);
            auto numRowsToAppend = 0u;
            for (auto i = 0u; i < maxNumRowsToAppend; i++) {
                const auto row = rows[numRowsTryAppended + i];
                // TODO(Guodong): Should skip deleted rows here.
                if (row == INVALID_ROW_IDX) {
                    continue;
                }
                numRowsToAppend++;
                scanState->rowIdxVector->setValue<row_idx_t>(i, row);
            }
            scanChunk.state->getSelVectorUnsafe().setSelSize(numRowsToAppend);
            if (numRowsToAppend > 0) {
                lookup(lock, &DUMMY_CHECKPOINT_TRANSACTION, *scanState);
                for (auto idx = 0u; idx < numColumnsToCheckpoint; idx++) {
                    dataChunksToFlush[idx]->getData().append(scanChunk.valueVectors[idx].get(),
                        scanChunk.state->getSelVector());
                }
            }
            numRowsTryAppended += maxNumRowsToAppend;
        }
        auto gapSize = csrState.newHeader->getGapSize(offset);
        while (gapSize > 0) {
            // Gaps should only happen at the end of the CSR region.
            KU_ASSERT((offset == numNodes - 1) ||
                      (offset + 1) % StorageConstants::CSR_LEAF_REGION_SIZE == 0);
            const auto numGapsToAppend = std::min(gapSize, DEFAULT_VECTOR_CAPACITY);
            KU_ASSERT(dummyChunk.state->getSelVector().isUnfiltered());
            dummyChunk.state->getSelVectorUnsafe().setSelSize(numGapsToAppend);
            for (auto columnID = 0u; columnID < numColumnsToCheckpoint; columnID++) {
                dataChunksToFlush[columnID]->getData().append(
                    dummyChunk.valueVectors[columnID].get(), dummyChunk.state->getSelVector());
            }
            gapSize -= numGapsToAppend;
        }
    }

    // Flush data chunks to disk.
    for (const auto& chunk : dataChunksToFlush) {
        chunk->getData().flush(csrState.dataFH);
    }
    csrState.newHeader->offset->getData().flush(csrState.dataFH);
    csrState.newHeader->length->getData().flush(csrState.dataFH);
    persistentChunkGroup = std::make_unique<ChunkedCSRNodeGroup>(std::move(*csrState.newHeader),
        std::move(dataChunksToFlush), 0);
    // TODO(Guodong): Use `finalizeCheckpoint`.
    chunkedGroups.clear(lock);
    // Set `numRows` back to 0 is to reflect that the in mem part of the node group is empty.
    numRows = 0;
    csrIndex.reset();
}

void CSRNodeGroup::initScanStateFromScanChunk(const CSRNodeGroupCheckpointState& csrState,
    const DataChunk& dataChunk, TableScanState& scanState) {
    scanState.rowIdxVector->setState(dataChunk.state);
    scanState.outState = dataChunk.state.get();
    scanState.nodeGroupScanState = std::make_unique<NodeGroupScanState>(csrState.columnIDs.size());
    for (auto i = 0u; i < csrState.columnIDs.size(); i++) {
        scanState.outputVectors.push_back(dataChunk.valueVectors[i].get());
    }
}

void CSRNodeGroup::populateCSRLengthInMemOnly(const UniqLock& lock, offset_t numNodes,
    const CSRNodeGroupCheckpointState& csrState) {
    for (auto offset = 0u; offset < numNodes; offset++) {
        auto rows = csrIndex->indices[offset].getRows();
        const length_t length = rows.size();
        auto lengthAfterDelete = length;
        for (auto i = 0u; i < rows.size(); i++) {
            const auto row = rows[i];
            auto [chunkIdx, rowInChunk] =
                StorageUtils::getQuotientRemainder(row, ChunkedNodeGroup::CHUNK_CAPACITY);
            const auto chunkedGroup = chunkedGroups.getGroup(lock, chunkIdx);
            const auto isDeleted =
                chunkedGroup->isDeleted(&DUMMY_CHECKPOINT_TRANSACTION, rowInChunk);
            if (isDeleted) {
                csrIndex->indices[offset].turnToNonSequential();
                csrIndex->indices[offset].setInvalid(i);
                lengthAfterDelete--;
            }
        }
        KU_ASSERT(lengthAfterDelete <= length);
        csrState.newHeader->length->getData().setValue<length_t>(lengthAfterDelete, offset);
    }
}

std::vector<CSRRegion> CSRNodeGroup::mergeRegionsToCheckpoint(
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
        auto region = leafRegions[leafRegionIdx];
        if (!region.needCheckpoint()) {
            leafRegionIdx++;
            continue;
        }
        while (!isWithinDensityBound(*csrState.oldHeader, leafRegions, region)) {
            region = CSRRegion::upgradeLevel(leafRegions, region);
            if (region.level > DEFAULT_PACKED_CSR_INFO.calibratorTreeHeight) {
                // Hit the top level already. Need to re-distribute.
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
    std::sort(mergedRegions.begin(), mergedRegions.end(),
        [](const CSRRegion& a, const CSRRegion& b) {
            return a.getLeftLeafRegionIdx() < b.getLeftLeafRegionIdx();
        });
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

void CSRNodeGroup::finalizeCheckpoint(const UniqLock& lock) {
    // Clean up versions and in mem chunked groups.
    persistentChunkGroup->resetNumRowsFromChunks();
    persistentChunkGroup->resetVersionAndUpdateInfo();
    chunkedGroups.clear(lock);
    // Set `numRows` back to 0 is to reflect that the in mem part of the node group is empty.
    numRows = 0;
    csrIndex.reset();
}

} // namespace storage
} // namespace kuzu

#include "storage/store/csr_rel_table_data.h"

#include "common/column_data_format.h"
#include "common/enums/rel_direction.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/stats/rels_store_statistics.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

common::offset_t CSRHeaderColumns::getNumNodes(
    Transaction* transaction, node_group_idx_t nodeGroupIdx) const {
    auto numPersistentNodeGroups = offset->getNumNodeGroups(transaction);
    return nodeGroupIdx >= numPersistentNodeGroups ?
               0 :
               offset->getMetadata(nodeGroupIdx, transaction->getType()).numValues;
}

PackedCSRInfo::PackedCSRInfo() {
    calibratorTreeHeight =
        StorageConstants::NODE_GROUP_SIZE_LOG2 - StorageConstants::CSR_SEGMENT_SIZE_LOG2;
    lowDensityStep =
        (double)(StorageConstants::TOP_CSR_DENSITY - StorageConstants::LEAF_LOW_CSR_DENSITY) /
        (double)(calibratorTreeHeight);
    highDensityStep =
        (double)(StorageConstants::LEAF_HIGH_CSR_DENSITY - StorageConstants::TOP_CSR_DENSITY) /
        (double)(calibratorTreeHeight);
}

PackedCSRRegion::PackedCSRRegion(common::vector_idx_t regionIdx, common::vector_idx_t level)
    : regionIdx{regionIdx}, level{level} {
    auto startSegmentIdx = regionIdx << level;
    leftBoundary = startSegmentIdx << common::StorageConstants::CSR_SEGMENT_SIZE_LOG2;
    rightBoundary = leftBoundary + (common::StorageConstants::CSR_SEGMENT_SIZE << level) - 1;
}

void PackedCSRRegion::setSizeChange(const std::vector<int64_t>& sizeChangesPerSegment) {
    sizeChange = 0;
    auto startSegmentIdx = regionIdx << level;
    auto endSegmentIdx = startSegmentIdx + (1 << level) - 1;
    for (auto segmentIdx = startSegmentIdx; segmentIdx <= endSegmentIdx; segmentIdx++) {
        sizeChange += sizeChangesPerSegment[segmentIdx];
    }
}

CSRRelTableData::CSRRelTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, catalog::RelTableSchema* tableSchema,
    RelsStoreStats* relsStoreStats, RelDataDirection direction, bool enableCompression)
    : RelTableData{dataFH, metadataFH, bufferManager, wal, tableSchema, relsStoreStats, direction,
          enableCompression, ColumnDataFormat::CSR} {
    // No NULL values is allowed for the csr offset column.
    auto csrOffsetMetadataDAHInfo =
        relsStoreStats->getCSROffsetMetadataDAHInfo(&DUMMY_WRITE_TRANSACTION, tableID, direction);
    auto csrOffsetColumnName = StorageUtils::getColumnName("", StorageUtils::ColumnType::CSR_OFFSET,
        RelDataDirectionUtils::relDirectionToString(direction));
    csrHeaderColumns.offset = std::make_unique<Column>(csrOffsetColumnName, LogicalType::UINT64(),
        *csrOffsetMetadataDAHInfo, dataFH, metadataFH, bufferManager, wal, &DUMMY_READ_TRANSACTION,
        RWPropertyStats::empty(), enableCompression, false /* requireNUllColumn */);
    auto csrLengthMetadataDAHInfo =
        relsStoreStats->getCSRLengthMetadataDAHInfo(&DUMMY_WRITE_TRANSACTION, tableID, direction);
    auto csrLengthColumnName = StorageUtils::getColumnName("", StorageUtils::ColumnType::CSR_LENGTH,
        RelDataDirectionUtils::relDirectionToString(direction));
    csrHeaderColumns.length = std::make_unique<Column>(csrLengthColumnName, LogicalType::UINT64(),
        *csrLengthMetadataDAHInfo, dataFH, metadataFH, bufferManager, wal, &DUMMY_READ_TRANSACTION,
        RWPropertyStats::empty(), enableCompression, false /* requireNUllColumn */);
    packedCSRInfo = PackedCSRInfo();
}

void CSRRelTableData::initializeReadState(Transaction* transaction,
    std::vector<column_id_t> columnIDs, ValueVector* inNodeIDVector, RelDataReadState* readState) {
    RelTableData::initializeReadState(transaction, columnIDs, inNodeIDVector, readState);
    auto nodeOffset =
        inNodeIDVector->readNodeOffset(inNodeIDVector->state->selVector->selectedPositions[0]);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    // Reset to read from beginning for the csr of the new node offset.
    readState->posInCurrentCSR = 0;
    if (readState->isOutOfRange(nodeOffset)) {
        // Scan csr offsets and populate csr list entries for the new node group.
        readState->startNodeOffset = startNodeOffset;
        csrHeaderColumns.offset->scan(
            transaction, nodeGroupIdx, readState->csrHeaderChunks.offset.get());
        csrHeaderColumns.length->scan(
            transaction, nodeGroupIdx, readState->csrHeaderChunks.length.get());
        KU_ASSERT(readState->csrHeaderChunks.offset->getNumValues() ==
                  readState->csrHeaderChunks.length->getNumValues());
        readState->numNodes = readState->csrHeaderChunks.offset->getNumValues();
        readState->populateCSRListEntries();
        if (transaction->isWriteTransaction()) {
            readState->localNodeGroup = getLocalNodeGroup(transaction, nodeGroupIdx);
        }
    }
    if (nodeOffset != readState->currentNodeOffset) {
        readState->currentNodeOffset = nodeOffset;
    }
}

void CSRRelTableData::scan(Transaction* transaction, TableReadState& readState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    auto& relReadState = common::ku_dynamic_cast<TableReadState&, RelDataReadState&>(readState);
    KU_ASSERT(dataFormat == ColumnDataFormat::CSR);
    if (relReadState.readFromLocalStorage) {
        auto offsetInChunk = relReadState.currentNodeOffset - relReadState.startNodeOffset;
        KU_ASSERT(relReadState.localNodeGroup);
        auto numValuesRead = relReadState.localNodeGroup->scanCSR(
            offsetInChunk, relReadState.posInCurrentCSR, relReadState.columnIDs, outputVectors);
        relReadState.posInCurrentCSR += numValuesRead;
        return;
    }
    auto [startOffset, endOffset] = relReadState.getStartAndEndOffset();
    auto numRowsToRead = endOffset - startOffset;
    outputVectors[0]->state->selVector->resetSelectorToUnselectedWithSize(numRowsToRead);
    outputVectors[0]->state->setOriginalSize(numRowsToRead);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(relReadState.currentNodeOffset);
    adjColumn->scan(transaction, nodeGroupIdx, startOffset, endOffset, outputVectors[0],
        0 /* offsetInVector */);
    auto relIDVectorIdx = INVALID_VECTOR_IDX;
    for (auto i = 0u; i < relReadState.columnIDs.size(); i++) {
        auto columnID = relReadState.columnIDs[i];
        auto outputVectorId = i + 1; // Skip output from adj column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        if (columnID == REL_ID_COLUMN_ID) {
            relIDVectorIdx = outputVectorId;
        }
        columns[relReadState.columnIDs[i]]->scan(transaction, nodeGroupIdx, startOffset, endOffset,
            outputVectors[outputVectorId], 0 /* offsetInVector */);
    }
    if (transaction->isWriteTransaction() && relReadState.localNodeGroup) {
        auto nodeOffset =
            inNodeIDVector->readNodeOffset(inNodeIDVector->state->selVector->selectedPositions[0]);
        KU_ASSERT(relIDVectorIdx != INVALID_VECTOR_IDX);
        auto relIDVector = outputVectors[relIDVectorIdx];
        relReadState.localNodeGroup->applyLocalChangesForCSRColumns(
            nodeOffset - relReadState.startNodeOffset, relReadState.columnIDs, relIDVector,
            outputVectors);
    }
}

bool CSRRelTableData::checkIfNodeHasRels(Transaction* transaction, ValueVector* srcNodeIDVector) {
    auto nodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = srcNodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    auto readState = csrHeaderColumns.length->getReadState(transaction->getType(), nodeGroupIdx);
    if (offsetInChunk >= readState.metadata.numValues) {
        return false;
    }
    length_t length;
    csrHeaderColumns.length->scan(transaction, readState, offsetInChunk, offsetInChunk + 1,
        reinterpret_cast<uint8_t*>(&length));
    return length > 0;
}

void CSRRelTableData::append(NodeGroup* nodeGroup) {
    auto csrNodeGroup = static_cast<CSRNodeGroup*>(nodeGroup);
    csrHeaderColumns.offset->append(
        csrNodeGroup->getCSROffsetChunk(), nodeGroup->getNodeGroupIdx());
    csrHeaderColumns.length->append(
        csrNodeGroup->getCSRLengthChunk(), nodeGroup->getNodeGroupIdx());
    RelTableData::append(nodeGroup);
}

void CSRRelTableData::resizeColumns(node_group_idx_t /*numNodeGroups*/) {
    // NOTE: This is a special logic for regular columns only.
    return;
}

static length_t getGapSizeForNode(const CSRHeaderChunks& header, offset_t nodeOffset) {
    return header.getEndCSROffset(nodeOffset) - header.getStartCSROffset(nodeOffset) -
           header.getCSRLength(nodeOffset);
}

static length_t getRegionCapacity(const CSRHeaderChunks& header, PackedCSRRegion region) {
    auto [startNodeOffset, endNodeOffset] = region.getNodeOffsetBoundaries();
    return header.getEndCSROffset(endNodeOffset) - header.getStartCSROffset(startNodeOffset);
}

length_t CSRRelTableData::getNewRegionSize(const CSRHeaderChunks& header,
    const std::vector<int64_t>& sizeChangesPerSegment, PackedCSRRegion& region) {
    auto [startNodeOffsetInNG, endNodeOffsetInNG] = region.getNodeOffsetBoundaries();
    endNodeOffsetInNG = std::min(endNodeOffsetInNG, header.offset->getNumValues() - 1);
    int64_t oldSize = 0;
    for (auto offsetInNG = startNodeOffsetInNG; offsetInNG <= endNodeOffsetInNG; offsetInNG++) {
        oldSize += header.getCSRLength(offsetInNG);
    }
    region.setSizeChange(sizeChangesPerSegment);
    return oldSize + region.sizeChange;
}

static PackedCSRRegion upgradeLevel(const PackedCSRRegion& region) {
    auto regionIdx = region.regionIdx >> 1;
    return PackedCSRRegion{regionIdx, region.level + 1};
}

// TODO: This is a naive implementation. We can do better by passing in left and right pos for node.
static uint64_t findPosOfRelIDFromArray(ColumnChunk* relIDInRegion, offset_t relOffset) {
    for (auto i = 0u; i < relIDInRegion->getNumValues(); i++) {
        if (relIDInRegion->getValue<offset_t>(i) == relOffset) {
            return i;
        }
    }
    return UINT64_MAX;
}

void CSRRelTableData::prepareLocalTableToCommit(
    Transaction* transaction, LocalTableData* localTable) {
    auto localRelTableData = ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(localTable);
    for (auto& [nodeGroupIdx, nodeGroup] : localRelTableData->nodeGroups) {
        auto relNG = ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(nodeGroup.get());
        prepareCommitNodeGroup(transaction, nodeGroupIdx, relNG);
    }
}

bool CSRRelTableData::isWithinDensityBound(const CSRHeaderChunks& header,
    const std::vector<int64_t>& sizeChangesPerSegment, PackedCSRRegion& region) {
    auto sizeInRegion = getNewRegionSize(header, sizeChangesPerSegment, region);
    auto capacityInRegion = getRegionCapacity(header, region);
    auto ratio = (double)sizeInRegion / (double)capacityInRegion;
    auto [low, high] = getDensityRange(region.level);
    return ratio <= high;
}

density_range_t CSRRelTableData::getDensityRange(uint64_t level) const {
    if (level == 0) {
        return std::make_pair(
            StorageConstants::LEAF_LOW_CSR_DENSITY, StorageConstants::LEAF_HIGH_CSR_DENSITY);
    }
    if (level == packedCSRInfo.calibratorTreeHeight) {
        return std::make_pair(StorageConstants::TOP_CSR_DENSITY, StorageConstants::TOP_CSR_DENSITY);
    }
    auto low = StorageConstants::TOP_CSR_DENSITY - (packedCSRInfo.lowDensityStep * (double)(level));
    auto high =
        StorageConstants::TOP_CSR_DENSITY - (packedCSRInfo.highDensityStep * (double)(level));
    return std::make_pair(low, high);
}

static vector_idx_t getSegmentIdx(offset_t offset) {
    return offset >> common::StorageConstants::CSR_SEGMENT_SIZE_LOG2;
}

void CSRRelTableData::LocalState::initChangesPerSegment() {
    auto numSegments = StorageConstants::NODE_GROUP_SIZE / StorageConstants::CSR_SEGMENT_SIZE;
    sizeChangesPerSegment.resize(numSegments, 0 /*initValue*/);
    hasChangesPerSegment.resize(numSegments, false /*initValue*/);
    auto relNGInfo = ku_dynamic_cast<RelNGInfo*, CSRRelNGInfo*>(localNG->getRelNGInfo());
    for (auto& [offset, insertions] : relNGInfo->adjInsertInfo) {
        auto segmentIdx = getSegmentIdx(offset);
        sizeChangesPerSegment[segmentIdx] += insertions.size();
        hasChangesPerSegment[segmentIdx] = true;
    }
    for (auto& [offset, deletions] : relNGInfo->deleteInfo) {
        auto segmentIdx = getSegmentIdx(offset);
        sizeChangesPerSegment[segmentIdx] -= deletions.size();
        hasChangesPerSegment[segmentIdx] = true;
    }
    for (auto& updateInfoPerColumn : relNGInfo->updateInfoPerChunk) {
        for (auto& [offset, updates] : updateInfoPerColumn) {
            auto segmentIdx = getSegmentIdx(offset);
            hasChangesPerSegment[segmentIdx] = true;
        }
    }
}

void CSRRelTableData::applyUpdatesToChunk(ColumnChunk* relIDChunk, const PackedCSRRegion& region,
    LocalVectorCollection* localChunk, const update_insert_info_t& updateInfo, ColumnChunk* chunk) {
    std::map<offset_t, row_idx_t> csrOffsetInRegionToRowIdx;
    auto [leftNodeBoundary, rightNodeBoundary] = region.getNodeOffsetBoundaries();
    for (auto& [nodeOffset, updates] : updateInfo) {
        if (nodeOffset < leftNodeBoundary || nodeOffset > rightNodeBoundary) {
            continue;
        }
        for (auto [relID, rowIdx] : updates) {
            auto csrOffsetInRegion = findPosOfRelIDFromArray(relIDChunk, relID);
            KU_ASSERT(csrOffsetInRegion != UINT64_MAX);
            csrOffsetInRegionToRowIdx[csrOffsetInRegion] = rowIdx;
        }
    }
    Column::applyLocalChunkToColumnChunk(localChunk, chunk, csrOffsetInRegionToRowIdx);
}

void CSRRelTableData::applyInsertionsToChunk(const PersistentState& persistentState,
    const LocalState& localState, LocalVectorCollection* localChunk,
    const update_insert_info_t& insertInfo, ColumnChunk* newChunk) {
    std::map<offset_t, row_idx_t> csrOffsetToRowIdx;
    auto [leftNodeBoundary, rightNodeBoundary] = localState.region.getNodeOffsetBoundaries();
    for (auto& [nodeOffset, insertions] : insertInfo) {
        if (nodeOffset < leftNodeBoundary || nodeOffset > rightNodeBoundary) {
            continue;
        }
        // TODO: Separate this into a function.
        auto csrOffsetInRegion = localState.header.getStartCSROffset(nodeOffset) +
                                 persistentState.header.getCSRLength(nodeOffset) -
                                 localState.leftCSROffset;
        for (auto& [_, rowIdx] : insertions) {
            KU_ASSERT(csrOffsetInRegion != UINT64_MAX);
            csrOffsetToRowIdx[csrOffsetInRegion++] = rowIdx;
        }
    }
    Column::applyLocalChunkToColumnChunk(localChunk, newChunk, csrOffsetToRowIdx);
}

// TODO(Guodong): This should be refactored to share the same control logic with
// `applyDeletionsToColumn`.
void CSRRelTableData::applyDeletionsToChunk(const PersistentState& persistentState,
    const LocalState& localState, const delete_info_t& deleteInfo, ColumnChunk* chunk) {
    for (auto& [offset, deletions] : deleteInfo) {
        if (localState.region.isOutOfBoundary(offset)) {
            continue;
        }
        auto length = persistentState.header.getCSRLength(offset);
        auto newLength = length - deletions.size();
        if (newLength == 0) {
            // No need to slide. Just skip.
            continue;
        }
        auto csrOffset = persistentState.header.getStartCSROffset(offset);
        std::vector<offset_t> deletionsInRegion;
        for (auto relOffset : deletions) {
            auto posInRegion = findPosOfRelIDFromArray(persistentState.relIDChunk.get(), relOffset);
            KU_ASSERT(posInRegion != UINT64_MAX);
            deletionsInRegion.push_back(posInRegion + localState.leftCSROffset);
        }
        std::sort(deletionsInRegion.begin(), deletionsInRegion.end());
        uint64_t offsetToCopyFrom = 0, offsetToCopyInto = 0;
        for (auto deletedOffset : deletionsInRegion) {
            auto offsetInCSRList = deletedOffset - csrOffset;
            auto numValuesToCopy = offsetInCSRList - offsetToCopyFrom;
            chunk->copy(chunk, offsetToCopyFrom, offsetToCopyInto, numValuesToCopy);
            offsetToCopyInto += numValuesToCopy;
            offsetToCopyFrom = offsetInCSRList + 1;
        }
        if (offsetToCopyFrom < length) {
            chunk->copy(chunk, offsetToCopyFrom, offsetToCopyInto, length - offsetToCopyFrom);
        }
    }
}

void CSRRelTableData::distributeAndUpdateRegion(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, column_id_t columnID, const PersistentState& persistentState,
    LocalState& localState) {
    KU_ASSERT(columnID < columns.size() || columnID == INVALID_COLUMN_ID);
    auto [leftNodeBoundary, rightNodeBoundary] = localState.region.getNodeOffsetBoundaries();
    auto column = columnID == INVALID_COLUMN_ID ? adjColumn.get() : columns[columnID].get();
    KU_ASSERT(localState.regionCapacity >= (localState.rightCSROffset - localState.leftCSROffset));
    // First, scan the whole region to a temp chunk.
    auto oldSize = persistentState.rightCSROffset - persistentState.leftCSROffset + 1;
    auto chunk = ColumnChunkFactory::createColumnChunk(
        column->getDataType()->copy(), enableCompression, oldSize);
    column->scan(transaction, nodeGroupIdx, chunk.get(), persistentState.leftCSROffset,
        persistentState.rightCSROffset + 1);
    auto relNGInfo = ku_dynamic_cast<RelNGInfo*, CSRRelNGInfo*>(localState.localNG->getRelNGInfo());
    auto& updateInfo = relNGInfo->getUpdateInfo(columnID);
    auto localChunk = getLocalChunk(localState, columnID);
    applyUpdatesToChunk(
        persistentState.relIDChunk.get(), localState.region, localChunk, updateInfo, chunk.get());
    applyDeletionsToChunk(persistentState, localState, relNGInfo->deleteInfo, chunk.get());
    // Second, create a new temp chunk for the region.
    auto newSize = localState.rightCSROffset - localState.leftCSROffset + 1;
    auto newChunk = ColumnChunkFactory::createColumnChunk(
        column->getDataType()->copy(), enableCompression, newSize);
    newChunk->getNullChunk()->resetToAllNull();
    auto maxNumNodesToDistribute = std::min(
        rightNodeBoundary - leftNodeBoundary + 1, persistentState.header.offset->getNumValues());
    // Third, copy the rels to the new chunk.
    for (auto i = 0u; i < maxNumNodesToDistribute; i++) {
        auto nodeOffset = i + leftNodeBoundary;
        auto csrOffsetInRegion =
            persistentState.header.getStartCSROffset(nodeOffset) - persistentState.leftCSROffset;
        auto length = persistentState.header.getCSRLength(nodeOffset);
        auto newCSROffsetInRegion =
            localState.header.getStartCSROffset(nodeOffset) - localState.leftCSROffset;
        KU_ASSERT(!relNGInfo->deleteInfo.contains(nodeOffset));
        KU_ASSERT(newCSROffsetInRegion >= newChunk->getNumValues());
        newChunk->copy(chunk.get(), csrOffsetInRegion, newCSROffsetInRegion, length);
    }
    auto& insertInfo = relNGInfo->getInsertInfo(columnID);
    applyInsertionsToChunk(persistentState, localState, localChunk, insertInfo, newChunk.get());
    column->prepareCommitForChunk(transaction, nodeGroupIdx, localState.leftCSROffset,
        newChunk.get(), 0 /*dataOffset*/, newChunk->getNumValues());
}

std::vector<PackedCSRRegion> CSRRelTableData::findRegionsToUpdate(
    const CSRHeaderChunks& headerChunks, LocalState& localState) {
    std::vector<PackedCSRRegion> regions;
    auto segmentIdx = 0u;
    auto numSegments = StorageConstants::NODE_GROUP_SIZE / StorageConstants::CSR_SEGMENT_SIZE;
    while (segmentIdx < numSegments) {
        if (!localState.hasChangesPerSegment[segmentIdx]) {
            // Skip the segment if no updates/deletions/insertions happen inside it.
            segmentIdx++;
            continue;
        }
        // Traverse from the leaf level (level 0) to higher levels to find a region that can satisfy
        // the density threshold.
        PackedCSRRegion region{segmentIdx, 0 /* level */};
        while (!isWithinDensityBound(headerChunks, localState.sizeChangesPerSegment, region)) {
            region = upgradeLevel(region);
            if (region.level > packedCSRInfo.calibratorTreeHeight) {
                // Already hit the top level. Break here.
                break;
            }
        }
        // Skip segments in the found region.
        segmentIdx = (region.regionIdx << region.level) + (1u << region.level);
        regions.push_back(region);
    }
    return regions;
}

void CSRRelTableData::updateColumns(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    PersistentState& persistentState, LocalState& localState) {
    auto localInfo = ku_dynamic_cast<RelNGInfo*, CSRRelNGInfo*>(localState.localNG->getRelNGInfo());
    // Scan RelID column chunk when there are updates or deletions.
    // TODO(Guodong): Should track for each region if it has updates or deletions.
    if (localInfo->hasUpdates() || !localInfo->deleteInfo.empty()) {
        // NOTE: There is an implicit trick happening. Due to the mismatch of storage type and
        // in-memory representation of INTERNAL_ID, we only store offset as INT64 on disk. Here
        // we directly read relID's offset part from disk into an INT64 column chunk.
        persistentState.relIDChunk = ColumnChunkFactory::createColumnChunk(
            LogicalType::INT64(), enableCompression, localState.regionCapacity);
        columns[REL_ID_COLUMN_ID]->scan(transaction, nodeGroupIdx, persistentState.relIDChunk.get(),
            persistentState.leftCSROffset, persistentState.rightCSROffset + 1);
    }
    if (localState.region.level == 0) {
        updateRegion(transaction, nodeGroupIdx, INVALID_COLUMN_ID, persistentState, localState);
        for (auto columnID = 0u; columnID < columns.size(); columnID++) {
            updateRegion(transaction, nodeGroupIdx, columnID, persistentState, localState);
        }
    } else {
        distributeAndUpdateRegion(
            transaction, nodeGroupIdx, INVALID_COLUMN_ID, persistentState, localState);
        for (auto columnID = 0u; columnID < columns.size(); columnID++) {
            distributeAndUpdateRegion(
                transaction, nodeGroupIdx, columnID, persistentState, localState);
        }
    }
}

void CSRRelTableData::findPositionsForInsertions(
    offset_t nodeOffset, length_t numInsertions, LocalState& localState) {
    auto& header = localState.header;
    KU_ASSERT(nodeOffset < header.offset->getNumValues());
    // Try insert to the end of nodeOffset.
    auto gapSize = getGapSizeForNode(header, nodeOffset);
    auto numRelsToInsertToGap = std::min(numInsertions, gapSize);
    auto numInsertionsLeft = numInsertions - numRelsToInsertToGap;
    // TODO: Try insert to the end of nodeOffset - 1.
    // Slide for insertions.
    if (numInsertionsLeft > 0) {
        slideForInsertions(nodeOffset, numInsertionsLeft, localState);
    }
}

void CSRRelTableData::slideForInsertions(
    offset_t nodeOffset, length_t numInsertions, LocalState& localState) {
    // Now, we have to slide. Heuristically, the sliding happens both left and right.
    auto& header = localState.header;
    auto [leftBoundary, rightBoundary] = localState.region.getNodeOffsetBoundaries();
    auto leftSize = 0u, rightSize = 0u;
    for (auto i = leftBoundary; i < nodeOffset; i++) {
        leftSize += header.getCSRLength(i);
    }
    KU_ASSERT(localState.header.getStartCSROffset(nodeOffset) >= leftSize);
    auto gapSizeOfLeftSide = localState.header.getStartCSROffset(nodeOffset) - leftSize;
    for (auto i = nodeOffset + 1; i <= rightBoundary; i++) {
        rightSize += header.getCSRLength(i);
    }
    KU_ASSERT(localState.header.getEndCSROffset(rightBoundary) >=
              localState.header.getEndCSROffset(nodeOffset));
    KU_ASSERT((localState.header.getEndCSROffset(rightBoundary) -
                  localState.header.getEndCSROffset(nodeOffset)) >= rightSize);
    auto gapSizeOfRightSide = localState.header.getEndCSROffset(rightBoundary) -
                              localState.header.getEndCSROffset(nodeOffset) - rightSize;
    uint64_t numInsertionsLeft, numInsertionsRight;
    if (gapSizeOfLeftSide > gapSizeOfRightSide) {
        numInsertionsLeft = std::min(numInsertions, gapSizeOfLeftSide);
        numInsertionsRight = numInsertions - numInsertionsLeft;
    } else {
        numInsertionsRight = std::min(numInsertions, gapSizeOfRightSide);
        numInsertionsLeft = numInsertions - numInsertionsRight;
    }
    if (numInsertionsLeft > 0) {
        slideLeftForInsertions(nodeOffset, leftBoundary, localState, numInsertionsLeft);
    }
    if (numInsertionsRight > 0) {
        slideRightForInsertions(nodeOffset, rightBoundary, localState, numInsertionsRight);
    }
}

void CSRRelTableData::slideLeftForInsertions(common::offset_t nodeOffset,
    common::offset_t leftBoundary, LocalState& localState, uint64_t numValuesToInsert) {
    KU_ASSERT(nodeOffset >= 1); // We cannot slide the left neighbor of the first node.
    offset_t leftNodeToSlide = nodeOffset - 1;
    std::unordered_map<offset_t, length_t> leftSlides;
    while (leftNodeToSlide >= leftBoundary) {
        if (numValuesToInsert == 0) {
            break;
        }
        auto gapSize = getGapSizeForNode(localState.header, leftNodeToSlide);
        leftSlides[leftNodeToSlide] = std::max(gapSize, numValuesToInsert);
        numValuesToInsert -= std::min(gapSize, numValuesToInsert);
        if (leftNodeToSlide == 0) {
            break;
        }
        leftNodeToSlide--;
    }
    // Update header offsets.
    for (auto i = leftNodeToSlide; i < nodeOffset; i++) {
        if (!leftSlides.contains(i)) {
            continue;
        }
        auto slideSize = leftSlides.at(i);
        auto oldOffset = localState.header.getEndCSROffset(i);
        localState.header.offset->setValue(oldOffset - slideSize, i);
    }
}

// SlideRight is a bit different from slideLeft in that we are actually sliding the startCSROffsets
// of nodes, instead of endCSROffsets.
void CSRRelTableData::slideRightForInsertions(offset_t nodeOffset, offset_t rightBoundary,
    LocalState& localState, uint64_t numValuesToInsert) {
    offset_t rightNodeToSlide = nodeOffset + 1;
    std::unordered_map<offset_t, length_t> rightSlides;
    while (rightNodeToSlide <= rightBoundary) {
        if (numValuesToInsert == 0) {
            break;
        }
        auto gapSize = getGapSizeForNode(localState.header, rightNodeToSlide);
        rightSlides[rightNodeToSlide] = std::max(gapSize, numValuesToInsert);
        numValuesToInsert -= std::min(gapSize, numValuesToInsert);
        if (rightNodeToSlide == rightBoundary) {
            break;
        }
        rightNodeToSlide++;
    }
    for (auto i = rightNodeToSlide; i > nodeOffset; i--) {
        if (!rightSlides.contains(i)) {
            continue;
        }
        auto slideSize = rightSlides.at(i);
        auto oldOffset = localState.header.getStartCSROffset(i);
        localState.header.offset->setValue<offset_t>(oldOffset + slideSize, i - 1);
    }
}

LocalVectorCollection* CSRRelTableData::getLocalChunk(
    const CSRRelTableData::LocalState& localState, column_id_t columnID) {
    return columnID == INVALID_COLUMN_ID ? localState.localNG->getAdjChunk() :
                                           localState.localNG->getPropertyChunk(columnID);
}

Column* CSRRelTableData::getColumn(column_id_t columnID) const {
    return columnID == INVALID_COLUMN_ID ? adjColumn.get() : columns[columnID].get();
}

void CSRRelTableData::updateRegion(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    column_id_t columnID, const CSRRelTableData::PersistentState& persistentState,
    LocalState& localState) {
    auto column = getColumn(columnID);
    applyUpdatesToColumn(
        transaction, nodeGroupIdx, columnID, localState, persistentState.relIDChunk.get(), column);
    applyDeletionsToColumn(transaction, nodeGroupIdx, localState, persistentState, column);
    applySliding(transaction, nodeGroupIdx, localState, persistentState, column);
    applyInsertionsToColumn(
        transaction, nodeGroupIdx, columnID, localState, persistentState, column);
}

void CSRRelTableData::applyUpdatesToColumn(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    column_id_t columnID, LocalState& localState, ColumnChunk* relIDChunk, Column* column) {
    std::map<offset_t, row_idx_t> writeInfo;
    auto relNGInfo = ku_dynamic_cast<RelNGInfo*, CSRRelNGInfo*>(localState.localNG->getRelNGInfo());
    auto& updateInfo = relNGInfo->getUpdateInfo(columnID);
    for (auto& [offset, updatesPerNode] : updateInfo) {
        if (localState.region.isOutOfBoundary(offset)) {
            // TODO: Should also partition local storage into regions. So we can avoid this check.
            continue;
        }
        for (auto& [relID, rowIdx] : updatesPerNode) {
            auto posInRegion = findPosOfRelIDFromArray(relIDChunk, relID);
            KU_ASSERT(posInRegion != UINT64_MAX);
            auto csrOffset = posInRegion + localState.leftCSROffset;
            writeInfo[csrOffset] = rowIdx;
        }
    }
    if (!writeInfo.empty()) {
        auto localChunk = getLocalChunk(localState, columnID);
        column->prepareCommitForChunk(
            transaction, nodeGroupIdx, localChunk, {} /*insertInfo*/, writeInfo, {} /*deleteInfo*/);
    }
}

void CSRRelTableData::applyInsertionsToColumn(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, column_id_t columnID, LocalState& localState,
    const PersistentState& persistentState, Column* column) {
    std::map<offset_t, row_idx_t> writeInfo;
    auto relNGInfo = ku_dynamic_cast<RelNGInfo*, CSRRelNGInfo*>(localState.localNG->getRelNGInfo());
    auto& insertInfo = relNGInfo->getInsertInfo(columnID);
    auto& deleteInfo = relNGInfo->getDeleteInfo();
    for (auto& [offset, insertions] : insertInfo) {
        if (localState.region.isOutOfBoundary(offset)) {
            continue;
        }
        auto startCSROffset = localState.header.getStartCSROffset(offset);
        auto length = localState.header.getCSRLength(offset);
        KU_ASSERT(length >= insertions.size());
        KU_ASSERT((startCSROffset + persistentState.header.getCSRLength(offset) -
                      (deleteInfo.contains(offset) ? deleteInfo.at(offset).size() : 0) +
                      insertions.size()) <= localState.header.getEndCSROffset(offset));
        auto idx = startCSROffset + length - insertions.size();
        for (auto& [relID, rowIdx] : insertions) {
            writeInfo[idx++] = rowIdx;
        }
    }
    auto localChunk = getLocalChunk(localState, columnID);
    column->prepareCommitForChunk(transaction, nodeGroupIdx, localChunk, writeInfo, {}, {});
}

// TODO(Guodong): 1. When there are insertions, we can avoid sliding by caching deleted positions
//                for insertions.
//                2. Moving from the back of the CSR list to deleted positions, so we can avoid
//                slidings and benefit from this when there is few deletions.
void CSRRelTableData::applyDeletionsToColumn(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, LocalState& localState, const PersistentState& persistentState,
    Column* column) {
    auto relNGInfo = ku_dynamic_cast<RelNGInfo*, CSRRelNGInfo*>(localState.localNG->getRelNGInfo());
    auto& deleteInfo = relNGInfo->getDeleteInfo();
    for (auto& [offset, deletions] : deleteInfo) {
        if (localState.region.isOutOfBoundary(offset)) {
            continue;
        }
        auto length = persistentState.header.getCSRLength(offset);
        auto newLength = length - deletions.size();
        if (newLength == 0) {
            // No need to slide. Just skip.
            continue;
        }
        auto csrOffset = persistentState.header.getStartCSROffset(offset);
        auto tmpChunk = ColumnChunkFactory::createColumnChunk(
            column->getDataType()->copy(), enableCompression, length);
        column->scan(transaction, nodeGroupIdx, tmpChunk.get(), csrOffset, csrOffset + length);
        auto newChunk = ColumnChunkFactory::createColumnChunk(
            column->getDataType()->copy(), enableCompression, newLength);
        std::vector<offset_t> deletionsInRegion;
        for (auto relOffset : deletions) {
            auto posInRegion = findPosOfRelIDFromArray(persistentState.relIDChunk.get(), relOffset);
            KU_ASSERT(posInRegion != UINT64_MAX);
            deletionsInRegion.push_back(posInRegion + localState.leftCSROffset);
        }
        std::sort(deletionsInRegion.begin(), deletionsInRegion.end());
        uint64_t offsetToCopyFrom = 0, offsetToCopyInto = 0;
        for (auto deletedOffset : deletionsInRegion) {
            auto offsetInCSRList = deletedOffset - csrOffset;
            auto numValuesToCopy = offsetInCSRList - offsetToCopyFrom;
            newChunk->copy(tmpChunk.get(), offsetToCopyFrom, offsetToCopyInto, numValuesToCopy);
            offsetToCopyInto += numValuesToCopy;
            offsetToCopyFrom = offsetInCSRList + 1;
        }
        if (offsetToCopyFrom < length) {
            newChunk->copy(
                tmpChunk.get(), offsetToCopyFrom, offsetToCopyInto, length - offsetToCopyFrom);
        }
        column->prepareCommitForChunk(
            transaction, nodeGroupIdx, csrOffset, newChunk.get(), 0, newLength);
    }
}

// TODO(Guodong): 1. Check if necessary before call this function to avoid unncessary checks on
//                old/new offset and length.
//                2. Optimize the sliding by moving the suffix/prefix depending on shifting
//                left/right.
void CSRRelTableData::applySliding(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    LocalState& localState, const PersistentState& persistentState, Column* column) {
    auto [leftBoundary, rightBoundary] = localState.region.getNodeOffsetBoundaries();
    for (auto i = leftBoundary; i <= rightBoundary; i++) {
        auto oldOffset = persistentState.header.getStartCSROffset(i);
        auto newOffset = localState.header.getStartCSROffset(i);
        if (oldOffset == newOffset) {
            continue;
        }
        auto length = persistentState.header.getCSRLength(i);
        if (length == 0) {
            continue;
        }
        auto chunk = ColumnChunkFactory::createColumnChunk(
            column->getDataType()->copy(), enableCompression, length);
        column->scan(transaction, nodeGroupIdx, chunk.get(), oldOffset, oldOffset + length);
        column->prepareCommitForChunk(
            transaction, nodeGroupIdx, newOffset, chunk.get(), 0 /*dataOffset*/, length);
    }
}

static offset_t getMaxNumNodesInRegion(
    const CSRHeaderChunks& header, const PackedCSRRegion& region, const CSRRelNGInfo* localInfo) {
    auto numNodes = header.offset->getNumValues();
    KU_ASSERT(numNodes == header.length->getNumValues());
    for (auto& [offset, _] : localInfo->adjInsertInfo) {
        if (!region.isOutOfBoundary(offset) && offset >= numNodes) {
            numNodes = offset + 1;
        }
    }
    return numNodes;
}

void CSRRelTableData::updateCSRHeader(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    PersistentState& persistentState, LocalState& localState) {
    auto localInfo = ku_dynamic_cast<RelNGInfo*, CSRRelNGInfo*>(localState.localNG->getRelNGInfo());
    auto [leftBoundary, rightBoundary] = localState.region.getNodeOffsetBoundaries();
    auto& header = persistentState.header;
    auto maxNumNodesInRegion = getMaxNumNodesInRegion(header, localState.region, localInfo);
    // Update the region boundary based on actual num nodes in the region.
    localState.region.leftBoundary = std::min(leftBoundary, header.offset->getNumValues());
    localState.region.rightBoundary = std::min(rightBoundary, maxNumNodesInRegion - 1);
    persistentState.leftCSROffset = header.getStartCSROffset(localState.region.leftBoundary);
    persistentState.rightCSROffset = header.getEndCSROffset(localState.region.rightBoundary);
    localState.header = CSRHeaderChunks(enableCompression, maxNumNodesInRegion);
    auto& newHeader = localState.header;
    newHeader.copyFrom(header);
    newHeader.fillDefaultValues(localState.region.rightBoundary + 1);
    if (localInfo->adjInsertInfo.empty() && localInfo->deleteInfo.empty()) {
        // No need to update the csr header.
        localState.leftCSROffset = persistentState.leftCSROffset;
        localState.rightCSROffset = persistentState.rightCSROffset;
        return;
    }
    for (auto& [offset, deletions] : localInfo->deleteInfo) {
        auto oldLength = newHeader.getCSRLength(offset);
        int64_t newLength = (int64_t)oldLength - deletions.size();
        KU_ASSERT(newLength >= 0);
        newHeader.length->setValue<length_t>(newLength, offset);
    }
    for (auto& [offset, _] : localInfo->adjInsertInfo) {
        auto oldLength = newHeader.getCSRLength(offset);
        auto numInsertions = localInfo->adjInsertInfo.at(offset).size();
        if (localState.region.level == 0) {
            findPositionsForInsertions(offset, numInsertions, localState);
        }
        int64_t newLength = (int64_t)oldLength + numInsertions;
        KU_ASSERT(newLength >= 0);
        newHeader.length->setValue<length_t>(newLength, offset);
    }
    if (localState.region.level > 0) {
        distributeOffsets(header, localState, localState.region.leftBoundary, maxNumNodesInRegion);
    } else {
        localState.regionSize =
            getNewRegionSize(header, localState.sizeChangesPerSegment, localState.region);
        localState.regionCapacity = getRegionCapacity(header, localState.region);
    }
    KU_ASSERT(newHeader.sanityCheck());
    localState.leftCSROffset = newHeader.getStartCSROffset(localState.region.leftBoundary);
    localState.rightCSROffset = newHeader.getEndCSROffset(localState.region.rightBoundary);
    csrHeaderColumns.offset->prepareCommitForChunk(transaction, nodeGroupIdx,
        localState.region.leftBoundary, newHeader.offset.get(), localState.region.leftBoundary,
        newHeader.offset->getNumValues() - localState.region.leftBoundary);
    csrHeaderColumns.length->prepareCommitForChunk(transaction, nodeGroupIdx,
        localState.region.leftBoundary, newHeader.length.get(), localState.region.leftBoundary,
        newHeader.length->getNumValues() - localState.region.leftBoundary);
}

void CSRRelTableData::distributeOffsets(const CSRHeaderChunks& header, LocalState& localState,
    offset_t leftBoundary, offset_t rightBoundary) {
    if (localState.region.level > packedCSRInfo.calibratorTreeHeight) {
        // Need to resize the capacity and reset regionToDistribute to the top level one.
        localState.region =
            PackedCSRRegion{0, static_cast<vector_idx_t>(packedCSRInfo.calibratorTreeHeight)};
        localState.regionSize =
            getNewRegionSize(header, localState.sizeChangesPerSegment, localState.region);
        localState.regionCapacity =
            divideAndRoundUpTo(localState.regionSize, StorageConstants::TOP_CSR_DENSITY);
    } else {
        localState.regionSize =
            getNewRegionSize(header, localState.sizeChangesPerSegment, localState.region);
        localState.regionCapacity = getRegionCapacity(header, localState.region);
    }
    auto gapSpace = localState.regionCapacity - localState.regionSize;
    double gapRatio = divideNoRoundUp(gapSpace, localState.regionCapacity);
    auto& newHeader = localState.header;
    for (auto nodeOffset = leftBoundary; nodeOffset < rightBoundary; nodeOffset++) {
        int64_t newLength = newHeader.getCSRLength(nodeOffset);
        auto newGap = std::min(gapSpace, multiplyAndRoundUpTo(gapRatio, newLength));
        gapSpace -= newGap;
        auto startCSROffset = newHeader.getStartCSROffset(nodeOffset);
        auto newOffset = startCSROffset + newLength + newGap;
        newHeader.offset->setValue(newOffset, nodeOffset);
    }
}

void CSRRelTableData::prepareCommitNodeGroup(
    Transaction* transaction, node_group_idx_t nodeGroupIdx, LocalRelNG* localRelNG) {
    auto numNodesInPersistentStorage = csrHeaderColumns.getNumNodes(transaction, nodeGroupIdx);
    PersistentState persistentState(numNodesInPersistentStorage);
    csrHeaderColumns.scan(transaction, nodeGroupIdx, persistentState.header);
    LocalState localState(localRelNG);
    auto regions = findRegionsToUpdate(persistentState.header, localState);
    for (auto& region : regions) {
        localState.region = region;
        updateCSRHeader(transaction, nodeGroupIdx, persistentState, localState);
        KU_ASSERT((region.level >= packedCSRInfo.calibratorTreeHeight && regions.size() == 1) ||
                  region.level < packedCSRInfo.calibratorTreeHeight);
        updateColumns(transaction, nodeGroupIdx, persistentState, localState);
    }
}

void CSRRelTableData::checkpointInMemory() {
    csrHeaderColumns.offset->checkpointInMemory();
    csrHeaderColumns.length->checkpointInMemory();
    RelTableData::checkpointInMemory();
}

void CSRRelTableData::rollbackInMemory() {
    csrHeaderColumns.offset->rollbackInMemory();
    csrHeaderColumns.length->rollbackInMemory();
    RelTableData::rollbackInMemory();
}

} // namespace storage
} // namespace kuzu

#include "storage/store/rel_table_data.h"

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/enums/rel_direction.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/store/rel_table.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

PackedCSRInfo::PackedCSRInfo() {
    calibratorTreeHeight =
        StorageConstants::NODE_GROUP_SIZE_LOG2 - StorageConstants::CSR_SEGMENT_SIZE_LOG2;
    highDensityStep =
        (StorageConstants::LEAF_HIGH_CSR_DENSITY - StorageConstants::PACKED_CSR_DENSITY) /
        static_cast<double>(calibratorTreeHeight);
}

PackedCSRRegion::PackedCSRRegion(idx_t regionIdx, idx_t level)
    : regionIdx{regionIdx}, level{level} {
    const auto startSegmentIdx = regionIdx << level;
    leftBoundary = startSegmentIdx << StorageConstants::CSR_SEGMENT_SIZE_LOG2;
    rightBoundary = leftBoundary + (StorageConstants::CSR_SEGMENT_SIZE << level) - 1;
}

bool PackedCSRRegion::isWithin(const PackedCSRRegion& other) const {
    if (other.level <= level) {
        return false;
    }
    auto [left, right] = getSegmentBoundaries();
    auto [otherLeft, otherRight] = other.getSegmentBoundaries();
    return left >= otherLeft && right <= otherRight;
}

void PackedCSRRegion::setSizeChange(const std::vector<int64_t>& sizeChangesPerSegment) {
    sizeChange = 0;
    const auto startSegmentIdx = regionIdx << level;
    const auto endSegmentIdx = startSegmentIdx + (1 << level) - 1;
    for (auto segmentIdx = startSegmentIdx; segmentIdx <= endSegmentIdx; segmentIdx++) {
        sizeChange += sizeChangesPerSegment[segmentIdx];
    }
}

RelTableData::RelTableData(BMFileHandle* dataFH, BufferManager* bufferManager, WAL* wal,
    const TableCatalogEntry* tableEntry, RelDataDirection direction, bool enableCompression,
    Deserializer* deSer)
    : dataFH{dataFH}, tableID{tableEntry->getTableID()}, tableName{tableEntry->getName()},
      bufferManager{bufferManager}, wal{wal}, enableCompression{enableCompression},
      direction{direction} {
    multiplicity = tableEntry->constCast<RelTableCatalogEntry>().getMultiplicity(direction);
    initCSRHeaderColumns();
    initPropertyColumns(tableEntry);
    nodeGroups = std::make_unique<NodeGroupCollection>(getColumnTypes(), enableCompression, 0,
        dataFH, deSer);
}

void RelTableData::initCSRHeaderColumns() {
    // No NULL values is allowed for the csr length and offset column.
    auto csrOffsetColumnName = StorageUtils::getColumnName("", StorageUtils::ColumnType::CSR_OFFSET,
        RelDataDirectionUtils::relDirectionToString(direction));
    csrHeaderColumns.offset = std::make_unique<Column>(csrOffsetColumnName, LogicalType::UINT64(),
        dataFH, bufferManager, wal, enableCompression, false /* requireNUllColumn */);
    auto csrLengthColumnName = StorageUtils::getColumnName("", StorageUtils::ColumnType::CSR_LENGTH,
        RelDataDirectionUtils::relDirectionToString(direction));
    csrHeaderColumns.length = std::make_unique<Column>(csrLengthColumnName, LogicalType::UINT64(),
        dataFH, bufferManager, wal, enableCompression, false /* requireNUllColumn */);
}

void RelTableData::initPropertyColumns(const TableCatalogEntry* tableEntry) {
    // Columns (nbrID + properties).
    auto& properties = tableEntry->getPropertiesRef();
    const auto maxColumnID =
        std::max_element(properties.begin(), properties.end(), [](auto& a, auto& b) {
            return a.getColumnID() < b.getColumnID();
        })->getColumnID();
    // The first column is reserved for NBR_ID, which is not a property.
    columns.resize(maxColumnID + 2);
    auto nbrIDColName = StorageUtils::getColumnName("NBR_ID", StorageUtils::ColumnType::DEFAULT,
        RelDataDirectionUtils::relDirectionToString(direction));
    auto nbrIDColumn = std::make_unique<InternalIDColumn>(nbrIDColName, dataFH, bufferManager, wal,
        enableCompression);
    columns[NBR_ID_COLUMN_ID] = std::move(nbrIDColumn);
    // Property columns.
    for (auto i = 0u; i < properties.size(); i++) {
        auto& property = properties[i];
        const auto columnID = property.getColumnID() + 1; // Skip NBR_ID column.
        const auto colName =
            StorageUtils::getColumnName(property.getName(), StorageUtils::ColumnType::DEFAULT,
                RelDataDirectionUtils::relDirectionToString(direction));
        columns[columnID] = ColumnFactory::createColumn(colName, property.getDataType().copy(),
            dataFH, bufferManager, wal, enableCompression);
    }
    // Set common tableID for nbrIDColumn and relIDColumn.
    const auto nbrTableID = tableEntry->constCast<RelTableCatalogEntry>().getNbrTableID(direction);
    columns[NBR_ID_COLUMN_ID]->cast<InternalIDColumn>().setCommonTableID(nbrTableID);
    columns[REL_ID_COLUMN_ID]->cast<InternalIDColumn>().setCommonTableID(tableID);
}

bool RelTableData::update(Transaction* transaction, ValueVector& boundNodeIDVector,
    const ValueVector& relIDVector, column_id_t columnID, const ValueVector& dataVector) const {
    KU_ASSERT(boundNodeIDVector.state->getSelVector().getSelSize() == 1);
    KU_ASSERT(relIDVector.state->getSelVector().getSelSize() == 1);
    const auto boundNodePos = boundNodeIDVector.state->getSelVector()[0];
    const auto relIDPos = relIDVector.state->getSelVector()[0];
    if (boundNodeIDVector.isNull(boundNodePos) || relIDVector.isNull(relIDPos)) {
        return false;
    }
    const auto rowIdx = findMatchingRow(transaction, boundNodeIDVector, relIDVector);
    KU_ASSERT(rowIdx != INVALID_ROW_IDX);
    const auto boundNodeOffset = boundNodeIDVector.getValue<nodeID_t>(boundNodePos).offset;
    auto [nodeGroupIdx, offsetInGroup] =
        StorageUtils::getQuotientRemainder(boundNodeOffset, StorageConstants::NODE_GROUP_SIZE);

    auto& csrNodeGroup = getNodeGroup(nodeGroupIdx)->cast<CSRNodeGroup>();
    csrNodeGroup.update(transaction, rowIdx, columnID, dataVector);
    return true;
}

bool RelTableData::delete_(Transaction* transaction, ValueVector& boundNodeIDVector,
    const ValueVector& relIDVector) const {
    const auto boundNodePos = boundNodeIDVector.state->getSelVector()[0];
    const auto relIDPos = relIDVector.state->getSelVector()[0];
    if (boundNodeIDVector.isNull(boundNodePos) || relIDVector.isNull(relIDPos)) {
        return false;
    }
    const auto rowIdx = findMatchingRow(transaction, boundNodeIDVector, relIDVector);
    if (rowIdx == INVALID_ROW_IDX) {
        return false;
    }
    const auto boundNodeOffset = boundNodeIDVector.getValue<nodeID_t>(boundNodePos).offset;
    auto [nodeGroupIdx, offsetInGroup] =
        StorageUtils::getQuotientRemainder(boundNodeOffset, StorageConstants::NODE_GROUP_SIZE);
    auto& csrNodeGroup = getNodeGroup(nodeGroupIdx)->cast<CSRNodeGroup>();
    return csrNodeGroup.delete_(transaction, rowIdx);
}

row_idx_t RelTableData::findMatchingRow(Transaction* transaction, ValueVector& boundNodeIDVector,
    const ValueVector& relIDVector) const {
    KU_ASSERT(boundNodeIDVector.state->getSelVector().getSelSize() == 1);
    KU_ASSERT(relIDVector.state->getSelVector().getSelSize() == 1);
    const auto boundNodePos = boundNodeIDVector.state->getSelVector()[0];
    const auto relIDPos = relIDVector.state->getSelVector()[0];
    const auto boundNodeOffset = boundNodeIDVector.getValue<nodeID_t>(boundNodePos).offset;
    const auto relOffset = relIDVector.getValue<nodeID_t>(relIDPos).offset;
    const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(boundNodeOffset);

    DataChunk scanChunk(2);
    // RelID output vector.
    scanChunk.insert(0, std::make_shared<ValueVector>(LogicalType::INTERNAL_ID()));
    // Row idx output vector.
    scanChunk.insert(1, std::make_shared<ValueVector>(LogicalType::INT64()));
    std::vector<column_id_t> columnIDs;
    std::vector<Column*> columns{getColumn(REL_ID_COLUMN_ID), nullptr};
    columnIDs.push_back(REL_ID_COLUMN_ID);
    columnIDs.push_back(ROW_IDX_COLUMN_ID);
    const auto scanState = std::make_unique<RelTableScanState>(tableID, columnIDs, columns,
        csrHeaderColumns.offset.get(), csrHeaderColumns.length.get(), direction);
    scanState->boundNodeIDVector = &boundNodeIDVector;
    scanState->outputVectors.push_back(scanChunk.getValueVector(0).get());
    scanState->outputVectors.push_back(scanChunk.getValueVector(1).get());
    scanState->IDVector = scanState->outputVectors[0];
    scanState->source = TableScanSource::COMMITTED;
    scanState->boundNodeOffset = boundNodeOffset;
    scanState->nodeGroup = getNodeGroup(nodeGroupIdx);
    scanState->nodeGroup->initializeScanState(transaction, *scanState);
    row_idx_t matchingRowIdx = INVALID_ROW_IDX;
    while (true) {
        const auto scanResult = scanState->nodeGroup->scan(transaction, *scanState);
        if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
            break;
        }
        for (auto i = 0u; i < scanState->IDVector->state->getSelVector().getSelSize(); i++) {
            const auto pos = scanState->IDVector->state->getSelVector()[i];
            if (scanState->IDVector->getValue<internalID_t>(pos).offset == relOffset) {
                const auto rowIdxPos = scanState->outputVectors[1]->state->getSelVector()[i];
                matchingRowIdx = scanState->outputVectors[1]->getValue<row_idx_t>(rowIdxPos);
                break;
            }
        }
        if (matchingRowIdx != INVALID_ROW_IDX) {
            break;
        }
    }
    return matchingRowIdx;
}

bool RelTableData::checkIfNodeHasRels(Transaction*, offset_t) const {
    // auto [nodeGroupIdx, offsetInChunk] =
    // StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset); if (nodeGroupIdx >=
    // csrHeaderColumns.length->getNumNodeGroups(transaction)) {
    //     return false;
    // }
    // ChunkState readState;
    // csrHeaderColumns.length->initChunkState(transaction, nodeGroupIdx, readState);
    // if (offsetInChunk >= readState.metadata.numValues) {
    //     return false;
    // }
    // length_t length;
    // csrHeaderColumns.length->scan(transaction, readState, offsetInChunk, offsetInChunk + 1,
    //     reinterpret_cast<uint8_t*>(&length));
    // return length > 0;
}

static length_t getGapSizeForNode(const ChunkedCSRHeader& header, offset_t nodeOffset) {
    return header.getEndCSROffset(nodeOffset) - header.getStartCSROffset(nodeOffset) -
           header.getCSRLength(nodeOffset);
}

static length_t getRegionCapacity(const ChunkedCSRHeader& header, const PackedCSRRegion& region) {
    auto [startNodeOffset, endNodeOffset] = region.getNodeOffsetBoundaries();
    return header.getEndCSROffset(endNodeOffset) - header.getStartCSROffset(startNodeOffset);
}

length_t RelTableData::getNewRegionSize(const ChunkedCSRHeader& header,
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

static uint64_t findPosOfRelIDFromArray(const ColumnChunkData* relIDInRegion, offset_t startPos,
    offset_t endPos, offset_t relOffset) {
    KU_ASSERT(endPos <= relIDInRegion->getNumValues());
    for (auto i = startPos; i < endPos; i++) {
        if (relIDInRegion->getValue<offset_t>(i) == relOffset) {
            return i;
        }
    }
    return UINT64_MAX;
}

offset_t RelTableData::findCSROffsetInRegion(const PersistentState& persistentState,
    offset_t nodeOffset, offset_t relOffset) {
    auto startPos =
        persistentState.header.getStartCSROffset(nodeOffset) - persistentState.leftCSROffset;
    auto endPos = startPos + persistentState.header.getCSRLength(nodeOffset);
    auto posInCSRList =
        findPosOfRelIDFromArray(persistentState.relIDChunk.get(), startPos, endPos, relOffset);
    KU_ASSERT(posInCSRList != UINT64_MAX);
    return posInCSRList + persistentState.leftCSROffset;
}

// void RelTableData::prepareLocalTableToCommit(Transaction* transaction, LocalTableData*
// localTable) { auto localRelTableData = ku_dynamic_cast<LocalTableData*,
// LocalRelTableData*>(localTable); for (auto& [nodeGroupIdx, nodeGroup] :
// localRelTableData->nodeGroups) { auto relNG = ku_dynamic_cast<LocalNodeGroup*,
// LocalRelNG*>(nodeGroup.get()); prepareCommitNodeGroup(transaction, nodeGroupIdx, relNG);
// }
// }

bool RelTableData::isWithinDensityBound(const ChunkedCSRHeader& header,
    const std::vector<int64_t>& sizeChangesPerSegment, PackedCSRRegion& region) const {
    auto sizeInRegion = getNewRegionSize(header, sizeChangesPerSegment, region);
    auto capacityInRegion = getRegionCapacity(header, region);
    auto ratio = static_cast<double>(sizeInRegion) / static_cast<double>(capacityInRegion);
    return ratio <= getHighDensity(region.level);
}

double RelTableData::getHighDensity(uint64_t level) const {
    KU_ASSERT(level <= packedCSRInfo.calibratorTreeHeight);
    if (level == 0) {
        return StorageConstants::LEAF_HIGH_CSR_DENSITY;
    }
    return StorageConstants::PACKED_CSR_DENSITY +
           (packedCSRInfo.highDensityStep *
               static_cast<double>(packedCSRInfo.calibratorTreeHeight - level));
}

// RelTableData::LocalState::LocalState(LocalRelNG* localNG) : localNG{localNG} {
// localNG->getChangesPerCSRSegment(sizeChangesPerSegment, hasChangesPerSegment);
// }

// void RelTableData::applyUpdatesToChunk(const PersistentState& persistentState,
// const LocalState& localState, const ChunkDataCollection& localChunk, ColumnChunkData* chunk,
// column_id_t columnID) {
// offset_to_row_idx_t csrOffsetInRegionToRowIdx;
// auto [leftNodeBoundary, rightNodeBoundary] = localState.region.getNodeOffsetBoundaries();
// auto& updateChunk = localState.localNG->getUpdateChunks(columnID);
// for (auto& [nodeOffset, updates] : updateChunk.getSrcNodeOffsetToRelOffsets()) {
//     if (nodeOffset < leftNodeBoundary || nodeOffset > rightNodeBoundary) {
//         continue;
//     }
//     for (auto relOffset : updates) {
//         auto csrOffsetInRegion = findCSROffsetInRegion(persistentState, nodeOffset,
//         relOffset); csrOffsetInRegionToRowIdx[csrOffsetInRegion] =
//             updateChunk.getRowIdxFromOffset(relOffset);
//     }
// }
// Column::applyLocalChunkToColumnChunk(localChunk, chunk, csrOffsetInRegionToRowIdx);
// }

// void RelTableData::applyInsertionsToChunk(const PersistentState& persistentState,
// const LocalState& localState, const ChunkDataCollection& localChunk,
// ColumnChunkData* newChunk) {
// offset_to_row_idx_t csrOffsetToRowIdx;
// auto [leftNodeBoundary, rightNodeBoundary] = localState.region.getNodeOffsetBoundaries();
// auto& insertChunks = localState.localNG->insertChunks;
// for (auto& [nodeOffset, insertions] : insertChunks.getSrcNodeOffsetToRelOffsets()) {
//     if (nodeOffset < leftNodeBoundary || nodeOffset > rightNodeBoundary) {
//         continue;
//     }
//     auto csrOffsetInRegion = localState.header.getStartCSROffset(nodeOffset) +
//                              persistentState.header.getCSRLength(nodeOffset) -
//                              localState.leftCSROffset;
//     for (auto relOffset : insertions) {
//         KU_ASSERT(csrOffsetInRegion != UINT64_MAX);
//         csrOffsetToRowIdx[csrOffsetInRegion++] = insertChunks.getRowIdxFromOffset(relOffset);
//     }
// }
// Column::applyLocalChunkToColumnChunk(localChunk, newChunk, csrOffsetToRowIdx);
// }

// TODO(Guodong): This should be refactored to share the same control logic with
// `applyDeletionsToColumn`.
void RelTableData::applyDeletionsToChunk(const PersistentState&, const LocalState&,
    ColumnChunkData*) {
    // auto& deleteInfo = localState.localNG->deleteInfo;
    // for (auto& [offset, deletions] : deleteInfo.getSrcNodeOffsetToRelOffsetVec()) {
    //     if (localState.region.isOutOfBoundary(offset)) {
    //         continue;
    //     }
    //     auto length = persistentState.header.getCSRLength(offset);
    //     auto newLength = length - deletions.size();
    //     if (newLength == 0) {
    //         // No need to slide. Just skip.
    //         continue;
    //     }
    //     std::vector<offset_t> deletionsInRegion;
    //     for (auto relOffset : deletions) {
    //         auto csrOffsetInRegion = findCSROffsetInRegion(persistentState, offset, relOffset);
    //         deletionsInRegion.push_back(csrOffsetInRegion + localState.leftCSROffset);
    //     }
    //     auto csrOffset = persistentState.header.getStartCSROffset(offset);
    //     std::sort(deletionsInRegion.begin(), deletionsInRegion.end());
    //     uint64_t offsetToCopyFrom = 0, offsetToCopyInto = 0;
    //     for (auto deletedOffset : deletionsInRegion) {
    //         auto offsetInCSRList = deletedOffset - csrOffset;
    //         auto numValuesToCopy = offsetInCSRList - offsetToCopyFrom;
    //         chunk->copy(chunk, offsetToCopyFrom, offsetToCopyInto, numValuesToCopy);
    //         offsetToCopyInto += numValuesToCopy;
    //         offsetToCopyFrom = offsetInCSRList + 1;
    //     }
    //     if (offsetToCopyFrom < length) {
    //         chunk->copy(chunk, offsetToCopyFrom, offsetToCopyInto, length - offsetToCopyFrom);
    //     }
    // }
}

void RelTableData::distributeAndUpdateColumn(Transaction*, node_group_idx_t, bool, column_id_t,
    const PersistentState&, const LocalState&) const {
    // auto column = getColumn(columnID);
    // auto [leftNodeBoundary, rightNodeBoundary] = localState.region.getNodeOffsetBoundaries();
    // KU_ASSERT(localState.regionCapacity >= (localState.rightCSROffset -
    // localState.leftCSROffset));
    // // First, scan the whole region to a temp chunk.
    // auto oldSize = persistentState.rightCSROffset - persistentState.leftCSROffset + 1;
    // auto chunk = ColumnChunkFactory::createColumnChunkData(*column->getDataType().copy(),
    //     enableCompression, oldSize, ResidencyState::IN_MEMORY);
    // ChunkState chunkState;
    // column->initChunkState(transaction, nodeGroupIdx, chunkState);
    // column->scan(transaction, chunkState, chunk.get(), persistentState.leftCSROffset,
    //     persistentState.rightCSROffset + 1);
    // auto localUpdateChunk =
    //     localState.localNG->getUpdateChunks(columnID).getLocalChunk(0 /*columnID*/);
    // applyUpdatesToChunk(persistentState, localState, localUpdateChunk, chunk.get(), columnID);
    // applyDeletionsToChunk(persistentState, localState, chunk.get());
    // // Second, create a new temp chunk for the region.
    // auto newSize = localState.rightCSROffset - localState.leftCSROffset + 1;
    // auto newChunk = ColumnChunkFactory::createColumnChunkData(*column->getDataType().copy(),
    //     enableCompression, newSize, ResidencyState::IN_MEMORY);
    // newChunk->getNullData()->resetToAllNull();
    // auto maxNumNodesToDistribute = std::min(rightNodeBoundary - leftNodeBoundary + 1,
    //     persistentState.header.offset->getNumValues());
    // // Third, copy the rels to the new chunk.
    // for (auto i = 0u; i < maxNumNodesToDistribute; i++) {
    //     auto nodeOffset = i + leftNodeBoundary;
    //     auto csrOffsetInRegion =
    //         persistentState.header.getStartCSROffset(nodeOffset) - persistentState.leftCSROffset;
    //     auto length = persistentState.header.getCSRLength(nodeOffset);
    //     if (length == 0) {
    //         continue;
    //     }
    //     auto newCSROffsetInRegion =
    //         localState.header.getStartCSROffset(nodeOffset) - localState.leftCSROffset;
    //     KU_ASSERT(newCSROffsetInRegion >= newChunk->getNumValues());
    //     newChunk->copy(chunk.get(), csrOffsetInRegion, newCSROffsetInRegion, length);
    // }
    // auto insertLocalChunk = localState.localNG->insertChunks.getLocalChunk(columnID);
    // applyInsertionsToChunk(persistentState, localState, insertLocalChunk, newChunk.get());
    // std::vector<offset_t> dstOffsets;
    // dstOffsets.resize(newChunk->getNumValues());
    // fillSequence(dstOffsets, localState.leftCSROffset);
    // KU_ASSERT(newChunk->sanityCheck());
    // column->prepareCommitForChunk(transaction, nodeGroupIdx, isNewNodeGroup, dstOffsets,
    //     newChunk.get(), 0 /*srcOffset*/);
}

std::vector<PackedCSRRegion> RelTableData::findRegions(const ChunkedCSRHeader& headerChunks,
    LocalState& localState) const {
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
            if (region.level >= packedCSRInfo.calibratorTreeHeight) {
                // Already hit the top level. Skip any other segments and directly return here.
                return {region};
            }
        }
        // Skip segments in the found region.
        segmentIdx = (region.regionIdx << region.level) + (1u << region.level);
        // Loop through found regions and eliminate the ones that are under the realm of the
        // currently found region.
        std::erase_if(regions, [&](const PackedCSRRegion& r) { return r.isWithin(region); });
        regions.push_back(region);
    }
    return regions;
}

void RelTableData::updateRegion(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, PersistentState& persistentState, LocalState& localState) {
    // Scan RelID column chunk when there are updates or deletions.
    // TODO(Guodong): Should track for each region if it has updates or deletions.
    // if (localState.localNG->hasUpdatesOrDeletions()) {
    //     // NOTE: There is an implicit trick happening. Due to the mismatch of storage type and
    //     // in-memory representation of INTERNAL_ID, we only store offset as INT64 on disk. Here
    //     // we directly read relID's offset part from disk into an INT64 column chunk.
    //     persistentState.relIDChunk =
    //         ColumnChunkFactory::createColumnChunkData(*LogicalType::INT64(), enableCompression,
    //             localState.regionCapacity, ResidencyState::IN_MEMORY);
    //     ChunkState chunkState;
    //     getColumn(REL_ID_COLUMN_ID)->initChunkState(transaction, nodeGroupIdx, chunkState);
    //     getColumn(REL_ID_COLUMN_ID)
    //         ->scan(transaction, chunkState, persistentState.relIDChunk.get(),
    //             persistentState.leftCSROffset, persistentState.rightCSROffset + 1);
    // }
    if (localState.region.level == 0) {
        for (auto columnID = 0u; columnID < columns.size(); columnID++) {
            updateColumn(transaction, nodeGroupIdx, isNewNodeGroup, columnID, persistentState,
                localState);
        }
    } else {
        for (auto columnID = 0u; columnID < columns.size(); columnID++) {
            distributeAndUpdateColumn(transaction, nodeGroupIdx, isNewNodeGroup, columnID,
                persistentState, localState);
        }
    }
}

void RelTableData::findPositionsForInsertions(offset_t nodeOffset, length_t numInsertions,
    LocalState& localState) {
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
        localState.needSliding = true;
    }
}

void RelTableData::slideForInsertions(offset_t nodeOffset, length_t numInsertions,
    const LocalState& localState) {
    // Now, we have to slide. Heuristically, the sliding happens both left and right.
    auto& header = localState.header;
    auto [leftBoundary, rightBoundary] = localState.region.getNodeOffsetBoundaries();
    auto leftSize = 0u, rightSize = 0u;
    for (auto i = leftBoundary; i < nodeOffset; i++) {
        leftSize += header.getCSRLength(i);
    }
    KU_ASSERT(localState.header.getStartCSROffset(nodeOffset) >= leftSize);
    auto gapSizeOfLeftSide = localState.header.getStartCSROffset(nodeOffset) -
                             localState.header.getStartCSROffset(leftBoundary) - leftSize;
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

void RelTableData::slideLeftForInsertions(offset_t nodeOffset, offset_t leftBoundary,
    const LocalState& localState, uint64_t numValuesToInsert) {
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
        localState.header.offset->getData().setValue(oldOffset - slideSize, i);
    }
}

// SlideRight is a bit different from slideLeft in that we are actually sliding the startCSROffsets
// of nodes, instead of endCSROffsets.
void RelTableData::slideRightForInsertions(offset_t nodeOffset, offset_t rightBoundary,
    const LocalState& localState, uint64_t numValuesToInsert) {
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
        localState.header.offset->getData().setValue<offset_t>(oldOffset + slideSize, i - 1);
    }
}

void RelTableData::updateColumn(Transaction*, node_group_idx_t, bool, column_id_t,
    const PersistentState&, LocalState&) {
    // auto column = getColumn(columnID);
    // applyUpdatesToColumn(transaction, nodeGroupIdx, isNewNodeGroup, columnID, persistentState,
    //     localState, column);
    // applyDeletionsToColumn(transaction, nodeGroupIdx, isNewNodeGroup, localState,
    // persistentState,
    //     column);
    // applySliding(transaction, nodeGroupIdx, isNewNodeGroup, localState, persistentState, column);
    // applyInsertionsToColumn(transaction, nodeGroupIdx, isNewNodeGroup, columnID, localState,
    //     persistentState, column);
}

void RelTableData::applyUpdatesToColumn(Transaction*, node_group_idx_t, bool, column_id_t,
    const PersistentState&, const LocalState&, Column*) {
    // offset_to_row_idx_t writeInfo;
    // auto& updateChunk = localState.localNG->getUpdateChunks(columnID);
    // for (auto& [srcOffset, updatesPerNode] : updateChunk.getSrcNodeOffsetToRelOffsets()) {
    //     if (localState.region.isOutOfBoundary(srcOffset)) {
    //         // TODO: Should also partition local storage into regions. So we can avoid this
    //         check. continue;
    //     }
    //     for (auto relOffset : updatesPerNode) {
    //         auto csrOffsetInRegion = findCSROffsetInRegion(persistentState, srcOffset,
    //         relOffset); writeInfo[csrOffsetInRegion] =
    //         updateChunk.getRowIdxFromOffset(relOffset);
    //     }
    // }
    // if (!writeInfo.empty()) {
    //     auto localChunk = updateChunk.getLocalChunk(0 /*columnID*/);
    //     column->prepareCommitForChunk(transaction, nodeGroupIdx, isNewNodeGroup, {},
    //         {} /*insertInfo*/, localChunk, writeInfo, {} /*deleteInfo*/);
    // }
}

void RelTableData::applyInsertionsToColumn(Transaction*, node_group_idx_t, bool, column_id_t,
    const LocalState&, const PersistentState&, Column*) {
    // (void)persistentState; // Avoid unused variable warning.
    // offset_to_row_idx_t writeInfo;
    // auto& insertChunks = localState.localNG->insertChunks;
    // for (auto& [offset, insertions] : insertChunks.getSrcNodeOffsetToRelOffsets()) {
    //     if (localState.region.isOutOfBoundary(offset)) {
    //         continue;
    //     }
    //     auto startCSROffset = localState.header.getStartCSROffset(offset);
    //     auto length = localState.header.getCSRLength(offset);
    //     KU_ASSERT(length >= insertions.size());
    //     KU_ASSERT((startCSROffset + persistentState.header.getCSRLength(offset) -
    //                   localState.localNG->deleteInfo.getNumDeletedRelsFromSrcOffset(offset) +
    //                   insertions.size()) <= localState.header.getEndCSROffset(offset));
    //     auto idx = startCSROffset + length - insertions.size();
    //     for (auto relOffset : insertions) {
    //         writeInfo[idx++] = insertChunks.getRowIdxFromOffset(relOffset);
    //     }
    // }
    // auto localChunk = insertChunks.getLocalChunk(columnID);
    // column->prepareCommitForChunk(transaction, nodeGroupIdx, isNewNodeGroup, localChunk,
    // writeInfo,
    // {}, {}, {});
}

std::vector<std::pair<offset_t, offset_t>> RelTableData::getSlidesForDeletions(
    const PersistentState&, const LocalState&) {
    std::vector<std::pair<offset_t, offset_t>> slides;
    // for (auto& [offset, deletions] :
    //     localState.localNG->deleteInfo.getSrcNodeOffsetToRelOffsetVec()) {
    //     if (localState.region.isOutOfBoundary(offset)) {
    //         continue;
    //     }
    //     auto length = persistentState.header.getCSRLength(offset);
    //     auto newLength = length - deletions.size();
    //     if (newLength == 0) {
    //         // No need to slide. Just skip.
    //         continue;
    //     }
    //     auto startCSROffset = persistentState.header.getStartCSROffset(offset);
    //     std::vector<offset_t> deletionsInChunk;
    //     for (auto relOffset : deletions) {
    //         auto csrOffsetInRegion = findCSROffsetInRegion(persistentState, offset, relOffset);
    //         deletionsInChunk.push_back(csrOffsetInRegion);
    //     }
    //     std::sort(deletionsInChunk.begin(), deletionsInChunk.end());
    //     KU_ASSERT(deletionsInChunk.begin() <= deletionsInChunk.end());
    //     uint64_t offsetToCopyFrom = startCSROffset, offsetToCopyInto = startCSROffset;
    //     for (auto deletedOffset : deletionsInChunk) {
    //         KU_ASSERT(deletedOffset >= offsetToCopyFrom);
    //         auto numValuesToCopy = deletedOffset - offsetToCopyFrom;
    //         for (auto k = 0u; k < numValuesToCopy; k++) {
    //             slides.push_back({offsetToCopyFrom + k, offsetToCopyInto + k});
    //         }
    //         offsetToCopyInto += numValuesToCopy;
    //         offsetToCopyFrom = deletedOffset + 1;
    //     }
    //     while (offsetToCopyFrom < (startCSROffset + length)) {
    //         slides.push_back({offsetToCopyFrom++, offsetToCopyInto++});
    //     }
    // }
    return slides;
}

// TODO(Guodong): 1. When there are insertions, we can avoid sliding by caching deleted positions
//                for insertions.
//                2. Moving from the back of the CSR list to deleted positions, so we can avoid
//                slidings and benefit from this when there is few deletions.
//                3. `getSlidesForDeletions` can be done once for all columns.
void RelTableData::applyDeletionsToColumn(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, const LocalState& localState, const PersistentState& persistentState,
    Column* column) const {
    auto slides = getSlidesForDeletions(persistentState, localState);
    if (slides.empty()) {
        return;
    }
    auto chunk = ColumnChunkFactory::createColumnChunkData(column->getDataType().copy(),
        enableCompression, slides.size(), ResidencyState::IN_MEMORY);
    std::vector<offset_t> dstOffsets;
    dstOffsets.resize(slides.size());
    auto tmpChunkForRead = ColumnChunkFactory::createColumnChunkData(column->getDataType().copy(),
        enableCompression, 1 /*capacity*/, ResidencyState::IN_MEMORY);
    ChunkState chunkState;
    // column->initChunkState(transaction, nodeGroupIdx, chunkState);
    for (auto i = 0u; i < slides.size(); i++) {
        column->scan(transaction, chunkState, tmpChunkForRead.get(), slides[i].first,
            slides[i].first + 1);
        chunk->append(tmpChunkForRead.get(), 0, 1);
        dstOffsets[i] = slides[i].second;
    }
    column->prepareCommitForChunk(transaction, nodeGroupIdx, isNewNodeGroup, dstOffsets,
        chunk.get(), 0);
}

// TODO(Guodong): Optimize the sliding by moving the suffix/prefix depending on shifting
//                left/right.
// TODO: applySliding should work for all columns. so no redundant computation of slidings.
void RelTableData::applySliding(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, const LocalState& localState, const PersistentState& persistentState,
    Column* column) const {
    if (!localState.needSliding) {
        return;
    }
    auto [leftBoundary, rightBoundary] = localState.region.getNodeOffsetBoundaries();
    std::vector<std::pair<offset_t, offset_t>> slides;
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
        for (auto k = 0u; k < length; k++) {
            slides.push_back({oldOffset + k, newOffset + k});
        }
    }
    if (slides.empty()) {
        return;
    }
    auto chunk = ColumnChunkFactory::createColumnChunkData(column->getDataType().copy(),
        enableCompression, slides.size(), ResidencyState::IN_MEMORY);
    std::vector<offset_t> dstOffsets;
    dstOffsets.resize(slides.size());
    auto tmpChunkForRead = ColumnChunkFactory::createColumnChunkData(column->getDataType().copy(),
        enableCompression, 1 /*capacity*/, ResidencyState::IN_MEMORY);
    ChunkState chunkState;
    // column->initChunkState(transaction, nodeGroupIdx, chunkState);
    for (auto i = 0u; i < slides.size(); i++) {
        column->scan(transaction, chunkState, tmpChunkForRead.get(), slides[i].first,
            slides[i].first + 1);
        chunk->append(tmpChunkForRead.get(), 0, 1);
        dstOffsets[i] = slides[i].second;
    }
    column->prepareCommitForChunk(transaction, nodeGroupIdx, isNewNodeGroup, dstOffsets,
        chunk.get(), 0);
}

offset_t RelTableData::getMaxNumNodesInRegion(const ChunkedCSRHeader&, const PackedCSRRegion&,
    const ChunkedNodeGroup*) {
    //     auto numNodes = header.offset->getNumValues();
    //     KU_ASSERT(numNodes == header.length->getNumValues());
    //     for (auto& [offset, _] : localNG->insertChunks.getSrcNodeOffsetToRelOffsets()) {
    //         if (!region.isOutOfBoundary(offset) && offset >= numNodes) {
    //             numNodes = offset + 1;
    //         }
    //     }
    //     return numNodes;
}

void RelTableData::updateCSRHeader(Transaction*, node_group_idx_t, bool,
    PersistentState& persistentState, LocalState& localState) {
    // auto [leftBoundary, rightBoundary] = localState.region.getNodeOffsetBoundaries();
    // auto& header = persistentState.header;
    // auto maxNumNodesInRegion =
    // getMaxNumNodesInRegion(header, localState.region, localState.localNG);
    // Update the region boundary based on actual num nodes in the region.
    // localState.region.leftBoundary = std::min(leftBoundary, header.offset->getNumValues());
    // localState.region.rightBoundary = std::min(rightBoundary, maxNumNodesInRegion - 1);
    // persistentState.leftCSROffset = header.getStartCSROffset(localState.region.leftBoundary);
    // persistentState.rightCSROffset = header.getEndCSROffset(localState.region.rightBoundary);
    // localState.header =
    //     ChunkedCSRHeader(enableCompression, maxNumNodesInRegion, ResidencyState::IN_MEMORY);
    // auto& newHeader = localState.header;
    // newHeader.copyFrom(header);
    // newHeader.fillDefaultValues(localState.region.rightBoundary + 1);
    // if (localState.localNG->insertChunks.isEmpty() && localState.localNG->deleteInfo.isEmpty()) {
    //     // No need to update the csr header.
    //     localState.leftCSROffset = persistentState.leftCSROffset;
    //     localState.rightCSROffset = persistentState.rightCSROffset;
    //     return;
    // }
    // for (auto& [offset, deletions] :
    //     localState.localNG->deleteInfo.getSrcNodeOffsetToRelOffsetVec()) {
    //     if (localState.region.isOutOfBoundary(offset)) {
    //         continue;
    //     }
    //     auto oldLength = newHeader.getCSRLength(offset);
    //     int64_t newLength = static_cast<int64_t>(oldLength) - deletions.size();
    //     KU_ASSERT(newLength >= 0);
    //     newHeader.length->getData().setValue<length_t>(newLength, offset);
    //     newHeader.length->getData().getNullData()->setNull(offset, false);
    // }
    // for (auto& [offset, _] : localState.localNG->insertChunks.getSrcNodeOffsetToRelOffsets()) {
    //     if (localState.region.isOutOfBoundary(offset)) {
    //         continue;
    //     }
    //     auto oldLength = newHeader.getCSRLength(offset);
    //     auto numInsertions = localState.localNG->getNumInsertedRels(offset);
    //     if (localState.region.level == 0) {
    //         findPositionsForInsertions(offset, numInsertions, localState);
    //     }
    //     int64_t newLength = static_cast<int64_t>(oldLength) + numInsertions;
    //     KU_ASSERT(newLength >= 0);
    //     newHeader.length->getData().setValue<length_t>(newLength, offset);
    //     newHeader.length->getData().getNullData()->setNull(offset, false);
    // }
    // if (localState.region.level > 0) {
    //     distributeOffsets(header, localState, localState.region.leftBoundary,
    //     maxNumNodesInRegion);
    // } else {
    //     localState.regionSize =
    //         getNewRegionSize(header, localState.sizeChangesPerSegment, localState.region);
    //     localState.regionCapacity = getRegionCapacity(header, localState.region);
    // }
    // localState.leftCSROffset = newHeader.getStartCSROffset(localState.region.leftBoundary);
    // localState.rightCSROffset = newHeader.getEndCSROffset(localState.region.rightBoundary);
    // std::vector<offset_t> dstOffsets;
    // dstOffsets.resize(newHeader.offset->getNumValues() - localState.region.leftBoundary);
    // fillSequence(dstOffsets, localState.region.leftBoundary);
    // commitCSRHeaderChunk(transaction, isNewNodeGroup, nodeGroupIdx,
    // csrHeaderColumns.offset.get(),
    //     &newHeader.offset->getData(), localState, dstOffsets);
    // commitCSRHeaderChunk(transaction, isNewNodeGroup, nodeGroupIdx,
    // csrHeaderColumns.length.get(),
    //     &newHeader.length->getData(), localState, dstOffsets);
}

void RelTableData::commitCSRHeaderChunk(Transaction* transaction, bool isNewNodeGroup,
    node_group_idx_t, Column* column, ColumnChunkData* chunk, const LocalState& localState,
    const std::vector<offset_t>& dstOffsets) {
    ChunkState state;
    // column->initChunkState(transaction, nodeGroupIdx, state);
    if (!isNewNodeGroup) {
        if (column->canCommitInPlace(state, dstOffsets, chunk, localState.region.leftBoundary)) {
            // TODO: We're assuming dstOffsets are consecutive here. This is a bad interface.
            column->write(state, dstOffsets[0], chunk, localState.region.leftBoundary,
                dstOffsets.size());
            // column->getMetadataDA()->update(nodeGroupIdx, state.metadata);
            return;
        }
    }
    column->commitColumnChunkOutOfPlace(transaction, state, isNewNodeGroup, dstOffsets, chunk,
        localState.region.leftBoundary);
}

void RelTableData::distributeOffsets(const ChunkedCSRHeader& header, LocalState& localState,
    offset_t leftBoundary, offset_t rightBoundary) const {
    if (localState.region.level >= packedCSRInfo.calibratorTreeHeight) {
        // Need to resize the capacity and reset regionToDistribute to the top level one.
        localState.region =
            PackedCSRRegion{0, static_cast<idx_t>(packedCSRInfo.calibratorTreeHeight)};
        localState.regionSize =
            getNewRegionSize(header, localState.sizeChangesPerSegment, localState.region);
        localState.regionCapacity = StorageUtils::divideAndRoundUpTo(localState.regionSize,
            StorageConstants::PACKED_CSR_DENSITY);
    } else {
        localState.regionSize =
            getNewRegionSize(header, localState.sizeChangesPerSegment, localState.region);
        localState.regionCapacity = getRegionCapacity(header, localState.region);
    }
    KU_ASSERT(localState.regionSize <= localState.regionCapacity);
    auto gapSpace = localState.regionCapacity - localState.regionSize;
    double gapRatio = divideNoRoundUp(gapSpace, localState.regionCapacity);
    auto& newHeader = localState.header;
    for (auto nodeOffset = leftBoundary; nodeOffset < rightBoundary; nodeOffset++) {
        int64_t newLength = newHeader.getCSRLength(nodeOffset);
        auto newGap = std::min(gapSpace, multiplyAndRoundUpTo(gapRatio, newLength));
        gapSpace -= newGap;
        auto startCSROffset = newHeader.getStartCSROffset(nodeOffset);
        auto newOffset = startCSROffset + newLength + newGap;
        newHeader.offset->getData().setValue(newOffset, nodeOffset);
        newHeader.offset->getData().getNullData()->setNull(nodeOffset, false);
    }
    localState.needSliding = true;
}

// void RelTableData::prepareCommitNodeGroup(Transaction* transaction, node_group_idx_t
// nodeGroupIdx,
//     LocalRelNG* localRelNG) {
//     auto numNodesInPersistentStorage = csrHeaderColumns.getNumNodes(transaction, nodeGroupIdx);
//     PersistentState persistentState(numNodesInPersistentStorage);
//     csrHeaderColumns.scan(transaction, nodeGroupIdx, persistentState.header);
//     LocalState localState(localRelNG);
//     auto regions = findRegions(persistentState.header, localState);
//     for (auto& region : regions) {
//         localState.setRegion(region);
//         auto isNewNG = isNewNodeGroup(transaction, nodeGroupIdx);
//         updateCSRHeader(transaction, nodeGroupIdx, isNewNG, persistentState, localState);
//         KU_ASSERT((region.level >= packedCSRInfo.calibratorTreeHeight && regions.size() == 1) ||
//                   region.level < packedCSRInfo.calibratorTreeHeight);
//         updateRegion(transaction, nodeGroupIdx, isNewNG, persistentState, localState);
//     }
// }

void RelTableData::serialize(Serializer& serializer) {
    nodeGroups->serialize(serializer);
}

// LocalRelNG* RelTableData::getLocalNodeGroup(Transaction* transaction,
//     node_group_idx_t nodeGroupIdx) const {
//     auto localTable = transaction->getLocalStorage()->getLocalTable(tableID);
//     if (!localTable) {
//         return nullptr;
//     }
//     auto localRelTable = ku_dynamic_cast<LocalTable*, LocalRelTable*>(localTable);
//     auto localTableData = localRelTable->getTableData(direction);
//     LocalRelNG* localNodeGroup = nullptr;
//     if (localTableData->nodeGroups.contains(nodeGroupIdx)) {
//         localNodeGroup = ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(
//             localTableData->nodeGroups.at(nodeGroupIdx).get());
//     }
//     return localNodeGroup;
// }

} // namespace storage
} // namespace kuzu

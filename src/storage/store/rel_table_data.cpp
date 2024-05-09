#include "storage/store/rel_table_data.h"

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/enums/rel_direction.h"
#include "common/exception/message.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/stats/rels_store_statistics.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelDataReadState::RelDataReadState()
    : startNodeOffset{INVALID_OFFSET}, numNodes{0}, currentNodeOffset{0}, posInCurrentCSR{0},
      readFromPersistentStorage{false}, readFromLocalStorage{false}, localNodeGroup{nullptr} {
    csrListEntries.resize(StorageConstants::NODE_GROUP_SIZE, {0, 0});
}

bool RelDataReadState::hasMoreToReadFromLocalStorage() const {
    KU_ASSERT(localNodeGroup);
    return posInCurrentCSR <
           localNodeGroup->getNumInsertedRels(currentNodeOffset - startNodeOffset);
}

bool RelDataReadState::trySwitchToLocalStorage() {
    if (localNodeGroup &&
        localNodeGroup->getNumInsertedRels(currentNodeOffset - startNodeOffset) > 0) {
        readFromLocalStorage = true;
        posInCurrentCSR = 0;
        return true;
    }
    return false;
}

bool RelDataReadState::hasMoreToRead(Transaction* transaction) {
    if (transaction->isWriteTransaction()) {
        if (readFromLocalStorage) {
            // Already read from local storage. Check if there are more in local storage.
            return hasMoreToReadFromLocalStorage();
        } else {
            if (hasMoreToReadInPersistentStorage()) {
                return true;
            } else {
                // Try switch to read from local storage.
                return trySwitchToLocalStorage();
            }
        }
    } else {
        return hasMoreToReadInPersistentStorage();
    }
}

void RelDataReadState::populateCSRListEntries() {
    for (auto i = 0u; i < numNodes; i++) {
        csrListEntries[i].size = csrHeaderChunks.getCSRLength(i);
        KU_ASSERT(csrListEntries[i].size <=
                  csrHeaderChunks.getEndCSROffset(i) - csrHeaderChunks.getStartCSROffset(i));
        csrListEntries[i].offset = csrHeaderChunks.getStartCSROffset(i);
    }
}

std::pair<offset_t, offset_t> RelDataReadState::getStartAndEndOffset() {
    auto currCSRListEntry = csrListEntries[currentNodeOffset - startNodeOffset];
    auto currCSRSize = currCSRListEntry.size;
    auto startOffset = currCSRListEntry.offset + posInCurrentCSR;
    auto numRowsToRead = std::min(currCSRSize - posInCurrentCSR, DEFAULT_VECTOR_CAPACITY);
    posInCurrentCSR += numRowsToRead;
    return {startOffset, startOffset + numRowsToRead};
}

offset_t CSRHeaderColumns::getNumNodes(Transaction* transaction,
    node_group_idx_t nodeGroupIdx) const {
    auto numPersistentNodeGroups = offset->getNumNodeGroups(transaction);
    return nodeGroupIdx >= numPersistentNodeGroups ?
               0 :
               offset->getMetadata(nodeGroupIdx, transaction->getType()).numValues;
}

PackedCSRInfo::PackedCSRInfo() {
    calibratorTreeHeight =
        StorageConstants::NODE_GROUP_SIZE_LOG2 - StorageConstants::CSR_SEGMENT_SIZE_LOG2;
    lowDensityStep =
        (double)(StorageConstants::PACKED_CSR_DENSITY - StorageConstants::LEAF_LOW_CSR_DENSITY) /
        (double)(calibratorTreeHeight);
    highDensityStep =
        (double)(StorageConstants::LEAF_HIGH_CSR_DENSITY - StorageConstants::PACKED_CSR_DENSITY) /
        (double)(calibratorTreeHeight);
}

PackedCSRRegion::PackedCSRRegion(vector_idx_t regionIdx, vector_idx_t level)
    : regionIdx{regionIdx}, level{level} {
    auto startSegmentIdx = regionIdx << level;
    leftBoundary = startSegmentIdx << StorageConstants::CSR_SEGMENT_SIZE_LOG2;
    rightBoundary = leftBoundary + (StorageConstants::CSR_SEGMENT_SIZE << level) - 1;
}

bool PackedCSRRegion::isWithin(const PackedCSRRegion& other) const {
    if (other.level >= level) {
        return false;
    }
    auto [left, right] = getSegmentBoundaries();
    auto [otherLeft, otherRight] = other.getSegmentBoundaries();
    return left >= otherLeft && right <= otherRight;
}

void PackedCSRRegion::setSizeChange(const std::vector<int64_t>& sizeChangesPerSegment) {
    sizeChange = 0;
    auto startSegmentIdx = regionIdx << level;
    auto endSegmentIdx = startSegmentIdx + (1 << level) - 1;
    for (auto segmentIdx = startSegmentIdx; segmentIdx <= endSegmentIdx; segmentIdx++) {
        sizeChange += sizeChangesPerSegment[segmentIdx];
    }
}

RelTableData::RelTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, TableCatalogEntry* tableEntry,
    RelsStoreStats* relsStoreStats, RelDataDirection direction, bool enableCompression)
    : TableData{dataFH, metadataFH, tableEntry, bufferManager, wal, enableCompression},
      direction{direction} {
    multiplicity = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(tableEntry)
                       ->getMultiplicity(direction);
    // No NULL values is allowed for the csr offset column.
    auto csrOffsetMetadataDAHInfo =
        relsStoreStats->getCSROffsetMetadataDAHInfo(&DUMMY_WRITE_TRANSACTION, tableID, direction);
    auto csrOffsetColumnName = StorageUtils::getColumnName("", StorageUtils::ColumnType::CSR_OFFSET,
        RelDataDirectionUtils::relDirectionToString(direction));
    csrHeaderColumns.offset = std::make_unique<Column>(csrOffsetColumnName, *LogicalType::UINT64(),
        *csrOffsetMetadataDAHInfo, dataFH, metadataFH, bufferManager, wal, &DUMMY_WRITE_TRANSACTION,
        RWPropertyStats::empty(), enableCompression, false /* requireNUllColumn */);
    auto csrLengthMetadataDAHInfo =
        relsStoreStats->getCSRLengthMetadataDAHInfo(&DUMMY_WRITE_TRANSACTION, tableID, direction);
    auto csrLengthColumnName = StorageUtils::getColumnName("", StorageUtils::ColumnType::CSR_LENGTH,
        RelDataDirectionUtils::relDirectionToString(direction));
    csrHeaderColumns.length = std::make_unique<Column>(csrLengthColumnName, *LogicalType::UINT64(),
        *csrLengthMetadataDAHInfo, dataFH, metadataFH, bufferManager, wal, &DUMMY_WRITE_TRANSACTION,
        RWPropertyStats::empty(), enableCompression, false /* requireNUllColumn */);
    // Columns (nbrID + properties).
    auto& properties = tableEntry->getPropertiesRef();
    auto maxColumnID = std::max_element(properties.begin(), properties.end(), [](auto& a, auto& b) {
        return a.getColumnID() < b.getColumnID();
    })->getColumnID();
    // The first column is reserved for NBR_ID, which is not a property.
    columns.resize(maxColumnID + 2);
    auto nbrIDMetadataDAHInfo = relsStoreStats->getColumnMetadataDAHInfo(&DUMMY_WRITE_TRANSACTION,
        tableID, NBR_ID_COLUMN_ID, direction);
    auto nbrIDColName = StorageUtils::getColumnName("NBR_ID", StorageUtils::ColumnType::DEFAULT,
        RelDataDirectionUtils::relDirectionToString(direction));
    auto nbrIDColumn = std::make_unique<InternalIDColumn>(nbrIDColName, *nbrIDMetadataDAHInfo,
        dataFH, metadataFH, bufferManager, wal, &DUMMY_WRITE_TRANSACTION, RWPropertyStats::empty(),
        enableCompression);
    columns[NBR_ID_COLUMN_ID] = std::move(nbrIDColumn);
    // Property columns.
    for (auto i = 0u; i < properties.size(); i++) {
        auto& property = properties[i];
        auto columnID = property.getColumnID() + 1; // Skip NBR_ID column.
        auto metadataDAHInfo = relsStoreStats->getColumnMetadataDAHInfo(&DUMMY_WRITE_TRANSACTION,
            tableID, columnID, direction);
        auto colName =
            StorageUtils::getColumnName(property.getName(), StorageUtils::ColumnType::DEFAULT,
                RelDataDirectionUtils::relDirectionToString(direction));
        columns[columnID] = ColumnFactory::createColumn(colName, *property.getDataType()->copy(),
            *metadataDAHInfo, dataFH, metadataFH, bufferManager, wal, &DUMMY_WRITE_TRANSACTION,
            RWPropertyStats(relsStoreStats, tableID, property.getPropertyID()), enableCompression);
    }
    // Set common tableID for nbrIDColumn and relIDColumn.
    auto nbrTableID = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(tableEntry)
                          ->getNbrTableID(direction);
    ku_dynamic_cast<Column*, InternalIDColumn*>(columns[NBR_ID_COLUMN_ID].get())
        ->setCommonTableID(nbrTableID);
    ku_dynamic_cast<Column*, InternalIDColumn*>(columns[REL_ID_COLUMN_ID].get())
        ->setCommonTableID(tableID);
    packedCSRInfo = PackedCSRInfo();
}

// TODO: avoid scan the whole group of the csr header. just scan the vector.
void RelTableData::initializeReadState(Transaction* transaction, std::vector<column_id_t> columnIDs,
    const ValueVector& inNodeIDVector, RelDataReadState& readState) {
    readState.direction = direction;
    readState.columnIDs.clear();
    readState.columnIDs.push_back(NBR_ID_COLUMN_ID);
    readState.columnIDs.insert(readState.columnIDs.end(), columnIDs.begin(), columnIDs.end());
    // Reset to read from persistent storage.
    readState.readFromLocalStorage = false;
    auto nodeOffset = inNodeIDVector.readNodeOffset(inNodeIDVector.state->getSelVector()[0]);
    // Reset to read from beginning for the csr of the new node offset.
    readState.posInCurrentCSR = 0;
    if (readState.isOutOfRange(nodeOffset)) {
        // Scan csr offsets and populate csr list entries for the new node group.
        auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        readState.startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        csrHeaderColumns.scan(transaction, nodeGroupIdx, readState.csrHeaderChunks);
        KU_ASSERT(readState.csrHeaderChunks.offset->getNumValues() ==
                  readState.csrHeaderChunks.length->getNumValues());
        readState.numNodes = readState.csrHeaderChunks.offset->getNumValues();
        readState.readFromPersistentStorage =
            nodeGroupIdx < columns[REL_ID_COLUMN_ID]->getNumNodeGroups(transaction) &&
            (nodeOffset - readState.startNodeOffset) < readState.numNodes;
        if (readState.readFromPersistentStorage) {
            readState.populateCSRListEntries();
            initializeColumnReadStates(transaction, readState, nodeGroupIdx);
        }
        if (transaction->isWriteTransaction()) {
            readState.localNodeGroup = getLocalNodeGroup(transaction, nodeGroupIdx);
        }
    }
    if (nodeOffset != readState.currentNodeOffset) {
        readState.currentNodeOffset = nodeOffset;
    }
}

void RelTableData::initializeColumnReadStates(Transaction* transaction, RelDataReadState& readState,
    node_group_idx_t nodeGroupIdx) {
    readState.columnStates.resize(readState.columnIDs.size());
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        auto columnID = readState.columnIDs[i];
        if (columnID == INVALID_COLUMN_ID) {
            continue;
        }
        getColumn(columnID)->initChunkState(transaction, nodeGroupIdx, readState.columnStates[i]);
    }
}

void RelTableData::scan(Transaction* transaction, TableDataReadState& readState,
    const ValueVector& inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    auto& relReadState = ku_dynamic_cast<TableDataReadState&, RelDataReadState&>(readState);
    if (relReadState.readFromLocalStorage) {
        auto offsetInChunk = relReadState.currentNodeOffset - relReadState.startNodeOffset;
        KU_ASSERT(relReadState.localNodeGroup);
        auto numValuesRead = relReadState.localNodeGroup->scanCSR(offsetInChunk,
            relReadState.posInCurrentCSR, relReadState.columnIDs, outputVectors);
        relReadState.posInCurrentCSR += numValuesRead;
        return;
    }
    KU_ASSERT(relReadState.readFromPersistentStorage);
    auto [startOffset, endOffset] = relReadState.getStartAndEndOffset();
    auto numRowsToRead = endOffset - startOffset;
    outputVectors[0]->state->getSelVectorUnsafe().setToUnfiltered(numRowsToRead);
    outputVectors[0]->state->setOriginalSize(numRowsToRead);
    auto relIDVectorIdx = INVALID_VECTOR_IDX;
    for (auto i = 0u; i < relReadState.columnIDs.size(); i++) {
        auto columnID = relReadState.columnIDs[i];
        auto outputVectorId = i; // Skip output from nbrID column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        if (columnID == REL_ID_COLUMN_ID) {
            relIDVectorIdx = outputVectorId;
        }
        getColumn(columnID)->scan(transaction, relReadState.columnStates[i], startOffset, endOffset,
            outputVectors[outputVectorId], 0 /* offsetInVector */);
    }
    if (transaction->isWriteTransaction() && relReadState.localNodeGroup) {
        auto nodeOffset = inNodeIDVector.readNodeOffset(inNodeIDVector.state->getSelVector()[0]);
        KU_ASSERT(relIDVectorIdx != INVALID_VECTOR_IDX);
        auto relIDVector = outputVectors[relIDVectorIdx];
        relReadState.localNodeGroup->applyLocalChangesToScannedVectors(
            nodeOffset - relReadState.startNodeOffset, relReadState.columnIDs, relIDVector,
            outputVectors);
    }
}

void RelTableData::lookup(Transaction* /*transaction*/, TableDataReadState& /*readState*/,
    const ValueVector& /*inNodeIDVector*/, const std::vector<ValueVector*>& /*outputVectors*/) {
    KU_ASSERT(false);
}

bool RelTableData::delete_(Transaction* transaction, ValueVector* srcNodeIDVector,
    ValueVector* relIDVector) {
    auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    auto localRelTable = ku_dynamic_cast<LocalTable*, LocalRelTable*>(localTable);
    auto localTableData = localRelTable->getTableData(direction);
    return localTableData->delete_(srcNodeIDVector, relIDVector);
}

void RelTableData::checkRelMultiplicityConstraint(Transaction* transaction,
    ValueVector* srcNodeIDVector) const {
    KU_ASSERT(srcNodeIDVector->state->isFlat() && multiplicity == RelMultiplicity::ONE);
    auto nodeIDPos = srcNodeIDVector->state->getSelVector()[0];
    auto nodeOffset = srcNodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    if (checkIfNodeHasRels(transaction, nodeOffset)) {
        throw RuntimeException(ExceptionMessage::violateRelMultiplicityConstraint(tableName,
            std::to_string(nodeOffset), RelDataDirectionUtils::relDirectionToString(direction)));
    }
}

bool RelTableData::checkIfNodeHasRels(Transaction* transaction, offset_t nodeOffset) const {
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    if (nodeGroupIdx >= csrHeaderColumns.length->getNumNodeGroups(transaction)) {
        return false;
    }
    Column::ChunkState readState;
    csrHeaderColumns.length->initChunkState(transaction, nodeGroupIdx, readState);
    if (offsetInChunk >= readState.metadata.numValues) {
        return false;
    }
    length_t length;
    csrHeaderColumns.length->scan(transaction, readState, offsetInChunk, offsetInChunk + 1,
        reinterpret_cast<uint8_t*>(&length));
    return length > 0;
}

void RelTableData::append(ChunkedNodeGroup* nodeGroup) {
    auto csrNodeGroup = ku_dynamic_cast<ChunkedNodeGroup*, ChunkedCSRNodeGroup*>(nodeGroup);
    csrHeaderColumns.append(csrNodeGroup->getCSRHeader(), nodeGroup->getNodeGroupIdx());
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        getColumn(columnID)->append(&nodeGroup->getColumnChunkUnsafe(columnID),
            nodeGroup->getNodeGroupIdx());
    }
}

static length_t getGapSizeForNode(const ChunkedCSRHeader& header, offset_t nodeOffset) {
    return header.getEndCSROffset(nodeOffset) - header.getStartCSROffset(nodeOffset) -
           header.getCSRLength(nodeOffset);
}

static length_t getRegionCapacity(const ChunkedCSRHeader& header, PackedCSRRegion region) {
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

static uint64_t findPosOfRelIDFromArray(ColumnChunk* relIDInRegion, offset_t startPos,
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
    offset_t nodeOffset, offset_t relOffset) const {
    auto startPos =
        persistentState.header.getStartCSROffset(nodeOffset) - persistentState.leftCSROffset;
    auto endPos = startPos + persistentState.header.getCSRLength(nodeOffset);
    auto posInCSRList =
        findPosOfRelIDFromArray(persistentState.relIDChunk.get(), startPos, endPos, relOffset);
    KU_ASSERT(posInCSRList != UINT64_MAX);
    return posInCSRList + persistentState.leftCSROffset;
}

bool RelTableData::isNewNodeGroup(Transaction* transaction, node_group_idx_t nodeGroupIdx) const {
    if (nodeGroupIdx >= getNumNodeGroups(transaction) ||
        getNbrIDColumn()->getMetadata(nodeGroupIdx, transaction->getType()).numValues == 0) {
        return true;
    }
    return false;
}

void RelTableData::prepareLocalTableToCommit(Transaction* transaction, LocalTableData* localTable) {
    auto localRelTableData = ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(localTable);
    for (auto& [nodeGroupIdx, nodeGroup] : localRelTableData->nodeGroups) {
        auto relNG = ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(nodeGroup.get());
        prepareCommitNodeGroup(transaction, nodeGroupIdx, relNG);
    }
}

bool RelTableData::isWithinDensityBound(const ChunkedCSRHeader& header,
    const std::vector<int64_t>& sizeChangesPerSegment, PackedCSRRegion& region) {
    auto sizeInRegion = getNewRegionSize(header, sizeChangesPerSegment, region);
    auto capacityInRegion = getRegionCapacity(header, region);
    auto ratio = (double)sizeInRegion / (double)capacityInRegion;
    return ratio <= getHighDensity(region.level);
}

double RelTableData::getHighDensity(uint64_t level) const {
    KU_ASSERT(level <= packedCSRInfo.calibratorTreeHeight);
    if (level == 0) {
        return StorageConstants::LEAF_HIGH_CSR_DENSITY;
    }
    return StorageConstants::PACKED_CSR_DENSITY +
           (packedCSRInfo.highDensityStep * (double)(packedCSRInfo.calibratorTreeHeight - level));
}

RelTableData::LocalState::LocalState(LocalRelNG* localNG) : localNG{localNG} {
    localNG->getChangesPerCSRSegment(sizeChangesPerSegment, hasChangesPerSegment);
}

void RelTableData::applyUpdatesToChunk(const PersistentState& persistentState,
    LocalState& localState, const ChunkCollection& localChunk, ColumnChunk* chunk,
    column_id_t columnID) {
    offset_to_row_idx_t csrOffsetInRegionToRowIdx;
    auto [leftNodeBoundary, rightNodeBoundary] = localState.region.getNodeOffsetBoundaries();
    auto& updateChunk = localState.localNG->getUpdateChunks(columnID);
    for (auto& [nodeOffset, updates] : updateChunk.getSrcNodeOffsetToRelOffsets()) {
        if (nodeOffset < leftNodeBoundary || nodeOffset > rightNodeBoundary) {
            continue;
        }
        for (auto relOffset : updates) {
            auto csrOffsetInRegion = findCSROffsetInRegion(persistentState, nodeOffset, relOffset);
            csrOffsetInRegionToRowIdx[csrOffsetInRegion] =
                updateChunk.getRowIdxFromOffset(relOffset);
        }
    }
    Column::applyLocalChunkToColumnChunk(localChunk, chunk, csrOffsetInRegionToRowIdx);
}

void RelTableData::applyInsertionsToChunk(const PersistentState& persistentState,
    const LocalState& localState, const ChunkCollection& localChunk, ColumnChunk* newChunk) {
    offset_to_row_idx_t csrOffsetToRowIdx;
    auto [leftNodeBoundary, rightNodeBoundary] = localState.region.getNodeOffsetBoundaries();
    auto& insertChunks = localState.localNG->insertChunks;
    for (auto& [nodeOffset, insertions] : insertChunks.getSrcNodeOffsetToRelOffsets()) {
        if (nodeOffset < leftNodeBoundary || nodeOffset > rightNodeBoundary) {
            continue;
        }
        auto csrOffsetInRegion = localState.header.getStartCSROffset(nodeOffset) +
                                 persistentState.header.getCSRLength(nodeOffset) -
                                 localState.leftCSROffset;
        for (auto relOffset : insertions) {
            KU_ASSERT(csrOffsetInRegion != UINT64_MAX);
            csrOffsetToRowIdx[csrOffsetInRegion++] = insertChunks.getRowIdxFromOffset(relOffset);
        }
    }
    Column::applyLocalChunkToColumnChunk(localChunk, newChunk, csrOffsetToRowIdx);
}

// TODO(Guodong): This should be refactored to share the same control logic with
// `applyDeletionsToColumn`.
void RelTableData::applyDeletionsToChunk(const PersistentState& persistentState,
    const LocalState& localState, ColumnChunk* chunk) {
    auto& deleteInfo = localState.localNG->deleteInfo;
    for (auto& [offset, deletions] : deleteInfo.getSrcNodeOffsetToRelOffsetVec()) {
        if (localState.region.isOutOfBoundary(offset)) {
            continue;
        }
        auto length = persistentState.header.getCSRLength(offset);
        auto newLength = length - deletions.size();
        if (newLength == 0) {
            // No need to slide. Just skip.
            continue;
        }
        std::vector<offset_t> deletionsInRegion;
        for (auto relOffset : deletions) {
            auto csrOffsetInRegion = findCSROffsetInRegion(persistentState, offset, relOffset);
            deletionsInRegion.push_back(csrOffsetInRegion + localState.leftCSROffset);
        }
        auto csrOffset = persistentState.header.getStartCSROffset(offset);
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

void RelTableData::distributeAndUpdateColumn(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, bool isNewNodeGroup, column_id_t columnID,
    const PersistentState& persistentState, LocalState& localState) {
    auto column = getColumn(columnID);
    auto [leftNodeBoundary, rightNodeBoundary] = localState.region.getNodeOffsetBoundaries();
    KU_ASSERT(localState.regionCapacity >= (localState.rightCSROffset - localState.leftCSROffset));
    // First, scan the whole region to a temp chunk.
    auto oldSize = persistentState.rightCSROffset - persistentState.leftCSROffset + 1;
    auto chunk = ColumnChunkFactory::createColumnChunk(*column->getDataType().copy(),
        enableCompression, oldSize);
    column->scan(transaction, nodeGroupIdx, chunk.get(), persistentState.leftCSROffset,
        persistentState.rightCSROffset + 1);
    auto localUpdateChunk =
        localState.localNG->getUpdateChunks(columnID).getLocalChunk(0 /*columnID*/);
    applyUpdatesToChunk(persistentState, localState, localUpdateChunk, chunk.get(), columnID);
    applyDeletionsToChunk(persistentState, localState, chunk.get());
    // Second, create a new temp chunk for the region.
    auto newSize = localState.rightCSROffset - localState.leftCSROffset + 1;
    auto newChunk = ColumnChunkFactory::createColumnChunk(*column->getDataType().copy(),
        enableCompression, newSize);
    newChunk->getNullChunk()->resetToAllNull();
    auto maxNumNodesToDistribute = std::min(rightNodeBoundary - leftNodeBoundary + 1,
        persistentState.header.offset->getNumValues());
    // Third, copy the rels to the new chunk.
    for (auto i = 0u; i < maxNumNodesToDistribute; i++) {
        auto nodeOffset = i + leftNodeBoundary;
        auto csrOffsetInRegion =
            persistentState.header.getStartCSROffset(nodeOffset) - persistentState.leftCSROffset;
        auto length = persistentState.header.getCSRLength(nodeOffset);
        if (length == 0) {
            continue;
        }
        auto newCSROffsetInRegion =
            localState.header.getStartCSROffset(nodeOffset) - localState.leftCSROffset;
        KU_ASSERT(newCSROffsetInRegion >= newChunk->getNumValues());
        newChunk->copy(chunk.get(), csrOffsetInRegion, newCSROffsetInRegion, length);
    }
    auto insertLocalChunk = localState.localNG->insertChunks.getLocalChunk(columnID);
    applyInsertionsToChunk(persistentState, localState, insertLocalChunk, newChunk.get());
    std::vector<offset_t> dstOffsets;
    dstOffsets.resize(newChunk->getNumValues());
    fillSequence(dstOffsets, localState.leftCSROffset);
    KU_ASSERT(newChunk->sanityCheck());
    column->prepareCommitForChunk(transaction, nodeGroupIdx, isNewNodeGroup, dstOffsets,
        newChunk.get(), 0 /*srcOffset*/);
}

std::vector<PackedCSRRegion> RelTableData::findRegions(const ChunkedCSRHeader& headerChunks,
    LocalState& localState) {
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
    if (localState.localNG->hasUpdatesOrDeletions()) {
        // NOTE: There is an implicit trick happening. Due to the mismatch of storage type and
        // in-memory representation of INTERNAL_ID, we only store offset as INT64 on disk. Here
        // we directly read relID's offset part from disk into an INT64 column chunk.
        persistentState.relIDChunk = ColumnChunkFactory::createColumnChunk(*LogicalType::INT64(),
            enableCompression, localState.regionCapacity);
        getColumn(REL_ID_COLUMN_ID)
            ->scan(transaction, nodeGroupIdx, persistentState.relIDChunk.get(),
                persistentState.leftCSROffset, persistentState.rightCSROffset + 1);
    }
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
    LocalState& localState) {
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
    LocalState& localState, uint64_t numValuesToInsert) {
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
void RelTableData::slideRightForInsertions(offset_t nodeOffset, offset_t rightBoundary,
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

void RelTableData::updateColumn(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, column_id_t columnID, const RelTableData::PersistentState& persistentState,
    LocalState& localState) {
    auto column = getColumn(columnID);
    applyUpdatesToColumn(transaction, nodeGroupIdx, isNewNodeGroup, columnID, persistentState,
        localState, column);
    applyDeletionsToColumn(transaction, nodeGroupIdx, isNewNodeGroup, localState, persistentState,
        column);
    applySliding(transaction, nodeGroupIdx, isNewNodeGroup, localState, persistentState, column);
    applyInsertionsToColumn(transaction, nodeGroupIdx, isNewNodeGroup, columnID, localState,
        persistentState, column);
}

void RelTableData::applyUpdatesToColumn(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, column_id_t columnID, const PersistentState& persistentState,
    LocalState& localState, Column* column) {
    offset_to_row_idx_t writeInfo;
    auto& updateChunk = localState.localNG->getUpdateChunks(columnID);
    for (auto& [srcOffset, updatesPerNode] : updateChunk.getSrcNodeOffsetToRelOffsets()) {
        if (localState.region.isOutOfBoundary(srcOffset)) {
            // TODO: Should also partition local storage into regions. So we can avoid this check.
            continue;
        }
        for (auto relOffset : updatesPerNode) {
            auto csrOffsetInRegion = findCSROffsetInRegion(persistentState, srcOffset, relOffset);
            writeInfo[csrOffsetInRegion] = updateChunk.getRowIdxFromOffset(relOffset);
        }
    }
    if (!writeInfo.empty()) {
        auto localChunk = updateChunk.getLocalChunk(0 /*columnID*/);
        column->prepareCommitForChunk(transaction, nodeGroupIdx, isNewNodeGroup, {},
            {} /*insertInfo*/, localChunk, writeInfo, {} /*deleteInfo*/);
    }
}

void RelTableData::applyInsertionsToColumn(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, column_id_t columnID, LocalState& localState,
    const PersistentState& persistentState, Column* column) {
    (void)persistentState; // Avoid unused variable warning.
    offset_to_row_idx_t writeInfo;
    auto& insertChunks = localState.localNG->insertChunks;
    for (auto& [offset, insertions] : insertChunks.getSrcNodeOffsetToRelOffsets()) {
        if (localState.region.isOutOfBoundary(offset)) {
            continue;
        }
        auto startCSROffset = localState.header.getStartCSROffset(offset);
        auto length = localState.header.getCSRLength(offset);
        KU_ASSERT(length >= insertions.size());
        KU_ASSERT((startCSROffset + persistentState.header.getCSRLength(offset) -
                      localState.localNG->deleteInfo.getNumDeletedRelsFromSrcOffset(offset) +
                      insertions.size()) <= localState.header.getEndCSROffset(offset));
        auto idx = startCSROffset + length - insertions.size();
        for (auto relOffset : insertions) {
            writeInfo[idx++] = insertChunks.getRowIdxFromOffset(relOffset);
        }
    }
    auto localChunk = insertChunks.getLocalChunk(columnID);
    column->prepareCommitForChunk(transaction, nodeGroupIdx, isNewNodeGroup, localChunk, writeInfo,
        {}, {}, {});
}

std::vector<std::pair<offset_t, offset_t>> RelTableData::getSlidesForDeletions(
    const PersistentState& persistentState, const LocalState& localState) {
    std::vector<std::pair<offset_t, offset_t>> slides;
    for (auto& [offset, deletions] :
        localState.localNG->deleteInfo.getSrcNodeOffsetToRelOffsetVec()) {
        if (localState.region.isOutOfBoundary(offset)) {
            continue;
        }
        auto length = persistentState.header.getCSRLength(offset);
        auto newLength = length - deletions.size();
        if (newLength == 0) {
            // No need to slide. Just skip.
            continue;
        }
        auto startCSROffset = persistentState.header.getStartCSROffset(offset);
        std::vector<offset_t> deletionsInChunk;
        for (auto relOffset : deletions) {
            auto csrOffsetInRegion = findCSROffsetInRegion(persistentState, offset, relOffset);
            deletionsInChunk.push_back(csrOffsetInRegion);
        }
        std::sort(deletionsInChunk.begin(), deletionsInChunk.end());
        KU_ASSERT(deletionsInChunk.begin() <= deletionsInChunk.end());
        uint64_t offsetToCopyFrom = startCSROffset, offsetToCopyInto = startCSROffset;
        for (auto deletedOffset : deletionsInChunk) {
            KU_ASSERT(deletedOffset >= offsetToCopyFrom);
            auto numValuesToCopy = deletedOffset - offsetToCopyFrom;
            for (auto k = 0u; k < numValuesToCopy; k++) {
                slides.push_back({offsetToCopyFrom + k, offsetToCopyInto + k});
            }
            offsetToCopyInto += numValuesToCopy;
            offsetToCopyFrom = deletedOffset + 1;
        }
        while (offsetToCopyFrom < (startCSROffset + length)) {
            slides.push_back({offsetToCopyFrom++, offsetToCopyInto++});
        }
    }
    return slides;
}

// TODO(Guodong): 1. When there are insertions, we can avoid sliding by caching deleted positions
//                for insertions.
//                2. Moving from the back of the CSR list to deleted positions, so we can avoid
//                slidings and benefit from this when there is few deletions.
//                3. `getSlidesForDeletions` can be done once for all columns.
void RelTableData::applyDeletionsToColumn(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, LocalState& localState, const PersistentState& persistentState,
    Column* column) {
    auto slides = getSlidesForDeletions(persistentState, localState);
    if (slides.empty()) {
        return;
    }
    auto chunk = ColumnChunkFactory::createColumnChunk(*column->getDataType().copy(),
        enableCompression, slides.size());
    std::vector<offset_t> dstOffsets;
    dstOffsets.resize(slides.size());
    auto tmpChunkForRead =
        ColumnChunkFactory::createColumnChunk(*column->getDataType().copy(), enableCompression, 1);
    for (auto i = 0u; i < slides.size(); i++) {
        column->scan(transaction, nodeGroupIdx, tmpChunkForRead.get(), slides[i].first,
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
    bool isNewNodeGroup, LocalState& localState, const PersistentState& persistentState,
    Column* column) {
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
    auto chunk = ColumnChunkFactory::createColumnChunk(*column->getDataType().copy(),
        enableCompression, slides.size());
    std::vector<offset_t> dstOffsets;
    dstOffsets.resize(slides.size());
    auto tmpChunkForRead =
        ColumnChunkFactory::createColumnChunk(*column->getDataType().copy(), enableCompression, 1);
    for (auto i = 0u; i < slides.size(); i++) {
        column->scan(transaction, nodeGroupIdx, tmpChunkForRead.get(), slides[i].first,
            slides[i].first + 1);
        chunk->append(tmpChunkForRead.get(), 0, 1);
        dstOffsets[i] = slides[i].second;
    }
    column->prepareCommitForChunk(transaction, nodeGroupIdx, isNewNodeGroup, dstOffsets,
        chunk.get(), 0);
}

offset_t RelTableData::getMaxNumNodesInRegion(const ChunkedCSRHeader& header,
    const PackedCSRRegion& region, const LocalRelNG* localNG) {
    auto numNodes = header.offset->getNumValues();
    KU_ASSERT(numNodes == header.length->getNumValues());
    for (auto& [offset, _] : localNG->insertChunks.getSrcNodeOffsetToRelOffsets()) {
        if (!region.isOutOfBoundary(offset) && offset >= numNodes) {
            numNodes = offset + 1;
        }
    }
    return numNodes;
}

void RelTableData::updateCSRHeader(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, PersistentState& persistentState, LocalState& localState) {
    auto [leftBoundary, rightBoundary] = localState.region.getNodeOffsetBoundaries();
    auto& header = persistentState.header;
    auto maxNumNodesInRegion =
        getMaxNumNodesInRegion(header, localState.region, localState.localNG);
    // Update the region boundary based on actual num nodes in the region.
    localState.region.leftBoundary = std::min(leftBoundary, header.offset->getNumValues());
    localState.region.rightBoundary = std::min(rightBoundary, maxNumNodesInRegion - 1);
    persistentState.leftCSROffset = header.getStartCSROffset(localState.region.leftBoundary);
    persistentState.rightCSROffset = header.getEndCSROffset(localState.region.rightBoundary);
    localState.header = ChunkedCSRHeader(enableCompression, maxNumNodesInRegion);
    auto& newHeader = localState.header;
    newHeader.copyFrom(header);
    newHeader.fillDefaultValues(localState.region.rightBoundary + 1);
    if (localState.localNG->insertChunks.isEmpty() && localState.localNG->deleteInfo.isEmpty()) {
        // No need to update the csr header.
        localState.leftCSROffset = persistentState.leftCSROffset;
        localState.rightCSROffset = persistentState.rightCSROffset;
        return;
    }
    for (auto& [offset, deletions] :
        localState.localNG->deleteInfo.getSrcNodeOffsetToRelOffsetVec()) {
        if (localState.region.isOutOfBoundary(offset)) {
            continue;
        }
        auto oldLength = newHeader.getCSRLength(offset);
        int64_t newLength = (int64_t)oldLength - deletions.size();
        KU_ASSERT(newLength >= 0);
        newHeader.length->setValue<length_t>(newLength, offset);
        newHeader.length->getNullChunk()->setNull(offset, false);
    }
    for (auto& [offset, _] : localState.localNG->insertChunks.getSrcNodeOffsetToRelOffsets()) {
        if (localState.region.isOutOfBoundary(offset)) {
            continue;
        }
        auto oldLength = newHeader.getCSRLength(offset);
        auto numInsertions = localState.localNG->getNumInsertedRels(offset);
        if (localState.region.level == 0) {
            findPositionsForInsertions(offset, numInsertions, localState);
        }
        int64_t newLength = (int64_t)oldLength + numInsertions;
        KU_ASSERT(newLength >= 0);
        newHeader.length->setValue<length_t>(newLength, offset);
        newHeader.length->getNullChunk()->setNull(offset, false);
    }
    if (localState.region.level > 0) {
        distributeOffsets(header, localState, localState.region.leftBoundary, maxNumNodesInRegion);
    } else {
        localState.regionSize =
            getNewRegionSize(header, localState.sizeChangesPerSegment, localState.region);
        localState.regionCapacity = getRegionCapacity(header, localState.region);
    }
    localState.leftCSROffset = newHeader.getStartCSROffset(localState.region.leftBoundary);
    localState.rightCSROffset = newHeader.getEndCSROffset(localState.region.rightBoundary);
    std::vector<offset_t> dstOffsets;
    dstOffsets.resize(newHeader.offset->getNumValues() - localState.region.leftBoundary);
    fillSequence(dstOffsets, localState.region.leftBoundary);
    commitCSRHeaderChunk(transaction, isNewNodeGroup, nodeGroupIdx, csrHeaderColumns.offset.get(),
        newHeader.offset.get(), localState, dstOffsets);
    commitCSRHeaderChunk(transaction, isNewNodeGroup, nodeGroupIdx, csrHeaderColumns.length.get(),
        newHeader.length.get(), localState, dstOffsets);
}

void RelTableData::commitCSRHeaderChunk(Transaction* transaction, bool isNewNodeGroup,
    node_group_idx_t nodeGroupIdx, Column* column, ColumnChunk* chunk, LocalState& localState,
    const std::vector<common::offset_t>& dstOffsets) {
    if (!isNewNodeGroup) {
        Column::ChunkState state;
        column->initChunkState(transaction, nodeGroupIdx, state);
        if (column->canCommitInPlace(state, dstOffsets, chunk, localState.region.leftBoundary)) {
            // TODO: We're assuming dstOffsets are consecutive here. This is a bad interface.
            column->write(state, dstOffsets[0], chunk, localState.region.leftBoundary,
                dstOffsets.size());
            column->getMetadataDA()->update(nodeGroupIdx, state.metadata);
            return;
        }
    }
    column->commitColumnChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup, dstOffsets,
        chunk, localState.region.leftBoundary);
}

void RelTableData::distributeOffsets(const ChunkedCSRHeader& header, LocalState& localState,
    offset_t leftBoundary, offset_t rightBoundary) {
    if (localState.region.level > packedCSRInfo.calibratorTreeHeight) {
        // Need to resize the capacity and reset regionToDistribute to the top level one.
        localState.region =
            PackedCSRRegion{0, static_cast<vector_idx_t>(packedCSRInfo.calibratorTreeHeight)};
        localState.regionSize =
            getNewRegionSize(header, localState.sizeChangesPerSegment, localState.region);
        localState.regionCapacity = StorageUtils::divideAndRoundUpTo(localState.regionSize,
            StorageConstants::PACKED_CSR_DENSITY);
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
        newHeader.offset->getNullChunk()->setNull(nodeOffset, false);
    }
    localState.needSliding = true;
}

void RelTableData::prepareCommitNodeGroup(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    LocalRelNG* localRelNG) {
    auto numNodesInPersistentStorage = csrHeaderColumns.getNumNodes(transaction, nodeGroupIdx);
    PersistentState persistentState(numNodesInPersistentStorage);
    csrHeaderColumns.scan(transaction, nodeGroupIdx, persistentState.header);
    LocalState localState(localRelNG);
    auto regions = findRegions(persistentState.header, localState);
    for (auto& region : regions) {
        localState.setRegion(region);
        auto isNewNG = isNewNodeGroup(transaction, nodeGroupIdx);
        updateCSRHeader(transaction, nodeGroupIdx, isNewNG, persistentState, localState);
        KU_ASSERT((region.level >= packedCSRInfo.calibratorTreeHeight && regions.size() == 1) ||
                  region.level < packedCSRInfo.calibratorTreeHeight);
        updateRegion(transaction, nodeGroupIdx, isNewNG, persistentState, localState);
    }
}

LocalRelNG* RelTableData::getLocalNodeGroup(Transaction* transaction,
    node_group_idx_t nodeGroupIdx) {
    auto localTable = transaction->getLocalStorage()->getLocalTable(tableID);
    if (!localTable) {
        return nullptr;
    }
    auto localRelTable = ku_dynamic_cast<LocalTable*, LocalRelTable*>(localTable);
    auto localTableData = localRelTable->getTableData(direction);
    LocalRelNG* localNodeGroup = nullptr;
    if (localTableData->nodeGroups.contains(nodeGroupIdx)) {
        localNodeGroup = ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(
            localTableData->nodeGroups.at(nodeGroupIdx).get());
    }
    return localNodeGroup;
}

void RelTableData::prepareCommit() {
    csrHeaderColumns.offset->prepareCommit();
    csrHeaderColumns.length->prepareCommit();
    TableData::prepareCommit();
}

void RelTableData::checkpointInMemory() {
    csrHeaderColumns.offset->checkpointInMemory();
    csrHeaderColumns.length->checkpointInMemory();
    TableData::checkpointInMemory();
}

void RelTableData::rollbackInMemory() {
    csrHeaderColumns.offset->rollbackInMemory();
    csrHeaderColumns.length->rollbackInMemory();
    TableData::rollbackInMemory();
}

} // namespace storage
} // namespace kuzu

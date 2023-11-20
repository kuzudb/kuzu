#include "storage/store/rel_table_data.h"

#include "common/assert.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/local_storage/local_table.h"
#include "storage/stats/rels_store_statistics.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelDataReadState::RelDataReadState(ColumnDataFormat dataFormat)
    : dataFormat{dataFormat}, startNodeOffset{0}, numNodes{0}, currentNodeOffset{0},
      posInCurrentCSR{0}, readFromLocalStorage{false}, localNodeGroup{nullptr} {
    csrListEntries.resize(StorageConstants::NODE_GROUP_SIZE, {0, 0});
    csrOffsetChunk =
        ColumnChunkFactory::createColumnChunk(LogicalType::INT64(), false /* enableCompression */);
}

bool RelDataReadState::hasMoreToReadFromLocalStorage() {
    KU_ASSERT(localNodeGroup);
    return posInCurrentCSR < localNodeGroup->getRelNGInfo()->getNumInsertedTuples(
                                 currentNodeOffset - startNodeOffset);
}

bool RelDataReadState::trySwitchToLocalStorage() {
    if (localNodeGroup && localNodeGroup->getRelNGInfo()->getNumInsertedTuples(
                              currentNodeOffset - startNodeOffset) > 0) {
        readFromLocalStorage = true;
        posInCurrentCSR = 0;
        return true;
    }
    return false;
}

bool RelDataReadState::hasMoreToRead(transaction::Transaction* transaction) {
    if (dataFormat == ColumnDataFormat::REGULAR) {
        return false;
    }
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
    auto csrOffsets = (offset_t*)csrOffsetChunk->getData();
    csrListEntries[0].offset = 0;
    csrListEntries[0].size = csrOffsets[0];
    for (auto i = 1; i < numNodes; i++) {
        csrListEntries[i].offset = csrOffsets[i - 1];
        csrListEntries[i].size = csrOffsets[i] - csrOffsets[i - 1];
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

RelTableData::RelTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, RelTableSchema* tableSchema,
    RelsStoreStats* relsStoreStats, RelDataDirection direction, bool enableCompression)
    : TableData{dataFH, metadataFH, tableSchema->tableID, bufferManager, wal, enableCompression,
          getDataFormatFromSchema(tableSchema, direction)},
      direction{direction}, csrOffsetColumn{nullptr} {
    if (dataFormat == ColumnDataFormat::CSR) {
        auto csrOffsetMetadataDAHInfo = relsStoreStats->getCSROffsetMetadataDAHInfo(
            &DUMMY_WRITE_TRANSACTION, tableID, direction);
        // No NULL values is allowed for the csr offset column.
        csrOffsetColumn = std::make_unique<Column>(LogicalType::INT64(), *csrOffsetMetadataDAHInfo,
            dataFH, metadataFH, bufferManager, wal, &DUMMY_READ_TRANSACTION,
            RWPropertyStats::empty(), enableCompression, false /* requireNUllColumn */);
    }
    auto adjMetadataDAHInfo =
        relsStoreStats->getAdjMetadataDAHInfo(&DUMMY_WRITE_TRANSACTION, tableID, direction);
    adjColumn = ColumnFactory::createColumn(LogicalType::INTERNAL_ID(), *adjMetadataDAHInfo, dataFH,
        metadataFH, bufferManager, wal, &DUMMY_READ_TRANSACTION, RWPropertyStats::empty(),
        enableCompression);
    auto properties = tableSchema->getProperties();
    columns.reserve(properties.size());
    for (auto i = 0u; i < properties.size(); i++) {
        auto property = tableSchema->getProperties()[i];
        auto metadataDAHInfo = relsStoreStats->getPropertyMetadataDAHInfo(
            &DUMMY_WRITE_TRANSACTION, tableID, i, direction);
        columns.push_back(ColumnFactory::createColumn(properties[i]->getDataType()->copy(),
            *metadataDAHInfo, dataFH, metadataFH, bufferManager, wal, &DUMMY_READ_TRANSACTION,
            RWPropertyStats(relsStoreStats, tableID, property->getPropertyID()),
            enableCompression));
    }
    // Set common tableID for adjColumn and relIDColumn.
    dynamic_cast<InternalIDColumn*>(adjColumn.get())
        ->setCommonTableID(tableSchema->getNbrTableID(direction));
    dynamic_cast<InternalIDColumn*>(columns[REL_ID_COLUMN_ID].get())->setCommonTableID(tableID);
}

void RelTableData::initializeReadState(Transaction* transaction,
    std::vector<common::column_id_t> columnIDs, ValueVector* inNodeIDVector,
    RelDataReadState* readState) {
    readState->direction = direction;
    readState->columnIDs = std::move(columnIDs);
    auto nodeOffset =
        inNodeIDVector->readNodeOffset(inNodeIDVector->state->selVector->selectedPositions[0]);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (dataFormat == ColumnDataFormat::CSR) {
        auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        // Reset to read from beginning for the csr of the new node offset.
        readState->posInCurrentCSR = 0;
        if (readState->isOutOfRange(nodeOffset)) {
            // Scan csr offsets and populate csr list entries for the new node group.
            readState->startNodeOffset = startNodeOffset;
            csrOffsetColumn->scan(transaction, nodeGroupIdx, readState->csrOffsetChunk.get());
            readState->numNodes = readState->csrOffsetChunk->getNumValues();
            readState->populateCSRListEntries();
            if (transaction->isWriteTransaction()) {
                readState->localNodeGroup = getLocalNodeGroup(transaction, nodeGroupIdx);
            }
        }
        if (nodeOffset != readState->currentNodeOffset) {
            readState->currentNodeOffset = nodeOffset;
        }
    }
    // Reset to read from persistent storage.
    readState->readFromLocalStorage = false;
}

LocalRelNG* RelTableData::getLocalNodeGroup(
    transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx) {
    auto localTableData = transaction->getLocalStorage()->getLocalTableData(
        tableID, getDataIdxFromDirection(direction));
    LocalRelNG* localNodeGroup = nullptr;
    if (localTableData) {
        auto localRelTableData =
            ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(localTableData);
        if (localRelTableData->nodeGroups.contains(nodeGroupIdx)) {
            localNodeGroup = ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(
                localRelTableData->nodeGroups.at(nodeGroupIdx).get());
        }
    }
    return localNodeGroup;
}

void RelTableData::scanRegularColumns(Transaction* transaction, RelDataReadState& readState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    adjColumn->scan(transaction, inNodeIDVector, outputVectors[0]);
    if (transaction->isReadOnly() && !ValueVector::discardNull(*outputVectors[0])) {
        return;
    }
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        auto columnID = readState.columnIDs[i];
        auto outputVectorId = i + 1; // Skip output from adj column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        columns[readState.columnIDs[i]]->scan(
            transaction, inNodeIDVector, outputVectors[outputVectorId]);
    }
    if (transaction->isWriteTransaction()) {
        auto nodeOffset = inNodeIDVector->readNodeOffset(0);
        auto localNodeGroup =
            getLocalNodeGroup(transaction, StorageUtils::getNodeGroupIdx(nodeOffset));
        if (localNodeGroup) {
            localNodeGroup->applyLocalChangesForRegularColumns(
                inNodeIDVector, readState.columnIDs, outputVectors);
        }
        ValueVector::discardNull(*outputVectors[0]);
    }
}

void RelTableData::scanCSRColumns(Transaction* transaction, RelDataReadState& readState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(dataFormat == ColumnDataFormat::CSR);
    if (readState.readFromLocalStorage) {
        auto offsetInChunk = readState.currentNodeOffset - readState.startNodeOffset;
        auto numValuesRead = readState.localNodeGroup->scanCSR(
            offsetInChunk, readState.posInCurrentCSR, readState.columnIDs, outputVectors);
        readState.posInCurrentCSR += numValuesRead;
        return;
    }
    auto [startOffset, endOffset] = readState.getStartAndEndOffset();
    auto numRowsToRead = endOffset - startOffset;
    outputVectors[0]->state->selVector->resetSelectorToUnselectedWithSize(numRowsToRead);
    outputVectors[0]->state->setOriginalSize(numRowsToRead);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(readState.currentNodeOffset);
    adjColumn->scan(transaction, nodeGroupIdx, startOffset, endOffset, outputVectors[0],
        0 /* offsetInVector */);
    auto relIDVectorIdx = INVALID_VECTOR_IDX;
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        auto columnID = readState.columnIDs[i];
        auto outputVectorId = i + 1; // Skip output from adj column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        if (columnID == REL_ID_COLUMN_ID) {
            relIDVectorIdx = outputVectorId;
        }
        columns[readState.columnIDs[i]]->scan(transaction, nodeGroupIdx, startOffset, endOffset,
            outputVectors[outputVectorId], 0 /* offsetInVector */);
    }
    if (transaction->isWriteTransaction() && readState.localNodeGroup) {
        auto nodeOffset =
            inNodeIDVector->readNodeOffset(inNodeIDVector->state->selVector->selectedPositions[0]);
        KU_ASSERT(relIDVectorIdx != INVALID_VECTOR_IDX);
        auto relIDVector = outputVectors[relIDVectorIdx];
        readState.localNodeGroup->applyLocalChangesForCSRColumns(
            nodeOffset - readState.startNodeOffset, readState.columnIDs, relIDVector,
            outputVectors);
    }
}

void RelTableData::lookup(Transaction* transaction, TableReadState& readState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(dataFormat == ColumnDataFormat::REGULAR);
    // Note: The scan operator should guarantee that the first property in the output is adj column.
    adjColumn->lookup(transaction, inNodeIDVector, outputVectors[0]);
    if (transaction->isReadOnly() && !ValueVector::discardNull(*outputVectors[0])) {
        return;
    }
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        auto columnID = readState.columnIDs[i];
        auto outputVectorId = i + 1; // Skip output from adj column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        columns[readState.columnIDs[i]]->lookup(
            transaction, inNodeIDVector, outputVectors[outputVectorId]);
    }
    if (transaction->isWriteTransaction()) {
        for (auto pos = 0u; pos < inNodeIDVector->state->selVector->selectedSize; pos++) {
            auto selPos = inNodeIDVector->state->selVector->selectedPositions[pos];
            auto nodeOffset = inNodeIDVector->readNodeOffset(selPos);
            auto [nodeGroupIdx, offsetInChunk] =
                StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
            auto localNodeGroup = getLocalNodeGroup(transaction, nodeGroupIdx);
            if (localNodeGroup) {
                localNodeGroup->applyLocalChangesForRegularColumns(
                    offsetInChunk, readState.columnIDs, outputVectors, selPos);
            }
        }
        ValueVector::discardNull(*outputVectors[0]);
    }
}

void RelTableData::insert(transaction::Transaction* transaction, ValueVector* srcNodeIDVector,
    ValueVector* dstNodeIDVector, const std::vector<ValueVector*>& propertyVectors) {
    auto localTableData = ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(
        transaction->getLocalStorage()->getOrCreateLocalTableData(
            tableID, columns, TableType::REL, dataFormat, getDataIdxFromDirection(direction)));
    auto checkPersistentStorage =
        localTableData->insert(srcNodeIDVector, dstNodeIDVector, propertyVectors);
    auto [nodeGroupIdx, offset] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(
        srcNodeIDVector->getValue<nodeID_t>(srcNodeIDVector->state->selVector->selectedPositions[0])
            .offset);
    if (checkPersistentStorage && !adjColumn->isNull(transaction, nodeGroupIdx, offset)) {
        throw RuntimeException{"Many-one, one-one relationship violated."};
    }
}

void RelTableData::update(transaction::Transaction* transaction, column_id_t columnID,
    ValueVector* srcNodeIDVector, ValueVector* relIDVector, ValueVector* propertyVector) {
    KU_ASSERT(columnID < columns.size() && columnID != REL_ID_COLUMN_ID);
    auto localTableData = ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(
        transaction->getLocalStorage()->getOrCreateLocalTableData(
            tableID, columns, TableType::REL, dataFormat, getDataIdxFromDirection(direction)));
    localTableData->update(srcNodeIDVector, relIDVector, columnID, propertyVector);
}

bool RelTableData::delete_(transaction::Transaction* transaction, ValueVector* srcNodeIDVector,
    ValueVector* dstNodeIDVector, ValueVector* relIDVector) {
    auto localTableData = ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(
        transaction->getLocalStorage()->getOrCreateLocalTableData(
            tableID, columns, TableType::REL, dataFormat, getDataIdxFromDirection(direction)));
    return localTableData->delete_(srcNodeIDVector, dstNodeIDVector, relIDVector);
}

bool RelTableData::checkIfNodeHasRels(Transaction* transaction, ValueVector* srcNodeIDVector) {
    auto nodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = srcNodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    if (dataFormat == ColumnDataFormat::CSR) {
        auto readState = csrOffsetColumn->getReadState(transaction->getType(), nodeGroupIdx);
        std::vector<offset_t> offsets;
        offsets.resize(2);
        csrOffsetColumn->scan(transaction, readState, offsetInChunk == 0 ? 0 : offsetInChunk - 1,
            offsetInChunk + 1, reinterpret_cast<uint8_t*>(&offsets[0]));
        int64_t csrSize =
            offsetInChunk == 0 ? offsets[0] : (int64_t)(offsets[1]) - (int64_t)(offsets[0]);
        return csrSize > 0;
    } else {
        return !adjColumn->isNull(transaction, nodeGroupIdx, offsetInChunk);
    }
}

void RelTableData::append(NodeGroup* nodeGroup) {
    if (dataFormat == ColumnDataFormat::CSR) {
        auto csrNodeGroup = static_cast<CSRNodeGroup*>(nodeGroup);
        csrOffsetColumn->append(csrNodeGroup->getCSROffsetChunk(), nodeGroup->getNodeGroupIdx());
    }
    adjColumn->append(nodeGroup->getColumnChunk(0), nodeGroup->getNodeGroupIdx());
    for (auto columnID = 0; columnID < columns.size(); columnID++) {
        columns[columnID]->append(
            nodeGroup->getColumnChunk(columnID + 1), nodeGroup->getNodeGroupIdx());
    }
}

void RelTableData::resizeColumns(node_group_idx_t nodeGroupIdx) {
    if (dataFormat == ColumnDataFormat::CSR) {
        // TODO(Guodong): This is a special logic for regular columns only, and should be organized
        // in a better way.
        return;
    }
    auto currentNumNodeGroups = adjColumn->getNumNodeGroups(&DUMMY_WRITE_TRANSACTION);
    if (nodeGroupIdx < currentNumNodeGroups) {
        return;
    }
    std::vector<std::unique_ptr<LogicalType>> columnTypes;
    columnTypes.reserve(columns.size() + 1);
    columnTypes.push_back(LogicalType::INTERNAL_ID());
    for (auto& column : columns) {
        columnTypes.push_back(column->getDataType()->copy());
    }
    auto nodeGroup = std::make_unique<NodeGroup>(
        columnTypes, enableCompression, StorageConstants::NODE_GROUP_SIZE);
    nodeGroup->setAllNull();
    nodeGroup->setNumValues(0);
    for (auto i = currentNumNodeGroups; i <= nodeGroupIdx; i++) {
        nodeGroup->finalize(i);
        append(nodeGroup.get());
    }
}

void RelTableData::prepareLocalTableToCommit(
    Transaction* transaction, LocalTableData* localTableData) {
    auto localRelTableData = ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(localTableData);
    if (dataFormat == ColumnDataFormat::REGULAR) {
        prepareCommitForRegularColumns(transaction, localRelTableData);
    } else {
        prepareCommitForCSRColumns(transaction, localRelTableData);
    }
}

void RelTableData::prepareCommitForRegularColumns(
    Transaction* transaction, LocalRelTableData* localTableData) {
    for (auto& [nodeGroupIdx, nodeGroup] : localTableData->nodeGroups) {
        auto relNG = ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(nodeGroup.get());
        KU_ASSERT(relNG);
        auto relNodeGroupInfo =
            ku_dynamic_cast<RelNGInfo*, RegularRelNGInfo*>(relNG->getRelNGInfo());
        adjColumn->prepareCommitForChunk(transaction, nodeGroupIdx, relNG->getAdjChunk(),
            relNodeGroupInfo->adjInsertInfo, {} /* updateInfo */, relNodeGroupInfo->deleteInfo);
        for (auto columnID = 0u; columnID < columns.size(); columnID++) {
            columns[columnID]->prepareCommitForChunk(transaction, nodeGroupIdx,
                relNG->getPropertyChunk(columnID), relNodeGroupInfo->insertInfoPerChunk[columnID],
                relNodeGroupInfo->updateInfoPerChunk[columnID], relNodeGroupInfo->deleteInfo);
        }
    }
}

void RelTableData::prepareCommitForCSRColumns(
    Transaction* transaction, LocalRelTableData* localTableData) {
    for (auto& [nodeGroupIdx, nodeGroup] : localTableData->nodeGroups) {
        auto relNG = ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(nodeGroup.get());
        KU_ASSERT(relNG);
        auto relNodeGroupInfo = ku_dynamic_cast<RelNGInfo*, CSRRelNGInfo*>(relNG->getRelNGInfo());
        // First, scan the whole csr offset column chunk, whose size is NODE_GROUP_SIZE.
        auto csrOffsetChunk = ColumnChunkFactory::createColumnChunk(
            LogicalType::INT64(), false /* enableCompression */);
        csrOffsetColumn->scan(transaction, nodeGroupIdx, csrOffsetChunk.get());
        // Next, scan the whole relID column chunk.
        // TODO: We can only scan partial relID column chunk based on csr offset of the max
        // nodeOffset.
        auto numRels = csrOffsetChunk->getNumValues() == 0 ?
                           0 :
                           csrOffsetChunk->getValue<offset_t>(csrOffsetChunk->getNumValues() - 1);
        // NOTE: There is an implicit trick happening. Due to the mismatch of storage type and
        // in-memory representation of INTERNAL_ID, we only store offset as INT64 on disk. Here
        // we directly read relID's offset part from disk into an INT64 column chunk.
        // TODO: The term of relID and relOffset is mixed. We should use relOffset instead.
        auto relIDChunk = ColumnChunkFactory::createColumnChunk(
            LogicalType::INT64(), false /* enableCompression */, numRels);
        columns[REL_ID_COLUMN_ID]->scan(transaction, nodeGroupIdx, relIDChunk.get());
        if (relNodeGroupInfo->deleteInfo.empty() && relNodeGroupInfo->adjInsertInfo.empty()) {
            // We don't need to update the csr offset column if there is no deletion or insertion.
            // Thus, we can fall back to directly update the adj column and property columns based
            // on csr offsets.
            prepareCommitCSRNGWithoutSliding(transaction, nodeGroupIdx, relNodeGroupInfo,
                csrOffsetChunk.get(), relIDChunk.get(), relNG);
        } else {
            // We need to update the csr offset column. Thus, we cannot simply fall back to directly
            // update the adj column and property columns based on csr offsets.
            prepareCommitCSRNGWithSliding(transaction, nodeGroupIdx, relNodeGroupInfo,
                csrOffsetChunk.get(), relIDChunk.get(), relNG);
        }
    }
}

static std::pair<offset_t, offset_t> getCSRStartAndEndOffset(
    offset_t offsetInNodeGroup, offset_t* csrOffsets) {
    return offsetInNodeGroup == 0 ?
               std::make_pair((offset_t)0, csrOffsets[offsetInNodeGroup]) :
               std::make_pair(csrOffsets[offsetInNodeGroup - 1], csrOffsets[offsetInNodeGroup]);
}

static uint64_t findPosOfRelIDFromArray(
    offset_t* relIDArray, uint64_t startPos, uint64_t endPos, offset_t relOffset) {
    for (auto i = startPos; i < endPos; i++) {
        if (relIDArray[i] == relOffset) {
            return i;
        }
    }
    return UINT64_MAX;
}

void RelTableData::prepareCommitCSRNGWithoutSliding(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, CSRRelNGInfo* relNodeGroupInfo, ColumnChunk* csrOffsetChunk,
    ColumnChunk* relIDChunk, LocalRelNG* localNodeGroup) {
    // We can figure out the actual csr offset of each value to be updated based on csr and relID
    // chunks.
    auto csrOffsets = (offset_t*)csrOffsetChunk->getData();
    auto relIDs = (offset_t*)relIDChunk->getData();
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        std::map<offset_t, row_idx_t> csrOffsetToRowIdx;
        auto& updateInfo = relNodeGroupInfo->updateInfoPerChunk[columnID];
        for (auto& [offsetInChunk, relIDToRowIdx] : updateInfo) {
            auto [startCSROffset, endCSROffset] =
                getCSRStartAndEndOffset(offsetInChunk, csrOffsets);
            for (auto [relID, rowIdx] : relIDToRowIdx) {
                auto csrOffset =
                    findPosOfRelIDFromArray(relIDs, startCSROffset, endCSROffset, relID);
                KU_ASSERT(csrOffset != UINT64_MAX);
                csrOffsetToRowIdx[csrOffset] = rowIdx;
            }
        }
        if (!csrOffsetToRowIdx.empty()) {
            auto localChunk = localNodeGroup->getLocalColumnChunk(columnID);
            columns[columnID]->prepareCommitForChunk(transaction, nodeGroupIdx, localChunk,
                {} /* insertInfo */, csrOffsetToRowIdx, {} /* deleteInfo */);
        }
    }
}

void RelTableData::prepareCommitCSRNGWithSliding(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, CSRRelNGInfo* relNodeGroupInfo, ColumnChunk* csrOffsetChunk,
    ColumnChunk* relIDChunk, LocalRelNG* localNodeGroup) {
    // We need to update the csr offset column. Thus, we cannot simply fall back to directly update
    // the adj column and property columns based on csr offsets. Instead, we need to for loop each
    // node in the node group, slide accordingly, and update the csr offset column, adj column and
    // property columns.
    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    // Slide column by column.
    // First we slide the csr offset column chunk, and keep the slided csr offset column chunk in
    // memory, so it can be used for assertion checking later.
    auto slidedCSROffsetChunk = slideCSROffsetChunk(csrOffsetChunk, relNodeGroupInfo);
    csrOffsetColumn->append(slidedCSROffsetChunk.get(), nodeGroupIdx);
    // Then we slide the adj column chunk, rel id column chunk, and all property column chunks.
    auto slidedAdjColumnChunk =
        slideCSRColumnChunk(transaction, csrOffsetChunk, slidedCSROffsetChunk.get(), relIDChunk,
            relNodeGroupInfo->adjInsertInfo, {} /* updateInfo */, relNodeGroupInfo->deleteInfo,
            nodeGroupStartOffset, adjColumn.get(), localNodeGroup->getAdjChunk());
    adjColumn->append(slidedAdjColumnChunk.get(), nodeGroupIdx);
    slidedAdjColumnChunk.reset();
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        auto slidedColumnChunk = slideCSRColumnChunk(transaction, csrOffsetChunk,
            slidedCSROffsetChunk.get(), relIDChunk, relNodeGroupInfo->insertInfoPerChunk[columnID],
            relNodeGroupInfo->updateInfoPerChunk[columnID], relNodeGroupInfo->deleteInfo,
            nodeGroupStartOffset, columns[columnID].get(),
            localNodeGroup->getLocalColumnChunk(columnID));
        columns[columnID]->append(slidedColumnChunk.get(), nodeGroupIdx);
        slidedColumnChunk.reset();
    }
}

std::unique_ptr<ColumnChunk> RelTableData::slideCSROffsetChunk(
    ColumnChunk* csrOffsetChunk, CSRRelNGInfo* relNodeGroupInfo) {
    auto csrOffsets = (offset_t*)csrOffsetChunk->getData();
    auto slidedCSRChunk =
        ColumnChunkFactory::createColumnChunk(LogicalType::INT64(), enableCompression);
    int64_t currentCSROffset = 0;
    auto currentNumSrcNodesInNG = csrOffsetChunk->getNumValues();
    auto newNumSrcNodesInNG = currentNumSrcNodesInNG;
    for (auto offsetInNG = 0; offsetInNG < currentNumSrcNodesInNG; offsetInNG++) {
        int64_t numRowsInCSR = offsetInNG == 0 ?
                                   csrOffsets[offsetInNG] :
                                   csrOffsets[offsetInNG] - csrOffsets[offsetInNG - 1];
        KU_ASSERT(numRowsInCSR >= 0);
        int64_t numDeletions = relNodeGroupInfo->deleteInfo.contains(offsetInNG) ?
                                   relNodeGroupInfo->deleteInfo[offsetInNG].size() :
                                   0;
        int64_t numInsertions = relNodeGroupInfo->adjInsertInfo.contains(offsetInNG) ?
                                    relNodeGroupInfo->adjInsertInfo[offsetInNG].size() :
                                    0;
        int64_t numRowsAfterSlide = numRowsInCSR + numInsertions - numDeletions;
        KU_ASSERT(numRowsAfterSlide >= 0);
        currentCSROffset += numRowsAfterSlide;
        slidedCSRChunk->setValue<offset_t>(currentCSROffset, offsetInNG);
    }
    for (auto offsetInNG = currentNumSrcNodesInNG; offsetInNG < StorageConstants::NODE_GROUP_SIZE;
         offsetInNG++) {
        // Assert that should only have insertions for these nodes.
        KU_ASSERT(!relNodeGroupInfo->deleteInfo.contains(offsetInNG));
        for (auto columnID = 0u; columnID < columns.size(); columnID++) {
            KU_ASSERT(!relNodeGroupInfo->updateInfoPerChunk[columnID].contains(offsetInNG));
        }
        auto numRowsInCSR = 0;
        if (relNodeGroupInfo->adjInsertInfo.contains(offsetInNG)) {
            // This node is inserted now. Update csr offset accordingly.
            numRowsInCSR += relNodeGroupInfo->adjInsertInfo.at(offsetInNG).size();
            newNumSrcNodesInNG = offsetInNG + 1;
        }
        currentCSROffset += numRowsInCSR;
        slidedCSRChunk->setValue<offset_t>(currentCSROffset, offsetInNG);
    }
    slidedCSRChunk->setNumValues(newNumSrcNodesInNG);
    return slidedCSRChunk;
}

std::unique_ptr<ColumnChunk> RelTableData::slideCSRColumnChunk(Transaction* transaction,
    ColumnChunk* csrOffsetChunk, ColumnChunk* slidedCSROffsetChunkForCheck, ColumnChunk* relIDChunk,
    const offset_to_offset_to_row_idx_t& insertInfo,
    const offset_to_offset_to_row_idx_t& updateInfo, const offset_to_offset_set_t& deleteInfo,
    node_group_idx_t nodeGroupIdx, Column* column, LocalVectorCollection* localChunk) {
    auto oldCapacity = csrOffsetChunk->getNumValues() == 0 ?
                           0 :
                           csrOffsetChunk->getValue<offset_t>(csrOffsetChunk->getNumValues() - 1);
    auto newCapacity = slidedCSROffsetChunkForCheck->getNumValues() == 0 ?
                           0 :
                           slidedCSROffsetChunkForCheck->getValue<offset_t>(
                               slidedCSROffsetChunkForCheck->getNumValues() - 1);
    // TODO: No need to allocate the new column chunk if this is relID.
    auto columnChunk = ColumnChunkFactory::createColumnChunk(
        column->getDataType()->copy(), enableCompression, oldCapacity);
    column->scan(transaction, nodeGroupIdx, columnChunk.get());
    auto newColumnChunk = ColumnChunkFactory::createColumnChunk(
        column->getDataType()->copy(), enableCompression, newCapacity);
    auto currentNumSrcNodesInNG = csrOffsetChunk->getNumValues();
    auto csrOffsets = (offset_t*)csrOffsetChunk->getData();
    auto relIDs = (offset_t*)relIDChunk->getData();
    for (auto offsetInNG = 0; offsetInNG < currentNumSrcNodesInNG; offsetInNG++) {
        auto [startCSROffset, endCSROffset] = getCSRStartAndEndOffset(offsetInNG, csrOffsets);
        auto hasDeletions = deleteInfo.contains(offsetInNG);
        auto hasUpdates = updateInfo.contains(offsetInNG);
        auto hasInsertions = insertInfo.contains(offsetInNG);
        if (!hasDeletions && !hasUpdates && !hasInsertions) {
            // Append the whole csr.
            newColumnChunk->append(
                columnChunk.get(), startCSROffset, endCSROffset - startCSROffset);
            continue;
        }
        for (auto csrOffset = startCSROffset; csrOffset < endCSROffset; csrOffset++) {
            auto relID = relIDs[csrOffset];
            if (hasDeletions && deleteInfo.at(offsetInNG).contains(relID)) {
                // This rel is deleted now. Skip.
            } else if (hasUpdates && updateInfo.at(offsetInNG).contains(relID)) {
                // This rel is updated. Append from local data to the column chunk.
                auto rowIdx = updateInfo.at(offsetInNG).at(relID);
                auto localVector = localChunk->getLocalVector(rowIdx)->getVector();
                auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
                localVector->state->selVector->selectedPositions[0] = offsetInVector;
                newColumnChunk->append(localVector);
            } else {
                // This rel is not updated. Append to the column chunk.
                newColumnChunk->append(columnChunk.get(), csrOffset, 1 /* numValuesToAppend */);
            }
        }
        if (hasInsertions) {
            // Append the newly inserted rels.
            for (auto [_, rowIdx] : insertInfo.at(offsetInNG)) {
                auto localVector = localChunk->getLocalVector(rowIdx)->getVector();
                auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
                localVector->state->selVector->selectedPositions[0] = offsetInVector;
                newColumnChunk->append(localVector);
            }
        }
    }
    if (!insertInfo.empty()) {
        // Append the newly inserted rels.
        for (auto offsetInNG = currentNumSrcNodesInNG;
             offsetInNG < StorageConstants::NODE_GROUP_SIZE; offsetInNG++) {
            if (insertInfo.contains(offsetInNG)) {
                for (auto [relID, rowIdx] : insertInfo.at(offsetInNG)) {
                    auto localVector = localChunk->getLocalVector(rowIdx)->getVector();
                    auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
                    localVector->state->selVector->selectedPositions[0] = offsetInVector;
                    newColumnChunk->append(localVector);
                }
            }
        }
    }
    KU_ASSERT(newColumnChunk->getNumValues() == newCapacity);
    return newColumnChunk;
}

void RelTableData::checkpointInMemory() {
    if (csrOffsetColumn) {
        csrOffsetColumn->checkpointInMemory();
    }
    adjColumn->checkpointInMemory();
    TableData::checkpointInMemory();
}

void RelTableData::rollbackInMemory() {
    if (csrOffsetColumn) {
        csrOffsetColumn->rollbackInMemory();
    }
    adjColumn->rollbackInMemory();
    TableData::rollbackInMemory();
}

} // namespace storage
} // namespace kuzu

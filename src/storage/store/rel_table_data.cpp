#include "storage/store/rel_table_data.h"

#include "common/assert.h"
#include "common/exception/not_implemented.h"
#include "storage/stats/rels_store_statistics.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelDataReadState::RelDataReadState(ColumnDataFormat dataFormat)
    : dataFormat{dataFormat}, startNodeOffsetInState{0}, numNodesInState{0},
      currentCSRNodeOffset{0}, posInCurrentCSR{0} {
    csrListEntries.resize(StorageConstants::NODE_GROUP_SIZE, {0, 0});
    csrOffsetChunk = ColumnChunkFactory::createColumnChunk(
        LogicalType{LogicalTypeID::INT64}, false /* enableCompression */);
}

void RelDataReadState::populateCSRListEntries() {
    auto csrOffsets = (offset_t*)csrOffsetChunk->getData();
    csrListEntries[0].offset = 0;
    csrListEntries[0].size = csrOffsets[0];
    for (auto i = 1; i < numNodesInState; i++) {
        csrListEntries[i].offset = csrOffsets[i - 1];
        csrListEntries[i].size = csrOffsets[i] - csrOffsets[i - 1];
    }
}

std::pair<offset_t, offset_t> RelDataReadState::getStartAndEndOffset() {
    auto currCSRListEntry = csrListEntries[currentCSRNodeOffset - startNodeOffsetInState];
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
            Transaction::getDummyWriteTrx().get(), tableID, direction);
        // No NULL values is allowed for the csr offset column.
        csrOffsetColumn =
            std::make_unique<Column>(LogicalType{LogicalTypeID::INT64}, *csrOffsetMetadataDAHInfo,
                dataFH, metadataFH, bufferManager, wal, Transaction::getDummyReadOnlyTrx().get(),
                RWPropertyStats(relsStoreStats, tableID, INVALID_PROPERTY_ID), enableCompression,
                false /* requireNUllColumn */);
    }
    auto adjMetadataDAHInfo = relsStoreStats->getAdjMetadataDAHInfo(
        Transaction::getDummyWriteTrx().get(), tableID, direction);
    adjColumn =
        ColumnFactory::createColumn(LogicalType{LogicalTypeID::INTERNAL_ID}, *adjMetadataDAHInfo,
            dataFH, metadataFH, bufferManager, wal, Transaction::getDummyReadOnlyTrx().get(),
            RWPropertyStats(relsStoreStats, tableID, INVALID_PROPERTY_ID), enableCompression);
    auto properties = tableSchema->getProperties();
    columns.reserve(properties.size());
    for (auto i = 0u; i < properties.size(); i++) {
        auto property = tableSchema->getProperties()[i];
        auto metadataDAHInfo = relsStoreStats->getPropertyMetadataDAHInfo(
            Transaction::getDummyWriteTrx().get(), tableID, i, direction);
        columns.push_back(
            ColumnFactory::createColumn(*properties[i]->getDataType(), *metadataDAHInfo, dataFH,
                metadataFH, bufferManager, wal, Transaction::getDummyReadOnlyTrx().get(),
                RWPropertyStats(relsStoreStats, tableID, property->getPropertyID()),
                enableCompression));
    }
    // Set common tableID for adjColumn and relIDColumn.
    dynamic_cast<InternalIDColumn*>(adjColumn.get())
        ->setCommonTableID(tableSchema->getNbrTableID(direction));
    dynamic_cast<InternalIDColumn*>(columns[REL_ID_COLUMN_ID].get())->setCommonTableID(tableID);
}

void RelTableData::initializeReadState(Transaction* transaction, std::vector<column_id_t> columnIDs,
    ValueVector* inNodeIDVector, TableReadState* readState) {
    auto relReadState = ku_dynamic_cast<TableReadState*, RelDataReadState*>(readState);
    relReadState->direction = direction;
    relReadState->columnIDs = std::move(columnIDs);
    if (dataFormat == ColumnDataFormat::REGULAR) {
        return;
    }
    auto nodeOffset =
        inNodeIDVector->readNodeOffset(inNodeIDVector->state->selVector->selectedPositions[0]);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    relReadState->posInCurrentCSR = 0;
    if (relReadState->isOutOfRange(nodeOffset)) {
        // Scan csr offsets and populate csr list entries for the new node group.
        relReadState->startNodeOffsetInState = startNodeOffset;
        csrOffsetColumn->scan(transaction, nodeGroupIdx, relReadState->csrOffsetChunk.get());
        relReadState->numNodesInState = relReadState->csrOffsetChunk->getNumValues();
        relReadState->populateCSRListEntries();
    }
    if (nodeOffset != relReadState->currentCSRNodeOffset) {
        relReadState->currentCSRNodeOffset = nodeOffset;
    }
}

void RelTableData::scanRegularColumns(Transaction* transaction, RelDataReadState& readState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    // TODO: If write transaction, apply local changes.
    adjColumn->scan(transaction, inNodeIDVector, outputVectors[0]);
    if (!ValueVector::discardNull(*outputVectors[0])) {
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
}

void RelTableData::scanCSRColumns(Transaction* transaction, RelDataReadState& readState,
    ValueVector* /*inNodeIDVector*/, const std::vector<ValueVector*>& outputVectors) {
    // TODO: If write transaction, apply local changes.
    KU_ASSERT(dataFormat == ColumnDataFormat::CSR);
    auto [startOffset, endOffset] = readState.getStartAndEndOffset();
    auto numRowsToRead = endOffset - startOffset;
    outputVectors[0]->state->selVector->resetSelectorToUnselectedWithSize(numRowsToRead);
    outputVectors[0]->state->setOriginalSize(numRowsToRead);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(readState.currentCSRNodeOffset);
    adjColumn->scan(transaction, nodeGroupIdx, startOffset, endOffset, outputVectors[0],
        0 /* offsetInVector */);
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        auto columnID = readState.columnIDs[i];
        auto outputVectorId = i + 1; // Skip output from adj column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        columns[readState.columnIDs[i]]->scan(transaction, nodeGroupIdx, startOffset, endOffset,
            outputVectors[outputVectorId], 0 /* offsetInVector */);
    }
}

void RelTableData::lookup(Transaction* transaction, TableReadState& readState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    // TODO: If write transaction, apply local changes.
    // Note: The scan operator should guarantee that the first property in the output is adj column.
    adjColumn->lookup(transaction, inNodeIDVector, outputVectors[0]);
    if (!ValueVector::discardNull(*outputVectors[0])) {
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
}

void RelTableData::insert(transaction::Transaction* transaction, ValueVector* srcNodeIDVector,
    ValueVector* dstNodeIDVector, const std::vector<ValueVector*>& propertyVectors) {
    auto localTableData = transaction->getLocalStorage()->getOrCreateLocalRelTableData(
        tableID, direction, dataFormat, columns);
    localTableData->insert(srcNodeIDVector, dstNodeIDVector, propertyVectors);
}

void RelTableData::update(transaction::Transaction* transaction, column_id_t columnID,
    ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector, ValueVector* relIDVector,
    ValueVector* propertyVector) {
    auto localTableData = transaction->getLocalStorage()->getOrCreateLocalRelTableData(
        tableID, direction, dataFormat, columns);
    localTableData->update(srcNodeIDVector, dstNodeIDVector, relIDVector, columnID, propertyVector);
}

void RelTableData::delete_(transaction::Transaction* transaction, ValueVector* srcNodeIDVector,
    ValueVector* dstNodeIDVector, ValueVector* relIDVector) {
    auto localTableData = transaction->getLocalStorage()->getOrCreateLocalRelTableData(
        tableID, direction, dataFormat, columns);
    localTableData->delete_(srcNodeIDVector, dstNodeIDVector, relIDVector);
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

void RelTableData::prepareCommit(Transaction* transaction, LocalTable* localTable) {
    auto localRelTable = ku_dynamic_cast<LocalTable*, LocalRelTable*>(localTable);
    KU_ASSERT(localRelTable);
    auto localRelTableData = localRelTable->getRelTableData(direction);
    if (dataFormat == ColumnDataFormat::REGULAR) {
        prepareCommitRegularColumns(transaction, localRelTableData);
    } else {
        prepareCommitCSRColumns(transaction, localRelTableData);
    }
}

void RelTableData::prepareCommitRegularColumns(
    Transaction* transaction, LocalRelTableData* localTableData) {
    for (auto& [nodeGroupIdx, nodeGroup] : localTableData->nodeGroups) {
        auto relNodeGroupInfo =
            ku_dynamic_cast<RelNGInfo*, RegularRelNGInfo*>(nodeGroup->getRelNodeGroupInfo());
        // TODO(Guodong): Should apply constratin check here. Cannot create a rel if already exists.
        adjColumn->prepareCommitForChunk(transaction, nodeGroupIdx, nodeGroup->getAdjColumn(),
            relNodeGroupInfo->adjInsertInfo, {} /* updateInfo */, relNodeGroupInfo->deleteInfo);
        for (auto columnID = 0u; columnID < columns.size(); columnID++) {
            auto localChunk = nodeGroup->getLocalColumnChunk(columnID);
            columns[columnID]->prepareCommitForChunk(transaction, nodeGroupIdx, localChunk,
                relNodeGroupInfo->insertInfoPerColumn[columnID],
                relNodeGroupInfo->updateInfoPerColumn[columnID], relNodeGroupInfo->deleteInfo);
        }
    }
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

// TODO: Generally, we should consider reading out a local csr offset when reading a rel. The local
// csr offset can be used to speed up commiting updates and deletions without searching over relIDs.
void RelTableData::prepareCommitCSRColumns(
    Transaction* transaction, LocalRelTableData* localTableData) {
    for (auto& [nodeGroupIdx, nodeGroup] : localTableData->nodeGroups) {
        auto relNodeGroupInfo =
            ku_dynamic_cast<RelNGInfo*, CSRRelNGInfo*>(nodeGroup->getRelNodeGroupInfo());
        // First, scan the whole csr offset column chunk, whose size is NODE_GROUP_SZIE.
        auto csrOffsetChunk = ColumnChunkFactory::createColumnChunk(
            LogicalType{LogicalTypeID::INT64}, false /* enableCompression */);
        csrOffsetColumn->scan(&DUMMY_WRITE_TRANSACTION, nodeGroupIdx, csrOffsetChunk.get());
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
            LogicalType{LogicalTypeID::INT64}, false /* enableCompression */, numRels);
        columns[REL_ID_COLUMN_ID]->scan(transaction, nodeGroupIdx, relIDChunk.get());
        if (relNodeGroupInfo->deleteInfo.empty() && relNodeGroupInfo->adjInsertInfo.empty()) {
            // We don't need to update the csr offset column if there is no deletion or insertion.
            // Thus, we can fall back to directly update the adj column and property columns based
            // on csr offsets.
            prepareCommitCSRNGWithoutSliding(transaction, nodeGroupIdx, relNodeGroupInfo,
                csrOffsetChunk.get(), relIDChunk.get(), nodeGroup.get());
        } else {
            // We need to update the csr offset column. Thus, we cannot simply fall back to directly
            // update the adj column and property columns based on csr offsets.
            prepareCommitCSRNGWithSliding(transaction, nodeGroupIdx, relNodeGroupInfo,
                csrOffsetChunk.get(), relIDChunk.get(), nodeGroup.get());
        }
    }
}

static std::pair<offset_t, offset_t> getCSRStartAndEndOffset(
    offset_t nodeGroupStartOffset, offset_t* csrOffsets, offset_t nodeOffset) {
    auto offsetInNodeGroup = nodeOffset - nodeGroupStartOffset;
    return offsetInNodeGroup == 0 ?
               std::make_pair((offset_t)0, csrOffsets[offsetInNodeGroup]) :
               std::make_pair(csrOffsets[offsetInNodeGroup - 1], csrOffsets[offsetInNodeGroup]);
}

void RelTableData::prepareCommitCSRNGWithoutSliding(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, CSRRelNGInfo* relNodeGroupInfo, ColumnChunk* csrOffsetChunk,
    ColumnChunk* relIDChunk, LocalRelNG* localNodeGroup) {
    // We can figure out the actual csr offset of each value to be updated based on csr and relID
    // chunks.
    auto csrOffsets = (offset_t*)csrOffsetChunk->getData();
    auto relIDs = (offset_t*)relIDChunk->getData();
    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        std::map<offset_t, row_idx_t> csrOffsetToRowIdx;
        auto& updateInfo = relNodeGroupInfo->updateInfoPerColumn[columnID];
        for (auto& [nodeOffset, relIDToRowIdx] : updateInfo) {
            for (auto [relID, rowIdx] : relIDToRowIdx) {
                auto [startCSROffset, endCSROffset] =
                    getCSRStartAndEndOffset(nodeGroupStartOffset, csrOffsets, nodeOffset);
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

std::unique_ptr<ColumnChunk> RelTableData::slideCSROffsetColumnChunk(
    ColumnChunk* csrOffsetChunk, CSRRelNGInfo* relNodeGroupInfo, offset_t nodeGroupStartOffset) {
    auto csrOffsets = (offset_t*)csrOffsetChunk->getData();
    auto slidedCSRChunk =
        ColumnChunkFactory::createColumnChunk(LogicalType{LogicalTypeID::INT64}, enableCompression);
    int64_t currentCSROffset = 0;
    auto currentNumSrcNodesInNG = csrOffsetChunk->getNumValues();
    auto newNumSrcNodesInNG = currentNumSrcNodesInNG;
    for (auto i = 0; i < currentNumSrcNodesInNG; i++) {
        auto nodeOffset = nodeGroupStartOffset + i;
        int64_t numRowsInCSR = i == 0 ? csrOffsets[i] : csrOffsets[i] - csrOffsets[i - 1];
        KU_ASSERT(numRowsInCSR >= 0);
        int64_t numDeletions = relNodeGroupInfo->deleteInfo.contains(nodeOffset) ?
                                   relNodeGroupInfo->deleteInfo[nodeOffset].size() :
                                   0;
        int64_t numInsertions = relNodeGroupInfo->adjInsertInfo.contains(nodeOffset) ?
                                    relNodeGroupInfo->adjInsertInfo[nodeOffset].size() :
                                    0;
        int64_t numRowsAfterSlide = numRowsInCSR + numInsertions - numDeletions;
        KU_ASSERT(numRowsAfterSlide >= 0);
        currentCSROffset += numRowsAfterSlide;
        slidedCSRChunk->setValue<offset_t>(currentCSROffset, i);
    }
    for (auto i = currentNumSrcNodesInNG; i < StorageConstants::NODE_GROUP_SIZE; i++) {
        auto nodeOffset = nodeGroupStartOffset + i;
        // Assert that should only have insertions for these nodes.
        KU_ASSERT(!relNodeGroupInfo->deleteInfo.contains(nodeOffset));
        for (auto columnID = 0u; columnID < columns.size(); columnID++) {
            KU_ASSERT(!relNodeGroupInfo->updateInfoPerColumn[columnID].contains(nodeOffset));
        }
        auto numRowsInCSR = 0;
        if (relNodeGroupInfo->adjInsertInfo.contains(nodeOffset)) {
            // This node is inserted now. Update csr offset accordingly.
            numRowsInCSR += relNodeGroupInfo->adjInsertInfo.at(nodeOffset).size();
            newNumSrcNodesInNG = i + 1;
        }
        currentCSROffset += numRowsInCSR;
        slidedCSRChunk->setValue<offset_t>(currentCSROffset, i);
    }
    slidedCSRChunk->setNumValues(newNumSrcNodesInNG);
    return slidedCSRChunk;
}

std::unique_ptr<ColumnChunk> RelTableData::slideCSRColunnChunk(Transaction* transaction,
    ColumnChunk* csrOffsetChunk, ColumnChunk* slidedCSROffsetChunkForCheck, ColumnChunk* relIDChunk,
    const csr_offset_to_row_idx_t& insertInfo, const csr_offset_to_row_idx_t& updateInfo,
    const std::map<common::offset_t, std::unordered_set<common::offset_t>>& deleteInfo,
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
        column->getDataType(), enableCompression, oldCapacity);
    column->scan(transaction, nodeGroupIdx, columnChunk.get());
    auto newColumnChunk = ColumnChunkFactory::createColumnChunk(
        column->getDataType(), enableCompression, newCapacity);
    auto currentNumSrcNodesInNG = csrOffsetChunk->getNumValues();
    auto csrOffsets = (offset_t*)csrOffsetChunk->getData();
    auto relIDs = (offset_t*)relIDChunk->getData();
    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    for (auto i = 0; i < currentNumSrcNodesInNG; i++) {
        auto nodeOffset = nodeGroupStartOffset + i;
        auto [startCSROffset, endCSROffset] =
            getCSRStartAndEndOffset(nodeGroupStartOffset, csrOffsets, nodeOffset);
        auto hasDeletions = deleteInfo.contains(nodeOffset);
        auto hasUpdates = updateInfo.contains(nodeOffset);
        auto hasInsertions = insertInfo.contains(nodeOffset);
        if (!hasDeletions && !hasUpdates && !hasInsertions) {
            // Append the whole csr.
            newColumnChunk->append(
                columnChunk.get(), startCSROffset, endCSROffset - startCSROffset);
            continue;
        }
        for (auto csrOffset = startCSROffset; csrOffset < endCSROffset; csrOffset++) {
            auto relID = relIDs[csrOffset];
            if (hasDeletions && deleteInfo.at(nodeOffset).contains(relID)) {
                // This rel is deleted now. Skip.
            } else if (hasUpdates && updateInfo.at(nodeOffset).contains(relID)) {
                // This rel is inserted or updated. Append from local data to the column chunk.
                auto rowIdx = insertInfo.at(nodeOffset).at(relID);
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
            for (auto [_, rowIdx] : insertInfo.at(nodeOffset)) {
                auto localVector = localChunk->getLocalVector(rowIdx)->getVector();
                auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
                localVector->state->selVector->selectedPositions[0] = offsetInVector;
                newColumnChunk->append(localVector);
            }
        }
    }
    if (!insertInfo.empty()) {
        // Append the newly inserted rels.
        for (auto i = currentNumSrcNodesInNG; i < StorageConstants::NODE_GROUP_SIZE; i++) {
            auto nodeOffset = nodeGroupStartOffset + i;
            if (insertInfo.contains(nodeOffset)) {
                for (auto [relID, rowIdx] : insertInfo.at(nodeOffset)) {
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

void RelTableData::prepareCommitCSRNGWithSliding(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, CSRRelNGInfo* relNodeGroupInfo, ColumnChunk* csrOffsetChunk,
    ColumnChunk* relIDChunk, LocalRelNG* localNodeGroup) {
    // We need to update the csr offset column. Thus, we cannot simply fall back to directly update
    // the adj column and property columns based on csr offsets. Instead, we need to for loop each
    // node in the node group, slide accordingly, and update the csr offset column, adj column and
    // property columns.
    auto csrOffsets = (offset_t*)csrOffsetChunk->getData();
    auto relIDs = (offset_t*)relIDChunk->getData();
    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    auto currentNumSrcNodesInNG = csrOffsetChunk->getNumValues();
    auto newNumSrcNodesInNG = currentNumSrcNodesInNG;
    // Slide column by column.
    // First we slide the csr offset column chunk, and keep the slided csr offset column chunk in
    // memory, so it can be used for assertion checking later.
    auto slidedCSROffsetChunk =
        slideCSROffsetColumnChunk(csrOffsetChunk, relNodeGroupInfo, nodeGroupStartOffset);
    csrOffsetColumn->append(slidedCSROffsetChunk.get(), nodeGroupIdx);
    // Then we slide the adj column chunk, rel id column chunk, and all property column chunks.
    auto slidedAdjColumnChunk =
        slideCSRColunnChunk(transaction, csrOffsetChunk, slidedCSROffsetChunk.get(), relIDChunk,
            relNodeGroupInfo->adjInsertInfo, {}, relNodeGroupInfo->deleteInfo, nodeGroupStartOffset,
            adjColumn.get(), localNodeGroup->getAdjColumn());
    adjColumn->append(slidedAdjColumnChunk.get(), nodeGroupIdx);
    slidedAdjColumnChunk.reset();
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        auto slidedColumnChunk = slideCSRColunnChunk(transaction, csrOffsetChunk,
            slidedCSROffsetChunk.get(), relIDChunk, relNodeGroupInfo->insertInfoPerColumn[columnID],
            relNodeGroupInfo->updateInfoPerColumn[columnID], relNodeGroupInfo->deleteInfo,
            nodeGroupStartOffset, columns[columnID].get(),
            localNodeGroup->getLocalColumnChunk(columnID));
        columns[columnID]->append(slidedColumnChunk.get(), nodeGroupIdx);
        slidedColumnChunk.reset();
    }
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

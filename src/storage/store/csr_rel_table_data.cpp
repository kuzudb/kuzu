#include "storage/store/csr_rel_table_data.h"

#include "common/assert.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/local_storage/local_table.h"
#include "storage/stats/rels_store_statistics.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

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
    auto readState = csrHeaderColumns.offset->getReadState(transaction->getType(), nodeGroupIdx);
    std::vector<offset_t> offsets;
    offsets.resize(2);
    csrHeaderColumns.offset->scan(transaction, readState,
        offsetInChunk == 0 ? 0 : offsetInChunk - 1, offsetInChunk + 1,
        reinterpret_cast<uint8_t*>(&offsets[0]));
    int64_t csrSize =
        offsetInChunk == 0 ? offsets[0] : (int64_t)(offsets[1]) - (int64_t)(offsets[0]);
    return csrSize > 0;
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
    // TODO(Guodong): This is a special logic for regular columns only, and should be organized
    // in a better way.
    return;
}

void CSRRelTableData::prepareLocalTableToCommit(
    Transaction* transaction, LocalTableData* localTable) {
    auto localRelTableData = ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(localTable);
    for (auto& [nodeGroupIdx, nodeGroup] : localRelTableData->nodeGroups) {
        auto relNG = ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(nodeGroup.get());
        auto relNodeGroupInfo = ku_dynamic_cast<RelNGInfo*, CSRRelNGInfo*>(relNG->getRelNGInfo());
        auto csrOffsetChunk = ColumnChunkFactory::createColumnChunk(
            LogicalType::UINT64(), false /* enableCompression */);
        auto csrLengthChunk = ColumnChunkFactory::createColumnChunk(LogicalType::UINT64(), false);
        csrHeaderColumns.offset->scan(transaction, nodeGroupIdx, csrOffsetChunk.get());
        csrHeaderColumns.length->scan(transaction, nodeGroupIdx, csrLengthChunk.get());
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
                csrOffsetChunk.get(), csrLengthChunk.get(), relIDChunk.get(), relNG);
        } else {
            // We need to update the csr offset column. Thus, we cannot simply fall back to directly
            // update the adj column and property columns based on csr offsets.
            prepareCommitCSRNGWithSliding(transaction, nodeGroupIdx, relNodeGroupInfo,
                csrOffsetChunk.get(), csrLengthChunk.get(), relIDChunk.get(), relNG);
        }
    }
}

static std::pair<offset_t, offset_t> getCSRStartAndEndOffset(offset_t offsetInNodeGroup,
    const ColumnChunk& csrOffsetChunk, const ColumnChunk& csrLengthChunk) {
    auto endOffset = csrOffsetChunk.getValue<offset_t>(offsetInNodeGroup);
    auto length = csrLengthChunk.getValue<offset_t>(offsetInNodeGroup);
    KU_ASSERT(offsetInNodeGroup == 0 ?
                  endOffset == length :
                  csrOffsetChunk.getValue<offset_t>(offsetInNodeGroup - 1) + length == endOffset);
    return std::make_pair(endOffset - length, endOffset);
}

static uint64_t findPosOfRelIDFromArray(
    const offset_t* relIDArray, uint64_t startPos, uint64_t endPos, offset_t relOffset) {
    for (auto i = startPos; i < endPos; i++) {
        if (relIDArray[i] == relOffset) {
            return i;
        }
    }
    return UINT64_MAX;
}

void CSRRelTableData::prepareCommitCSRNGWithoutSliding(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, CSRRelNGInfo* relNodeGroupInfo, ColumnChunk* csrOffsetChunk,
    ColumnChunk* csrLengthChunk, ColumnChunk* relIDChunk, LocalRelNG* localNodeGroup) {
    // We can figure out the actual csr offset of each value to be updated based on csr and relID
    // chunks.
    auto relIDs = (offset_t*)relIDChunk->getData();
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        std::map<offset_t, row_idx_t> csrOffsetToRowIdx;
        auto& updateInfo = relNodeGroupInfo->updateInfoPerChunk[columnID];
        for (auto& [offsetInChunk, relIDToRowIdx] : updateInfo) {
            auto [startCSROffset, endCSROffset] =
                getCSRStartAndEndOffset(offsetInChunk, *csrOffsetChunk, *csrLengthChunk);
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

void CSRRelTableData::prepareCommitCSRNGWithSliding(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, CSRRelNGInfo* relNodeGroupInfo, ColumnChunk* csrOffsetChunk,
    ColumnChunk* csrLengthChunk, ColumnChunk* relIDChunk, LocalRelNG* localNodeGroup) {
    // We need to update the csr offset column. Thus, we cannot simply fall back to directly update
    // the adj column and property columns based on csr offsets. Instead, we need to for loop each
    // node in the node group, slide accordingly, and update the csr offset column, adj column and
    // property columns.
    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    // Slide column by column.
    // First we slide the csr offset column chunk, and keep the slided csr offset column chunk in
    // memory, so it can be used for assertion checking later.
    auto [slidedCSROffsetChunk, slidedCSRLengthChunk] =
        slideCSRAuxChunks(csrOffsetChunk, csrLengthChunk, relNodeGroupInfo);
    csrHeaderColumns.offset->append(slidedCSROffsetChunk.get(), nodeGroupIdx);
    csrHeaderColumns.length->append(slidedCSRLengthChunk.get(), nodeGroupIdx);
    // Then we slide the adj column chunk, rel id column chunk, and all property column chunks.
    auto slidedAdjColumnChunk = slideCSRColumnChunk(transaction, csrOffsetChunk, csrLengthChunk,
        slidedCSROffsetChunk.get(), relIDChunk, relNodeGroupInfo->adjInsertInfo,
        {} /* updateInfo */, relNodeGroupInfo->deleteInfo, nodeGroupStartOffset, adjColumn.get(),
        localNodeGroup->getAdjChunk());
    adjColumn->append(slidedAdjColumnChunk.get(), nodeGroupIdx);
    slidedAdjColumnChunk.reset();
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        auto slidedColumnChunk = slideCSRColumnChunk(transaction, csrOffsetChunk, csrLengthChunk,
            slidedCSROffsetChunk.get(), relIDChunk, relNodeGroupInfo->insertInfoPerChunk[columnID],
            relNodeGroupInfo->updateInfoPerChunk[columnID], relNodeGroupInfo->deleteInfo,
            nodeGroupStartOffset, columns[columnID].get(),
            localNodeGroup->getLocalColumnChunk(columnID));
        columns[columnID]->append(slidedColumnChunk.get(), nodeGroupIdx);
        slidedColumnChunk.reset();
    }
}

std::pair<std::unique_ptr<ColumnChunk>, std::unique_ptr<ColumnChunk>>
CSRRelTableData::slideCSRAuxChunks(
    ColumnChunk* csrOffsetChunk, ColumnChunk* csrLengthChunk, CSRRelNGInfo* relNodeGroupInfo) {
    auto slidedCSROffsetChunk =
        ColumnChunkFactory::createColumnChunk(LogicalType::UINT64(), enableCompression);
    auto slidedCSRLengthChunk =
        ColumnChunkFactory::createColumnChunk(LogicalType::UINT64(), enableCompression);
    int64_t currentCSROffset = 0;
    auto currentNumSrcNodesInNG = csrOffsetChunk->getNumValues();
    auto newNumSrcNodesInNG = currentNumSrcNodesInNG;
    for (auto offsetInNG = 0u; offsetInNG < currentNumSrcNodesInNG; offsetInNG++) {
        int64_t numRowsInCSR = csrLengthChunk->getValue<offset_t>(offsetInNG);
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
        slidedCSROffsetChunk->setValue<offset_t>(currentCSROffset, offsetInNG);
        slidedCSRLengthChunk->setValue<length_t>(numRowsAfterSlide, offsetInNG);
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
        slidedCSROffsetChunk->setValue<offset_t>(currentCSROffset, offsetInNG);
        slidedCSRLengthChunk->setValue<length_t>(numRowsInCSR, offsetInNG);
    }
    slidedCSROffsetChunk->setNumValues(newNumSrcNodesInNG);
    slidedCSRLengthChunk->setNumValues(newNumSrcNodesInNG);
    return std::make_pair(std::move(slidedCSROffsetChunk), std::move(slidedCSRLengthChunk));
}

std::unique_ptr<ColumnChunk> CSRRelTableData::slideCSRColumnChunk(Transaction* transaction,
    ColumnChunk* csrOffsetChunk, ColumnChunk* csrLengthChunk, ColumnChunk* slidedCSROffsetChunk,
    ColumnChunk* relIDChunk, const offset_to_offset_to_row_idx_t& insertInfo,
    const offset_to_offset_to_row_idx_t& updateInfo, const offset_to_offset_set_t& deleteInfo,
    node_group_idx_t nodeGroupIdx, Column* column, LocalVectorCollection* localChunk) {
    auto oldCapacity = csrOffsetChunk->getNumValues() == 0 ?
                           0 :
                           csrOffsetChunk->getValue<offset_t>(csrOffsetChunk->getNumValues() - 1);
    auto newCapacity =
        slidedCSROffsetChunk->getNumValues() == 0 ?
            0 :
            slidedCSROffsetChunk->getValue<offset_t>(slidedCSROffsetChunk->getNumValues() - 1);
    // TODO: No need to allocate the new column chunk if this is relID.
    auto columnChunk = ColumnChunkFactory::createColumnChunk(
        column->getDataType()->copy(), enableCompression, oldCapacity);
    column->scan(transaction, nodeGroupIdx, columnChunk.get());
    auto newColumnChunk = ColumnChunkFactory::createColumnChunk(
        column->getDataType()->copy(), enableCompression, newCapacity);
    auto currentNumSrcNodesInNG = csrOffsetChunk->getNumValues();
    auto relIDs = (offset_t*)relIDChunk->getData();
    for (auto offsetInNG = 0u; offsetInNG < currentNumSrcNodesInNG; offsetInNG++) {
        auto [startCSROffset, endCSROffset] =
            getCSRStartAndEndOffset(offsetInNG, *csrOffsetChunk, *csrLengthChunk);
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

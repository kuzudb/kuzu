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
    : dataFormat{dataFormat}, startNodeOffsetInState{0}, numNodesInState{0},
      currentCSRNodeOffset{0}, posInCurrentCSR{0} {
    csrListEntries.resize(StorageConstants::NODE_GROUP_SIZE, {0, 0});
    csrOffsetChunk =
        ColumnChunkFactory::createColumnChunk(LogicalType::INT64(), false /* enableCompression */);
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

void RelTableData::initializeReadState(Transaction* transaction, RelDataDirection /*direction*/,
    std::vector<common::column_id_t> columnIDs, ValueVector* inNodeIDVector,
    RelDataReadState* readState) {
    readState->direction = direction;
    readState->columnIDs = std::move(columnIDs);
    if (dataFormat == ColumnDataFormat::REGULAR) {
        return;
    }
    auto nodeOffset =
        inNodeIDVector->readNodeOffset(inNodeIDVector->state->selVector->selectedPositions[0]);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    readState->posInCurrentCSR = 0;
    if (readState->isOutOfRange(nodeOffset)) {
        // Scan csr offsets and populate csr list entries for the new node group.
        readState->startNodeOffsetInState = startNodeOffset;
        csrOffsetColumn->scan(transaction, nodeGroupIdx, readState->csrOffsetChunk.get());
        readState->numNodesInState = readState->csrOffsetChunk->getNumValues();
        readState->populateCSRListEntries();
    }
    if (nodeOffset != readState->currentCSRNodeOffset) {
        readState->currentCSRNodeOffset = nodeOffset;
    }
}

void RelTableData::scanRegularColumns(Transaction* transaction, RelDataReadState& readState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
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

void RelTableData::update(transaction::Transaction* transaction, column_id_t columnID,
    ValueVector* srcNodeIDVector, ValueVector* relIDVector, ValueVector* propertyVector) {
    KU_ASSERT(columnID < columns.size() && columnID != REL_ID_COLUMN_ID);
    auto localTableData = ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(
        transaction->getLocalStorage()->getOrCreateLocalTableData(
            tableID, columns, TableType::REL, dataFormat, getDataIdxFromDirection(direction)));
    localTableData->update(srcNodeIDVector, relIDVector, columnID, propertyVector);
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
    transaction::Transaction* transaction, kuzu::storage::LocalRelTableData* localTableData) {
    for (auto& [nodeGroupIdx, nodeGroup] : localTableData->nodeGroups) {
        auto relNG = ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(nodeGroup.get());
        KU_ASSERT(relNG);
        auto relNodeGroupInfo =
            ku_dynamic_cast<RelNGInfo*, RegularRelNGInfo*>(relNG->getRelNGInfo());
        adjColumn->prepareCommitForChunk(transaction, nodeGroupIdx, relNG->getAdjChunk(),
            relNodeGroupInfo->adjInsertInfo, {}, relNodeGroupInfo->deleteInfo);
        for (auto columnID = 0u; columnID < columns.size(); columnID++) {
            columns[columnID]->prepareCommitForChunk(transaction, nodeGroupIdx,
                relNG->getPropertyChunk(columnID), relNodeGroupInfo->insertInfoPerChunk[columnID],
                relNodeGroupInfo->updateInfoPerChunk[columnID], relNodeGroupInfo->deleteInfo);
        }
    }
}

void RelTableData::prepareCommitForCSRColumns(
    transaction::Transaction* transaction, kuzu::storage::LocalRelTableData* localTableData) {
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
            KU_UNREACHABLE;
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
    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        std::map<offset_t, row_idx_t> csrOffsetToRowIdx;
        auto& updateInfo = relNodeGroupInfo->updateInfoPerChunk[columnID];
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

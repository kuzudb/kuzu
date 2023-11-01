#include "storage/store/rel_table_data.h"

#include "storage/stats/rels_store_statistics.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelDataReadState::RelDataReadState(ColumnDataFormat dataFormat)
    : dataFormat{dataFormat}, startNodeOffset{0}, numNodes{0}, currentNodeOffset{0},
      currentPosInCSRList{0} {
    csrListEntries.resize(StorageConstants::NODE_GROUP_SIZE, {0, 0});
    csrOffsetChunk = ColumnChunkFactory::createColumnChunk(
        LogicalType{LogicalTypeID::INT64}, false /* enableCompression */);
}

void RelDataReadState::populateCSRListEntries() {
    auto csrOffsets = (common::offset_t*)csrOffsetChunk->getData();
    csrListEntries[0].offset = 0;
    csrListEntries[0].size = csrOffsets[0];
    for (auto i = 1; i < numNodes; i++) {
        csrListEntries[i].offset = csrOffsets[i - 1];
        csrListEntries[i].size = csrOffsets[i] - csrOffsets[i - 1];
    }
}

std::pair<common::offset_t, common::offset_t> RelDataReadState::getStartAndEndOffset() {
    auto currCSRListEntry = csrListEntries[currentNodeOffset - startNodeOffset];
    auto currCSRSize = currCSRListEntry.size;
    auto startOffset = currCSRListEntry.offset + currentPosInCSRList;
    auto numRowsToRead =
        std::min(currCSRSize - currentPosInCSRList, common::DEFAULT_VECTOR_CAPACITY);
    currentPosInCSRList += numRowsToRead;
    return {startOffset, startOffset + numRowsToRead};
}

RelTableData::RelTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, RelTableSchema* tableSchema,
    RelsStoreStats* relsStoreStats, RelDataDirection direction, bool enableCompression)
    : TableData{dataFH, metadataFH, tableSchema->tableID, bufferManager, wal, enableCompression,
          getDataFormatFromSchema(tableSchema, direction)},
      csrOffsetColumn{nullptr} {
    if (dataFormat == ColumnDataFormat::CSR_COL) {
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

void RelTableData::initializeReadState(
    Transaction* /*transaction*/, ValueVector* inNodeIDVector, RelDataReadState* readState) {
    assert(dataFormat == ColumnDataFormat::CSR_COL);
    auto nodeOffset =
        inNodeIDVector->readNodeOffset(inNodeIDVector->state->selVector->selectedPositions[0]);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    readState->currentPosInCSRList = 0;
    if (readState->isOutOfRange(nodeOffset)) {
        // Scan csr offsets and populate csr list entries for the new node group.
        readState->startNodeOffset = startNodeOffset;
        csrOffsetColumn->scan(nodeGroupIdx, readState->csrOffsetChunk.get());
        readState->numNodes = readState->csrOffsetChunk->getNumValues();
        readState->populateCSRListEntries();
    }
    if (nodeOffset != readState->currentNodeOffset) {
        readState->currentNodeOffset = nodeOffset;
    }
}

void RelTableData::scanRegularColumns(Transaction* transaction, RelDataReadState& /*readState*/,
    ValueVector* inNodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    adjColumn->scan(transaction, inNodeIDVector, outputVectors[0]);
    if (!ValueVector::discardNull(*outputVectors[0])) {
        return;
    }
    for (auto i = 0u; i < columnIDs.size(); i++) {
        auto columnID = columnIDs[i];
        auto outputVectorId = i + 1; // Skip output from adj column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        columns[columnIDs[i]]->scan(transaction, inNodeIDVector, outputVectors[outputVectorId]);
    }
}

void RelTableData::scanCSRColumns(Transaction* transaction, RelDataReadState& readState,
    ValueVector* inNodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    assert(dataFormat == ColumnDataFormat::CSR_COL && inNodeIDVector->state->isFlat());
    auto [startOffset, endOffset] = readState.getStartAndEndOffset();
    auto numRowsToRead = endOffset - startOffset;
    outputVectors[0]->state->selVector->resetSelectorToUnselectedWithSize(numRowsToRead);
    outputVectors[0]->state->setOriginalSize(numRowsToRead);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(readState.currentNodeOffset);
    adjColumn->scan(transaction, nodeGroupIdx, startOffset, endOffset, outputVectors[0],
        0 /* offsetInVector */);
    for (auto i = 0u; i < columnIDs.size(); i++) {
        auto columnID = columnIDs[i];
        auto outputVectorId = i + 1; // Skip output from adj column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        columns[columnIDs[i]]->scan(transaction, nodeGroupIdx, startOffset, endOffset,
            outputVectors[outputVectorId], 0 /* offsetInVector */);
    }
}

void RelTableData::lookup(Transaction* transaction, ValueVector* inNodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    // Note: The scan operator should guarantee that the first property in the output is adj column.
    adjColumn->lookup(transaction, inNodeIDVector, outputVectors[0]);
    if (!ValueVector::discardNull(*outputVectors[0])) {
        return;
    }
    for (auto i = 0u; i < columnIDs.size(); i++) {
        auto columnID = columnIDs[i];
        auto outputVectorId = i + 1; // Skip output from adj column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        columns[columnIDs[i]]->lookup(transaction, inNodeIDVector, outputVectors[outputVectorId]);
    }
}

void RelTableData::append(NodeGroup* nodeGroup) {
    if (dataFormat == ColumnDataFormat::CSR_COL) {
        auto csrNodeGroup = static_cast<CSRNodeGroup*>(nodeGroup);
        csrOffsetColumn->append(csrNodeGroup->getCSROffsetChunk(), nodeGroup->getNodeGroupIdx());
    }
    adjColumn->append(nodeGroup->getColumnChunk(0), nodeGroup->getNodeGroupIdx());
    for (auto columnID = 0; columnID < columns.size(); columnID++) {
        columns[columnID]->append(
            nodeGroup->getColumnChunk(columnID + 1), nodeGroup->getNodeGroupIdx());
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

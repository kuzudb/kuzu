#include "storage/local_table.h"

#include "storage/copier/string_column_chunk.h"
#include "storage/store/node_table.h"
#include "storage/store/string_node_column.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void LocalVector::scan(ValueVector* resultVector) const {
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto posInLocalVector = vector->state->selVector->selectedPositions[i];
        auto posInResultVector = resultVector->state->selVector->selectedPositions[i];
        resultVector->copyFromVectorData(posInResultVector, vector.get(), posInLocalVector);
    }
}

void LocalVector::lookup(
    sel_t offsetInLocalVector, ValueVector* resultVector, sel_t offsetInResultVector) {
    if (!validityMask[offsetInLocalVector]) {
        return;
    }
    resultVector->copyFromVectorData(offsetInResultVector, vector.get(), offsetInLocalVector);
}

void LocalVector::update(
    sel_t offsetInLocalVector, ValueVector* updateVector, sel_t offsetInUpdateVector) {
    vector->copyFromVectorData(offsetInLocalVector, updateVector, offsetInUpdateVector);
    if (!validityMask[offsetInLocalVector]) {
        vector->state->selVector->selectedPositions[vector->state->selVector->selectedSize++] =
            offsetInLocalVector;
        validityMask[offsetInLocalVector] = true;
    }
}

void StringLocalVector::update(
    sel_t offsetInLocalVector, common::ValueVector* updateVector, sel_t offsetInUpdateVector) {
    auto kuStr = updateVector->getValue<ku_string_t>(offsetInUpdateVector);
    if (kuStr.len > BufferPoolConstants::PAGE_4KB_SIZE) {
        throw RuntimeException(
            ExceptionMessage::overLargeStringValueException(std::to_string(kuStr.len)));
    } else if (!ku_string_t::isShortString(kuStr.len)) {
        ovfStringLength += kuStr.len;
    }
    LocalVector::update(offsetInLocalVector, updateVector, offsetInUpdateVector);
}

std::unique_ptr<LocalVector> LocalVectorFactory::createLocalVectorData(
    const LogicalType& logicalType, MemoryManager* mm) {
    switch (logicalType.getPhysicalType()) {
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringLocalVector>(mm);
    }
    default: {
        return std::make_unique<LocalVector>(logicalType, mm);
    }
    }
}

void LocalColumnChunk::scan(vector_idx_t vectorIdx, ValueVector* resultVector) {
    if (!vectors.contains(vectorIdx)) {
        return;
    }
    vectors.at(vectorIdx)->scan(resultVector);
}

void LocalColumnChunk::lookup(vector_idx_t vectorIdx, sel_t offsetInLocalVector,
    ValueVector* resultVector, sel_t offsetInResultVector) {
    if (!vectors.contains(vectorIdx)) {
        return;
    }
    vectors.at(vectorIdx)->lookup(offsetInLocalVector, resultVector, offsetInResultVector);
}

void LocalColumnChunk::update(vector_idx_t vectorIdx, sel_t offsetInLocalVector,
    ValueVector* updateVector, sel_t offsetInUpdateVector) {
    if (!vectors.contains(vectorIdx)) {
        vectors.emplace(
            vectorIdx, LocalVectorFactory::createLocalVectorData(updateVector->dataType, mm));
    }
    vectors.at(vectorIdx)->update(offsetInLocalVector, updateVector, offsetInUpdateVector);
}

void LocalColumn::scan(ValueVector* nodeIDVector, ValueVector* outputVector) {
    assert(nodeIDVector->isSequential());
    auto nodeID = nodeIDVector->getValue<nodeID_t>(0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeID.offset);
    if (!chunks.contains(nodeGroupIdx)) {
        return;
    }
    auto vectorIdxInChunk = StorageUtils::getVectorIdxInChunk(nodeID.offset, nodeGroupIdx);
    chunks.at(nodeGroupIdx)->scan(vectorIdxInChunk, outputVector);
}

void LocalColumn::lookup(ValueVector* nodeIDVector, ValueVector* outputVector) {
    // TODO(Guodong): Should optimize. Sort nodeIDVector by node group idx.
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = nodeIDVector->getValue<nodeID_t>(pos);
        auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeID.offset);
        if (!chunks.contains(nodeGroupIdx)) {
            return;
        }
        auto [vectorIdxInChunk, offsetInVector] =
            StorageUtils::getVectorIdxInChunkAndOffsetInVector(nodeID.offset, nodeGroupIdx);
        chunks.at(nodeGroupIdx)
            ->lookup(vectorIdxInChunk, offsetInVector, outputVector,
                outputVector->state->selVector->selectedPositions[i]);
    }
}

void LocalColumn::update(
    ValueVector* nodeIDVector, ValueVector* propertyVector, MemoryManager* mm) {
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = nodeIDVector->getValue<nodeID_t>(pos);
        update(nodeID.offset, propertyVector,
            propertyVector->state->selVector->selectedPositions[i], mm);
    }
}

void LocalColumn::update(offset_t nodeOffset, ValueVector* propertyVector,
    sel_t posInPropertyVector, MemoryManager* mm) {
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!chunks.contains(nodeGroupIdx)) {
        chunks.emplace(nodeGroupIdx, std::make_unique<LocalColumnChunk>(mm));
    }
    auto chunk = chunks.at(nodeGroupIdx).get();
    auto [vectorIdxInChunk, offsetInVector] =
        StorageUtils::getVectorIdxInChunkAndOffsetInVector(nodeOffset, nodeGroupIdx);
    chunk->update(vectorIdxInChunk, offsetInVector, propertyVector, posInPropertyVector);
}

void LocalColumn::prepareCommit() {
    for (auto& [nodeGroupIdx, _] : chunks) {
        prepareCommitForChunk(nodeGroupIdx);
    }
}

void LocalColumn::prepareCommitForChunk(node_group_idx_t nodeGroupIdx) {
    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    auto chunk = chunks.at(nodeGroupIdx).get();
    for (auto& [vectorIdx, vector] : chunk->vectors) {
        auto vectorStartOffset =
            nodeGroupStartOffset + StorageUtils::getStartOffsetOfVector(vectorIdx);
        for (auto i = 0u; i < vector->vector->state->selVector->selectedSize; i++) {
            auto pos = vector->vector->state->selVector->selectedPositions[i];
            assert(vector->validityMask[pos]);
            column->write(vectorStartOffset + pos, vector->vector.get(), pos);
        }
    }
}

void StringLocalColumn::prepareCommitForChunk(node_group_idx_t nodeGroupIdx) {
    auto localChunk = chunks.at(nodeGroupIdx).get();
    auto stringColumn = reinterpret_cast<StringNodeColumn*>(column);
    auto overflowMetadata =
        stringColumn->getOverflowMetadataDA()->get(nodeGroupIdx, TransactionType::WRITE);
    auto ovfStringLengthInChunk = 0u;
    for (auto& [_, localVector] : localChunk->vectors) {
        auto stringLocalVector = reinterpret_cast<StringLocalVector*>(localVector.get());
        ovfStringLengthInChunk += stringLocalVector->ovfStringLength;
    }
    if (overflowMetadata.lastOffsetInPage + ovfStringLengthInChunk <=
        BufferPoolConstants::PAGE_4KB_SIZE) {
        // Write the updated overflow strings to the overflow string buffer.
        LocalColumn::prepareCommitForChunk(nodeGroupIdx);
    } else {
        commitLocalChunkOutOfPlace(nodeGroupIdx, localChunk);
    }
}

void StringLocalColumn::commitLocalChunkOutOfPlace(
    node_group_idx_t nodeGroupIdx, LocalColumnChunk* localChunk) {
    auto stringColumn = reinterpret_cast<StringNodeColumn*>(column);
    // Trigger rewriting the column chunk to another new place.
    auto columnChunk = ColumnChunkFactory::createColumnChunk(column->getDataType());
    auto stringColumnChunk = reinterpret_cast<StringColumnChunk*>(columnChunk.get());
    // First scan the whole column chunk into StringColumnChunk.
    stringColumn->scan(nodeGroupIdx, stringColumnChunk);
    for (auto& [vectorIdx, vector] : localChunk->vectors) {
        stringColumnChunk->update(vector->vector.get(), vectorIdx);
    }
    // Append the updated StringColumnChunk back to column.
    auto numPages = stringColumnChunk->getNumPages();
    auto startPageIdx = column->dataFH->addNewPages(numPages);
    column->append(stringColumnChunk, startPageIdx, nodeGroupIdx);
}

std::unique_ptr<LocalColumn> LocalColumnFactory::createLocalColumn(NodeColumn* column) {
    switch (column->getDataType().getPhysicalType()) {
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringLocalColumn>(column);
    }
    default: {
        return std::make_unique<LocalColumn>(column);
    }
    }
}

void LocalTable::scan(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    for (auto i = 0u; i < columnIDs.size(); i++) {
        auto columnID = columnIDs[i];
        if (!columns.contains(columnID)) {
            continue;
        }
        columns.at(columnID)->scan(nodeIDVector, outputVectors[i]);
    }
}

void LocalTable::lookup(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    for (auto i = 0u; i < columnIDs.size(); i++) {
        auto columnID = columnIDs[i];
        if (!columns.contains(columnID)) {
            continue;
        }
        columns.at(columnID)->lookup(nodeIDVector, outputVectors[i]);
    }
}

void LocalTable::update(property_id_t propertyID, ValueVector* nodeIDVector,
    ValueVector* propertyVector, MemoryManager* mm) {
    if (!columns.contains(propertyID)) {
        columns.emplace(propertyID,
            LocalColumnFactory::createLocalColumn(table->getPropertyColumn(propertyID)));
    }
    columns.at(propertyID)->update(nodeIDVector, propertyVector, mm);
}

void LocalTable::update(property_id_t propertyID, offset_t nodeOffset, ValueVector* propertyVector,
    sel_t posInPropertyVector, MemoryManager* mm) {
    if (!columns.contains(propertyID)) {
        columns.emplace(propertyID,
            LocalColumnFactory::createLocalColumn(table->getPropertyColumn(propertyID)));
    }
    columns.at(propertyID)->update(nodeOffset, propertyVector, posInPropertyVector, mm);
}

void LocalTable::prepareCommit() {
    for (auto& [_, column] : columns) {
        column->prepareCommit();
    }
}

} // namespace storage
} // namespace kuzu

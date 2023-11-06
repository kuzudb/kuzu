#include "storage/local_table.h"

#include "common/exception/message.h"
#include "common/exception/runtime.h"
#include "storage/store/node_table.h"
#include "storage/store/string_column.h"
#include "storage/store/struct_column.h"
#include "storage/store/var_list_column.h"
#include "storage/store/var_list_column_chunk.h"

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
    sel_t offsetInLocalVector, ValueVector* updateVector, sel_t offsetInUpdateVector) {
    auto kuStr = updateVector->getValue<ku_string_t>(offsetInUpdateVector);
    if (kuStr.len > BufferPoolConstants::PAGE_4KB_SIZE) {
        throw RuntimeException(ExceptionMessage::overLargeStringValueException(kuStr.len));
    } else if (!ku_string_t::isShortString(kuStr.len)) {
        ovfStringLength += kuStr.len;
    }
    LocalVector::update(offsetInLocalVector, updateVector, offsetInUpdateVector);
}

void StructLocalVector::scan(ValueVector* resultVector) const {
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto posInLocalVector = vector->state->selVector->selectedPositions[i];
        auto posInResultVector = resultVector->state->selVector->selectedPositions[i];
        resultVector->setNull(posInResultVector, vector->isNull(posInLocalVector));
    }
}

void StructLocalVector::lookup(
    sel_t offsetInLocalVector, ValueVector* resultVector, sel_t offsetInResultVector) {
    if (!validityMask[offsetInLocalVector]) {
        return;
    }
    resultVector->setNull(offsetInResultVector, vector->isNull(offsetInLocalVector));
}

void StructLocalVector::update(
    sel_t offsetInLocalVector, ValueVector* updateVector, sel_t offsetInUpdateVector) {
    vector->setNull(offsetInLocalVector, updateVector->isNull(offsetInUpdateVector));
    if (!validityMask[offsetInLocalVector]) {
        vector->state->selVector->selectedPositions[vector->state->selVector->selectedSize++] =
            offsetInLocalVector;
        validityMask[offsetInLocalVector] = true;
    }
}

std::unique_ptr<LocalVector> LocalVectorFactory::createLocalVectorData(
    const LogicalType& logicalType, MemoryManager* mm) {
    switch (logicalType.getPhysicalType()) {
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringLocalVector>(mm);
    }
    case PhysicalTypeID::STRUCT: {
        return std::make_unique<StructLocalVector>(mm);
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
        vectors.emplace(vectorIdx, LocalVectorFactory::createLocalVectorData(dataType, mm));
    }
    vectors.at(vectorIdx)->update(offsetInLocalVector, updateVector, offsetInUpdateVector);
}

void LocalColumn::scan(ValueVector* nodeIDVector, ValueVector* outputVector) {
    KU_ASSERT(nodeIDVector->isSequential());
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
        if (nodeIDVector->isNull(pos)) {
            continue;
        }
        auto nodeID = nodeIDVector->getValue<nodeID_t>(pos);
        update(nodeID.offset, propertyVector,
            propertyVector->state->selVector->selectedPositions[i], mm);
    }
}

void LocalColumn::update(offset_t nodeOffset, ValueVector* propertyVector,
    sel_t posInPropertyVector, MemoryManager* mm) {
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!chunks.contains(nodeGroupIdx)) {
        chunks.emplace(nodeGroupIdx, std::make_unique<LocalColumnChunk>(column->getDataType(), mm));
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
    KU_ASSERT(chunks.contains(nodeGroupIdx));
    auto chunk = chunks.at(nodeGroupIdx).get();
    // Figure out if the chunk needs to be re-compressed
    auto metadata = column->getMetadata(nodeGroupIdx, TransactionType::WRITE).compMeta;
    if (!metadata.canAlwaysUpdateInPlace()) {
        for (auto& [vectorIdx, vector] : chunk->vectors) {
            for (auto i = 0u; i < vector->vector->state->selVector->selectedSize; i++) {
                auto pos = vector->vector->state->selVector->selectedPositions[i];
                KU_ASSERT(vector->validityMask[pos]);
                if (!metadata.canUpdateInPlace(
                        *vector->vector, pos, column->getDataType().getPhysicalType())) {
                    return commitLocalChunkOutOfPlace(nodeGroupIdx, chunk);
                }
            }
        }
    }
    commitLocalChunkInPlace(nodeGroupIdx, chunk);
}

void LocalColumn::commitLocalChunkInPlace(
    node_group_idx_t nodeGroupIdx, LocalColumnChunk* localChunk) {
    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    for (auto& [vectorIdx, vector] : localChunk->vectors) {
        auto vectorStartOffset =
            nodeGroupStartOffset + StorageUtils::getStartOffsetOfVectorInChunk(vectorIdx);
        for (auto i = 0u; i < vector->vector->state->selVector->selectedSize; i++) {
            auto pos = vector->vector->state->selVector->selectedPositions[i];
            KU_ASSERT(vector->validityMask[pos]);
            column->write(vectorStartOffset + pos, vector->vector.get(), pos);
        }
    }
}

void LocalColumn::commitLocalChunkOutOfPlace(
    node_group_idx_t nodeGroupIdx, LocalColumnChunk* localChunk) {
    // Trigger rewriting the column chunk to another new place.
    auto columnChunk =
        ColumnChunkFactory::createColumnChunk(column->getDataType(), enableCompression);
    // First scan the whole column chunk into ColumnChunk.
    column->scan(nodeGroupIdx, columnChunk.get());
    for (auto& [vectorIdx, vector] : localChunk->vectors) {
        columnChunk->write(vector->vector.get(), vectorIdx << DEFAULT_VECTOR_CAPACITY_LOG_2);
    }
    // Append the updated ColumnChunk back to column.
    column->append(columnChunk.get(), nodeGroupIdx);
}

void StringLocalColumn::prepareCommitForChunk(node_group_idx_t nodeGroupIdx) {
    KU_ASSERT(chunks.contains(nodeGroupIdx));
    auto localChunk = chunks.at(nodeGroupIdx).get();
    auto stringColumn = reinterpret_cast<StringColumn*>(column);
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
        commitLocalChunkInPlace(nodeGroupIdx, localChunk);
    } else {
        commitLocalChunkOutOfPlace(nodeGroupIdx, localChunk);
    }
}

void VarListLocalColumn::prepareCommitForChunk(node_group_idx_t nodeGroupIdx) {
    KU_ASSERT(chunks.contains(nodeGroupIdx));
    auto chunk = chunks.at(nodeGroupIdx).get();
    auto varListColumn = reinterpret_cast<VarListColumn*>(column);
    auto listColumnChunkInStorage =
        ColumnChunkFactory::createColumnChunk(column->getDataType(), enableCompression);
    auto columnChunkToUpdate =
        ColumnChunkFactory::createColumnChunk(column->getDataType(), enableCompression);
    varListColumn->scan(nodeGroupIdx, listColumnChunkInStorage.get());
    offset_t nextOffsetToWrite = 0;
    auto numNodesInGroup =
        nodeGroupIdx >= column->getNumNodeGroups(&DUMMY_READ_TRANSACTION) ?
            0 :
            column->getMetadata(nodeGroupIdx, TransactionType::READ_ONLY).numValues;
    for (auto& [vectorIdx, localVector] : chunk->vectors) {
        auto startOffsetInChunk = StorageUtils::getStartOffsetOfVectorInChunk(vectorIdx);
        auto listVector = localVector->vector.get();
        listVector->state->selVector->selectedSize = 1;
        for (auto i = 0u; i < DEFAULT_VECTOR_CAPACITY; i++) {
            if (!localVector->validityMask[i]) {
                continue;
            }
            auto offsetInChunk = startOffsetInChunk + i;
            if (offsetInChunk > nextOffsetToWrite) {
                // Fill non-updated data from listColumnChunkInStorage.
                columnChunkToUpdate->append(listColumnChunkInStorage.get(), nextOffsetToWrite,
                    nextOffsetToWrite, offsetInChunk - nextOffsetToWrite);
            }
            listVector->state->selVector->selectedPositions[0] = i;
            columnChunkToUpdate->append(listVector, offsetInChunk);
            nextOffsetToWrite = offsetInChunk + 1;
        }
    }

    if (nextOffsetToWrite < numNodesInGroup) {
        columnChunkToUpdate->append(listColumnChunkInStorage.get(), nextOffsetToWrite,
            nextOffsetToWrite, numNodesInGroup - nextOffsetToWrite);
    }

    column->append(columnChunkToUpdate.get(), nodeGroupIdx);
    auto dataColumnChunk =
        reinterpret_cast<VarListColumnChunk*>(columnChunkToUpdate.get())->getDataColumnChunk();
    reinterpret_cast<VarListColumn*>(column)->dataColumn->append(dataColumnChunk, nodeGroupIdx);
}

StructLocalColumn::StructLocalColumn(Column* column, bool enableCompression)
    : LocalColumn{column, enableCompression} {
    KU_ASSERT(column->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    auto structColumn = static_cast<StructColumn*>(column);
    auto dataType = structColumn->getDataType();
    auto structFields = StructType::getFields(&dataType);
    fields.resize(structFields.size());
    for (auto i = 0u; i < structFields.size(); i++) {
        fields[i] =
            LocalColumnFactory::createLocalColumn(structColumn->getChild(i), enableCompression);
    }
}

void StructLocalColumn::scan(ValueVector* nodeIDVector, ValueVector* resultVector) {
    LocalColumn::scan(nodeIDVector, resultVector);
    auto fieldVectors = StructVector::getFieldVectors(resultVector);
    KU_ASSERT(fieldVectors.size() == fields.size());
    for (auto i = 0u; i < fields.size(); i++) {
        fields[i]->scan(nodeIDVector, fieldVectors[i].get());
    }
}

void StructLocalColumn::lookup(ValueVector* nodeIDVector, ValueVector* resultVector) {
    LocalColumn::lookup(nodeIDVector, resultVector);
    auto fieldVectors = StructVector::getFieldVectors(resultVector);
    KU_ASSERT(fieldVectors.size() == fields.size());
    for (auto i = 0u; i < fields.size(); i++) {
        fields[i]->lookup(nodeIDVector, fieldVectors[i].get());
    }
}

void StructLocalColumn::update(
    ValueVector* nodeIDVector, ValueVector* propertyVector, MemoryManager* mm) {
    LocalColumn::update(nodeIDVector, propertyVector, mm);
    auto propertyFieldVectors = StructVector::getFieldVectors(propertyVector);
    KU_ASSERT(propertyFieldVectors.size() == fields.size());
    for (auto i = 0u; i < fields.size(); i++) {
        fields[i]->update(nodeIDVector, propertyFieldVectors[i].get(), mm);
    }
}

void StructLocalColumn::update(offset_t nodeOffset, ValueVector* propertyVector,
    sel_t posInPropertyVector, MemoryManager* mm) {
    LocalColumn::update(nodeOffset, propertyVector, posInPropertyVector, mm);
    auto propertyFieldVectors = StructVector::getFieldVectors(propertyVector);
    KU_ASSERT(propertyFieldVectors.size() == fields.size());
    for (auto i = 0u; i < fields.size(); i++) {
        fields[i]->update(nodeOffset, propertyFieldVectors[i].get(), posInPropertyVector, mm);
    }
}

void StructLocalColumn::prepareCommitForChunk(node_group_idx_t nodeGroupIdx) {
    LocalColumn::prepareCommitForChunk(nodeGroupIdx);
    for (auto& field : fields) {
        field->prepareCommitForChunk(nodeGroupIdx);
    }
}

std::unique_ptr<LocalColumn> LocalColumnFactory::createLocalColumn(
    Column* column, bool enableCompression) {
    switch (column->getDataType().getPhysicalType()) {
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringLocalColumn>(column, enableCompression);
    }
    case PhysicalTypeID::STRUCT: {
        return std::make_unique<StructLocalColumn>(column, enableCompression);
    }
    case PhysicalTypeID::VAR_LIST: {
        return std::make_unique<VarListLocalColumn>(column, enableCompression);
    }
    default: {
        return std::make_unique<LocalColumn>(column, enableCompression);
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

void LocalTable::update(column_id_t columnID, ValueVector* nodeIDVector,
    ValueVector* propertyVector, MemoryManager* mm) {
    if (!columns.contains(columnID)) {
        columns.emplace(columnID,
            LocalColumnFactory::createLocalColumn(table->getColumn(columnID), enableCompression));
    }
    columns.at(columnID)->update(nodeIDVector, propertyVector, mm);
}

void LocalTable::update(column_id_t columnID, offset_t nodeOffset, ValueVector* propertyVector,
    sel_t posInPropertyVector, MemoryManager* mm) {
    if (!columns.contains(columnID)) {
        columns.emplace(columnID,
            LocalColumnFactory::createLocalColumn(table->getColumn(columnID), enableCompression));
    }
    columns.at(columnID)->update(nodeOffset, propertyVector, posInPropertyVector, mm);
}

void LocalTable::prepareCommit() {
    for (auto& [_, column] : columns) {
        column->prepareCommit();
    }
}

} // namespace storage
} // namespace kuzu

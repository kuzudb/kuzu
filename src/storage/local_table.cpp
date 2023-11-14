#include "storage/local_table.h"

#include "common/exception/message.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void LocalVector::read(
    sel_t offsetInLocalVector, ValueVector* resultVector, sel_t offsetInResultVector) {
    resultVector->copyFromVectorData(offsetInResultVector, vector.get(), offsetInLocalVector);
}

void LocalVector::append(ValueVector* valueVector) {
    KU_ASSERT(valueVector->state->selVector->selectedSize == 1);
    auto pos = valueVector->state->selVector->selectedPositions[0];
    if (valueVector->dataType.getPhysicalType() == PhysicalTypeID::STRING) {
        auto kuStr = valueVector->getValue<ku_string_t>(pos);
        if (kuStr.len > BufferPoolConstants::PAGE_4KB_SIZE) {
            throw RuntimeException(ExceptionMessage::overLargeStringValueException(kuStr.len));
        }
    }
    vector->copyFromVectorData(numValues, valueVector, pos);
    numValues++;
}

void LocalVectorCollection::read(
    row_idx_t rowIdx, ValueVector* outputVector, sel_t posInOutputVector) {
    auto vectorIdx = rowIdx >> DEFAULT_VECTOR_CAPACITY_LOG_2;
    auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
    KU_ASSERT(vectorIdx < vectors.size());
    vectors[vectorIdx]->read(offsetInVector, outputVector, posInOutputVector);
}

void LocalVectorCollection::insert(ValueVector* nodeIDVector, ValueVector* propertyVector) {
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    KU_ASSERT(!updateInfo.contains(nodeOffset));
    append(propertyVector);
    insertInfo[nodeOffset] = numRows;
    numRows++;
}

void LocalVectorCollection::update(ValueVector* nodeIDVector, ValueVector* propertyVector) {
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    append(propertyVector);
    if (insertInfo.contains(nodeOffset)) {
        // This node is in local storage, and had been newly inserted.
        insertInfo[nodeOffset] = numRows;
    } else {
        updateInfo[nodeOffset] = numRows;
    }
    numRows++;
}

row_idx_t LocalVectorCollection::getRowIdx(offset_t nodeOffset) {
    row_idx_t rowIdx = INVALID_ROW_IDX;
    if (updateInfo.contains(nodeOffset)) {
        // This node is in persistent storage, and had been updated.
        rowIdx = updateInfo[nodeOffset];
    } else if (insertInfo.contains(nodeOffset)) {
        // This node is in local storage, and had been newly inserted.
        rowIdx = insertInfo[nodeOffset];
    }
    return rowIdx;
}

void LocalVectorCollection::append(ValueVector* vector) {
    prepareAppend();
    auto lastVector = vectors.back().get();
    KU_ASSERT(!lastVector->isFull());
    lastVector->append(vector);
}

void LocalVectorCollection::prepareAppend() {
    if (vectors.empty()) {
        vectors.emplace_back(std::make_unique<LocalVector>(*dataType, mm));
    }
    auto lastVector = vectors.back().get();
    if (lastVector->isFull()) {
        vectors.emplace_back(std::make_unique<LocalVector>(*dataType, mm));
    }
}

void LocalNodeGroup::scan(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(columnIDs.size() == outputVectors.size());
    for (auto i = 0u; i < columnIDs.size(); i++) {
        auto columnID = columnIDs[i];
        KU_ASSERT(columnID < columns.size());
        for (auto pos = 0u; pos < nodeIDVector->state->selVector->selectedSize; pos++) {
            auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[pos];
            auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
            auto posInOutputVector = outputVectors[i]->state->selVector->selectedPositions[pos];
            lookup(nodeOffset, columnID, outputVectors[i], posInOutputVector);
        }
    }
}

void LocalNodeGroup::lookup(
    offset_t nodeOffset, column_id_t columnID, ValueVector* outputVector, sel_t posInOutputVector) {
    KU_ASSERT(columnID < columns.size());
    row_idx_t rowIdx = getRowIdx(columnID, nodeOffset);
    if (rowIdx != INVALID_ROW_IDX) {
        columns[columnID]->read(rowIdx, outputVector, posInOutputVector);
    }
}

void LocalNodeGroup::insert(
    ValueVector* nodeIDVector, const std::vector<ValueVector*>& propertyVectors) {
    KU_ASSERT(propertyVectors.size() == columns.size() &&
              nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return;
    }
    for (auto i = 0u; i < propertyVectors.size(); i++) {
        columns[i]->insert(nodeIDVector, propertyVectors[i]);
    }
}

void LocalNodeGroup::update(
    ValueVector* nodeIDVector, column_id_t columnID, ValueVector* propertyVector) {
    KU_ASSERT(columnID < columns.size() && nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return;
    }
    columns[columnID]->update(nodeIDVector, propertyVector);
}

void LocalNodeGroup::delete_(ValueVector* nodeIDVector) {
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return;
    }
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    for (auto i = 0u; i < columns.size(); i++) {
        columns[i]->delete_(nodeOffset);
    }
}

void LocalTable::scan(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    auto nodeGroupIdx = initializeLocalNodeGroup(nodeIDVector);
    nodeGroups.at(nodeGroupIdx)->scan(nodeIDVector, columnIDs, outputVectors);
}

void LocalTable::lookup(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[i];
        auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
        auto nodeGroupIdx = initializeLocalNodeGroup(nodeOffset);
        for (auto columnIdx = 0u; columnIdx < columnIDs.size(); columnIdx++) {
            auto columnID = columnIDs[columnIdx];
            auto outputVector = outputVectors[columnIdx];
            auto outputVectorPos = outputVector->state->selVector->selectedPositions[i];
            nodeGroups.at(nodeGroupIdx)
                ->lookup(nodeOffset, columnID, outputVector, outputVectorPos);
        }
    }
}

void LocalTable::insert(
    ValueVector* nodeIDVector, const std::vector<ValueVector*>& propertyVectors) {
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeGroupIdx = initializeLocalNodeGroup(nodeIDVector);
    nodeGroups.at(nodeGroupIdx)->insert(nodeIDVector, propertyVectors);
}

void LocalTable::update(
    ValueVector* nodeIDVector, column_id_t columnID, ValueVector* propertyVector) {
    auto nodeGroupIdx = initializeLocalNodeGroup(nodeIDVector);
    nodeGroups.at(nodeGroupIdx)->update(nodeIDVector, columnID, propertyVector);
}

void LocalTable::delete_(ValueVector* nodeIDVector) {
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!nodeGroups.contains(nodeGroupIdx)) {
        return;
    }
    nodeGroups.at(nodeGroupIdx)->delete_(nodeIDVector);
}

node_group_idx_t LocalTable::initializeLocalNodeGroup(ValueVector* nodeIDVector) {
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    return initializeLocalNodeGroup(nodeOffset);
}

node_group_idx_t LocalTable::initializeLocalNodeGroup(offset_t nodeOffset) {
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!nodeGroups.contains(nodeGroupIdx)) {
        nodeGroups.emplace(nodeGroupIdx, std::make_unique<LocalNodeGroup>(dataTypes, mm));
    }
    return nodeGroupIdx;
}

} // namespace storage
} // namespace kuzu

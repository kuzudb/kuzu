#include "storage/local_node_table.h"

#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void LocalNodeNG::scan(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
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

void LocalNodeNG::lookup(
    offset_t nodeOffset, column_id_t columnID, ValueVector* outputVector, sel_t posInOutputVector) {
    KU_ASSERT(columnID < columns.size());
    row_idx_t rowIdx = getRowIdx(columnID, nodeOffset);
    if (rowIdx != INVALID_ROW_IDX) {
        columns[columnID]->read(rowIdx, outputVector, posInOutputVector);
    }
}

void LocalNodeNG::insert(
    ValueVector* nodeIDVector, const std::vector<ValueVector*>& propertyVectors) {
    KU_ASSERT(propertyVectors.size() == columns.size() &&
              nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return;
    }
    for (auto i = 0u; i < propertyVectors.size(); i++) {
        auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
        KU_ASSERT(!updateInfoPerColumn[i].contains(nodeOffset));
        auto rowIdx = columns[i]->append(propertyVectors[i]);
        insertInfoPerColumn[i][nodeOffset] = rowIdx;
    }
}

void LocalNodeNG::update(
    ValueVector* nodeIDVector, column_id_t columnID, ValueVector* propertyVector) {
    KU_ASSERT(columnID < columns.size() && nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return;
    }
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto rowIdx = columns[columnID]->append(propertyVector);
    if (insertInfoPerColumn[columnID].contains(nodeOffset)) {
        // This node is in local storage, and had been newly inserted.
        insertInfoPerColumn[columnID][nodeOffset] = rowIdx;
    } else {
        updateInfoPerColumn[columnID][nodeOffset] = rowIdx;
    }
}

void LocalNodeNG::delete_(ValueVector* nodeIDVector) {
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return;
    }
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    for (auto i = 0u; i < columns.size(); i++) {
        insertInfoPerColumn[i].erase(nodeOffset);
        updateInfoPerColumn[i].erase(nodeOffset);
    }
}

void LocalNodeTable::scan(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!nodeGroups.contains(nodeGroupIdx)) {
        return;
    }
    nodeGroups.at(nodeGroupIdx)->scan(nodeIDVector, columnIDs, outputVectors);
}

void LocalNodeTable::lookup(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[i];
        auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
        auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        if (!nodeGroups.contains(nodeGroupIdx)) {
            continue;
        }
        for (auto columnIdx = 0u; columnIdx < columnIDs.size(); columnIdx++) {
            auto columnID = columnIDs[columnIdx];
            auto outputVector = outputVectors[columnIdx];
            auto outputVectorPos = outputVector->state->selVector->selectedPositions[i];
            nodeGroups.at(nodeGroupIdx)
                ->lookup(nodeOffset, columnID, outputVector, outputVectorPos);
        }
    }
}

void LocalNodeTable::insert(
    ValueVector* nodeIDVector, const std::vector<ValueVector*>& propertyVectors) {
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeGroupIdx = initializeLocalNodeGroup(nodeIDVector);
    nodeGroups.at(nodeGroupIdx)->insert(nodeIDVector, propertyVectors);
}

void LocalNodeTable::update(
    ValueVector* nodeIDVector, column_id_t columnID, ValueVector* propertyVector) {
    auto nodeGroupIdx = initializeLocalNodeGroup(nodeIDVector);
    nodeGroups.at(nodeGroupIdx)->update(nodeIDVector, columnID, propertyVector);
}

void LocalNodeTable::delete_(ValueVector* nodeIDVector) {
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!nodeGroups.contains(nodeGroupIdx)) {
        return;
    }
    nodeGroups.at(nodeGroupIdx)->delete_(nodeIDVector);
}

node_group_idx_t LocalNodeTable::initializeLocalNodeGroup(ValueVector* nodeIDVector) {
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!nodeGroups.contains(nodeGroupIdx)) {
        nodeGroups.emplace(nodeGroupIdx, std::make_unique<LocalNodeNG>(dataTypes, mm));
    }
    return nodeGroupIdx;
}

} // namespace storage
} // namespace kuzu

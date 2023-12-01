#include "storage/local_storage/local_node_table.h"

#include "common/cast.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void LocalNodeNG::scan(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(columnIDs.size() == outputVectors.size());
    for (auto i = 0u; i < columnIDs.size(); i++) {
        auto columnID = columnIDs[i];
        KU_ASSERT(columnID < chunks.size());
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
    KU_ASSERT(columnID < chunks.size());
    row_idx_t rowIdx = getRowIdx(columnID, nodeOffset - nodeGroupStartOffset);
    if (rowIdx != INVALID_ROW_IDX) {
        chunks[columnID]->read(rowIdx, outputVector, posInOutputVector);
    }
}

void LocalNodeNG::insert(
    ValueVector* nodeIDVector, const std::vector<ValueVector*>& propertyVectors) {
    KU_ASSERT(propertyVectors.size() == chunks.size() &&
              nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return;
    }
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(nodeOffset < StorageConstants::NODE_GROUP_SIZE);
    for (auto columnID = 0u; columnID < chunks.size(); columnID++) {
        auto rowIdx = chunks[columnID]->append(propertyVectors[columnID]);
        KU_ASSERT(!updateInfo[columnID].contains(nodeOffset));
        insertInfo[columnID][nodeOffset] = rowIdx;
    }
}

void LocalNodeNG::update(
    ValueVector* nodeIDVector, column_id_t columnID, ValueVector* propertyVector) {
    KU_ASSERT(columnID < chunks.size() && nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return;
    }
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(nodeOffset < StorageConstants::NODE_GROUP_SIZE);
    auto rowIdx = chunks[columnID]->append(propertyVector);
    if (insertInfo[columnID].contains(nodeOffset)) {
        // This node is in local storage, and had been newly inserted.
        insertInfo.at(columnID)[nodeOffset] = rowIdx;
    } else {
        updateInfo[columnID][nodeOffset] = rowIdx;
    }
}

void LocalNodeNG::delete_(ValueVector* nodeIDVector) {
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return;
    }
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(nodeOffset < StorageConstants::NODE_GROUP_SIZE);
    for (auto i = 0u; i < chunks.size(); i++) {
        insertInfo[i].erase(nodeOffset);
        updateInfo[i].erase(nodeOffset);
    }
}

row_idx_t LocalNodeNG::getRowIdx(column_id_t columnID, offset_t offsetInChunk) {
    KU_ASSERT(columnID < chunks.size());
    if (updateInfo[columnID].contains(offsetInChunk)) {
        // This node is in persistent storage, and had been updated.
        return updateInfo[columnID][offsetInChunk];
    } else if (insertInfo[columnID].contains(offsetInChunk)) {
        // This node is in local storage, and had been newly inserted.
        return insertInfo[columnID][offsetInChunk];
    } else {
        return INVALID_ROW_IDX;
    }
}

void LocalNodeTableData::scan(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!nodeGroups.contains(nodeGroupIdx)) {
        return;
    }
    auto localNodeGroup =
        ku_dynamic_cast<LocalNodeGroup*, LocalNodeNG*>(nodeGroups.at(nodeGroupIdx).get());
    KU_ASSERT(localNodeGroup);
    localNodeGroup->scan(nodeIDVector, columnIDs, outputVectors);
}

void LocalNodeTableData::lookup(ValueVector* nodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[i];
        auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
        auto localNodeGroup =
            ku_dynamic_cast<LocalNodeGroup*, LocalNodeNG*>(getOrCreateLocalNodeGroup(nodeIDVector));
        KU_ASSERT(localNodeGroup);
        for (auto columnIdx = 0u; columnIdx < columnIDs.size(); columnIdx++) {
            auto columnID = columnIDs[columnIdx];
            auto outputVector = outputVectors[columnIdx];
            auto outputVectorPos = outputVector->state->selVector->selectedPositions[i];
            localNodeGroup->lookup(nodeOffset, columnID, outputVector, outputVectorPos);
        }
    }
}

void LocalNodeTableData::insert(
    ValueVector* nodeIDVector, const std::vector<ValueVector*>& propertyVectors) {
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto localNodeGroup =
        ku_dynamic_cast<LocalNodeGroup*, LocalNodeNG*>(getOrCreateLocalNodeGroup(nodeIDVector));
    KU_ASSERT(localNodeGroup);
    localNodeGroup->insert(nodeIDVector, propertyVectors);
}

void LocalNodeTableData::update(
    ValueVector* nodeIDVector, column_id_t columnID, ValueVector* propertyVector) {
    auto localNodeGroup =
        ku_dynamic_cast<LocalNodeGroup*, LocalNodeNG*>(getOrCreateLocalNodeGroup(nodeIDVector));
    KU_ASSERT(localNodeGroup);
    localNodeGroup->update(nodeIDVector, columnID, propertyVector);
}

void LocalNodeTableData::delete_(ValueVector* nodeIDVector) {
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!nodeGroups.contains(nodeGroupIdx)) {
        return;
    }
    auto localNodeGroup =
        ku_dynamic_cast<LocalNodeGroup*, LocalNodeNG*>(nodeGroups.at(nodeGroupIdx).get());
    localNodeGroup->delete_(nodeIDVector);
}

LocalNodeGroup* LocalNodeTableData::getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector) {
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!nodeGroups.contains(nodeGroupIdx)) {
        auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        nodeGroups[nodeGroupIdx] =
            std::make_unique<LocalNodeNG>(nodeGroupStartOffset, dataTypes, mm);
    }
    return nodeGroups.at(nodeGroupIdx).get();
}

} // namespace storage
} // namespace kuzu

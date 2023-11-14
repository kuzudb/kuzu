#include "storage/local_rel_table.h"

#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void LocalRelNG::insert(ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector,
    const std::vector<ValueVector*>& propertyVectors) {
    KU_ASSERT(propertyVectors.size() == columns.size() &&
              srcNodeIDVector->state->selVector->selectedSize == 1 &&
              dstNodeIDVector->state->selVector->selectedSize == 1);
    auto adjColRowIdx = adjColumn->append(dstNodeIDVector);
    std::vector<row_idx_t> columnsRowIdx;
    columnsRowIdx.resize(columns.size());
    for (auto i = 0u; i < columns.size(); i++) {
        columnsRowIdx[i] = columns[i]->append(propertyVectors[i]);
    }
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto srcNodeOffset = srcNodeIDVector->getValue<nodeID_t>(srcNodeIDPos).offset;
    auto relIDPos = propertyVectors[0]->state->selVector->selectedPositions[0];
    auto relID = propertyVectors[0]->getValue<relID_t>(relIDPos).offset;
    relNGInfo->insert(srcNodeOffset, relID, adjColRowIdx, columnsRowIdx);
}

// TODO: Remove dstNodeIDVector.
void LocalRelNG::update(ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector,
    ValueVector* relIDVector, column_id_t columnID, ValueVector* propertyVector) {
    KU_ASSERT(columnID < columns.size() && srcNodeIDVector->state->selVector->selectedSize == 1 &&
              dstNodeIDVector->state->selVector->selectedSize == 1 &&
              relIDVector->state->selVector->selectedSize == 1);
    auto rowIdx = columns[columnID]->append(propertyVector);
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto srcNodeOffset = srcNodeIDVector->getValue<nodeID_t>(srcNodeIDPos).offset;
    auto relIDPos = relIDVector->state->selVector->selectedPositions[0];
    auto relID = relIDVector->getValue<relID_t>(relIDPos).offset;
    relNGInfo->update(srcNodeOffset, relID, rowIdx, columnID);
}

// TODO: Remove dstNodeIDVector.
void LocalRelNG::delete_(
    ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector, ValueVector* relIDVector) {
    KU_ASSERT(srcNodeIDVector->state->selVector->selectedSize == 1 &&
              dstNodeIDVector->state->selVector->selectedSize == 1 &&
              relIDVector->state->selVector->selectedSize == 1);
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto srcNodeOffset = srcNodeIDVector->getValue<nodeID_t>(srcNodeIDPos).offset;
    auto relIDPos = relIDVector->state->selVector->selectedPositions[0];
    auto relID = relIDVector->getValue<relID_t>(relIDPos).offset;
    relNGInfo->delete_(srcNodeOffset, relID);
}

void LocalRelTableData::scan(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    // TODO:
}

void LocalRelTableData::lookup(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    // TODO:
}

void LocalRelTableData::insert(ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector,
    const std::vector<ValueVector*>& propertyVectors) {
    KU_ASSERT(srcNodeIDVector->state->selVector->selectedSize == 1 &&
              dstNodeIDVector->state->selVector->selectedSize == 1);
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto dstNodeIDPos = dstNodeIDVector->state->selVector->selectedPositions[0];
    if (srcNodeIDVector->isNull(srcNodeIDPos) || dstNodeIDVector->isNull(dstNodeIDPos)) {
        return;
    }
    auto nodeGroupIdx = initializeLocalNodeGroup(srcNodeIDVector);
    KU_ASSERT(nodeGroups.contains(nodeGroupIdx));
    nodeGroups.at(nodeGroupIdx)->insert(srcNodeIDVector, dstNodeIDVector, propertyVectors);
}

void LocalRelTableData::update(ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector,
    ValueVector* relIDVector, column_id_t columnID, ValueVector* propertyVector) {
    KU_ASSERT(srcNodeIDVector->state->selVector->selectedSize == 1 &&
              dstNodeIDVector->state->selVector->selectedSize == 1 &&
              relIDVector->state->selVector->selectedSize == 1);
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto dstNodeIDPos = dstNodeIDVector->state->selVector->selectedPositions[0];
    auto relIDPos = relIDVector->state->selVector->selectedPositions[0];
    if (srcNodeIDVector->isNull(srcNodeIDPos) || dstNodeIDVector->isNull(dstNodeIDPos) ||
        relIDVector->isNull(relIDPos)) {
        return;
    }
    auto nodeGroupIdx = initializeLocalNodeGroup(srcNodeIDVector);
    KU_ASSERT(nodeGroups.contains(nodeGroupIdx));
    nodeGroups.at(nodeGroupIdx)
        ->update(srcNodeIDVector, dstNodeIDVector, relIDVector, columnID, propertyVector);
}

void LocalRelTableData::delete_(
    ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector, ValueVector* relIDVector) {
    KU_ASSERT(srcNodeIDVector->state->selVector->selectedSize == 1 &&
              dstNodeIDVector->state->selVector->selectedSize == 1 &&
              relIDVector->state->selVector->selectedSize == 1);
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto dstNodeIDPos = dstNodeIDVector->state->selVector->selectedPositions[0];
    auto relIDPos = relIDVector->state->selVector->selectedPositions[0];
    if (srcNodeIDVector->isNull(srcNodeIDPos) || dstNodeIDVector->isNull(dstNodeIDPos) ||
        relIDVector->isNull(relIDPos)) {
        return;
    }
    auto nodeGroupIdx = initializeLocalNodeGroup(srcNodeIDVector);
    KU_ASSERT(nodeGroups.contains(nodeGroupIdx));
    nodeGroups.at(nodeGroupIdx)->delete_(srcNodeIDVector, dstNodeIDVector, relIDVector);
}

node_group_idx_t LocalRelTableData::initializeLocalNodeGroup(ValueVector* nodeIDVector) {
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!nodeGroups.contains(nodeGroupIdx)) {
        nodeGroups[nodeGroupIdx] = std::make_unique<LocalRelNG>(dataFormat, dataTypes, mm);
    }
    return nodeGroupIdx;
}

void LocalRelTable::scan(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    // TODO: add direction. or should remove this function?
}

void LocalRelTable::lookup(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    // TODO: add direction. or should remove this function?
}

LocalRelTableData* LocalRelTable::getOrCreateRelTableData(
    RelDataDirection direction, ColumnDataFormat dataFormat) {
    auto& tableData = direction == RelDataDirection::FWD ? fwdRelTableData : bwdRelTableData;
    if (!tableData) {
        tableData = std::make_unique<LocalRelTableData>(dataFormat, copyTypes(dataTypes), mm);
    }
    return tableData.get();
}

} // namespace storage
} // namespace kuzu

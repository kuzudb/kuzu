#include "storage/local_storage/local_rel_table.h"

#include "common/cast.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void RegularRelNGInfo::insert(offset_t srcNodeOffset, offset_t /*relOffset*/,
    row_idx_t adjNodeRowIdx, const std::vector<row_idx_t>& propertyNodesRowIdx) {
    KU_ASSERT(propertyNodesRowIdx.size() == insertInfoPerChunk.size());
    if (deleteInfo.contains(srcNodeOffset)) {
        // We choose to ignore the insert operation if the node is deleted.
        return;
    }
    adjInsertInfo[srcNodeOffset] = adjNodeRowIdx;
    for (auto i = 0u; i < propertyNodesRowIdx.size(); ++i) {
        KU_ASSERT(!updateInfoPerChunk[i].contains(srcNodeOffset));
        insertInfoPerChunk[i][srcNodeOffset] = propertyNodesRowIdx[i];
    }
}

void RegularRelNGInfo::update(
    offset_t srcNodeOffset, offset_t /*relOffset*/, column_id_t columnID, row_idx_t rowIdx) {
    if (deleteInfo.contains(srcNodeOffset)) {
        // We choose to ignore the update operation if the node is deleted.
        return;
    }
    KU_ASSERT(columnID != REL_ID_COLUMN_ID); // Rel ID is immutable.
    KU_ASSERT(columnID < updateInfoPerChunk.size());
    if (insertInfoPerChunk[columnID].contains(srcNodeOffset)) {
        // Update newly inserted value.
        insertInfoPerChunk[columnID][srcNodeOffset] = rowIdx;
    } else {
        updateInfoPerChunk[columnID][srcNodeOffset] = rowIdx;
    }
}

bool RegularRelNGInfo::delete_(offset_t srcNodeOffset, offset_t /*relOffset*/) {
    if (adjInsertInfo.contains(srcNodeOffset)) {
        // Delete newly inserted tuple.
        adjInsertInfo.erase(srcNodeOffset);
        for (auto& insertInfo : insertInfoPerChunk) {
            insertInfo.erase(srcNodeOffset);
        }
    } else {
        if (deleteInfo.contains(srcNodeOffset)) {
            // The node is already deleted.
            return false;
        } else {
            deleteInfo.insert(srcNodeOffset);
        }
    }
    return true;
}

void CSRRelNGInfo::insert(offset_t srcNodeOffset, offset_t relOffset, row_idx_t adjNodeRowIdx,
    const std::vector<row_idx_t>& propertyNodesRowIdx) {
    KU_ASSERT(propertyNodesRowIdx.size() == insertInfoPerChunk.size());
    if (deleteInfo.contains(srcNodeOffset) && !contains(deleteInfo.at(srcNodeOffset), relOffset)) {
        // We choose to ignore the insert operation if the node is deleted.
        return;
    }
    if (adjInsertInfo.contains(srcNodeOffset)) {
        adjInsertInfo.at(srcNodeOffset)[relOffset] = adjNodeRowIdx;
    } else {
        adjInsertInfo[srcNodeOffset] = {{relOffset, adjNodeRowIdx}};
    }
    for (auto i = 0u; i < propertyNodesRowIdx.size(); ++i) {
        if (insertInfoPerChunk[i].contains(srcNodeOffset)) {
            insertInfoPerChunk[i].at(srcNodeOffset)[relOffset] = propertyNodesRowIdx[i];
        } else {
            insertInfoPerChunk[i][srcNodeOffset] = {{relOffset, propertyNodesRowIdx[i]}};
        }
    }
}

void CSRRelNGInfo::update(
    offset_t srcNodeOffset, offset_t relOffset, column_id_t columnID, row_idx_t rowIdx) {
    // REL_ID_COLUMN_ID is immutable.
    KU_ASSERT(columnID != REL_ID_COLUMN_ID && columnID < updateInfoPerChunk.size());
    if (deleteInfo.contains(srcNodeOffset) && !contains(deleteInfo.at(srcNodeOffset), relOffset)) {
        // We choose to ignore the update operation if the node is deleted.
        return;
    }
    if (insertInfoPerChunk[columnID].contains(srcNodeOffset) &&
        insertInfoPerChunk[columnID].at(srcNodeOffset).contains(relOffset)) {
        // Update newly inserted value.
        insertInfoPerChunk[columnID].at(srcNodeOffset)[relOffset] = rowIdx;
    } else {
        if (updateInfoPerChunk[columnID].contains(srcNodeOffset)) {
            updateInfoPerChunk[columnID].at(srcNodeOffset)[relOffset] = rowIdx;
        } else {
            updateInfoPerChunk[columnID][srcNodeOffset] = {{relOffset, rowIdx}};
        }
    }
}

bool CSRRelNGInfo::delete_(offset_t srcNodeOffset, offset_t relOffset) {
    if (adjInsertInfo.contains(srcNodeOffset) &&
        adjInsertInfo.at(srcNodeOffset).contains(relOffset)) {
        // Delete newly inserted tuple.
        adjInsertInfo.at(srcNodeOffset).erase(relOffset);
        if (adjInsertInfo.at(srcNodeOffset).empty()) {
            adjInsertInfo.erase(srcNodeOffset);
        }
        for (auto& insertInfo : insertInfoPerChunk) {
            insertInfo.at(srcNodeOffset).erase(relOffset);
            if (insertInfo.at(srcNodeOffset).empty()) {
                insertInfo.erase(srcNodeOffset);
            }
        }
    } else {
        if (deleteInfo.contains(srcNodeOffset)) {
            if (deleteInfo.at(srcNodeOffset).contains(relOffset)) {
                // The node is already deleted.
                return false;
            } else {
                deleteInfo.at(srcNodeOffset).insert(relOffset);
            }
        } else {
            deleteInfo[srcNodeOffset] = {relOffset};
        }
    }
    return true;
}

LocalRelNG::LocalRelNG(ColumnDataFormat dataFormat, std::vector<LogicalType*> dataTypes,
    kuzu::storage::MemoryManager* mm)
    : LocalNodeGroup{std::move(dataTypes), mm} {
    switch (dataFormat) {
    case ColumnDataFormat::REGULAR: {
        relNGInfo = std::make_unique<RegularRelNGInfo>(chunks.size());
    } break;
    case ColumnDataFormat::CSR: {
        relNGInfo = std::make_unique<CSRRelNGInfo>(chunks.size());
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    adjChunk = std::make_unique<LocalVectorCollection>(LogicalType::INTERNAL_ID(), mm);
}

void LocalRelNG::insert(ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector,
    const std::vector<ValueVector*>& propertyVectors) {
    KU_ASSERT(propertyVectors.size() == chunks.size() && propertyVectors.size() >= 1);
    auto adjNodeIDRowIdx = adjChunk->append(dstNodeIDVector);
    std::vector<row_idx_t> propertyValuesRowIdx;
    propertyValuesRowIdx.reserve(propertyVectors.size());
    for (auto i = 0u; i < propertyVectors.size(); ++i) {
        propertyValuesRowIdx.push_back(chunks[i]->append(propertyVectors[i]));
    }
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto srcNodeOffset = srcNodeIDVector->getValue<nodeID_t>(srcNodeIDPos).offset;
    auto relIDPos = propertyVectors[REL_ID_COLUMN_ID]->state->selVector->selectedPositions[0];
    auto relOffset = propertyVectors[REL_ID_COLUMN_ID]->getValue<relID_t>(relIDPos).offset;
    relNGInfo->insert(srcNodeOffset, relOffset, adjNodeIDRowIdx, propertyValuesRowIdx);
}

void LocalRelNG::update(ValueVector* srcNodeIDVector, ValueVector* relIDVector,
    column_id_t columnID, ValueVector* propertyVector) {
    KU_ASSERT(columnID < chunks.size());
    auto rowIdx = chunks[columnID]->append(propertyVector);
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto srcNodeOffset = srcNodeIDVector->getValue<nodeID_t>(srcNodeIDPos).offset;
    auto relIDPos = relIDVector->state->selVector->selectedPositions[0];
    auto relOffset = relIDVector->getValue<relID_t>(relIDPos).offset;
    relNGInfo->update(srcNodeOffset, relOffset, columnID, rowIdx);
}

bool LocalRelNG::delete_(ValueVector* srcNodeIDVector, ValueVector* relIDVector) {
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto srcNodeOffset = srcNodeIDVector->getValue<nodeID_t>(srcNodeIDPos).offset;
    auto relIDPos = relIDVector->state->selVector->selectedPositions[0];
    auto relOffset = relIDVector->getValue<relID_t>(relIDPos).offset;
    return relNGInfo->delete_(srcNodeOffset, relOffset);
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
    auto localNodeGroup =
        ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(getOrCreateLocalNodeGroup(srcNodeIDVector));
    KU_ASSERT(localNodeGroup);
    localNodeGroup->insert(srcNodeIDVector, dstNodeIDVector, propertyVectors);
}

void LocalRelTableData::update(ValueVector* srcNodeIDVector, ValueVector* relIDVector,
    column_id_t columnID, ValueVector* propertyVector) {
    KU_ASSERT(srcNodeIDVector->state->selVector->selectedSize == 1 &&
              relIDVector->state->selVector->selectedSize == 1);
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto relIDPos = relIDVector->state->selVector->selectedPositions[0];
    if (srcNodeIDVector->isNull(srcNodeIDPos) || relIDVector->isNull(relIDPos)) {
        return;
    }
    auto localNodeGroup =
        ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(getOrCreateLocalNodeGroup(srcNodeIDVector));
    localNodeGroup->update(srcNodeIDVector, relIDVector, columnID, propertyVector);
}

bool LocalRelTableData::delete_(
    ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector, ValueVector* relIDVector) {
    KU_ASSERT(srcNodeIDVector->state->selVector->selectedSize == 1 &&
              relIDVector->state->selVector->selectedSize == 1);
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto dstNodeIDPos = dstNodeIDVector->state->selVector->selectedPositions[0];
    if (srcNodeIDVector->isNull(srcNodeIDPos) || dstNodeIDVector->isNull(dstNodeIDPos)) {
        return false;
    }
    auto localNodeGroup =
        ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(getOrCreateLocalNodeGroup(srcNodeIDVector));
    return localNodeGroup->delete_(srcNodeIDVector, relIDVector);
}

LocalNodeGroup* LocalRelTableData::getOrCreateLocalNodeGroup(ValueVector* nodeIDVector) {
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!nodeGroups.contains(nodeGroupIdx)) {
        nodeGroups[nodeGroupIdx] = std::make_unique<LocalRelNG>(dataFormat, dataTypes, mm);
    }
    return nodeGroups.at(nodeGroupIdx).get();
}

} // namespace storage
} // namespace kuzu

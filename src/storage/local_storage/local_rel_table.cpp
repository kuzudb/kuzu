#include "storage/local_storage/local_rel_table.h"

#include "common/cast.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

bool RegularRelNGInfo::insert(offset_t srcOffsetInChunk, offset_t /*relOffset*/,
    row_idx_t adjNodeRowIdx, const std::vector<row_idx_t>& propertyNodesRowIdx) {
    KU_ASSERT(propertyNodesRowIdx.size() == insertInfoPerChunk.size());
    bool wasDeleted = deleteInfo.contains(srcOffsetInChunk);
    if (adjInsertInfo.contains(srcOffsetInChunk) && !wasDeleted) {
        throw RuntimeException{"Many-one, one-one relationship violated."};
    }
    if (wasDeleted) {
        deleteInfo.erase(srcOffsetInChunk);
    }
    adjInsertInfo[srcOffsetInChunk] = adjNodeRowIdx;
    for (auto i = 0u; i < propertyNodesRowIdx.size(); ++i) {
        KU_ASSERT(!updateInfoPerChunk[i].contains(srcOffsetInChunk));
        insertInfoPerChunk[i][srcOffsetInChunk] = propertyNodesRowIdx[i];
    }
    return !wasDeleted;
}

void RegularRelNGInfo::update(
    offset_t srcOffsetInChunk, offset_t /*relOffset*/, column_id_t columnID, row_idx_t rowIdx) {
    if (deleteInfo.contains(srcOffsetInChunk)) {
        // We choose to ignore the update operation if the node is deleted.
        return;
    }
    KU_ASSERT(columnID != REL_ID_COLUMN_ID); // Rel ID is immutable.
    KU_ASSERT(columnID < updateInfoPerChunk.size());
    if (insertInfoPerChunk[columnID].contains(srcOffsetInChunk)) {
        // Update newly inserted value.
        insertInfoPerChunk[columnID][srcOffsetInChunk] = rowIdx;
    } else {
        updateInfoPerChunk[columnID][srcOffsetInChunk] = rowIdx;
    }
}

bool RegularRelNGInfo::delete_(offset_t srcOffsetInChunk, offset_t /*relOffset*/) {
    if (adjInsertInfo.contains(srcOffsetInChunk)) {
        // Delete newly inserted tuple.
        adjInsertInfo.erase(srcOffsetInChunk);
    }
    if (deleteInfo.contains(srcOffsetInChunk)) {
        // The node is already deleted.
        return false;
    } else {
        deleteInfo.insert(srcOffsetInChunk);
    }
    return true;
}

uint64_t RegularRelNGInfo::getNumInsertedTuples(offset_t srcOffsetInChunk) {
    return adjInsertInfo.contains(srcOffsetInChunk) ? 1 : 0;
}

bool CSRRelNGInfo::insert(offset_t srcOffsetInChunk, offset_t relOffset, row_idx_t adjNodeRowIdx,
    const std::vector<row_idx_t>& propertyNodesRowIdx) {
    KU_ASSERT(propertyNodesRowIdx.size() == insertInfoPerChunk.size());
    if (deleteInfo.contains(srcOffsetInChunk) &&
        contains(deleteInfo.at(srcOffsetInChunk), relOffset)) {
        deleteInfo.at(srcOffsetInChunk).erase(relOffset);
    }
    if (adjInsertInfo.contains(srcOffsetInChunk)) {
        adjInsertInfo.at(srcOffsetInChunk)[relOffset] = adjNodeRowIdx;
    } else {
        adjInsertInfo[srcOffsetInChunk] = {{relOffset, adjNodeRowIdx}};
    }
    for (auto i = 0u; i < propertyNodesRowIdx.size(); ++i) {
        if (insertInfoPerChunk[i].contains(srcOffsetInChunk)) {
            insertInfoPerChunk[i].at(srcOffsetInChunk)[relOffset] = propertyNodesRowIdx[i];
        } else {
            insertInfoPerChunk[i][srcOffsetInChunk] = {{relOffset, propertyNodesRowIdx[i]}};
        }
    }
    return false;
}

void CSRRelNGInfo::update(
    offset_t srcOffsetInChunk, offset_t relOffset, column_id_t columnID, row_idx_t rowIdx) {
    // REL_ID_COLUMN_ID is immutable.
    KU_ASSERT(columnID != REL_ID_COLUMN_ID && columnID < updateInfoPerChunk.size());
    if (deleteInfo.contains(srcOffsetInChunk) &&
        contains(deleteInfo.at(srcOffsetInChunk), relOffset)) {
        // We choose to ignore the update operation if the node is deleted.
        return;
    }
    if (insertInfoPerChunk[columnID].contains(srcOffsetInChunk) &&
        insertInfoPerChunk[columnID].at(srcOffsetInChunk).contains(relOffset)) {
        // Update newly inserted value.
        insertInfoPerChunk[columnID].at(srcOffsetInChunk)[relOffset] = rowIdx;
    } else {
        if (updateInfoPerChunk[columnID].contains(srcOffsetInChunk)) {
            updateInfoPerChunk[columnID].at(srcOffsetInChunk)[relOffset] = rowIdx;
        } else {
            updateInfoPerChunk[columnID][srcOffsetInChunk] = {{relOffset, rowIdx}};
        }
    }
}

bool CSRRelNGInfo::delete_(offset_t srcOffsetInChunk, offset_t relOffset) {
    if (adjInsertInfo.contains(srcOffsetInChunk) &&
        adjInsertInfo.at(srcOffsetInChunk).contains(relOffset)) {
        // Delete newly inserted tuple.
        adjInsertInfo.at(srcOffsetInChunk).erase(relOffset);
        for (auto& insertInfo : insertInfoPerChunk) {
            insertInfo.at(srcOffsetInChunk).erase(relOffset);
        }
    } else {
        if (deleteInfo.contains(srcOffsetInChunk)) {
            if (deleteInfo.at(srcOffsetInChunk).contains(relOffset)) {
                // The node is already deleted.
                return false;
            } else {
                deleteInfo.at(srcOffsetInChunk).insert(relOffset);
            }
        } else {
            deleteInfo[srcOffsetInChunk] = {relOffset};
        }
    }
    return true;
}

uint64_t CSRRelNGInfo::getNumInsertedTuples(offset_t srcOffsetInChunk) {
    return adjInsertInfo.contains(srcOffsetInChunk) ? adjInsertInfo.at(srcOffsetInChunk).size() : 0;
}

LocalRelNG::LocalRelNG(offset_t nodeGroupStartOffset, ColumnDataFormat dataFormat,
    std::vector<LogicalType*> dataTypes, kuzu::storage::MemoryManager* mm)
    : LocalNodeGroup{nodeGroupStartOffset, std::move(dataTypes), mm} {
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

// TODO(Guodong): We should change the map between relID and rowIdx to a vector of pairs, which is
// more friendly for scan.
row_idx_t LocalRelNG::scanCSR(offset_t srcOffsetInChunk, offset_t posToReadForOffset,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(columnIDs.size() + 1 == outputVectors.size());
    auto csrRelNGInfo = ku_dynamic_cast<RelNGInfo*, CSRRelNGInfo*>(relNGInfo.get());
    KU_ASSERT(csrRelNGInfo);
    KU_ASSERT(csrRelNGInfo->adjInsertInfo.contains(srcOffsetInChunk));
    uint64_t posInVector = 0;
    auto iteratorIdx = 0u;
    for (auto& [relID, rowIdx] : csrRelNGInfo->adjInsertInfo.at(srcOffsetInChunk)) {
        if (iteratorIdx++ < posToReadForOffset) {
            continue;
        }
        auto posInLocalVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
        outputVectors[0]->copyFromVectorData(
            posInVector++, adjChunk->getLocalVector(rowIdx)->getVector(), posInLocalVector);
    }
    for (auto i = 0u; i < columnIDs.size(); ++i) {
        auto columnID = columnIDs[i];
        posInVector = 0;
        iteratorIdx = 0u;
        auto& insertInfo = csrRelNGInfo->insertInfoPerChunk[columnID];
        KU_ASSERT(insertInfo.contains(srcOffsetInChunk));
        for (auto& [relID, rowIdx] : insertInfo.at(srcOffsetInChunk)) {
            if (iteratorIdx++ < posToReadForOffset) {
                continue;
            }
            auto posInLocalVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
            outputVectors[i + 1]->copyFromVectorData(posInVector++,
                chunks[columnID]->getLocalVector(rowIdx)->getVector(), posInLocalVector);
        }
    }
    outputVectors[0]->state->selVector->resetSelectorToUnselectedWithSize(posInVector);
    return posInVector;
}

void LocalRelNG::applyLocalChangesForCSRColumns(offset_t srcOffsetInChunk,
    const std::vector<column_id_t>& columnIDs, ValueVector* relIDVector,
    const std::vector<ValueVector*>& outputVector) {
    KU_ASSERT(columnIDs.size() + 1 == outputVector.size());
    auto csrRelNGInfo = ku_dynamic_cast<RelNGInfo*, CSRRelNGInfo*>(relNGInfo.get());
    KU_ASSERT(csrRelNGInfo);
    // Apply updates first, as applying deletions might change selected state.
    for (auto i = 0u; i < columnIDs.size(); ++i) {
        auto columnID = columnIDs[i];
        applyCSRUpdates(srcOffsetInChunk, columnID, csrRelNGInfo->updateInfoPerChunk[columnID],
            relIDVector, outputVector);
    }
    // Apply deletions and update selVector if necessary.
    if (csrRelNGInfo->deleteInfo.contains(srcOffsetInChunk) &&
        csrRelNGInfo->deleteInfo.at(srcOffsetInChunk).size() > 0) {
        applyCSRDeletions(srcOffsetInChunk, csrRelNGInfo->deleteInfo, relIDVector);
    }
}

void LocalRelNG::applyLocalChangesForRegularColumns(ValueVector* srcNodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVector) {
    KU_ASSERT(columnIDs.size() + 1 == outputVector.size());
    auto regularRelNGInfo = ku_dynamic_cast<RelNGInfo*, RegularRelNGInfo*>(relNGInfo.get());
    KU_ASSERT(regularRelNGInfo);
    applyRegularChangesToVector(srcNodeIDVector, adjChunk.get(), {} /* updateInfo */,
        regularRelNGInfo->adjInsertInfo, regularRelNGInfo->deleteInfo, outputVector[0]);
    for (auto colIdx = 0u; colIdx < columnIDs.size(); colIdx++) {
        auto columnID = columnIDs[colIdx];
        // There is no need to apply deleteInfo on property columns, as adj column will be the one
        // always read first and used to check nulls.
        applyRegularChangesToVector(srcNodeIDVector, chunks[columnID].get(),
            regularRelNGInfo->updateInfoPerChunk[columnID],
            regularRelNGInfo->insertInfoPerChunk[columnID], {} /* deleteInfo */,
            outputVector[colIdx + 1]);
    }
}

void LocalRelNG::applyLocalChangesForRegularColumns(offset_t offsetInChunk,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors,
    sel_t posInVector) {
    KU_ASSERT(columnIDs.size() + 1 == outputVectors.size());
    auto regularRelNGInfo = ku_dynamic_cast<RelNGInfo*, RegularRelNGInfo*>(relNGInfo.get());
    KU_ASSERT(regularRelNGInfo);
    applyRegularChangesForOffset(offsetInChunk, adjChunk.get(), {} /* updateInfo */,
        regularRelNGInfo->adjInsertInfo, regularRelNGInfo->deleteInfo, outputVectors[0],
        posInVector);
    for (auto colIdx = 0u; colIdx < columnIDs.size(); colIdx++) {
        auto columnID = columnIDs[colIdx];
        applyRegularChangesForOffset(offsetInChunk, chunks[columnID].get(),
            regularRelNGInfo->updateInfoPerChunk[columnID],
            regularRelNGInfo->insertInfoPerChunk[columnID], {} /* deleteInfo */,
            outputVectors[colIdx + 1], posInVector);
    }
}

void LocalRelNG::applyCSRUpdates(offset_t srcOffsetInChunk, column_id_t columnID,
    const offset_to_offset_to_row_idx_t& updateInfo, ValueVector* relIDVector,
    const std::vector<ValueVector*>& outputVector) {
    if (!updateInfo.contains(srcOffsetInChunk) || updateInfo.at(srcOffsetInChunk).empty()) {
        return;
    }
    auto& updateInfoForOffset = updateInfo.at(srcOffsetInChunk);
    for (auto i = 0u; i < relIDVector->state->selVector->selectedSize; i++) {
        auto pos = relIDVector->state->selVector->selectedPositions[i];
        auto relOffset = relIDVector->getValue<relID_t>(pos).offset;
        if (updateInfoForOffset.contains(relOffset)) {
            auto rowIdx = updateInfoForOffset.at(relOffset);
            auto posInLocalVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
            outputVector[i + 1]->copyFromVectorData(
                pos, chunks[columnID]->getLocalVector(rowIdx)->getVector(), posInLocalVector);
        }
    }
}

void LocalRelNG::applyCSRDeletions(
    offset_t srcOffsetInChunk, const offset_to_offset_set_t& deleteInfo, ValueVector* relIDVector) {
    auto& deleteInfoForOffset = deleteInfo.at(srcOffsetInChunk);
    auto selectPos = 0u;
    auto selVector = std::make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
    selVector->resetSelectorToValuePosBuffer();
    for (auto i = 0u; i < relIDVector->state->selVector->selectedSize; i++) {
        auto relIDPos = relIDVector->state->selVector->selectedPositions[i];
        auto relOffset = relIDVector->getValue<relID_t>(relIDPos).offset;
        if (deleteInfoForOffset.contains(relOffset)) {
            continue;
        }
        selVector->selectedPositions[selectPos++] = relIDPos;
    }
    if (selectPos != relIDVector->state->selVector->selectedSize) {
        relIDVector->state->selVector->resetSelectorToValuePosBuffer();
        memcpy(relIDVector->state->selVector->selectedPositions, selVector->selectedPositions,
            selectPos * sizeof(sel_t));
        relIDVector->state->selVector->selectedSize = selectPos;
    }
}

void LocalRelNG::applyRegularChangesToVector(common::ValueVector* srcNodeIDVector,
    LocalVectorCollection* chunk, const offset_to_row_idx_t& updateInfo,
    const offset_to_row_idx_t& insertInfo, const offset_set_t& deleteInfo,
    common::ValueVector* outputVector) {
    if (updateInfo.empty() && insertInfo.empty() && deleteInfo.empty()) {
        return;
    }
    for (auto i = 0u; i < srcNodeIDVector->state->selVector->selectedSize; i++) {
        auto selPos = srcNodeIDVector->state->selVector->selectedPositions[i];
        auto offsetInChunk =
            srcNodeIDVector->getValue<nodeID_t>(selPos).offset - nodeGroupStartOffset;
        applyRegularChangesForOffset(
            offsetInChunk, chunk, updateInfo, insertInfo, deleteInfo, outputVector, selPos);
    }
}

void LocalRelNG::applyRegularChangesForOffset(common::offset_t offsetInChunk,
    LocalVectorCollection* chunk, const offset_to_row_idx_t& updateInfo,
    const offset_to_row_idx_t& insertInfo, const offset_set_t& deleteInfo,
    common::ValueVector* outputVector, common::sel_t posInVector) {
    row_idx_t rowIdx = updateInfo.contains(offsetInChunk) ? updateInfo.at(offsetInChunk) :
                       insertInfo.contains(offsetInChunk) ? insertInfo.at(offsetInChunk) :
                                                            INVALID_ROW_IDX;
    if (rowIdx != INVALID_ROW_IDX) {
        auto posInLocalVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
        outputVector->copyFromVectorData(
            posInVector, chunk->getLocalVector(rowIdx)->getVector(), posInLocalVector);
    } else if (deleteInfo.contains(offsetInChunk)) {
        outputVector->setNull(posInVector, true /* isNull */);
    }
}

bool LocalRelNG::insert(ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector,
    const std::vector<ValueVector*>& propertyVectors) {
    KU_ASSERT(propertyVectors.size() == chunks.size() && propertyVectors.size() >= 1);
    auto adjNodeIDRowIdx = adjChunk->append(dstNodeIDVector);
    std::vector<row_idx_t> propertyValuesRowIdx;
    propertyValuesRowIdx.reserve(propertyVectors.size());
    for (auto i = 0u; i < propertyVectors.size(); ++i) {
        propertyValuesRowIdx.push_back(chunks[i]->append(propertyVectors[i]));
    }
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto srcNodeOffset =
        srcNodeIDVector->getValue<nodeID_t>(srcNodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(srcNodeOffset < StorageConstants::NODE_GROUP_SIZE);
    auto relIDPos = propertyVectors[REL_ID_COLUMN_ID]->state->selVector->selectedPositions[0];
    auto relOffset = propertyVectors[REL_ID_COLUMN_ID]->getValue<relID_t>(relIDPos).offset;
    return relNGInfo->insert(srcNodeOffset, relOffset, adjNodeIDRowIdx, propertyValuesRowIdx);
}

void LocalRelNG::update(ValueVector* srcNodeIDVector, ValueVector* relIDVector,
    column_id_t columnID, ValueVector* propertyVector) {
    KU_ASSERT(columnID < chunks.size());
    auto rowIdx = chunks[columnID]->append(propertyVector);
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto srcNodeOffset =
        srcNodeIDVector->getValue<nodeID_t>(srcNodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(srcNodeOffset < StorageConstants::NODE_GROUP_SIZE);
    auto relIDPos = relIDVector->state->selVector->selectedPositions[0];
    auto relOffset = relIDVector->getValue<relID_t>(relIDPos).offset;
    relNGInfo->update(srcNodeOffset, relOffset, columnID, rowIdx);
}

bool LocalRelNG::delete_(ValueVector* srcNodeIDVector, ValueVector* relIDVector) {
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto srcNodeOffset =
        srcNodeIDVector->getValue<nodeID_t>(srcNodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(srcNodeOffset < StorageConstants::NODE_GROUP_SIZE);
    auto relIDPos = relIDVector->state->selVector->selectedPositions[0];
    auto relOffset = relIDVector->getValue<relID_t>(relIDPos).offset;
    return relNGInfo->delete_(srcNodeOffset, relOffset);
}

bool LocalRelTableData::insert(ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector,
    const std::vector<ValueVector*>& propertyVectors) {
    KU_ASSERT(srcNodeIDVector->state->selVector->selectedSize == 1 &&
              dstNodeIDVector->state->selVector->selectedSize == 1);
    auto srcNodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto dstNodeIDPos = dstNodeIDVector->state->selVector->selectedPositions[0];
    if (srcNodeIDVector->isNull(srcNodeIDPos) || dstNodeIDVector->isNull(dstNodeIDPos)) {
        return false;
    }
    auto localNodeGroup =
        ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(getOrCreateLocalNodeGroup(srcNodeIDVector));
    KU_ASSERT(localNodeGroup);
    return localNodeGroup->insert(srcNodeIDVector, dstNodeIDVector, propertyVectors);
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
              dstNodeIDVector->state->selVector->selectedSize == 1 &&
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
        auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        nodeGroups[nodeGroupIdx] =
            std::make_unique<LocalRelNG>(nodeGroupStartOffset, dataFormat, dataTypes, mm);
    }
    return nodeGroups.at(nodeGroupIdx).get();
}

} // namespace storage
} // namespace kuzu

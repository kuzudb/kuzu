#include "storage/local_storage/local_rel_table.h"

#include "common/cast.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

bool RelNGInfo::insert(offset_t srcOffsetInChunk, offset_t relOffset, row_idx_t adjNodeRowIdx,
    const std::vector<row_idx_t>& propertyNodesRowIdx) {
    KU_ASSERT(propertyNodesRowIdx.size() == insertInfoPerChunk.size());
    if (deleteInfo.contains(srcOffsetInChunk) &&
        contains(deleteInfo.at(srcOffsetInChunk), relOffset)) {
        deleteInfo.at(srcOffsetInChunk).erase(relOffset);
    }
    if (adjInsertInfo.contains(srcOffsetInChunk)) {
        if (multiplicity == RelMultiplicity::ONE) {
            throw RuntimeException("Inserting multiple edges to a single node in a "
                                   "ONE_ONE/MANY_ONE relationship is not allowed.");
        }
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

void RelNGInfo::update(
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

bool RelNGInfo::delete_(offset_t srcOffsetInChunk, offset_t relOffset) {
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

bool RelNGInfo::hasUpdates() {
    for (auto& updateInfo : updateInfoPerChunk) {
        if (!updateInfo.empty()) {
            return true;
        }
    }
    return false;
}

const update_insert_info_t& RelNGInfo::getEmptyInfo() {
    static update_insert_info_t emptyInfo;
    return emptyInfo;
}

uint64_t RelNGInfo::getNumInsertedTuples(offset_t srcOffsetInChunk) {
    return adjInsertInfo.contains(srcOffsetInChunk) ? adjInsertInfo.at(srcOffsetInChunk).size() : 0;
}

LocalRelNG::LocalRelNG(offset_t nodeGroupStartOffset, std::vector<LogicalType*> dataTypes,
    MemoryManager* mm, common::RelMultiplicity multiplicity)
    : LocalNodeGroup{nodeGroupStartOffset, std::move(dataTypes), mm} {
    relNGInfo = std::make_unique<RelNGInfo>(multiplicity, chunks.size());
    adjChunk = std::make_unique<LocalVectorCollection>(*LogicalType::INTERNAL_ID(), mm);
}

// TODO(Guodong): We should change the map between relID and rowIdx to a vector of pairs, which is
// more friendly for scan.
row_idx_t LocalRelNG::scanCSR(offset_t srcOffsetInChunk, offset_t posToReadForOffset,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(columnIDs.size() + 1 == outputVectors.size());
    KU_ASSERT(relNGInfo->adjInsertInfo.contains(srcOffsetInChunk));
    uint64_t posInVector = 0;
    auto iteratorIdx = 0u;
    for (auto& [relID, rowIdx] : relNGInfo->adjInsertInfo.at(srcOffsetInChunk)) {
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
        auto& insertInfo = relNGInfo->insertInfoPerChunk[columnID];
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
    const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(columnIDs.size() + 1 == outputVectors.size());
    // Apply updates first, as applying deletions might change selected state.
    for (auto i = 0u; i < columnIDs.size(); ++i) {
        applyCSRUpdates(srcOffsetInChunk, columnIDs[i], relIDVector, outputVectors[i + 1]);
    }
    // Apply deletions and update selVector if necessary.
    if (relNGInfo->deleteInfo.contains(srcOffsetInChunk) &&
        relNGInfo->deleteInfo.at(srcOffsetInChunk).size() > 0) {
        applyCSRDeletions(srcOffsetInChunk, relNGInfo->deleteInfo, relIDVector);
    }
}

void LocalRelNG::applyCSRUpdates(offset_t srcOffsetInChunk, column_id_t columnID,
    ValueVector* relIDVector, ValueVector* outputVector) {
    auto updateInfo = relNGInfo->updateInfoPerChunk[columnID];
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
            outputVector->copyFromVectorData(
                pos, chunks[columnID]->getLocalVector(rowIdx)->getVector(), posInLocalVector);
        }
    }
}

void LocalRelNG::applyCSRDeletions(
    offset_t srcOffsetInChunk, const delete_info_t& deleteInfo, ValueVector* relIDVector) {
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
            std::make_unique<LocalRelNG>(nodeGroupStartOffset, dataTypes, mm, multiplicity);
    }
    return nodeGroups.at(nodeGroupIdx).get();
}

} // namespace storage
} // namespace kuzu

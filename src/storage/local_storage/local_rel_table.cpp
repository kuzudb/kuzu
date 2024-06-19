#include "storage/local_storage/local_rel_table.h"

#include "storage/storage_utils.h"
#include "storage/store/rel_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

LocalRelNG::LocalRelNG(offset_t nodeGroupStartOffset, std::vector<LogicalType> dataTypes)
    : LocalNodeGroup{nodeGroupStartOffset, std::move(dataTypes)} {}

row_idx_t LocalRelNG::scanCSR(offset_t srcOffsetInChunk, offset_t posToReadForOffset,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(columnIDs.size() == outputVectors.size());
    std::vector<row_idx_t> rowIdxesToRead;
    rowIdxesToRead.reserve(DEFAULT_VECTOR_CAPACITY);
    auto& insertedRelOffsets = insertChunks.getRelOffsetsFromSrcOffset(srcOffsetInChunk);
    for (auto i = posToReadForOffset; i < insertedRelOffsets.size(); i++) {
        if (rowIdxesToRead.size() == DEFAULT_VECTOR_CAPACITY) {
            break;
        }
        rowIdxesToRead.push_back(insertChunks.getRowIdxFromOffset(insertedRelOffsets[i]));
    }
    for (auto i = 0u; i < columnIDs.size(); i++) {
        uint64_t posInOutputVector = 0;
        for (auto rowIdx : rowIdxesToRead) {
            insertChunks.readValueAtRowIdx(rowIdx, columnIDs[i], outputVectors[i],
                posInOutputVector++);
        }
    }
    auto numRelsRead = rowIdxesToRead.size();
    outputVectors[0]->state->getSelVectorUnsafe().setToUnfiltered(numRelsRead);
    return numRelsRead;
}

void LocalRelNG::applyLocalChangesToScannedVectors(offset_t srcOffset,
    const std::vector<column_id_t>& columnIDs, ValueVector* relIDVector,
    const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(columnIDs.size() == outputVectors.size());
    // Apply updates first, as applying deletions might change selected state.
    for (auto i = 0u; i < columnIDs.size(); ++i) {
        applyCSRUpdates(columnIDs[i], relIDVector, outputVectors[i]);
    }
    // Apply deletions and update selVector if necessary.
    applyCSRDeletions(srcOffset, relIDVector);
}

void LocalRelNG::applyCSRUpdates(column_id_t columnID, ValueVector* relIDVector,
    ValueVector* outputVector) {
    auto& updateChunk = updateChunks[columnID];
    for (auto i = 0u; i < relIDVector->state->getSelVector().getSelSize(); i++) {
        auto pos = relIDVector->state->getSelVector()[i];
        auto relOffset = relIDVector->getValue<relID_t>(pos).offset;
        if (updateChunk.hasOffset(relOffset)) {
            updateChunk.read(relOffset, 0, outputVector, pos);
        }
    }
}

void LocalRelNG::applyCSRDeletions(offset_t srcOffset, ValueVector* relIDVector) {
    if (deleteInfo.isEmpty(srcOffset)) {
        return;
    }
    auto selectPos = 0u;
    auto selVector = std::make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
    selVector->setToFiltered();
    for (auto i = 0u; i < relIDVector->state->getSelVector().getSelSize(); i++) {
        auto relIDPos = relIDVector->state->getSelVector()[i];
        auto relOffset = relIDVector->getValue<relID_t>(relIDPos).offset;
        if (deleteInfo.containsOffset(relOffset)) {
            continue;
        }
        selVector->operator[](selectPos++) = relIDPos;
    }
    if (selectPos != relIDVector->state->getSelVector().getSelSize()) {
        relIDVector->state->getSelVectorUnsafe().setToFiltered();
        memcpy(relIDVector->state->getSelVector().getSelectedPositions().data(),
            selVector->getSelectedPositions().data(), selectPos * sizeof(sel_t));
        relIDVector->state->getSelVectorUnsafe().setSelSize(selectPos);
    }
}

// nodeIDVectors: srcNodeIDVector, dstNodeIDVector.
bool LocalRelNG::insert(std::vector<ValueVector*> nodeIDVectors,
    std::vector<ValueVector*> propertyVectors) {
    KU_ASSERT(nodeIDVectors.size() == 2);
    auto srcNodeIDVector = nodeIDVectors[0];
    auto dstNodeIDVector = nodeIDVectors[1];
    KU_ASSERT(srcNodeIDVector->state->getSelVector().getSelSize() == 1 &&
              dstNodeIDVector->state->getSelVector().getSelSize() == 1);
    auto srcNodeIDPos = srcNodeIDVector->state->getSelVector()[0];
    auto dstNodeIDPos = dstNodeIDVector->state->getSelVector()[0];
    if (srcNodeIDVector->isNull(srcNodeIDPos) || dstNodeIDVector->isNull(dstNodeIDPos)) {
        return false;
    }
    auto srcNodeOffset =
        srcNodeIDVector->getValue<nodeID_t>(srcNodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(srcNodeOffset < StorageConstants::NODE_GROUP_SIZE);
    std::vector<ValueVector*> vectorsToInsert;
    vectorsToInsert.push_back(dstNodeIDVector);
    for (auto i = 0u; i < propertyVectors.size(); i++) {
        vectorsToInsert.push_back(propertyVectors[i]);
    }
    auto relIDPos = vectorsToInsert[LOCAL_REL_ID_COLUMN_ID]->state->getSelVector()[0];
    auto relOffset = vectorsToInsert[LOCAL_REL_ID_COLUMN_ID]->getValue<relID_t>(relIDPos).offset;
    insertChunks.append(srcNodeOffset, relOffset, vectorsToInsert);
    return true;
}

// IDVectors: srcNodeIDVector, relIDVector.
bool LocalRelNG::update(std::vector<ValueVector*> IDVectors, column_id_t columnID,
    ValueVector* propertyVector) {
    KU_ASSERT(IDVectors.size() == 2);
    auto srcNodeIDVector = IDVectors[0];
    auto relIDVector = IDVectors[1];
    KU_ASSERT(srcNodeIDVector->state->getSelVector().getSelSize() == 1 &&
              relIDVector->state->getSelVector().getSelSize() == 1);
    auto srcNodeIDPos = srcNodeIDVector->state->getSelVector()[0];
    auto relIDPos = relIDVector->state->getSelVector()[0];
    if (srcNodeIDVector->isNull(srcNodeIDPos) || relIDVector->isNull(relIDPos)) {
        return false;
    }
    auto srcNodeOffset =
        srcNodeIDVector->getValue<nodeID_t>(srcNodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(srcNodeOffset < StorageConstants::NODE_GROUP_SIZE && columnID < updateChunks.size());
    auto relOffset = relIDVector->getValue<relID_t>(relIDPos).offset;
    // Check if the rel is newly inserted or in persistent storage.
    if (insertChunks.hasOffset(relOffset)) {
        insertChunks.update(relOffset, columnID, propertyVector);
    } else {
        updateChunks[columnID].append(srcNodeOffset, relOffset, {propertyVector});
    }
    return true;
}

bool LocalRelNG::delete_(ValueVector* srcNodeVector, ValueVector* relIDVector) {
    KU_ASSERT(srcNodeVector->state->getSelVector().getSelSize() == 1 &&
              relIDVector->state->getSelVector().getSelSize() == 1);
    auto srcNodePos = srcNodeVector->state->getSelVector()[0];
    auto relIDPos = relIDVector->state->getSelVector()[0];
    if (srcNodeVector->isNull(srcNodePos) || relIDVector->isNull(relIDPos)) {
        return false;
    }
    auto srcNodeOffset =
        srcNodeVector->getValue<nodeID_t>(srcNodePos).offset - nodeGroupStartOffset;
    auto relOffset = relIDVector->getValue<relID_t>(relIDPos).offset;
    // If the rel is newly inserted, remove the rel from insertChunks.
    if (insertChunks.hasOffset(relOffset)) {
        insertChunks.remove(srcNodeOffset, relOffset);
        return true;
    }
    // If the rel is updated, remove the rel from updateChunks if exists.
    for (auto i = 0u; i < updateChunks.size(); i++) {
        if (updateChunks[i].hasOffset(relOffset)) {
            updateChunks[i].remove(srcNodeOffset, relOffset);
        }
    }
    if (!deleteInfo.deleteOffset(relOffset)) {
        return false;
    }
    deleteInfo.deleteRelAux(srcNodeOffset, relOffset);
    return true;
}

offset_t LocalRelNG::getNumInsertedRels(offset_t srcOffset) const {
    if (!insertChunks.hasRelOffsetsFromSrcOffset(srcOffset)) {
        return 0;
    }
    return insertChunks.getNumRelsFromSrcOffset(srcOffset);
}

void LocalRelNG::getChangesPerCSRSegment(std::vector<int64_t>& sizeChangesPerSegment,
    std::vector<bool>& hasChangesPerSegment) {
    auto numSegments = StorageConstants::NODE_GROUP_SIZE / StorageConstants::CSR_SEGMENT_SIZE;
    sizeChangesPerSegment.resize(numSegments, 0 /*initValue*/);
    hasChangesPerSegment.resize(numSegments, false /*initValue*/);
    for (auto& [srcOffset, insertions] : insertChunks.getSrcNodeOffsetToRelOffsets()) {
        auto segmentIdx = getSegmentIdx(srcOffset);
        sizeChangesPerSegment[segmentIdx] += insertions.size();
        hasChangesPerSegment[segmentIdx] = true;
    }
    for (auto& [srcOffset, deletions] : deleteInfo.getSrcNodeOffsetToRelOffsetVec()) {
        auto segmentIdx = getSegmentIdx(srcOffset);
        sizeChangesPerSegment[segmentIdx] -= deletions.size();
        hasChangesPerSegment[segmentIdx] = true;
    }
    for (auto& updateChunk : updateChunks) {
        for (auto& [srcOffset, _] : updateChunk.getSrcNodeOffsetToRelOffsets()) {
            auto segmentIdx = getSegmentIdx(srcOffset);
            hasChangesPerSegment[segmentIdx] = true;
        }
    }
}

LocalNodeGroup* LocalRelTableData::getOrCreateLocalNodeGroup(ValueVector* nodeIDVector) {
    auto nodeIDPos = nodeIDVector->state->getSelVector()[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!nodeGroups.contains(nodeGroupIdx)) {
        auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        nodeGroups[nodeGroupIdx] = std::make_unique<LocalRelNG>(nodeGroupStartOffset,
            common::LogicalType::copy(dataTypes));
    }
    return nodeGroups.at(nodeGroupIdx).get();
}

LocalRelTable::LocalRelTable(Table& table) : LocalTable{table} {
    auto& relTable = ku_dynamic_cast<Table&, RelTable&>(table);
    std::vector<LogicalType> types;
    types.reserve(relTable.getNumColumns());
    for (auto i = 0u; i < relTable.getNumColumns(); i++) {
        types.push_back(relTable.getColumn(i, RelDataDirection::FWD)->getDataType().copy());
    }
    // FWD and BWD local rel table data.
    auto tableID = relTable.getTableID();
    localTableDataCollection.push_back(
        std::make_unique<LocalRelTableData>(tableID, LogicalType::copy(types)));
    localTableDataCollection.push_back(
        std::make_unique<LocalRelTableData>(tableID, std::move(types)));
}

bool LocalRelTable::insert(TableInsertState& state) {
    auto& insertState = ku_dynamic_cast<TableInsertState&, RelTableInsertState&>(state);
    auto fwdIDVectors =
        std::vector<ValueVector*>{const_cast<ValueVector*>(&insertState.srcNodeIDVector),
            const_cast<ValueVector*>(&insertState.dstNodeIDVector)};
    auto bwdIDVectors =
        std::vector<ValueVector*>{const_cast<ValueVector*>(&insertState.dstNodeIDVector),
            const_cast<ValueVector*>(&insertState.srcNodeIDVector)};
    auto fwdInserted =
        getTableData(RelDataDirection::FWD)->insert(fwdIDVectors, insertState.propertyVectors);
    auto bwdInserted =
        getTableData(RelDataDirection::BWD)->insert(bwdIDVectors, insertState.propertyVectors);
    KU_ASSERT(fwdInserted == bwdInserted);
    return fwdInserted && bwdInserted;
}

bool LocalRelTable::update(TableUpdateState& updateState) {
    auto& state = ku_dynamic_cast<TableUpdateState&, RelTableUpdateState&>(updateState);
    auto fwdIDVectors = std::vector<ValueVector*>{const_cast<ValueVector*>(&state.srcNodeIDVector),
        const_cast<ValueVector*>(&state.relIDVector)};
    auto bwdIDVectors = std::vector<ValueVector*>{const_cast<ValueVector*>(&state.dstNodeIDVector),
        const_cast<ValueVector*>(&state.relIDVector)};
    auto fwdUpdated =
        getTableData(RelDataDirection::FWD)
            ->update(fwdIDVectors, state.columnID, const_cast<ValueVector*>(&state.propertyVector));
    auto bwdUpdated =
        getTableData(RelDataDirection::BWD)
            ->update(bwdIDVectors, state.columnID, const_cast<ValueVector*>(&state.propertyVector));
    KU_ASSERT(fwdUpdated == bwdUpdated);
    return fwdUpdated && bwdUpdated;
}

bool LocalRelTable::delete_(TableDeleteState& deleteState) {
    auto& state = ku_dynamic_cast<TableDeleteState&, RelTableDeleteState&>(deleteState);
    auto fwdDeleted = getTableData(RelDataDirection::FWD)
                          ->delete_(const_cast<ValueVector*>(&state.srcNodeIDVector),
                              const_cast<ValueVector*>(&state.relIDVector));
    auto bwdDeleted = getTableData(RelDataDirection::BWD)
                          ->delete_(const_cast<ValueVector*>(&state.dstNodeIDVector),
                              const_cast<ValueVector*>(&state.relIDVector));
    KU_ASSERT(fwdDeleted == bwdDeleted);
    return fwdDeleted && bwdDeleted;
}

} // namespace storage
} // namespace kuzu

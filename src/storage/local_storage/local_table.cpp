#include "storage/local_storage/local_table.h"

#include "storage/local_storage/local_node_table.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/store/column.h"
#include "storage/store/node_group.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

LocalNodeGroup::LocalNodeGroup(
    offset_t nodeGroupStartOffset, const std::vector<common::LogicalType>& dataTypes)
    : nodeGroupStartOffset{nodeGroupStartOffset}, insertChunks{LogicalType::copy(dataTypes)} {
    updateChunks.reserve(dataTypes.size());
    for (auto i = 0u; i < dataTypes.size(); i++) {
        std::vector<LogicalType> chunkCollectionTypes;
        chunkCollectionTypes.push_back(*dataTypes[i].copy());
        LocalChunkedGroupCollection localDataChunkCollection(std::move(chunkCollectionTypes));
        updateChunks.push_back(std::move(localDataChunkCollection));
    }
}

bool LocalNodeGroup::hasUpdatesOrDeletions() const {
    if (!deleteInfo.isEmpty()) {
        return true;
    }
    for (auto& updateChunk : updateChunks) {
        if (!updateChunk.isEmpty()) {
            return true;
        }
    }
    return false;
}

void LocalChunkedGroupCollection::readValueAtRowIdx(row_idx_t rowIdx, column_id_t columnID,
    ValueVector* outputVector, sel_t posInOutputVector) const {
    auto [chunkIdx, offsetInChunk] = getChunkIdxAndOffsetInChunk(rowIdx);
    auto& chunk = chunkedGroups.getChunkedGroup(chunkIdx)->getColumnChunk(columnID);
    chunk.lookup(offsetInChunk, *outputVector, posInOutputVector);
}

bool LocalChunkedGroupCollection::read(
    offset_t offset, column_id_t columnID, ValueVector* outputVector, sel_t posInOutputVector) {
    if (!offsetToRowIdx.contains(offset)) {
        return false;
    }
    auto rowIdx = offsetToRowIdx.at(offset);
    readValueAtRowIdx(rowIdx, columnID, outputVector, posInOutputVector);
    return true;
}

void LocalChunkedGroupCollection::update(
    offset_t offset, column_id_t columnID, ValueVector* propertyVector) {
    KU_ASSERT(offsetToRowIdx.contains(offset));
    auto rowIdx = offsetToRowIdx.at(offset);
    auto [chunkIdx, offsetInChunk] = getChunkIdxAndOffsetInChunk(rowIdx);
    auto chunk = chunkedGroups.getChunkedGroupUnsafe(chunkIdx)->getColumnChunkUnsafe(columnID);
    chunk->write(
        propertyVector, propertyVector->state->selVector->selectedPositions[0], offsetInChunk);
}

void LocalChunkedGroupCollection::remove(offset_t srcNodeOffset, offset_t relOffset) {
    KU_ASSERT(srcNodeOffsetToRelOffsets.contains(srcNodeOffset));
    remove(relOffset);
    offsetToRowIdx.erase(relOffset);
    auto& vec = srcNodeOffsetToRelOffsets.at(srcNodeOffset);
    vec.erase(std::remove(vec.begin(), vec.end(), relOffset), vec.end());
    if (vec.empty()) {
        srcNodeOffsetToRelOffsets.erase(srcNodeOffset);
    }
}

row_idx_t LocalChunkedGroupCollection::append(std::vector<ValueVector*> vectors) {
    KU_ASSERT(vectors.size() == dataTypes.size());
    if (chunkedGroups.getNumChunks() == 0 ||
        chunkedGroups.getChunkedGroup(chunkedGroups.getNumChunks() - 1)->getNumRows() ==
            CHUNK_CAPACITY) {
        chunkedGroups.append(std::make_unique<ChunkedNodeGroup>(
            dataTypes, false /*enableCompression*/, CHUNK_CAPACITY));
    }
    auto lastChunkGroup = chunkedGroups.getChunkedGroupUnsafe(chunkedGroups.getNumChunks() - 1);
    for (auto i = 0u; i < vectors.size(); i++) {
        KU_ASSERT(vectors[i]->state->selVector->selectedSize == 1);
        lastChunkGroup->getColumnChunkUnsafe(i)->append(vectors[i], *vectors[i]->state->selVector);
    }
    lastChunkGroup->setNumValues(lastChunkGroup->getNumRows() + 1);
    return numRows++;
}

bool LocalTableData::insert(
    std::vector<ValueVector*> nodeIDVectors, std::vector<ValueVector*> propertyVectors) {
    KU_ASSERT(nodeIDVectors.size() >= 1);
    auto localNodeGroup = getOrCreateLocalNodeGroup(nodeIDVectors[0]);
    return localNodeGroup->insert(nodeIDVectors, propertyVectors);
}

bool LocalTableData::update(
    std::vector<ValueVector*> nodeIDVectors, column_id_t columnID, ValueVector* propertyVector) {
    KU_ASSERT(nodeIDVectors.size() >= 1);
    auto localNodeGroup = getOrCreateLocalNodeGroup(nodeIDVectors[0]);
    return localNodeGroup->update(nodeIDVectors, columnID, propertyVector);
}

bool LocalTableData::delete_(ValueVector* nodeIDVector, ValueVector* extraVector) {
    auto localNodeGroup = getOrCreateLocalNodeGroup(nodeIDVector);
    return localNodeGroup->delete_(nodeIDVector, extraVector);
}

LocalTableData* LocalTable::getOrCreateLocalTableData(
    const std::vector<std::unique_ptr<Column>>& columns, vector_idx_t dataIdx,
    RelMultiplicity multiplicity) {
    if (localTableDataCollection.empty()) {
        std::vector<LogicalType> dataTypes;
        dataTypes.reserve(columns.size());
        for (auto& column : columns) {
            dataTypes.push_back(column->getDataType());
        }
        switch (tableType) {
        case TableType::NODE: {
            localTableDataCollection.reserve(1);
            localTableDataCollection.push_back(
                std::make_unique<LocalNodeTableData>(std::move(dataTypes)));
        } break;
        case TableType::REL: {
            KU_ASSERT(dataIdx < 2);
            localTableDataCollection.resize(2);
            localTableDataCollection[dataIdx] =
                std::make_unique<LocalRelTableData>(multiplicity, std::move(dataTypes));
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
    }
    KU_ASSERT(dataIdx < localTableDataCollection.size());
    if (!localTableDataCollection[dataIdx]) {
        KU_ASSERT(tableType == TableType::REL);
        std::vector<LogicalType> dataTypes;
        dataTypes.reserve(columns.size());
        for (auto& column : columns) {
            dataTypes.push_back(column->getDataType());
        }
        localTableDataCollection[dataIdx] =
            std::make_unique<LocalRelTableData>(multiplicity, std::move(dataTypes));
    }
    KU_ASSERT(localTableDataCollection[dataIdx] != nullptr);
    return localTableDataCollection[dataIdx].get();
}

} // namespace storage
} // namespace kuzu

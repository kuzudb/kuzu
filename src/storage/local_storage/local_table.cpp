#include "storage/local_storage/local_table.h"

#include "common/types/internal_id_t.h"
#include "storage/store/chunked_node_group.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

LocalNodeGroup::LocalNodeGroup(offset_t nodeGroupStartOffset,
    const std::vector<LogicalType>& dataTypes)
    : nodeGroupStartOffset{nodeGroupStartOffset}, insertChunks{LogicalType::copy(dataTypes)} {
    updateChunks.reserve(dataTypes.size());
    for (auto i = 0u; i < dataTypes.size(); i++) {
        std::vector<LogicalType> chunkCollectionTypes;
        chunkCollectionTypes.push_back(dataTypes[i].copy());
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

void LocalChunkedGroupCollection::appendChunkedGroup(ColumnChunkData* srcOffsetChunk,
    std::unique_ptr<ChunkedNodeGroup> chunkedGroup) {
    KU_ASSERT(chunkedGroup->getNumColumns() == dataTypes.size());
    for (auto i = 0u; i < chunkedGroup->getNumColumns(); i++) {
        KU_ASSERT(chunkedGroup->getColumnChunk(i).getDataType() == dataTypes[i]);
    }
    auto rowIdx = chunkedGroups.getNumChunkedGroups() * ChunkedNodeGroupCollection::CHUNK_CAPACITY;
    auto& relOffsetChunk = chunkedGroup->getColumnChunk(REL_ID_COLUMN_ID);
    for (auto i = 0u; i < srcOffsetChunk->getNumValues(); i++) {
        auto relOffset = relOffsetChunk.getValue<offset_t>(i);
        KU_ASSERT(!offsetToRowIdx.contains(relOffset));
        offsetToRowIdx[relOffset] = rowIdx++;
        auto srcOffset = srcOffsetChunk->getValue<offset_t>(i);
        srcNodeOffsetToRelOffsets[srcOffset].push_back(relOffset);
    }
    chunkedGroups.merge(std::move(chunkedGroup));
}

void LocalChunkedGroupCollection::readValueAtRowIdx(row_idx_t rowIdx, column_id_t columnID,
    ValueVector* outputVector, sel_t posInOutputVector) const {
    auto [chunkIdx, offsetInChunk] = getChunkIdxAndOffsetInChunk(rowIdx);
    auto& chunk = chunkedGroups.getChunkedGroup(chunkIdx)->getColumnChunk(columnID);
    chunk.lookup(offsetInChunk, *outputVector, posInOutputVector);
}

bool LocalChunkedGroupCollection::read(offset_t offset, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors, offset_t offsetInOutputVector) const {
    if (!offsetToRowIdx.contains(offset)) {
        return false;
    }
    const auto rowIdx = offsetToRowIdx.at(offset);
    for (auto i = 0u; i < columnIDs.size(); i++) {
        const auto posInOutputVector =
            outputVectors[i]->state->getSelVector()[offsetInOutputVector];
        if (columnIDs[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setNull(posInOutputVector, true);
            continue;
        }
        readValueAtRowIdx(rowIdx, columnIDs[i], outputVectors[i], posInOutputVector);
    }
    return true;
}

bool LocalChunkedGroupCollection::read(offset_t offset, column_id_t columnID,
    ValueVector* outputVector, sel_t posInOutputVector) const {
    if (!offsetToRowIdx.contains(offset)) {
        return false;
    }
    const auto rowIdx = offsetToRowIdx.at(offset);
    readValueAtRowIdx(rowIdx, columnID, outputVector, posInOutputVector);
    return true;
}

void LocalChunkedGroupCollection::update(offset_t offset, column_id_t columnID,
    ValueVector* propertyVector) {
    KU_ASSERT(offsetToRowIdx.contains(offset));
    const auto rowIdx = offsetToRowIdx.at(offset);
    auto [chunkIdx, offsetInChunk] = getChunkIdxAndOffsetInChunk(rowIdx);
    auto& chunk = chunkedGroups.getChunkedGroupUnsafe(chunkIdx)->getColumnChunkUnsafe(columnID);
    const auto pos = propertyVector->state->getSelVector()[0];
    chunk.write(propertyVector, pos, offsetInChunk);
}

void LocalChunkedGroupCollection::remove(offset_t srcNodeOffset, offset_t relOffset) {
    KU_ASSERT(srcNodeOffsetToRelOffsets.contains(srcNodeOffset));
    remove(relOffset);
    offsetToRowIdx.erase(relOffset);
    auto& vec = srcNodeOffsetToRelOffsets.at(srcNodeOffset);
    std::erase(vec, relOffset);
    if (vec.empty()) {
        srcNodeOffsetToRelOffsets.erase(srcNodeOffset);
    }
}

ChunkedNodeGroup* LocalChunkedGroupCollection::getLastChunkedGroupAndAddNewGroupIfNecessary() {
    if (chunkedGroups.getNumChunkedGroups() == 0 ||
        chunkedGroups.getChunkedGroup(chunkedGroups.getNumChunkedGroups() - 1)->getNumRows() ==
            ChunkedNodeGroupCollection::CHUNK_CAPACITY) {
        chunkedGroups.merge(std::make_unique<ChunkedNodeGroup>(dataTypes,
            false /*enableCompression*/, ChunkedNodeGroupCollection::CHUNK_CAPACITY));
    }
    return chunkedGroups.getChunkedGroupUnsafe(chunkedGroups.getNumChunkedGroups() - 1);
}

row_idx_t LocalChunkedGroupCollection::append(const std::vector<ValueVector*>& vectors) {
    KU_ASSERT(vectors.size() == dataTypes.size());
    const auto lastChunkGroup = getLastChunkedGroupAndAddNewGroupIfNecessary();
    for (auto i = 0u; i < vectors.size(); i++) {
        KU_ASSERT(vectors[i]->state->getSelVector().getSelSize() == 1);
        lastChunkGroup->getColumnChunkUnsafe(i).append(vectors[i],
            vectors[i]->state->getSelVector());
    }
    lastChunkGroup->setNumRows(lastChunkGroup->getNumRows() + 1);
    return numRows++;
}

void LocalChunkedGroupCollection::append(offset_t offset, ChunkedNodeGroup* nodeGroup,
    offset_t numValues) {
    KU_ASSERT(nodeGroup->getNumColumns() == dataTypes.size());
    offset_t appended = 0;
    do {
        const auto lastChunkGroup = getLastChunkedGroupAndAddNewGroupIfNecessary();
        const auto appendedInChunk =
            lastChunkGroup->append(nodeGroup, appended, numValues - appended);
        for (size_t i = 0; i < appendedInChunk; i++) {
            offsetToRowIdx[offset + appended + i] = numRows++;
        }
        appended += appendedInChunk;
    } while (appended < numValues);
}

bool LocalTableData::insert(std::vector<ValueVector*> nodeIDVectors,
    std::vector<ValueVector*> propertyVectors) {
    KU_ASSERT(nodeIDVectors.size() >= 1);
    const auto localNodeGroup = getOrCreateLocalNodeGroup(nodeIDVectors[0]);
    return localNodeGroup->insert(nodeIDVectors, propertyVectors);
}

bool LocalTableData::update(std::vector<ValueVector*> nodeIDVectors, column_id_t columnID,
    ValueVector* propertyVector) {
    KU_ASSERT(nodeIDVectors.size() >= 1);
    const auto localNodeGroup = getOrCreateLocalNodeGroup(nodeIDVectors[0]);
    return localNodeGroup->update(nodeIDVectors, columnID, propertyVector);
}

bool LocalTableData::delete_(ValueVector* nodeIDVector, ValueVector* extraVector) {
    const auto localNodeGroup = getOrCreateLocalNodeGroup(nodeIDVector);
    return localNodeGroup->delete_(nodeIDVector, extraVector);
}

} // namespace storage
} // namespace kuzu

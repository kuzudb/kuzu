#include "storage/store/chunked_node_group_collection.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void ChunkedNodeGroupCollection::append(const std::vector<ValueVector*>& vectors,
    const SelectionVector& selVector) {
    if (chunkedGroups.empty()) {
        chunkedGroups.push_back(std::make_unique<ChunkedNodeGroup>(types,
            false /*enableCompression*/, CHUNK_CAPACITY, 0 /*startOffset*/));
    }
    const auto numRowsToAppend = selVector.getSelSize();
    row_idx_t numRowsAppended = 0;
    SelectionVector tmpSelVector(numRowsToAppend);
    while (numRowsAppended < numRowsToAppend) {
        const auto& lastChunkedGroup = chunkedGroups.back();
        const auto numRowsToAppendInGroup = std::min(numRowsToAppend - numRowsAppended,
            CHUNK_CAPACITY - lastChunkedGroup->getNumRows());
        auto tmpSelVectorBuffer = tmpSelVector.getMultableBuffer();
        for (auto i = 0u; i < numRowsToAppendInGroup; i++) {
            tmpSelVectorBuffer[i] = selVector[numRowsAppended + i];
        }
        tmpSelVector.setToFiltered(numRowsToAppendInGroup);
        lastChunkedGroup->append(vectors, tmpSelVector, numRowsToAppendInGroup);
        if (lastChunkedGroup->isFull()) {
            auto startOffset = getNumRows();
            chunkedGroups.push_back(std::make_unique<ChunkedNodeGroup>(types,
                false /*enableCompression*/, CHUNK_CAPACITY, startOffset));
        }
        numRowsAppended += numRowsToAppendInGroup;
    }
}

void ChunkedNodeGroupCollection::append(Transaction*, const ChunkedNodeGroup& chunkedGroup) {
    if (chunkedGroups.empty()) {
        chunkedGroups.push_back(std::make_unique<ChunkedNodeGroup>(types,
            false /*enableCompression*/, CHUNK_CAPACITY, 0 /*startOffset*/));
    }
    row_idx_t numRowsAppended = 0u;
    row_idx_t numRowsToAppend = chunkedGroup.getNumRows();
    while (numRowsAppended < numRowsToAppend) {
        if (chunkedGroups.back()->isFull()) {
            chunkedGroups.push_back(std::make_unique<ChunkedNodeGroup>(types,
                false /*enableCompression*/, CHUNK_CAPACITY, getNumRows()));
        }
        const auto& chunkedGroupToCopyInto = chunkedGroups.back();
        KU_ASSERT(CHUNK_CAPACITY >= chunkedGroupToCopyInto->getNumRows());
        auto numToCopyIntoChunk = CHUNK_CAPACITY - chunkedGroupToCopyInto->getNumRows();
        const auto numToAppendInChunk = std::min(chunkedGroup.getNumRows(), numToCopyIntoChunk);
        chunkedGroupToCopyInto->append(chunkedGroup, 0, numToAppendInChunk);
        numRowsAppended += numToAppendInChunk;
    }
}

void ChunkedNodeGroupCollection::append(const ChunkedNodeGroupCollection& other, offset_t offset,
    offset_t numRowsToAppend) {
    if (chunkedGroups.empty()) {
        chunkedGroups.push_back(std::make_unique<ChunkedNodeGroup>(types,
            false /*enableCompression*/, CHUNK_CAPACITY, 0 /*startOffset*/));
    }
    row_idx_t numRowsAppended = 0u;
    while (numRowsAppended < numRowsToAppend) {
        const auto chunkIdx = offset / CHUNK_CAPACITY;
        const auto offsetInChunk = offset % CHUNK_CAPACITY;
        auto& chunkedGroupToCopyFrom = other.getChunkedGroup(chunkIdx);
        auto numToCopyFromChunk = chunkedGroupToCopyFrom.getNumRows() - offsetInChunk;
        if (chunkedGroups.back()->isFull()) {
            chunkedGroups.push_back(std::make_unique<ChunkedNodeGroup>(types,
                false /*enableCompression*/, CHUNK_CAPACITY, getNumRows()));
        }
        const auto& chunkedGroupToCopyInto = chunkedGroups.back();
        auto numToCopyIntoChunk = CHUNK_CAPACITY - chunkedGroupToCopyInto->getNumRows();
        const auto numToAppendInChunk = std::min(numToCopyFromChunk, numToCopyIntoChunk);
        chunkedGroupToCopyInto->append(chunkedGroupToCopyFrom, offsetInChunk, numToAppendInChunk);
        numRowsAppended += numToAppendInChunk;
        offset += numToAppendInChunk;
    }
}

void ChunkedNodeGroupCollection::merge(std::unique_ptr<ChunkedNodeGroup> chunkedGroup) {
    KU_ASSERT(chunkedGroup->getNumColumns() == types.size());
    for (auto i = 0u; i < chunkedGroup->getNumColumns(); i++) {
        KU_ASSERT(chunkedGroup->getColumnChunk(i).getDataType() == types[i]);
    }
    chunkedGroups.push_back(std::move(chunkedGroup));
}

void ChunkedNodeGroupCollection::merge(ChunkedNodeGroupCollection& other) {
    chunkedGroups.reserve(chunkedGroups.size() + other.chunkedGroups.size());
    for (auto& chunkedGroup : other.chunkedGroups) {
        merge(std::move(chunkedGroup));
    }
}

row_idx_t ChunkedNodeGroupCollection::getNumRows() const {
    row_idx_t numRows = 0;
    for (auto& chunkedGroup : chunkedGroups) {
        numRows += chunkedGroup->getNumRows();
    }
    return numRows;
}

} // namespace storage
} // namespace kuzu

#include "storage/store/chunked_node_group_collection.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

ChunkedNodeGroup& ChunkedNodeGroupCollection::findChunkedGroupFromOffset(
    const offset_t offset) const {
    KU_ASSERT(offset < getNumRows());
    for (const auto& chunkedGroup : chunkedGroups) {
        if (chunkedGroup->getStartNodeOffset() <= offset &&
            chunkedGroup->getStartNodeOffset() + chunkedGroup->getNumRows() >= offset) {
            return *chunkedGroup;
        }
    }
    KU_UNREACHABLE;
}

row_idx_t ChunkedNodeGroupCollection::append(const std::vector<ValueVector*>& vectors,
    const SelectionVector& selVector) {
    const auto numRowsBeforeAppend = getNumRows();
    if (chunkedGroups.empty()) {
        chunkedGroups.push_back(
            std::make_unique<ChunkedNodeGroup>(types, false /*enableCompression*/,
                ChunkedNodeGroup::CHUNK_CAPACITY, 0 /*startOffset*/, residencyState));
    }
    const auto numRowsToAppend = selVector.getSelSize();
    row_idx_t numRowsAppended = 0;
    SelectionVector tmpSelVector(numRowsToAppend);
    while (numRowsAppended < numRowsToAppend) {
        if (chunkedGroups.back()->isFullOrOnDisk()) {
            auto startOffset = getNumRows();
            chunkedGroups.push_back(
                std::make_unique<ChunkedNodeGroup>(types, false /*enableCompression*/,
                    ChunkedNodeGroup::CHUNK_CAPACITY, startOffset, residencyState));
        }
        const auto& lastChunkedGroup = chunkedGroups.back();
        const auto numRowsToAppendInGroup = std::min(numRowsToAppend - numRowsAppended,
            ChunkedNodeGroup::CHUNK_CAPACITY - lastChunkedGroup->getNumRows());
        auto tmpSelVectorBuffer = tmpSelVector.getMultableBuffer();
        for (auto i = 0u; i < numRowsToAppendInGroup; i++) {
            tmpSelVectorBuffer[i] = selVector[numRowsAppended + i];
        }
        tmpSelVector.setToFiltered(numRowsToAppendInGroup);
        lastChunkedGroup->append(vectors, tmpSelVector, numRowsToAppendInGroup);
        numRowsAppended += numRowsToAppendInGroup;
    }
    return numRowsBeforeAppend;
}

row_idx_t ChunkedNodeGroupCollection::append(const ChunkedNodeGroup& chunkedGroup,
    row_idx_t numRowsToAppend) {
    const auto numRowsBeforeAppend = getNumRows();
    if (chunkedGroups.empty()) {
        chunkedGroups.push_back(
            std::make_unique<ChunkedNodeGroup>(types, false /*enableCompression*/,
                ChunkedNodeGroup::CHUNK_CAPACITY, 0 /*startOffset*/, residencyState));
    }
    row_idx_t numRowsAppended = 0u;
    while (numRowsAppended < numRowsToAppend) {
        if (chunkedGroups.back()->isFullOrOnDisk()) {
            chunkedGroups.push_back(
                std::make_unique<ChunkedNodeGroup>(types, false /*enableCompression*/,
                    ChunkedNodeGroup::CHUNK_CAPACITY, getNumRows(), residencyState));
        }
        const auto& chunkedGroupToCopyInto = chunkedGroups.back();
        KU_ASSERT(ChunkedNodeGroup::CHUNK_CAPACITY >= chunkedGroupToCopyInto->getNumRows());
        auto numToCopyIntoChunk =
            ChunkedNodeGroup::CHUNK_CAPACITY - chunkedGroupToCopyInto->getNumRows();
        const auto numToAppendInChunk = std::min(chunkedGroup.getNumRows(), numToCopyIntoChunk);
        chunkedGroupToCopyInto->append(chunkedGroup, 0, numToAppendInChunk);
        numRowsAppended += numToAppendInChunk;
    }
    return numRowsBeforeAppend;
}

row_idx_t ChunkedNodeGroupCollection::append(const ChunkedNodeGroupCollection& other,
    offset_t offsetInOtherCollection, const offset_t numRowsToAppend) {
    const auto numRowsBeforeAppend = getNumRows();
    if (chunkedGroups.empty()) {
        chunkedGroups.push_back(
            std::make_unique<ChunkedNodeGroup>(types, false /*enableCompression*/,
                ChunkedNodeGroup::CHUNK_CAPACITY, 0 /*startOffset*/, residencyState));
    }
    row_idx_t numRowsAppended = 0u;
    while (numRowsAppended < numRowsToAppend) {
        const auto chunkIdx = offsetInOtherCollection / ChunkedNodeGroup::CHUNK_CAPACITY;
        const auto offsetInChunk = offsetInOtherCollection % ChunkedNodeGroup::CHUNK_CAPACITY;
        auto& chunkedGroupToCopyFrom = other.getChunkedGroup(chunkIdx);
        auto numToCopyFromChunk = chunkedGroupToCopyFrom.getNumRows() - offsetInChunk;
        if (chunkedGroups.back()->isFullOrOnDisk()) {
            chunkedGroups.push_back(
                std::make_unique<ChunkedNodeGroup>(types, false /*enableCompression*/,
                    ChunkedNodeGroup::CHUNK_CAPACITY, getNumRows(), residencyState));
        }
        const auto& chunkedGroupToCopyInto = chunkedGroups.back();
        auto numToCopyIntoChunk =
            ChunkedNodeGroup::CHUNK_CAPACITY - chunkedGroupToCopyInto->getNumRows();
        const auto numToAppendInChunk = std::min(numToCopyFromChunk, numToCopyIntoChunk);
        chunkedGroupToCopyInto->append(chunkedGroupToCopyFrom, offsetInChunk, numToAppendInChunk);
        numRowsAppended += numToAppendInChunk;
        offsetInOtherCollection += numToAppendInChunk;
    }
    return numRowsBeforeAppend;
}

void ChunkedNodeGroupCollection::merge(std::unique_ptr<ChunkedNodeGroup> chunkedGroup) {
    KU_ASSERT(chunkedGroup->getNumColumns() == types.size());
    for (auto i = 0u; i < chunkedGroup->getNumColumns(); i++) {
        KU_ASSERT(chunkedGroup->getColumnChunk(i).getDataType() == types[i]);
    }
    chunkedGroups.push_back(std::move(chunkedGroup));
    // TODO: Update version info.
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

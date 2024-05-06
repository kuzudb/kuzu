#include "storage/store/chunked_node_group_collection.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void ChunkedNodeGroupCollection::append(const std::vector<ValueVector*>& vectors,
    const SelectionVector& selVector) {
    if (chunkedGroups.empty()) {
        chunkedGroups.push_back(
            std::make_unique<ChunkedNodeGroup>(types, false /*enableCompression*/, CHUNK_CAPACITY));
    }
    auto numRowsToAppend = selVector.getSelSize();
    row_idx_t numRowsAppended = 0;
    SelectionVector tmpSelVector(numRowsToAppend);
    while (numRowsAppended < numRowsToAppend) {
        auto& lastChunkedGroup = chunkedGroups.back();
        auto numRowsToAppendInGroup = std::min(numRowsToAppend - numRowsAppended,
            static_cast<row_idx_t>(CHUNK_CAPACITY - lastChunkedGroup->getNumRows()));
        auto tmpSelVectorBuffer = tmpSelVector.getMultableBuffer();
        for (auto i = 0u; i < numRowsToAppendInGroup; i++) {
            tmpSelVectorBuffer[i] = selVector[numRowsAppended + i];
        }
        tmpSelVector.setToFiltered(numRowsToAppendInGroup);
        lastChunkedGroup->append(vectors, tmpSelVector, numRowsToAppendInGroup);
        if (lastChunkedGroup->getNumRows() == CHUNK_CAPACITY) {
            chunkedGroups.push_back(std::make_unique<ChunkedNodeGroup>(types,
                false /*enableCompression*/, CHUNK_CAPACITY));
        }
        numRowsAppended += numRowsToAppendInGroup;
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

} // namespace storage
} // namespace kuzu

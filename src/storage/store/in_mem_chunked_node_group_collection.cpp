#include "storage/store/in_mem_chunked_node_group_collection.h"

#include "storage/buffer_manager/memory_manager.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void InMemChunkedNodeGroupCollection::append(MemoryManager& memoryManager,
    const std::vector<ValueVector*>& vectors, row_idx_t startRowInVectors,
    row_idx_t numRowsToAppend) {
    if (chunkedGroups.empty()) {
        chunkedGroups.push_back(std::make_unique<ChunkedNodeGroup>(memoryManager, types,
            false /*enableCompression*/, common::StorageConfig::CHUNKED_NODE_GROUP_CAPACITY,
            0 /*startOffset*/, ResidencyState::IN_MEMORY));
    }
    row_idx_t numRowsAppended = 0;
    while (numRowsAppended < numRowsToAppend) {
        auto& lastChunkedGroup = chunkedGroups.back();
        auto numRowsToAppendInGroup = std::min(numRowsToAppend - numRowsAppended,
            common::StorageConfig::CHUNKED_NODE_GROUP_CAPACITY - lastChunkedGroup->getNumRows());
        lastChunkedGroup->append(&DUMMY_TRANSACTION, vectors, startRowInVectors,
            numRowsToAppendInGroup);
        if (lastChunkedGroup->getNumRows() == common::StorageConfig::CHUNKED_NODE_GROUP_CAPACITY) {
            lastChunkedGroup->setUnused(memoryManager);
            chunkedGroups.push_back(std::make_unique<ChunkedNodeGroup>(memoryManager, types,
                false /*enableCompression*/, common::StorageConfig::CHUNKED_NODE_GROUP_CAPACITY,
                0 /* startRowIdx */, ResidencyState::IN_MEMORY));
        }
        numRowsAppended += numRowsToAppendInGroup;
    }
}

void InMemChunkedNodeGroupCollection::merge(std::unique_ptr<ChunkedNodeGroup> chunkedGroup) {
    KU_ASSERT(chunkedGroup->getNumColumns() == types.size());
    for (auto i = 0u; i < chunkedGroup->getNumColumns(); i++) {
        KU_ASSERT(chunkedGroup->getColumnChunk(i).getDataType() == types[i]);
    }
    chunkedGroups.push_back(std::move(chunkedGroup));
}

void InMemChunkedNodeGroupCollection::merge(InMemChunkedNodeGroupCollection& other) {
    chunkedGroups.reserve(chunkedGroups.size() + other.chunkedGroups.size());
    for (auto& chunkedGroup : other.chunkedGroups) {
        merge(std::move(chunkedGroup));
    }
}

} // namespace storage
} // namespace kuzu

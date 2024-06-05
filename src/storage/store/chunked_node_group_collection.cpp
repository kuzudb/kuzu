#include "storage/store/chunked_node_group_collection.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

ChunkedNodeGroup& ChunkedNodeGroupCollection::findChunkedGroupFromOffset(const offset_t offset) {
    const auto lock = chunkedGroups.lock();
    KU_ASSERT(offset < getNumRows(lock));
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        if (chunkedGroup->getStartRowIdx() <= offset &&
            chunkedGroup->getStartRowIdx() + chunkedGroup->getNumRows() >= offset) {
            return *chunkedGroup;
        }
    }
    KU_UNREACHABLE;
}

row_idx_t ChunkedNodeGroupCollection::append(Transaction* transaction,
    const std::vector<ValueVector*>& vectors, row_idx_t startRowInVectors,
    row_idx_t numRowsToAppend) {
    const auto lock = chunkedGroups.lock();
    const auto numRowsBeforeAppend = getNumRows(lock);
    if (chunkedGroups.isEmpty(lock)) {
        chunkedGroups.appendGroup(lock,
            std::make_unique<ChunkedNodeGroup>(types, false /*enableCompression*/,
                ChunkedNodeGroup::CHUNK_CAPACITY, 0 /*startOffset*/, residencyState));
    }
    row_idx_t numRowsAppended = 0;
    while (numRowsAppended < numRowsToAppend) {
        if (chunkedGroups.getLastGroup(lock)->isFullOrOnDisk()) {
            chunkedGroups.appendGroup(lock,
                std::make_unique<ChunkedNodeGroup>(types, false /*enableCompression*/,
                    ChunkedNodeGroup::CHUNK_CAPACITY, numRowsBeforeAppend + numRowsAppended,
                    residencyState));
        }
        const auto& lastChunkedGroup = chunkedGroups.getLastGroup(lock);
        const auto numRowsToAppendInGroup = std::min(numRowsToAppend - numRowsAppended,
            ChunkedNodeGroup::CHUNK_CAPACITY - lastChunkedGroup->getNumRows());
        lastChunkedGroup->append(transaction, vectors, startRowInVectors + numRowsAppended,
            numRowsToAppendInGroup);
        numRowsAppended += numRowsToAppendInGroup;
    }
    return numRowsBeforeAppend;
}

row_idx_t ChunkedNodeGroupCollection::append(Transaction* transaction,
    const ChunkedNodeGroup& chunkedGroup, row_idx_t numRowsToAppend) {
    const auto lock = chunkedGroups.lock();
    const auto numRowsBeforeAppend = getNumRows(lock);
    if (chunkedGroups.isEmpty(lock)) {
        chunkedGroups.appendGroup(lock,
            std::make_unique<ChunkedNodeGroup>(types, false /*enableCompression*/,
                ChunkedNodeGroup::CHUNK_CAPACITY, 0 /*startOffset*/, residencyState));
    }
    row_idx_t numRowsAppended = 0u;
    while (numRowsAppended < numRowsToAppend) {
        if (chunkedGroups.getLastGroup(lock)->isFullOrOnDisk()) {
            chunkedGroups.appendGroup(lock,
                std::make_unique<ChunkedNodeGroup>(types, false /*enableCompression*/,
                    ChunkedNodeGroup::CHUNK_CAPACITY, numRowsBeforeAppend + numRowsAppended,
                    residencyState));
        }
        const auto& chunkedGroupToCopyInto = chunkedGroups.getLastGroup(lock);
        KU_ASSERT(ChunkedNodeGroup::CHUNK_CAPACITY >= chunkedGroupToCopyInto->getNumRows());
        auto numToCopyIntoChunk =
            ChunkedNodeGroup::CHUNK_CAPACITY - chunkedGroupToCopyInto->getNumRows();
        const auto numToAppendInChunk = std::min(chunkedGroup.getNumRows(), numToCopyIntoChunk);
        chunkedGroupToCopyInto->append(transaction, chunkedGroup, 0, numToAppendInChunk);
        numRowsAppended += numToAppendInChunk;
    }
    return numRowsBeforeAppend;
}

void ChunkedNodeGroupCollection::merge(std::unique_ptr<ChunkedNodeGroup> chunkedGroup) {
    KU_ASSERT(chunkedGroup->getNumColumns() == types.size());
    for (auto i = 0u; i < chunkedGroup->getNumColumns(); i++) {
        KU_ASSERT(chunkedGroup->getColumnChunk(i).getDataType() == types[i]);
    }
    const auto lock = chunkedGroups.lock();
    chunkedGroups.appendGroup(lock, std::move(chunkedGroup));
}

void ChunkedNodeGroupCollection::merge(std::unique_ptr<ChunkedNodeGroupCollection> other) {
    const auto lock = other->chunkedGroups.lock();
    auto otherNumGroups = other->chunkedGroups.getNumGroups(lock);
    for (auto i = 0u; i < otherNumGroups; i++) {
        merge(other->chunkedGroups.moveGroup(lock, i));
    }
    other->chunkedGroups.clear(lock);
}

row_idx_t ChunkedNodeGroupCollection::getNumRows(const UniqLock& lock) {
    KU_ASSERT(lock.isLocked());
    KU_UNUSED(lock);
    row_idx_t numRows = 0;
    for (auto& chunkedGroup : chunkedGroups.getAllGroups(lock)) {
        numRows += chunkedGroup->getNumRows();
    }
    return numRows;
}

} // namespace storage
} // namespace kuzu

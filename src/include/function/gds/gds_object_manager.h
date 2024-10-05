#pragma once

#include <atomic>

#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace function {

// ObjectBlock represents a pre-allocated amount of memory that can hold up to maxElements objects
// ObjectBlock should be accessed by a single thread.
template<typename T>
class ObjectBlock {
public:
    ObjectBlock(std::unique_ptr<storage::MemoryBuffer> block, uint64_t sizeInBytes)
        : block{std::move(block)} {
        maxElements.store(sizeInBytes / (sizeof(T)), std::memory_order_relaxed);
        nextPosToWrite.store(0, std::memory_order_relaxed);
    }

    T* reserveNext() {
        auto result = getData() + nextPosToWrite.load(std::memory_order_relaxed);
        nextPosToWrite.fetch_add(1, std::memory_order_relaxed);
        return result;
    }
    void revertLast() { nextPosToWrite.fetch_sub(1, std::memory_order_relaxed); }

    bool hasSpace() const {
        return nextPosToWrite.load(std::memory_order_relaxed) <
               maxElements.load(std::memory_order_relaxed);
    }

private:
    T* getData() const { return reinterpret_cast<T*>(block->getData()); }

private:
    std::unique_ptr<storage::MemoryBuffer> block;
    std::atomic<uint64_t> maxElements;
    std::atomic<uint64_t> nextPosToWrite;
};

// ObjectArraysMap represents a pre-allocated amount of object per tableID.
template<typename T>
class ObjectArraysMap {
public:
    void allocate(common::table_id_t tableID, common::offset_t numNodes,
        storage::MemoryManager* mm) {
        auto buffer = mm->allocateBuffer(false, numNodes * sizeof(T));
        bufferPerTable.insert({tableID, std::move(buffer)});
    }

    bool contains(common::table_id_t tableID) const { return bufferPerTable.contains(tableID); }

    T* getData(common::table_id_t tableID) const {
        KU_ASSERT(bufferPerTable.contains(tableID));
        return reinterpret_cast<T*>(bufferPerTable.at(tableID)->getData());
    }

private:
    common::table_id_map_t<std::unique_ptr<storage::MemoryBuffer>> bufferPerTable;
};

} // namespace function
} // namespace kuzu

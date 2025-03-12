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

    T* reserveNext() { return getData() + nextPosToWrite.fetch_add(1, std::memory_order_relaxed); }
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

// Pre-allocated array of objects.
template<typename T>
class ObjectArray {
public:
    ObjectArray(const common::offset_t size, storage::MemoryManager* mm,
        bool initializeToZero = false)
        : allocation{mm->allocateBuffer(initializeToZero, size * sizeof(T))} {
        data = std::span<T>(reinterpret_cast<T*>(allocation->getData()), size);
    }

    void set(const common::offset_t pos, const T value) {
        KU_ASSERT(pos < data.size());
        data[pos] = value;
    }

    T get(const common::offset_t pos) {
        KU_ASSERT(pos < data.size());
        return data[pos];
    }

private:
    template<typename U>
    friend class AtomicObjectArray;
    std::span<T> data;
    std::unique_ptr<storage::MemoryBuffer> allocation;
};

// Pre-allocated array of atomic objects.
template<typename T>
class AtomicObjectArray {
public:
    AtomicObjectArray(const common::offset_t size, storage::MemoryManager* mm,
        bool initializeToZero = false)
        : array{ObjectArray<std::atomic<T>>(size, mm, initializeToZero)} {}

    void setRelaxed(common::offset_t pos, const T& value) {
        KU_ASSERT(pos < array.data.size());
        array.data[pos].store(value, std::memory_order_relaxed);
    }

    T getRelaxed(const common::offset_t pos) {
        KU_ASSERT(pos < array.data.size());
        return array.data[pos].load(std::memory_order_relaxed);
    }

    bool compare_exchange_strong_max(const common::offset_t src, const common::offset_t dest) {
        auto srcValue = getRelaxed(src);
        auto dstValue = getRelaxed(dest);
        while (dstValue < srcValue) {
            if (array.data[dest].compare_exchange_strong(dstValue, srcValue)) {
                return true;
            }
        }
        return false;
    }

private:
    ObjectArray<std::atomic<T>> array;
};

// ObjectArraysMap represents a pre-allocated amount of object per tableID.
template<typename T>
class ObjectArraysMap {
public:
    void allocate(common::table_id_t tableID, common::offset_t maxOffset,
        storage::MemoryManager* mm) {
        auto buffer = mm->allocateBuffer(false, maxOffset * sizeof(T));
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

#pragma once

#include <atomic>

#include "storage/buffer_manager/memory_manager.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {

// ObjectBlock represents a pre-allocated amount of memory that can hold up to maxElements objects
// ObjectBlock should be accessed by a single thread.
template<typename T>
class ObjectBlock {
public:
    ObjectBlock(unique_ptr<storage::MemoryBuffer> block, uint64_t sizeInBytes)
        : block{move(block)} {
        maxElements.store(sizeInBytes / (sizeof(T)), memory_order_relaxed);
        nextPosToWrite.store(0, memory_order_relaxed);
    }

    T* reserveNext() { return getData() + nextPosToWrite.fetch_add(1, memory_order_relaxed); }
    void revertLast() { nextPosToWrite.fetch_sub(1, memory_order_relaxed); }

    bool hasSpace() const {
        return nextPosToWrite.load(memory_order_relaxed) < maxElements.load(memory_order_relaxed);
    }

private:
    T* getData() const { return reinterpret_cast<T*>(block->getData()); }

private:
    unique_ptr<storage::MemoryBuffer> block;
    atomic<uint64_t> maxElements;
    atomic<uint64_t> nextPosToWrite;
};

// Pre-allocated array of objects.
template<typename T>
class ObjectArray {
public:
    ObjectArray() : size{0} {}
    ObjectArray(const offset_t size, storage::MemoryManager* mm, bool initializeToZero = false)
        : size{size}, mm{mm} {
        allocate(size, mm, initializeToZero);
    }

    void allocate(const offset_t size, storage::MemoryManager* mm, bool initializeToZero) {
        allocation = mm->allocateBuffer(initializeToZero, size * sizeof(T));
        data = span<T>(reinterpret_cast<T*>(allocation->getData()), size);
        this->size = size;
    }

    offset_t getSize() const { return size; }

    void resizeAsNew(const offset_t newSize, storage::MemoryManager* mm) {
        if (newSize > size) {
            allocate(newSize, mm, false /* initializeToZero */);
        }
    }

    void set(const offset_t pos, const T value) {
        KU_ASSERT(pos < size);
        data[pos] = value;
    }

    T get(const offset_t pos) {
        KU_ASSERT(pos < size);
        return data[pos];
    }

    T& getRef(const offset_t pos) {
        KU_ASSERT(pos < size);
        return data[pos];
    }

private:
    template<typename U>
    friend class AtomicObjectArray;
    offset_t size;
    span<T> data;
    unique_ptr<storage::MemoryBuffer> allocation;
    storage::MemoryManager* mm = nullptr;
};

// Pre-allocated array of atomic objects.
template<typename T>
class AtomicObjectArray {
public:
    AtomicObjectArray() = default;
    AtomicObjectArray(const offset_t size, storage::MemoryManager* mm,
        bool initializeToZero = false)
        : array{ObjectArray<atomic<T>>(size, mm, initializeToZero)} {}

    offset_t getSize() const { return array.size; }

    void resizeAsNew(const offset_t newSize, storage::MemoryManager* mm) {
        array.resizeAsNew(newSize, mm);
    }

    void set(offset_t pos, const T& value, memory_order order = memory_order_seq_cst) {
        KU_ASSERT(pos < array.size);
        array.data[pos].store(value, order);
    }

    T get(const offset_t pos, memory_order order = memory_order_seq_cst) {
        KU_ASSERT(pos < array.size);
        return array.data[pos].load(order);
    }

    void fetchAdd(offset_t pos, const T& value, memory_order order = memory_order_seq_cst) {
        KU_ASSERT(pos < array.size);
        array.data[pos].fetch_add(value, order);
    }

    bool compare_exchange_max(const offset_t src, const offset_t dest,
        memory_order order = memory_order_seq_cst) {
        auto srcValue = get(src, order);
        auto dstValue = get(dest, order);
        // From https://en.cppreference.com/w/cpp/atomic/atomic/compare_exchange:
        // When a compare-and-exchange is in a loop, the weak version will yield better performance
        // on some platforms.
        while (dstValue < srcValue) {
            if (array.data[dest].compare_exchange_weak(dstValue, srcValue)) {
                return true;
            }
        }
        return false;
    }

private:
    ObjectArray<atomic<T>> array;
};

// ObjectArraysMap represents a pre-allocated amount of object per tableID.
template<typename T>
class GDSDenseObjectManager {
public:
    void allocate(table_id_t tableID, offset_t maxOffset, storage::MemoryManager* mm) {
        auto buffer = mm->allocateBuffer(false, maxOffset * sizeof(T));
        bufferPerTable.insert({tableID, move(buffer)});
    }

    T* getData(table_id_t tableID) const {
        KU_ASSERT(bufferPerTable.contains(tableID));
        return reinterpret_cast<T*>(bufferPerTable.at(tableID)->getData());
    }

private:
    table_id_map_t<unique_ptr<storage::MemoryBuffer>> bufferPerTable;
};

template<typename T>
class GDSSpareObjectManager {
public:
    explicit GDSSpareObjectManager(const table_id_map_t<offset_t>& nodeMaxOffsetMap) {
        for (auto& [tableID, _] : nodeMaxOffsetMap) {
            allocate(tableID);
        }
    }

    void allocate(table_id_t tableID) {
        KU_ASSERT(!mapPerTable.contains(tableID));
        mapPerTable.insert({tableID, {}});
    }

    const table_id_map_t<unordered_map<offset_t, T>>& getData() { return mapPerTable; }

    unordered_map<offset_t, T>* getMap(table_id_t tableID) {
        KU_ASSERT(mapPerTable.contains(tableID));
        return &mapPerTable.at(tableID);
    }

    unordered_map<offset_t, T>* getData(table_id_t tableID) {
        if (!mapPerTable.contains(tableID)) {
            mapPerTable.insert({tableID, {}});
        }
        KU_ASSERT(mapPerTable.contains(tableID));
        return &mapPerTable.at(tableID);
    }

    uint64_t size() const {
        uint64_t result = 0;
        for (auto [_, map] : mapPerTable) {
            result += map.size();
        }
        return result;
    }

private:
    table_id_map_t<unordered_map<offset_t, T>> mapPerTable;
};

} // namespace function
} // namespace kuzu

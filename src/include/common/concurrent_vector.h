#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>

#include "common/assert.h"
#include "common/copy_constructors.h"
#include "common/system_config.h"

namespace kuzu {
namespace common {

// An owned pointer class that supports concurrent initialization by trying to lock
// and having the thread which acquires the lock initialize the contents while other threads wait
template<class T>
class LockedPtr {
public:
    LockedPtr() : ptr{nullptr} {}
    LockedPtr(LockedPtr&& other) : ptr{other.ptr.load()} { other.ptr = nullptr; }

    // This should not be used in a concurrent context as it violates the assumption that once
    // initialized, the stored value is always non-null
    LockedPtr& operator=(LockedPtr&& other) {
        // The other object's destructor will handle destroying any pointer this object previously
        // held
        ptr.exchange(other.ptr);
        return *this;
    }

    // If it returns null, use set to initialize it
    // Once this returns non-null it will always return non-null
    T* get() { return ptr.load(); }

    // Thread-safe; only one thread will call the function to create it; other threads will block
    // The function must set it to a non-null value!
    template<class Func>
    bool set(Func func) {
        if (mtx.try_lock()) {
            if (ptr == nullptr) {
                ptr = func();
                KU_ASSERT(ptr != nullptr);
                return true;
            }
            mtx.unlock();
        } else {
            while (ptr == nullptr) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        }
        return false;
    }

    ~LockedPtr() {
        delete get();
        ptr = nullptr;
    }

private:
    std::atomic<T*> ptr;
    std::mutex mtx;
};

template<typename T, uint64_t BLOCK_SIZE = DEFAULT_VECTOR_CAPACITY,
    uint64_t INDEX_SIZE = BLOCK_SIZE>
// Vector which doesn't move when resizing
// The initial size is fixed, and new elements are added in fixed sized blocks which are indexed and
// the indices are chained in a linked list. Currently only one thread can write concurrently, but
// any number of threads can read, even when the vector is being written to.
//
// Accessing elements which existed when the vector was created is as fast as possible, requiring
// just one comparison and one pointer reads, and accessing new elements is still reasonably fast,
// usually requiring reading just two pointers, with a small amount of arithmetic, or maybe more if
// an extremely large number of elements has been added
// (access cost increases every BLOCK_SIZE * INDEX_SIZE elements).
class ConcurrentVector {
public:
    explicit ConcurrentVector(uint64_t initialNumElements, uint64_t initialBlockSize)
        : numElements{initialNumElements}, initialBlock{std::make_unique<T[]>(initialBlockSize)},
          initialBlockSize{initialBlockSize}, firstIndex{} {}

    DEFAULT_MOVE_CONSTRUCT(ConcurrentVector);

    // This is not thread-safe! Nothing should be accessing either object during the execution of
    // this function
    ConcurrentVector& operator=(ConcurrentVector&& other) {
        numElements = other.numElements.load();
        initialBlock = std::move(other.initialBlock);
        initialBlockSize = other.initialBlockSize;
        firstIndex = std::move(other.firstIndex);
        return *this;
    }

    // Allocates at least newSize elements. If another thread is resizing concurrently the larger
    // size will be used.
    void resize(uint64_t newSize) {
        uint64_t expectedSize = numElements.load();
        while (expectedSize < newSize && !numElements.compare_exchange_weak(expectedSize, newSize))
            ;
        allocateBlocks(newSize);
    }

    void push_back(T&& value) {
        auto index = numElements++;
        allocateBlocks(index + 1);
        (*this)[index] = std::move(value);
    }

    T& operator[](uint64_t elemPos) {
        if (elemPos < initialBlockSize) {
            KU_ASSERT(initialBlock);
            return initialBlock[elemPos];
        } else {
            auto blockNum = (elemPos - initialBlockSize) / BLOCK_SIZE;
            auto posInBlock = (elemPos - initialBlockSize) % BLOCK_SIZE;
            auto indexNum = blockNum / INDEX_SIZE;
            BlockIndex* index = firstIndex.get();
            KU_ASSERT(index != nullptr);
            while (indexNum > 0) {
                KU_ASSERT(index->nextIndex.get() != nullptr);
                index = index->nextIndex.get();
                indexNum--;
            }
            KU_ASSERT(index->blocks[blockNum % INDEX_SIZE].get() != nullptr);
            return index->blocks[blockNum % INDEX_SIZE].get()->data[posInBlock];
        }
    }

    uint64_t size() { return numElements; }

    void clear() { numElements = 0; }

protected:
    // Makes sure that sufficient blocks for the given size have been allocated
    // Never deallocates memory
    // Thread-safe
    // It may be the case that the blocks were allocated by another thread calling this function
    void allocateBlocks(uint64_t newSize) {
        if (newSize <= initialBlockSize) {
            return;
        }
        if (firstIndex.get() == nullptr) {
            firstIndex.set([]() { return new BlockIndex(); });
        }
        auto* index = firstIndex.get();
        KU_ASSERT(index != nullptr);
        uint64_t previousIndexSize = initialBlockSize;
        while (previousIndexSize + index->numBlocks * BLOCK_SIZE < newSize) {
            if (index->numBlocks < INDEX_SIZE) {
                // Add blocks until the index is full or the new size has been
                // reached
                while (index->numBlocks < INDEX_SIZE &&
                       previousIndexSize + index->numBlocks * BLOCK_SIZE < newSize) {
                    auto newBlockPosition = index->numBlocks.load();
                    if (newBlockPosition < INDEX_SIZE) {
                        if (index->blocks[newBlockPosition].set([&]() { return new Block(); })) {
                            index->numBlocks++;
                        }
                    }
                }
            } else {
                previousIndexSize += index->numBlocks * BLOCK_SIZE;
                if (index->nextIndex.get() != nullptr) {
                    index = index->nextIndex.get();
                } else {
                    index->nextIndex.set([]() { return new BlockIndex(); });
                    index = index->nextIndex.get();
                }
            }
        }
    }

private:
    std::atomic<uint64_t> numElements;
    std::unique_ptr<T[]> initialBlock;
    uint64_t initialBlockSize;
    struct Block {
        std::array<T, BLOCK_SIZE> data;
    };
    struct BlockIndex {
        BlockIndex() : nextIndex{}, blocks{}, numBlocks{0} {}
        LockedPtr<BlockIndex> nextIndex;
        std::array<LockedPtr<Block>, INDEX_SIZE> blocks;
        std::atomic<uint64_t> numBlocks;
    };
    LockedPtr<BlockIndex> firstIndex;
};
} // namespace common
} // namespace kuzu

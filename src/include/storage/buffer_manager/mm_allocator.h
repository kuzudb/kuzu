#pragma once

#include "memory_manager.h"

namespace kuzu {
namespace storage {

template<class T>
class KUZU_API MmAllocator {
public:
    typedef T value_type;

    explicit MmAllocator(MemoryManager* mm) : mm{mm} {}
    MmAllocator(const MmAllocator& other) { mm = other.mm; }
    MmAllocator& operator=(const MmAllocator& other) {
        if (this != &other) {
            mm = other.mm;
        }
        return *this;
    }
    DELETE_BOTH_MOVE(MmAllocator);

    [[nodiscard]] T* allocate(std::size_t size) {
        if (size > std::numeric_limits<std::size_t>::max() / sizeof(T))
            throw std::bad_array_new_length();

        auto buffer = mm->mallocBuffer(false, size * sizeof(T));
        auto p = reinterpret_cast<T*>(buffer.data());
        return p;
    }

    void deallocate(T* p, std::size_t size) noexcept {
        auto buffer = std::span<uint8_t>(reinterpret_cast<uint8_t*>(p), size * sizeof(T));
        if (buffer.data() != nullptr) {
            mm->freeBlock(common::INVALID_PAGE_IDX, buffer);
        }
    }

private:
    MemoryManager* mm;
};

template<class T, class U>
bool operator==(const MmAllocator<T>& a, const MmAllocator<U>& b) {
    return a.mm == b.mm;
}

template<class T, class U>
bool operator!=(const MmAllocator<T>& a, const MmAllocator<U>& b) {
    return a != b;
}

} // namespace storage
} // namespace kuzu

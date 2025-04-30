#pragma once

#include "memory_manager.h"

namespace kuzu {
namespace storage {

template<class T>
class KUZU_API MmAllocator {
public:
    using value_type = T;

    explicit MmAllocator(MemoryManager* mm) : mm{mm} {}

    MmAllocator(const MmAllocator& other) : mm{other.mm} {}
    MmAllocator& operator=(const MmAllocator& other) = default;
    DELETE_BOTH_MOVE(MmAllocator);

    [[nodiscard]] T* allocate(std::size_t size) {
        if (size > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
            throw std::bad_array_new_length();
        }

        auto buffer = mm->mallocBuffer(false, size * sizeof(T));
        auto p = reinterpret_cast<T*>(buffer.data());

        // Ensure proper alignment
        if (reinterpret_cast<std::uintptr_t>(p) % alignof(T) != 0) {
            throw std::bad_alloc();
        }

        return p;
    }

    void deallocate(T* p, std::size_t size) noexcept {
        if (!p || size == 0) {
            return;
        }

        auto buffer = std::span(reinterpret_cast<uint8_t*>(p), size * sizeof(T));
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

} // namespace storage
} // namespace kuzu

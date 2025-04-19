#pragma once

#include <iostream>

#include "common/copy_constructors.h"
#include "common/types/types.h"
#include <function/gds/gds_object_manager.h>

using namespace std;
using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace graph {

using weight_t = offset_t;
constexpr weight_t DEFAULT_WEIGHT = 1;

struct Neighbor {
    offset_t neighbor;
    weight_t weight;
};

template<class T>
class KUZU_API KuzuAllocator {
public:
    typedef T value_type;

    explicit KuzuAllocator(storage::MemoryManager* mm) : mm{mm} {}
    KuzuAllocator(const KuzuAllocator& other) { mm = other.mm; }
    KuzuAllocator& operator=(const KuzuAllocator& other) {
        if (this != &other) {
            mm = other.mm;
        }
        return *this;
    }
    DELETE_BOTH_MOVE(KuzuAllocator);

    [[nodiscard]] T* allocate(std::size_t size) {
        if (size > std::numeric_limits<std::size_t>::max() / sizeof(T))
            throw std::bad_array_new_length();

        auto buffer = mm->mallocBuffer(false, size * sizeof(T));
        auto p = reinterpret_cast<T*>(buffer.data());
        report(p, size);
        return p;
    }

    void deallocate(T* p, std::size_t size) noexcept {
        auto buffer = std::span<uint8_t>(reinterpret_cast<uint8_t*>(p), size * sizeof(T));
        if (buffer.data() != nullptr) {
            report(p, size, 0);
            mm->freeBlock(INVALID_PAGE_IDX, buffer);
        }
    }

private:
    storage::MemoryManager* mm;
    void report(T* p, std::size_t n, bool alloc = true) const {
        std::cout << (alloc ? "Alloc: " : "Dealloc: ") << sizeof(T) * n << " bytes at " << std::hex
                  << std::showbase << reinterpret_cast<void*>(p) << std::dec << '\n';
    }
};

template<class T, class U>
bool operator==(const KuzuAllocator<T>& a, const KuzuAllocator<U>& b) {
    return a.mm == b.mm;
}

template<class T, class U>
bool operator!=(const KuzuAllocator<T>& a, const KuzuAllocator<U>& b) {
    return a != b;
}

// CSR-like in-memory representation of an undirected weighted graph. Insert nodes in sequence
// by first calling `initNextNode()` and then insert all its neighbors using `insertNbr()`.
// Undirected edges should be explicitly inserted twice.
// Note: modifying the in-memory graph is NOT thread-safe.
struct InMemGraph {
    KuzuAllocator<offset_t> alloc;
    KuzuAllocator<Neighbor> alloc2;
    vector<offset_t, KuzuAllocator<offset_t>> csrOffsets;
    vector<Neighbor, KuzuAllocator<Neighbor>> csrEdges;
    offset_t numNodes = 0;
    offset_t numEdges = 0;

    InMemGraph(offset_t numNodes, storage::MemoryManager* mm);
    DELETE_BOTH_COPY(InMemGraph);

    // Resets to an empty graph. Reuses allocations, if any.
    void reset(offset_t numNodes);

    // Initialize the next node in sequence. Should be called before inserting edges for the node.
    void initNextNode();

    // Insert a neighbor of the last initialized node.
    void insertNbr(offset_t to, weight_t weight = DEFAULT_WEIGHT);
};

} // namespace graph
} // namespace kuzu

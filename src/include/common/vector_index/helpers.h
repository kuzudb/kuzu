#pragma once

#include "common/types/types.h"

namespace kuzu {
namespace common {

#define IS_ALIGNED(X, Y) ((uint64_t)(X) % (uint64_t)(Y) == 0)

// TODO(gaurav): Improve this class to be more efficient.
class VisitedTable {
public:
    explicit VisitedTable(size_t size) : visited(size, 0), visited_id(1){};
    inline void set(vector_id_t id) { visited[id] = visited_id; }
    inline bool get(vector_id_t id) { return visited[id] == visited_id; }
    inline void reset() {
        visited_id++;
        if (visited_id == 250) {
            std::fill(visited.begin(), visited.end(), 0);
            visited_id = 1;
        }
    }
    inline uint8_t* data() { return visited.data(); }

private:
    std::vector<uint8_t> visited;
    uint8_t visited_id;
};

struct NodeDistCloser {
    explicit NodeDistCloser(vector_id_t id, float dist) : id(id), dist(dist) {}
    vector_id_t id;
    float dist;
    bool operator<(const NodeDistCloser& other) const { return dist < other.dist; }
};

struct NodeDistFarther {
    explicit NodeDistFarther(vector_id_t id, float dist) : id(id), dist(dist) {}
    vector_id_t id;
    float dist;
    bool operator<(const NodeDistFarther& other) const { return dist > other.dist; }
};

static void allocAligned(void** ptr, size_t size, size_t align) {
    *ptr = nullptr;
    if (!IS_ALIGNED(size, align)) {
        throw InternalException(stringFormat("size: %lu, align: %lu\n", size, align));
    }
#ifdef __APPLE__
    int err = posix_memalign(ptr, align, size);
    if (err) {
        throw InternalException(stringFormat("posix_memalign failed with error code %d\n", err));
    }
#else
    *ptr = ::aligned_alloc(align, size);
#endif
    if (*ptr == nullptr) {
        throw InternalException("aligned_alloc failed\n");
    }
}

} // namespace common
} // namespace kuzu

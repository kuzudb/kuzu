#pragma once

#include "common/copy_constructors.h"
#include "common/types/types.h"

namespace kuzu {
namespace graph {

using weight_t = common::offset_t;
constexpr weight_t DEFAULT_WEIGHT = 1;

struct KUZU_API Neighbor {
    common::offset_t neighbor;
    weight_t weight;
};

// CSR-like in-memory representation of an undirected weighted graph. Insert nodes in sequence
// by first calling `initNextNode()` and then insert all its neighbors using `insertNbr()`.
// Undirected edges should be explicitly inserted twice.
// Note: modifying the in-memory graph is NOT thread-safe.
struct KUZU_API InMemGraph {
    std::vector<common::offset_t> csrOffsets;
    std::vector<Neighbor> csrEdges;
    common::offset_t numNodes = 0;
    common::offset_t numEdges = 0;

    explicit InMemGraph(common::offset_t numNodes);
    DELETE_BOTH_COPY(InMemGraph);

    // Resets to an empty graph. Reuses allocations, if any.
    void reset(common::offset_t numNodes);

    // Initialize the next node in sequence. Should be called before inserting edges for the node.
    void initNextNode();

    // Insert a neighbor of the last initialized node.
    void insertNbr(common::offset_t to, weight_t weight = DEFAULT_WEIGHT);
};

} // namespace graph
} // namespace kuzu

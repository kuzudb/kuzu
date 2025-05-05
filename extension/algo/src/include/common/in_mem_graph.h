#pragma once

#include "common/copy_constructors.h"
#include "common/types/types.h"
#include "function/gds/gds_object_manager.h"

namespace kuzu {
namespace algo_extension {

using weight_t = common::offset_t;
constexpr weight_t DEFAULT_WEIGHT = 1;

struct Neighbor {
    common::offset_t neighbor;
    weight_t weight;

    Neighbor(const common::offset_t neighbor, const weight_t weight)
        : neighbor{neighbor}, weight{weight} {}
};

// CSR-like in-memory representation of an undirected weighted graph. Insert nodes in sequence
// by first calling `initNextNode()` and then insert all its neighbors using `insertNbr()`.
// Undirected edges should be explicitly inserted twice.
// Note: modifying the in-memory graph is NOT thread-safe.
struct InMemGraph {
    function::ku_vector_t<common::offset_t> csrOffsets;
    function::ku_vector_t<Neighbor> csrEdges;
    common::offset_t numNodes = 0;
    common::offset_t numEdges = 0;

    InMemGraph(const common::offset_t numNodes, storage::MemoryManager* mm);
    DELETE_BOTH_COPY(InMemGraph);
    ~InMemGraph() = default;

    // Re-initializes to an empty graph. Reuses allocations if `numNodes` <= `this->numNodes`.
    void reinit(const common::offset_t numNodes);

    // Initializes the next node in the sequence to prepare for edges insertions for the node.
    void initNextNode();

    // Inserts a neighbor of the last initialized node.
    void insertNbr(const common::offset_t to, const weight_t weight = DEFAULT_WEIGHT);
};

} // namespace algo_extension
} // namespace kuzu

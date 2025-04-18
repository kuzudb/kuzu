#pragma once

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

// CSR-like in-memory representation of an undirected weighted graph. Insert nodes in sequence
// by first calling `initNextNode()` and then insert all its neighbors using `insertNbr()`.
// Undirected edges should be explicitly inserted twice.
// Note: modifying the in-memory graph is NOT thread-safe.
struct InMemGraph {
    vector<offset_t> csrOffsets;
    vector<Neighbor> csrEdges;
    offset_t numNodes = 0;
    offset_t numEdges = 0;

    explicit InMemGraph(offset_t numNodes);
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

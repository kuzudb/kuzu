#pragma once

#include "common/copy_constructors.h"
#include "common/types/types.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace graph {

constexpr double DEFAULT_WEIGHT = 1.0;

struct Neighbor {
    offset_t neighbor;
    double weight;
};

// CSR-like in-memory representation of an undirected weighted graph. Insert nodes in sequence
// by first calling `initNextNode()` and then insert all its neighbors using `insertNbr()`.
// Undirected edges should be explicitly inserted twice.
struct InMemGraph {
    vector<offset_t> csrOffsets;
    vector<Neighbor> csrEdges;
    offset_t numNodes = 0;
    offset_t numEdges = 0;

    explicit InMemGraph(offset_t numNodes);
    DELETE_BOTH_COPY(InMemGraph);

    // Resets to an empty graph. Reuses allocations, if any.
    void reset(offset_t numNodes);

    // Initialize the next node in sequence. To be called before inserting any edges for the node.
    void initNextNode();

    // Insert a neighbor edge of the last initialized node.
    void insertNbr(offset_t to, double weight = DEFAULT_WEIGHT);
};

} // namespace graph
} // namespace kuzu

#pragma once

#include <cstdint>
#include <iterator>
#include <memory>

#include "common/copy_constructors.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "transaction/transaction.h"
#include <span>

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
// by first calling `insertNewNode()` and then insert all its neighbors using `insertEdge()`.
// Undirected edges should be explicitly inserted twice.
struct InMemoryGraph {
    vector<offset_t> nodes;
    vector<Neighbor> neighbors;
    offset_t numNodes = 0;
    offset_t numEdges = 0;

    explicit InMemoryGraph(offset_t numNodes);
    DELETE_BOTH_COPY(InMemoryGraph);

    // Prepare the object for reuse.
    void reset(offset_t numNodes);

    void insertNewNode();

    void insertEdge(offset_t to, double weight = DEFAULT_WEIGHT);
};

} // namespace graph
} // namespace kuzu

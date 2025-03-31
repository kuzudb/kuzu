#include "graph/graph_mem.h"

namespace kuzu::graph {

InMemoryGraph::InMemoryGraph(const offset_t numNodes) {
    reset(numNodes);
}

void InMemoryGraph::reset(const offset_t numNodes_) {
    numNodes = numNodes_;
    nodes.reserve(numNodes);
    nodes.clear();
    neighbors.clear();
    numEdges = 0;
}

void InMemoryGraph::insertNewNode() {
    nodes.push_back(neighbors.size());
}

void InMemoryGraph::insertEdge(const offset_t to, const double weight) {
    Neighbor neighbor{to, weight};
    neighbors.push_back(neighbor);
    numEdges++;
}

} // namespace kuzu::graph

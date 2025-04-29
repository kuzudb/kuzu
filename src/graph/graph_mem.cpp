#include "graph/graph_mem.h"

using namespace kuzu::common;

namespace kuzu::graph {

InMemGraph::InMemGraph(offset_t numNodes) {
    reset(numNodes);
}

void InMemGraph::reset(offset_t numNodes_) {
    numNodes = numNodes_;
    csrOffsets.reserve(numNodes);
    csrOffsets.clear();
    csrEdges.clear();
    numEdges = 0;
}

void InMemGraph::initNextNode() {
    csrOffsets.push_back(csrEdges.size());
}

void InMemGraph::insertNbr(offset_t to, weight_t weight) {
    Neighbor neighbor{to, weight};
    csrEdges.push_back(neighbor);
    numEdges++;
}

} // namespace kuzu::graph

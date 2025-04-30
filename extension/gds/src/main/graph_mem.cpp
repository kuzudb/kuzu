#include "main/graph_mem.h"

namespace kuzu::graph {

InMemGraph::InMemGraph(common::offset_t numNodes, storage::MemoryManager* mm)
    : csrOffsets(mm), csrEdges(mm) {
    reset(numNodes);
}

void InMemGraph::reset(common::offset_t numNodes_) {
    numNodes = numNodes_;
    csrOffsets.vec.reserve(numNodes);
    csrOffsets.vec.clear();
    csrEdges.vec.clear();
    numEdges = 0;
}

void InMemGraph::initNextNode() {
    csrOffsets.vec.push_back(csrEdges.vec.size());
}

void InMemGraph::insertNbr(common::offset_t to, weight_t weight) {
    Neighbor neighbor{to, weight};
    csrEdges.vec.push_back(neighbor);
    numEdges++;
}

} // namespace kuzu::graph

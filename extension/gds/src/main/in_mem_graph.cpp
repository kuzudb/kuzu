#include "main/in_mem_graph.h"

namespace kuzu::graph {

InMemGraph::InMemGraph(common::offset_t numNodes, storage::MemoryManager* mm)
    : csrOffsets(mm), csrEdges(mm) {
    reinit(numNodes);
}

void InMemGraph::reinit(common::offset_t numNodes_) {
    numNodes = numNodes_;
    csrOffsets.reserve(numNodes);
    csrOffsets.clear();
    csrEdges.clear();
    numEdges = 0;
}

void InMemGraph::initNextNode() {
    csrOffsets.push_back(csrEdges.size());
}

void InMemGraph::insertNbr(common::offset_t to, weight_t weight) {
    csrEdges.emplace_back(to, weight);
    numEdges++;
}

} // namespace kuzu::graph

#include "common/in_mem_graph.h"

namespace kuzu {
namespace algo_extension {

InMemGraph::InMemGraph(const common::offset_t numNodes, storage::MemoryManager* mm)
    : csrOffsets(mm), csrEdges(mm) {
    reinit(numNodes);
}

void InMemGraph::reinit(const common::offset_t numNodes) {
    this->numNodes = numNodes;
    csrOffsets.reserve(numNodes);
    csrOffsets.clear();
    csrEdges.clear();
    numEdges = 0;
}

void InMemGraph::initNextNode() {
    csrOffsets.push_back(csrEdges.size());
}

void InMemGraph::insertNbr(const common::offset_t to, const weight_t weight) {
    csrEdges.emplace_back(to, weight);
    numEdges++;
}

} // namespace algo_extension
} // namespace kuzu

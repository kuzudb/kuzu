#include "function/gds/gds_task.h"

#include "graph/graph.h"

namespace kuzu {
namespace function {

void GDSTask::run() {
    RangeFrontierMorsel frontierMorsel;
    auto numApproxActiveNodesForNextIter = 0u;
    std::unique_ptr<graph::Graph> graph = sharedState->graph->copy();
    while (sharedState->frontiers.getNextFrontierMorsel(frontierMorsel)) {
        while (frontierMorsel.hasNextVertex()) {
            nodeID_t nodeID = frontierMorsel.getNextVertex();
            if (sharedState->frontiers.curFrontier->isActive(nodeID)) {
                auto nbrs = graph->scanFwd(nodeID, sharedState->relTableIDToScan);
                for (auto nbr : nbrs) {
                    if (sharedState->fc.edgeCompute(nbr)) {
                        sharedState->frontiers.nextFrontier->setActive(nbr);
                        numApproxActiveNodesForNextIter++;
                    }
                }
            }
        }
    }
    sharedState->frontiers.incrementApproxActiveNodesForNextIter(numApproxActiveNodesForNextIter);
}
} // namespace function
} // namespace kuzu

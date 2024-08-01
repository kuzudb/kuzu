#include "function/gds/gds_task.h"

#include "graph/graph.h"

// TODO(Semih): Remove
#include <iostream>
namespace kuzu {
namespace function {

void GDSTask::run() {
    RangeFrontierMorsel frontierMorsel;
    auto numApproxActiveNodesForNextIter = 0u;
    std::unique_ptr<graph::Graph> graph = sharedState->graph->copy();
    auto state = graph->prepareScan(sharedState->relTableIDToScan);
    auto localFc = sharedState->fc.clone();
    localFc->init();
    while (sharedState->frontiers.getNextFrontierMorsel(frontierMorsel)) {
        while (frontierMorsel.hasNextVertex()) {
            nodeID_t nodeID = frontierMorsel.getNextVertex();
            if (sharedState->frontiers.curFrontier->isActive(nodeID)) {
                auto nbrIDs = graph->scanFwd(nodeID, *state);
                for (auto nbrID : nbrIDs) {
                    if (localFc->edgeCompute(nodeID, nbrID)) {
                        sharedState->frontiers.nextFrontier->setActive(nbrID);
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

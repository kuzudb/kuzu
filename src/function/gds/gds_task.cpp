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
    while (sharedState->frontiers.getNextFrontierMorsel(frontierMorsel)) {
        while (frontierMorsel.hasNextVertex()) {
            nodeID_t nodeID = frontierMorsel.getNextVertex();
            if (sharedState->frontiers.curFrontier->isActive(nodeID)) {
                auto nbrIDs = graph->scanFwd(nodeID, sharedState->relTableIDToScan);
//                std::cout << "Scanning nbrs of nodeID: " << nodeID.offset << " numNbrs: " << nbrIDs.size() << std::endl;
                for (auto nbrID : nbrIDs) {
                    if (sharedState->fc.edgeCompute(nodeID, nbrID)) {
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

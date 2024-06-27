#include "function/gds/gds_task.h"
#include "graph/graph.h"
// TODO(Semih): Remove
#include <iostream>
#include <thread>

namespace kuzu {
namespace function {

void FrontierTask::run() {
    std::cout << std::this_thread::get_id() << " FrontierTask::run() begin" << std::endl;
    RangeFrontierMorsel frontierMorsel;
    auto numActiveNodes = 0u;
    std::unique_ptr<graph::Graph> graph = sharedState->graph->copy();
    auto curFrontier = sharedState->frontiers.getCurFrontier();
    auto nextFrontier = sharedState->frontiers.getNextFrontier();
    while (sharedState->frontiers.getNextFrontierMorsel(frontierMorsel)) {
        while (frontierMorsel.hasNextVertex()) {
            nodeID_t nodeID = frontierMorsel.getNextVertex();
            if (curFrontier->isActive(nodeID)) {
                auto nbrs = graph->scanFwd(nodeID, sharedState->relTableIDToScan);
                for (auto nbr : nbrs) {
                    if (sharedState->vu.edgeUpdate(nbr)) {
                        nextFrontier->setActive(nbr);
                        numActiveNodes++;
                    }
                }
            }
        }
    }
    sharedState->frontiers.incrementNextActiveNodes(numActiveNodes);
    std::cout << std::this_thread::get_id()
              << " FrontierTask::run() end. numLocalActiveNodes: " << numActiveNodes << std::endl;
}
} // namespace function
} // namespace kuzu
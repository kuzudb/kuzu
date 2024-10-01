#include "function/gds/vector_search_task.h"

using namespace kuzu::storage;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::graph;

namespace kuzu {
    namespace function {
        void VectorSearchTask::run() {
            auto threadId = sharedState->getThreadId();
            auto context = sharedState->context;
            auto readLocalState = localState->clone(context);
            auto efSearch = (sharedState->efSearch / sharedState->maxNumThreads) * 1.2;
            auto visited = sharedState->visited;
            auto dc = sharedState->distanceComputer;
            auto header = sharedState->indexHeader;
            auto nodeTableId = header->getNodeTableId();
            auto graph = sharedState->graph;
            auto state = graph->prepareScan(header->getCSRRelTableId());
            auto filterMask = sharedState->filterMask;
            auto maxK = sharedState->maxK;
            auto isFilteredSearch = filterMask->isEnabled();
            auto useInFilterSearch = false;
            // Find maxK value based on selectivity
            if (isFilteredSearch) {
                auto selectivity = static_cast<double>(filterMask->getNumMaskedNodes()) / header->getNumVectors();
                if (selectivity <= 0.005) {
                    // TODO: Add code to calculate distance for all nodes
                    printf("skipping search since selectivity too low\n");
                    return;
                }
                if (selectivity > 0.3) {
                    useInFilterSearch = true;
                }
                maxK = calculateMaxK(filterMask->getNumMaskedNodes(), header->getNumVectors());
            }
            std::priority_queue<NodeDistFarther> candidates;
            auto &results = sharedState->topKResults[threadId];
            auto &entrypoints = sharedState->entrypoints[threadId];
            for (auto &entrypoint: entrypoints) {
                candidates.emplace(entrypoint.id, entrypoint.dist);
                if (isFilteredSearch && isMasked(entrypoint.id, filterMask)) {
                    results.emplace(entrypoint.id, entrypoint.dist);
                    visited->set(entrypoint.id);
                }
            }

            while (!candidates.empty()) {
                auto candidate = candidates.top();
                candidates.pop();
                std::vector<common::nodeID_t> neighbors;
                if (isFilteredSearch && !useInFilterSearch) {
                    findFilteredNextKNeighbours({candidate.id, nodeTableId}, *state, neighbors,
                                                                      filterMask, visited, maxK, efSearch,
                                                                      sharedState->graph);
                    // If empty, work with unfiltered neighbours to move forward
                    if (candidates.empty() && neighbors.empty()) {
                        // Figure out how expensive this is!!
                        // Push a random masked neighbour to continue the search
                        for (offset_t i = 0; i < filterMask->getMaxOffset(); i++) {
                            if (filterMask->isMasked(i) && !visited->get(i)) {
                                neighbors.push_back({i, nodeTableId});
                                break;
                            }
                        }
                    }
                } else {
                    findNextKNeighbours({candidate.id, nodeTableId}, *state, neighbors, visited, graph);
                }
                // TODO: Try batch compute distances
                for (auto neighbor: neighbors) {
                    double dist;
                    auto embed = readLocalState->getEmbedding(context, neighbor.offset);
                    dc->computeDistance(embed, &dist);
                    if (results.size() < efSearch || dist < results.top().dist) {
                        candidates.emplace(neighbor.offset, dist);
                        if (!useInFilterSearch || isMasked(neighbor.offset, filterMask)) {
                            results.emplace(neighbor.offset, dist);
                            if (results.size() > efSearch) {
                                results.pop();
                            }
                        }
                    }
                }

                if (!results.empty() && candidate.dist > results.top().dist) {
                    break;
                }
            }


        }

        int VectorSearchTask::findFilteredNextKNeighbours(common::nodeID_t nodeID, GraphScanState &state,
                                                          std::vector<common::nodeID_t> &nbrs,
                                                          NodeOffsetLevelSemiMask *filterMask,
                                                          AtomicVisitedTable *visited, int maxK,
                                                          int maxNeighboursCheck, Graph *graph) {
            std::queue<std::pair<common::nodeID_t, int>> candidates;
            candidates.push({nodeID, 0});
            std::unordered_set<offset_t> visitedSet;
            auto neighboursChecked = 0;
            while (neighboursChecked <= maxNeighboursCheck && !candidates.empty()) {
                auto candidate = candidates.front();
                candidates.pop();
                auto currentDepth = candidate.second;
                if (visitedSet.contains(candidate.first.offset)) {
                    continue;
                }
                visitedSet.insert(candidate.first.offset);
                visited->set(candidate.first.offset);
                auto neighbors = graph->scanFwdRandom(candidate.first, state);
                neighboursChecked += 1;
                for (auto &neighbor: neighbors) {
                    if (visited->get(neighbor.offset)) {
                        continue;
                    }

                    if (filterMask->isMasked(neighbor.offset) && visited->getAndSet(neighbor.offset)) {
                        nbrs.push_back(neighbor);
                        if (nbrs.size() >= maxK) {
                            return neighboursChecked;
                        }
                    }
                    candidates.push({neighbor, currentDepth + 1});
                }
            }
            return neighboursChecked;
        }

        void VectorSearchTask::findNextKNeighbours(common::nodeID_t nodeID, GraphScanState &state,
                                                   std::vector<common::nodeID_t> &nbrs,
                                                   AtomicVisitedTable *visited,
                                                   Graph *graph) {
            // TODO: Implement like findFilteredNextKNeighbours
            auto unFilteredNbrs = graph->scanFwdRandom(nodeID, state);
            for (auto &neighbor: unFilteredNbrs) {
                if (visited->getAndSet(neighbor.offset)) {
                    nbrs.push_back(neighbor);
                }
            }
        }

        inline bool VectorSearchTask::isMasked(common::offset_t offset, NodeOffsetLevelSemiMask *filterMask) {
            return !filterMask->isEnabled() || filterMask->isMasked(offset);
        }

        int VectorSearchTask::calculateMaxK(uint64_t numMaskedNodes, uint64_t totalNodes) {
            auto selectivity = static_cast<double>(numMaskedNodes) / totalNodes;
            if (selectivity < 0.005) {
                return 20;
            } else if (selectivity < 0.01) {
                return 20;
            } else if (selectivity < 0.05) {
                return 20;
            } else if (selectivity < 0.1) {
                return 20;
            } else if (selectivity < 0.2) {
                return 30;
            } else if (selectivity < 0.4) {
                return 30;
            } else if (selectivity < 0.5) {
                return 40;
            } else if (selectivity < 0.8) {
                return 40;
            } else if (selectivity < 0.9) {
                return 40;
            } else {
                return 40;
            }
        }
    } // namespace function
} // namespace kuzu
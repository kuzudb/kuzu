#include "function/gds/vector_search_task.h"

using namespace kuzu::storage;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::graph;

namespace kuzu {
    namespace function {
        void VectorSearchTask::run() {
//            auto [partitionId, threadId] = sharedState->getWork();
//            auto context = sharedState->context;
//            auto readLocalState = localState->clone(context);
//            auto efSearch = ((float) sharedState->efSearch / sharedState->maxNumThreads) * 1.2;
//            auto visited = sharedState->visited;
//            auto dc = sharedState->distanceComputer;
//            auto header = sharedState->header;
//            auto nodeTableId = header->getNodeTableId();
//            auto graph = sharedState->graph;
//            auto filterMask = sharedState->filterMask;
//            auto maxK = sharedState->maxK;
//            auto partition = header->getPartitionHeader(partitionId);
//
//            auto state = graph->prepareScan(header->);
//            // sync into the final result after every 2 iterations
//            int syncAfterIter = 3;
//            auto isFilteredSearch = filterMask->isEnabled();
//            auto useInFilterSearch = false;
//            // Find maxK value based on selectivity
//            if (isFilteredSearch) {
//                auto selectivity = static_cast<double>(filterMask->getNumMaskedNodes()) / header->getNumVectors();
//                if (selectivity <= 0.005) {
//                    // TODO: Add code to calculate distance for all nodes
//                    printf("skipping search since selectivity too low\n");
//                    return;
//                }
//                if (selectivity > 0.3) {
//                    useInFilterSearch = true;
//                }
//                maxK = calculateMaxK(filterMask->getNumMaskedNodes(), header->getNumVectors());
//            }
//            // Initialize the results and candidates
//            std::priority_queue<NodeDistFarther> candidates;
//            std::vector<NodeDistFarther> localResults;
//            auto &entrypoints = sharedState->entrypoints[threadId];
//            for (auto &entrypoint: entrypoints) {
//                candidates.emplace(entrypoint.id, entrypoint.dist);
//            }
//
//            while (!candidates.empty()) {
//                auto queueSize = parallelResults->size();
//                double topDist = std::numeric_limits<double>::max();
//                auto topElement = parallelResults->top();
//                if (topElement != nullptr) {
//                    topDist = topElement->dist;
//                }
//                int searchIter = 0;
//                while (!candidates.empty()) {
//                    auto candidate = candidates.top();
//                    candidates.pop();
//                    std::vector<common::nodeID_t> neighbors;
//                    if (isFilteredSearch && !useInFilterSearch) {
//                        findFilteredNextKNeighbours({candidate.id, nodeTableId}, *state, neighbors,
//                                                    filterMask, visited, maxK, efSearch,
//                                                    sharedState->graph);
//                        // If empty, work with unfiltered neighbours to move forward
//                        if (candidates.empty() && neighbors.empty()) {
//                            // Figure out how expensive this is!!
//                            // Push a random masked neighbour to continue the search
//                            for (offset_t i = 0; i < filterMask->getMaxOffset(); i++) {
//                                if (filterMask->isMasked(i) && !visited->atomic_is_bit_set(i)) {
//                                    neighbors.push_back({i, nodeTableId});
//                                    visited->atomic_set_bit(i);
//                                    break;
//                                }
//                            }
//                        }
//                    } else {
//                        // TODO: Fix this!!
//                        findNextKNeighbours({candidate.id, nodeTableId}, *state, neighbors, visited, graph);
//                    }
//                    // TODO: Try batch compute distances
//                    for (auto neighbor: neighbors) {
//                        double dist;
//                        auto embed = readLocalState->getEmbedding(context, neighbor.offset);
//                        dc->computeDistance(embed, &dist);
//                        if (queueSize < efSearch || dist < topDist) {
//                            candidates.emplace(neighbor.offset, dist);
//                            if (!useInFilterSearch || isMasked(neighbor.offset, filterMask)) {
//                                localResults.emplace_back(neighbor.offset, dist);
//                                queueSize++;
//                            }
//                        }
//                    }
//                    searchIter++;
//                    if (searchIter == syncAfterIter) {
//                        break;
//                    }
//                }
                // Push to the parallel results
//                parallelResults->bulkPush(localResults.data(), localResults.size());
//                localResults.clear();
//                if (candidates.empty() || candidates.top().dist > parallelResults->top()->dist) {
//                    break;
//                }
//            }
        }

        int VectorSearchTask::findFilteredNextKNeighbours(common::nodeID_t nodeID,
                                                          GraphScanState &state,
                                                          std::vector<common::nodeID_t> &nbrs,
                                                          NodeOffsetLevelSemiMask *filterMask,
                                                          BitVectorVisitedTable *visited,
                                                          int maxK,
                                                          int maxNeighboursCheck,
                                                          Graph *graph) {
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
                visited->atomic_set_bit(candidate.first.offset);
                auto neighbors = graph->scanFwdRandom(candidate.first, state);
                neighboursChecked += 1;
                for (auto &neighbor: neighbors) {
                    if (visited->atomic_is_bit_set(neighbor.offset)) {
                        continue;
                    }

                    if (filterMask->isMasked(neighbor.offset)) {
                        nbrs.push_back(neighbor);
                        // Maybe try to do it with a getAndSet.
                        // TODO: Fix as this can cause duplicates!!
                        visited->atomic_set_bit(neighbor.offset);
                    }
                    candidates.push({neighbor, currentDepth + 1});
                }
                if (nbrs.size() >= maxK) {
                    return neighboursChecked;
                }
            }
            return neighboursChecked;
        }

        void VectorSearchTask::findNextKNeighbours(common::nodeID_t nodeID,
                                                   GraphScanState &state,
                                                   std::vector<common::nodeID_t> &nbrs,
                                                   BitVectorVisitedTable *visited,
                                                   int minK,
                                                   int maxK,
                                                   int maxNeighboursCheck,
                                                   Graph *graph) {
            // TODO: Implement like findFilteredNextKNeighbours
            auto unFilteredNbrs = graph->scanFwdRandom(nodeID, state);
            for (auto &neighbor: unFilteredNbrs) {
                if (!visited->atomic_is_bit_set(neighbor.offset)) {
                    nbrs.push_back(neighbor);
                    visited->atomic_set_bit(neighbor.offset);
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
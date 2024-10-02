#include <queue>

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/types/value/nested.h"
#include "common/vector_index/helpers.h"
#include "function/gds/gds.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "processor/operator/gds_call.h"
#include "processor/result/factorized_table.h"
#include "storage/index/vector_index_header.h"
#include "storage/storage_manager.h"
#include "storage/store/column.h"
#include "storage/store/node_table.h"
#include "common/vector_index/distance_computer.h"
#include "chrono"
#include "function/gds/vector_search_task.h"
#include "common/task_system/task_scheduler.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
    namespace function {
        struct VectorSearchBindData final : public GDSBindData {
            std::shared_ptr<binder::Expression> nodeInput;
            table_id_t nodeTableID = INVALID_TABLE_ID;
            property_id_t embeddingPropertyID = INVALID_PROPERTY_ID;
            int k = 100;
            int efSearch = 100;
            int maxK = 30;
            std::vector<float> queryVector;

            VectorSearchBindData(std::shared_ptr<binder::Expression> nodeInput,
                                 std::shared_ptr<binder::Expression> nodeOutput, table_id_t nodeTableID,
                                 property_id_t embeddingPropertyID, int k, int64_t efSearch, int maxK,
                                 std::vector<float> queryVector, bool outputAsNode)
                    : GDSBindData{std::move(nodeOutput), outputAsNode}, nodeInput(std::move(nodeInput)),
                      nodeTableID(nodeTableID), embeddingPropertyID(embeddingPropertyID), k(k),
                      efSearch(efSearch), maxK(maxK), queryVector(std::move(queryVector)) {};

            VectorSearchBindData(const VectorSearchBindData &other)
                    : GDSBindData{other}, nodeInput(other.nodeInput), nodeTableID(other.nodeTableID),
                      embeddingPropertyID(other.embeddingPropertyID), k(other.k), efSearch{other.efSearch},
                      maxK(other.maxK), queryVector(other.queryVector) {}

            bool hasNodeInput() const override { return true; }

            std::shared_ptr<binder::Expression> getNodeInput() const override { return nodeInput; }

            bool hasSelectiveOutput() const override { return true; }

            std::unique_ptr<GDSBindData> copy() const override {
                return std::make_unique<VectorSearchBindData>(*this);
            }
        };

        class VectorSearchLocalState : public GDSLocalState {
        public:
            explicit VectorSearchLocalState(main::ClientContext *context, table_id_t nodeTableId,
                                            property_id_t embeddingPropertyId) {
                auto mm = context->getMemoryManager();
                nodeInputVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
                nodeIdVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
                distanceVector = std::make_unique<ValueVector>(LogicalType::DOUBLE(), mm);
                nodeInputVector->state = DataChunkState::getSingleValueDataChunkState();
                nodeIdVector->state = DataChunkState::getSingleValueDataChunkState();
                distanceVector->state = DataChunkState::getSingleValueDataChunkState();
                vectors.push_back(nodeInputVector.get());
                vectors.push_back(nodeIdVector.get());
                vectors.push_back(distanceVector.get());

                indexHeader = context->getStorageManager()->getVectorIndexHeaderReadOnlyVersion(nodeTableId,
                                                                                                embeddingPropertyId);
                auto nodeTable = ku_dynamic_cast<Table *, NodeTable *>(
                        context->getStorageManager()->getTable(nodeTableId));
                embeddingColumn = nodeTable->getColumn(embeddingPropertyId);
                compressedColumn = nodeTable->getColumn(indexHeader->getCompressedPropertyId());

                // TODO: Replace with compressed vector. Should be stored in the
                embeddingVector = std::make_unique<ValueVector>(embeddingColumn->getDataType().copy(),
                                                                context->getMemoryManager());
                embeddingVector->state = DataChunkState::getSingleValueDataChunkState();
                compressedVector = std::make_unique<ValueVector>(compressedColumn->getDataType().copy(),
                                                                 context->getMemoryManager());
                compressedVector->state = DataChunkState::getSingleValueDataChunkState();
                inputNodeIdVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID().copy(),
                                                                  context->getMemoryManager());
                inputNodeIdVector->state = DataChunkState::getSingleValueDataChunkState();
                for (size_t i = 0; i < compressedColumn->getNumNodeGroups(context->getTx()); i++) {
                    readStates.push_back(std::make_unique<Column::ChunkState>());
                }
            }

            void materialize(graph::Graph *graph, std::priority_queue<NodeDistCloser> &result,
                             FactorizedTable &table, int k) const {
                // Remove elements until we have k elements

                // TODO: Rerank the results
                while (result.size() > k) {
                    auto candidate = result.top();
                    result.pop();
                }

                std::priority_queue<NodeDistFarther> reversed;
                while (!result.empty()) {
                    reversed.emplace(result.top().id, result.top().dist);
                    result.pop();
                }

                while (!reversed.empty()) {
                    auto res = reversed.top();
                    reversed.pop();
                    auto nodeID = nodeID_t{res.id, indexHeader->getNodeTableId()};
                    nodeInputVector->setValue<nodeID_t>(0, nodeID);
                    nodeIdVector->setValue<nodeID_t>(0, nodeID);
                    distanceVector->setValue<double>(0, res.dist);
                    table.append(vectors);
                }
            }

        public:
            std::unique_ptr<ValueVector> nodeInputVector;
            std::unique_ptr<ValueVector> nodeIdVector;
            std::unique_ptr<ValueVector> distanceVector;
            std::vector<ValueVector *> vectors;

            VectorIndexHeader *indexHeader;
            Column *embeddingColumn;
            std::unique_ptr<ValueVector> embeddingVector;
            Column *compressedColumn;
            std::unique_ptr<ValueVector> compressedVector;
            std::unique_ptr<ValueVector> inputNodeIdVector;
            std::vector<std::unique_ptr<Column::ChunkState>> readStates;
        };

        class VectorSearch : public GDSAlgorithm {
            static constexpr char DISTANCE_COLUMN_NAME[] = "_distance";

        public:
            VectorSearch() = default;

            VectorSearch(const VectorSearch &other) : GDSAlgorithm{other} {}

            /*
             * Inputs are
             *
             * embeddingProperty::ANY
             * efSearch::INT64
             * outputProperty::BOOL
             */
            std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
                return {LogicalTypeID::ANY, LogicalTypeID::NODE, LogicalTypeID::INT64, LogicalTypeID::LIST,
                        LogicalTypeID::INT64, LogicalTypeID::INT64, LogicalTypeID::INT64, LogicalTypeID::BOOL};
            }

            /*
             * Outputs are
             *
             * nodeID::INTERNAL_ID
             * distance::DOUBLE
             */
            binder::expression_vector getResultColumns(binder::Binder *binder) const override {
                expression_vector columns;
                auto &inputNode = bindData->getNodeInput()->constCast<NodeExpression>();
                columns.push_back(inputNode.getInternalID());
                auto &outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
                columns.push_back(outputNode.getInternalID());
                columns.push_back(binder->createVariable(DISTANCE_COLUMN_NAME, LogicalType::DOUBLE()));
                return columns;
            }

            void bind(const expression_vector &params, Binder *binder, GraphEntry &graphEntry) override {
                auto nodeOutput = bindNodeOutput(binder, graphEntry);
                auto nodeTableId = graphEntry.nodeTableIDs[0];
                auto nodeInput = params[1];
                auto embeddingPropertyId = ExpressionUtil::getLiteralValue<int64_t>(*params[2]);
                auto val = params[3]->constCast<LiteralExpression>().getValue();
                KU_ASSERT(val.getDataType().getLogicalTypeID() == LogicalTypeID::LIST);
                auto childSize = NestedVal::getChildrenSize(&val);
                std::vector<float> queryVector(childSize);
                for (size_t i = 0; i < childSize; i++) {
                    auto child = NestedVal::getChildVal(&val, i);
                    KU_ASSERT(child->getDataType().getLogicalTypeID() == LogicalTypeID::DOUBLE);
                    queryVector[i] = child->getValue<double>();
                }
                auto k = ExpressionUtil::getLiteralValue<int64_t>(*params[4]);
                auto efSearch = ExpressionUtil::getLiteralValue<int64_t>(*params[5]);
                auto maxK = ExpressionUtil::getLiteralValue<int64_t>(*params[6]);
                auto outputProperty = ExpressionUtil::getLiteralValue<bool>(*params[7]);
                bindData = std::make_unique<VectorSearchBindData>(nodeInput, nodeOutput, nodeTableId,
                                                                  embeddingPropertyId, k, efSearch, maxK, queryVector,
                                                                  outputProperty);
            }

            void initLocalState(main::ClientContext *context) override {
                auto bind = ku_dynamic_cast<GDSBindData *, VectorSearchBindData *>(bindData.get());
                localState = std::make_unique<VectorSearchLocalState>(context, bind->nodeTableID,
                                                                      bind->embeddingPropertyID);
            }

            void searchNNOnUpperLevel(ExecutionContext *context, VectorIndexHeader *header,
                                      const float *query, CosineDistanceComputer *dc, vector_id_t &nearest,
                                      double &nearestDist) {
                while (true) {
                    vector_id_t prev_nearest = nearest;
                    size_t begin, end;
                    auto neighbors = header->getNeighbors(nearest, begin, end);
                    for (size_t i = begin; i < end; i++) {
                        vector_id_t neighbor = neighbors[i];
                        if (neighbor == INVALID_VECTOR_ID) {
                            break;
                        }
                        double dist;
                        auto embed = getEmbedding(context, header->getActualId(neighbor));
                        dc->computeDistance(embed, &dist);
                        if (dist < nearestDist) {
                            nearest = neighbor;
                            nearestDist = dist;
                        }
                    }
                    if (prev_nearest == nearest) {
                        break;
                    }
                }
            }

            void searchANNOnUpperLayer(ExecutionContext *context, VectorIndexHeader *header,
                                       const float *query, CosineDistanceComputer *dc, vector_id_t &entrypoint,
                                       double &entrypointDist, std::priority_queue<NodeDistCloser> &results,
                                       BitVectorVisitedTable *visited, int efSearch) {
                std::priority_queue<NodeDistFarther> candidates;
                candidates.emplace(entrypoint, entrypointDist);
                results.emplace(entrypoint, entrypointDist);
                visited->set_bit(entrypoint);
                while (!candidates.empty()) {
                    auto candidate = candidates.top();
                    if (candidate.dist > results.top().dist) {
                        break;
                    }
                    candidates.pop();
                    size_t begin, end;
                    auto neighbors = header->getNeighbors(candidate.id, begin, end);

                    for (size_t i = begin; i < end; i++) {
                        auto neighbor = neighbors[i];
                        if (neighbor == INVALID_VECTOR_ID) {
                            break;
                        }
                        if (visited->is_bit_set(neighbor)) {
                            continue;
                        }
                        visited->set_bit(neighbor);
                        double dist;
                        auto actualNbr = header->getActualId(neighbor);
                        auto embed = getEmbedding(context, actualNbr);
                        dc->computeDistance(embed, &dist);
                        if (results.size() < efSearch || dist < results.top().dist) {
                            candidates.emplace(neighbor, dist);
                            results.emplace(actualNbr, dist);
                            if (results.size() > efSearch) {
                                results.pop();
                            }
                        }
                    }
                }
                // Reset the visited table
                visited->reset();
            }

            // TODO: Use in-mem compressed vectors
            // TODO: Maybe try using separate threads to separate io and computation (ideal async io)
            const uint8_t *getCompressedEmbedding(processor::ExecutionContext *context, vector_id_t id) {
                auto searchLocalState =
                        ku_dynamic_cast<GDSLocalState *, VectorSearchLocalState *>(localState.get());
                auto compressedColumn = searchLocalState->compressedColumn;
                auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(id);

                // Initialize the read state
                auto readState = searchLocalState->readStates[nodeGroupIdx].get();
                compressedColumn->initChunkState(context->clientContext->getTx(), nodeGroupIdx, *readState);

                // Read the embedding
                auto nodeIdVector = searchLocalState->inputNodeIdVector.get();
                nodeIdVector->setValue(0, id);
                auto resultVector = searchLocalState->compressedVector.get();
                compressedColumn->lookup(context->clientContext->getTx(), *readState, nodeIdVector,
                                         resultVector);
                return ListVector::getDataVector(resultVector)->getData();
            }

            const float *getEmbedding(processor::ExecutionContext *context, vector_id_t id) {
                auto searchLocalState =
                        ku_dynamic_cast<GDSLocalState *, VectorSearchLocalState *>(localState.get());
                auto embeddingColumn = searchLocalState->embeddingColumn;
                auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(id);

                // Initialize the read state
                auto readState = searchLocalState->readStates[nodeGroupIdx].get();
                embeddingColumn->initChunkState(context->clientContext->getTx(), nodeGroupIdx, *readState);

                // Read the embedding
                auto nodeIdVector = searchLocalState->inputNodeIdVector.get();
                nodeIdVector->setValue(0, id);
                auto resultVector = searchLocalState->embeddingVector.get();
                embeddingColumn->lookup(context->clientContext->getTx(), *readState, nodeIdVector,
                                        resultVector);
                return reinterpret_cast<float *>(ListVector::getDataVector(resultVector)->getData());
            }

            void findEntrypointUsingUpperLayer(ExecutionContext *context, VectorIndexHeader *header,
                                               const float *query, CosineDistanceComputer *dc, vector_id_t &entrypoint,
                                               double *entrypointDist) {
                uint8_t entrypointLevel;
                header->getEntrypoint(entrypoint, entrypointLevel);
                if (entrypointLevel == 1) {
                    auto embedding = getEmbedding(context, header->getActualId(entrypoint));
                    dc->computeDistance(embedding, entrypointDist);
                    searchNNOnUpperLevel(context, header, query, dc, entrypoint, *entrypointDist);
                    entrypoint = header->getActualId(entrypoint);
                } else {
                    auto embedding = getEmbedding(context, entrypoint);
                    dc->computeDistance(embedding, entrypointDist);
                }
            }

            inline int findFilteredNextKNeighbours(common::nodeID_t nodeID, GraphScanState &state,
                                                   std::vector<common::nodeID_t> &nbrs,
                                                   NodeOffsetLevelSemiMask *filterMask,
                                                   VisitedTable &visited, int maxK, int maxNeighboursCheck, int& depth) {
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
                    visited.set(candidate.first.offset);
                    depth = std::max(depth, currentDepth);
                    auto neighbors = sharedState->graph.get()->scanFwdRandom(candidate.first, state);
                    neighboursChecked += 1;
                    // TODO: Maybe make it prioritized, might help in correlated cases
                    for (auto &neighbor: neighbors) {
                        if (visited.get(neighbor.offset)) {
                            continue;
                        }
                        if (filterMask->isMasked(neighbor.offset)) {
                            nbrs.push_back(neighbor);
                            visited.set(neighbor.offset);
                            if (nbrs.size() >= maxK) {
                                return neighboursChecked;
                            }
                        }
                        candidates.push({neighbor, currentDepth + 1});
                    }
                }
                return neighboursChecked;
            }

            // findFilteredNextKNeighboursV2 (uses priority queue) expands the closest first
            // Assumption is that distance computation gets cheaper as we move forward, using quantization
//            inline int
//            findFilteredNextKNeighboursSmart(DistanceComputer *dc, ExecutionContext *context, common::nodeID_t entryPoint, GraphScanState &state,
//                                             std::vector<common::nodeID_t> &nbrs,
//                                             NodeOffsetLevelSemiMask *filterMask,
//                                             VisitedTable &visited, int minK, int maxNeighboursCheck, int& totalDistanceComputations, int& depth) {
//                auto graph = sharedState->graph.get();
//                auto tableId = entryPoint.tableID;
//                // Initialize the priority queue with the entry point
//
//                std::unordered_map<vector_id_t, std::vector<nodeID_t>> cachedNbrs;
//                std::priority_queue<NodeDistFarther> candidates;
//                candidates.emplace(entryPoint.offset, MAXFLOAT);
//                std::priority_queue<NodeDistFarther> exploreCandidates;
//                exploreCandidates.emplace(entryPoint.offset, MAXFLOAT);
//                std::unordered_set<offset_t> visitedSet;
//                auto neighboursChecked = 0;
//                while (neighboursChecked <= maxNeighboursCheck && !candidates.empty()) {
//                    auto candidate = candidates.top();
//                    candidates.pop();
//                    if (visitedSet.contains(candidate.id)) {
//                        continue;
//                    }
//                    visitedSet.insert(candidate.id);
//                    visited.set(candidate.id);
//                    auto neighbors = graph->scanFwdRandom({candidate.id, tableId}, state);
//                    neighboursChecked += 1;
//                    depth = std::max(depth, candidate.depth);
//                    for (auto &neighbor: neighbors) {
//                        if (visited.get(neighbor.offset)) {
//                            continue;
//                        }
//                        if (filterMask->isMasked(neighbor.offset)) {
//                            nbrs.push_back(neighbor);
//                            visited.set(neighbor.offset);
//                        }
//                    }
//
//                    if (nbrs.size() >= minK) {
//                        break;
//                    }
//                    cachedNbrs[candidate.id] = std::move(neighbors);
//                    if (candidates.empty()) {
//                        if (exploreCandidates.empty()) {
//                            break;
//                        }
//                        auto exploreCandidate = exploreCandidates.top();
//                        exploreCandidates.pop();
//                        for (auto &neighbor: cachedNbrs[exploreCandidate.id]) {
//                            if (visited.get(neighbor.offset) || visitedSet.contains(neighbor.offset)) {
//                                continue;
//                            }
//                            // TODO: This will get optimized when we cache the neighbours in memory
//                            auto embedding = getEmbedding(context, neighbor.offset);
//                            double dist;
//                            totalDistanceComputations += 1;
//                            dc->computeDistance(embedding, &dist);
//                            candidates.emplace(neighbor.offset, dist, exploreCandidate.depth + 1);
//                            exploreCandidates.emplace(neighbor.offset, dist, exploreCandidate.depth + 1);
//                        }
//                    }
//                }
//                return neighboursChecked;
//            }

            inline void findNextKNeighbours(common::nodeID_t nodeID, GraphScanState &state,
                                            std::vector<common::nodeID_t> &nbrs, AtomicVisitedTable* visited) {
                auto unFilteredNbrs = sharedState->graph.get()->scanFwdRandom(nodeID, state);
                for (auto &neighbor: unFilteredNbrs) {
                    if (visited->getAndSet(neighbor.offset)) {
                        nbrs.push_back(neighbor);
                    }
                }
            }

            inline bool isMasked(common::offset_t offset, NodeOffsetLevelSemiMask *filterMask) {
                return !filterMask->isEnabled() || filterMask->isMasked(offset);
            }

            inline int calculateMaxK(uint64_t numMaskedNodes, uint64_t totalNodes) {
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

            void rerankResults(std::priority_queue<NodeDistCloser> &results, int k) {
                std::priority_queue<NodeDistCloser> reranked;
                while (!results.empty()) {
                    reranked.push(results.top());
                    results.pop();
                }
                results = std::move(reranked);
            }

            void coreSearch(ExecutionContext *context, GDSLocalState *searchLocalState, GDSBindData *bindState, Graph *graph,
                            DistanceComputer *dc, const bool isFilteredSearch, const bool useInFilterSearch, NodeOffsetLevelSemiMask *filterMask,
                            GraphScanState *state, const vector_id_t entrypoint, const double entrypointDist, const int maxK,
                            std::priority_queue<NodeDistCloser> &results, AtomicVisitedTable* visited, VectorIndexHeader* header, const int efSearch) {

                std::priority_queue<NodeDistFarther> candidates;
                candidates.emplace(entrypoint, entrypointDist);

                visited->set(entrypoint);

                // Metrics
                int totalIterations = 0;
                int avgDepth = 0;
                int maxDepth = 0;
                int minDepth = 0;
                int totalDistanceComputations = 0;
                int totalCallsToScanNbr = 0;
                std::chrono::nanoseconds filterGraphSearchTime(0);
                std::chrono::nanoseconds randomFilterNeighbourTime(0);

                while (!candidates.empty()) {
                    auto candidate = candidates.top();
                    if (!results.empty() && candidate.dist > results.top().dist) {
                        break;
                    }
                    candidates.pop();

                    std::vector<common::nodeID_t> neighbors;
//                    if (isFilteredSearch && !useInFilterSearch) {
//                        int searchDepth = 0;
//                        auto startFilterSearch = std::chrono::high_resolution_clock::now();
//                        auto callsToScanNbr = findFilteredNextKNeighboursSmart(dc, context,
//                                                                               {candidate.id, header->getNodeTableId()},
//                                                                               *state, neighbors, filterMask, *visited,
//                                                                               maxK, 64,
//                                                                               totalDistanceComputations, searchDepth);
//                        avgDepth += searchDepth;
//                        maxDepth = std::max(maxDepth, searchDepth);
//                        minDepth = std::min(minDepth, searchDepth);
//                        totalIterations++;
//                        totalCallsToScanNbr += callsToScanNbr;
//                        auto endFilterSearch = std::chrono::high_resolution_clock::now();
//                        filterGraphSearchTime += std::chrono::duration_cast<std::chrono::nanoseconds>(
//                                endFilterSearch - startFilterSearch);
//
//                        if (candidates.empty() && neighbors.empty()) {
//                            auto startRandomNbr = std::chrono::high_resolution_clock::now();
//                            for (offset_t i = 0; i < filterMask->getMaxOffset(); i++) {
//                                if (filterMask->isMasked(i) && !visited->get(i)) {
//                                    neighbors.push_back({i, header->getNodeTableId()});
//                                    break;
//                                }
//                            }
//                            auto endRandomNbr = std::chrono::high_resolution_clock::now();
//                            randomFilterNeighbourTime += std::chrono::duration_cast<std::chrono::nanoseconds>(
//                                    endRandomNbr - startRandomNbr);
//                        }
//                    } else {
                    auto startSearch = std::chrono::high_resolution_clock::now();
                    findNextKNeighbours({candidate.id, header->getNodeTableId()}, *state, neighbors, visited);
                    auto endSearch = std::chrono::high_resolution_clock::now();
                    filterGraphSearchTime += std::chrono::duration_cast<std::chrono::nanoseconds>(endSearch - startSearch);
//                    }

                    for (auto neighbor: neighbors) {
                        double dist;
                        auto embed = getEmbedding(context, neighbor.offset);
                        dc->computeDistance(embed, &dist);
                        totalDistanceComputations++;

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
                }

                // Print metrics at the end of the search
                printf("Total distance computations: %d\n", totalDistanceComputations);
                printf("Total calls to scan neighbours: %d\n", totalCallsToScanNbr);
                printf("Total filter scan search time: %lld ns\n", filterGraphSearchTime.count());
                printf("Total random filter neighbour time: %lld ns\n", randomFilterNeighbourTime.count());
                printf("Avg depth: %f\n", static_cast<double>(avgDepth) / totalIterations);
                printf("Max depth: %d\n", maxDepth);
                printf("Min depth: %d\n", minDepth);
                printf("Iterations: %d\n", totalIterations);
            }

            void exec(ExecutionContext *context) override {
                auto searchLocalState = ku_dynamic_cast<GDSLocalState *, VectorSearchLocalState *>(localState.get());
                auto bindState = ku_dynamic_cast<GDSBindData *, VectorSearchBindData *>(bindData.get());
                auto header = searchLocalState->indexHeader;
                auto graph = sharedState->graph.get();
                auto nodeTableId = header->getNodeTableId();
                auto efSearch = bindState->efSearch;
                auto maxK = bindState->maxK;
                auto k = bindState->k;
                KU_ASSERT(bindState->queryVector.size() == header->getDim());
                auto query = bindState->queryVector.data();
                auto state = graph->prepareScan(header->getCSRRelTableId());
                auto visited = std::make_unique<BitVectorVisitedTable>(header->getNumVectors());
                auto dc = std::make_unique<CosineDistanceComputer>(query, header->getDim(), header->getNumVectors());
                dc->setQuery(query);
                auto filterMask = sharedState->inputNodeOffsetMasks.at(header->getNodeTableId()).get();
                auto maxNumThreads = context->clientContext->getMaxNumThreadForExec();
                std::unique_ptr<ParallelMultiQueue<NodeDistFarther>> parallelResults = std::make_unique<ParallelMultiQueue<NodeDistFarther>>(
                        maxNumThreads * 2, efSearch);

//                if (isFilteredSearch) {
//                    auto selectivity = static_cast<double>(filterMask->getNumMaskedNodes()) / header->getNumVectors();
//                    if (selectivity <= 0.005) {
//                        printf("skipping search since selectivity too low\n");
//                        return;
//                    }
//                    if (selectivity > 0.3) {
//                        useInFilterSearch = true;
//                    }
//                    maxK = calculateMaxK(filterMask->getNumMaskedNodes(), header->getNumVectors());
//                }

//                vector_id_t entrypoint;
//                double entrypointDist;
//                findEntrypointUsingUpperLayer(context, header, query, dc.get(), entrypoint, &entrypointDist);
//                std::priority_queue<NodeDistCloser> results;
//                coreSearch(context, searchLocalState, bindState, graph, dc.get(), isFilteredSearch, useInFilterSearch, filterMask, state.get(),
//                           entrypoint, entrypointDist, maxK, results, visited.get(), header, 64);
//                visited->reset();

                std::priority_queue<NodeDistCloser> candidates;
                uint8_t entrypointLevel;
                vector_id_t entrypoint;
                header->getEntrypoint(entrypoint, entrypointLevel);
                assert(entrypointLevel == 1);
                int entrypointNodes = maxNumThreads * 2;
                auto embedding = getEmbedding(context, header->getActualId(entrypoint));
                double entrypointDist;
                dc->computeDistance(embedding, &entrypointDist);
                searchANNOnUpperLayer(context, header, query, dc.get(), entrypoint, entrypointDist, candidates, visited.get(), 64);
                assert(candidates.size() > entrypointNodes);

                std::vector<std::vector<NodeDistCloser>> entrypointsPerThread(maxNumThreads);
                int count = 0;
                while (!candidates.empty()) {
                    auto res = candidates.top();
                    candidates.pop();
                    if (candidates.size() <= entrypointNodes) {
                        entrypointsPerThread[count % maxNumThreads].emplace_back(res.id, res.dist);
                        count++;
                        if (isMasked(res.id, filterMask)) {
                            parallelResults->push(NodeDistFarther(res.id, res.dist));
                            visited->atomic_set_bit(res.id);
                        }
                    }
                }
                auto taskSharedState = std::make_shared<VectorSearchTaskSharedState>(maxNumThreads, efSearch, maxK, context,
                                                                               graph, dc.get(), filterMask, header, visited.get(),
                                                                               parallelResults.get(),
                                                                               std::move(entrypointsPerThread));
                auto taskLocalState = std::make_unique<NodeTableLookupLocalState>(context,
                                                                                  searchLocalState->embeddingColumn,
                                                                                  nullptr, nullptr);
                auto task = std::make_shared<VectorSearchTask>(maxNumThreads, taskSharedState, std::move(taskLocalState));
                context->clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task, context);
                for (auto& queue : parallelResults->queues) {
                    while (queue->size() > 0) {
                        auto min = queue->popMin();
                        candidates.push(NodeDistCloser(min.id, min.dist));
                    }
                }
                searchLocalState->materialize(graph, candidates, *sharedState->fTable, k);
            }

            std::unique_ptr<GDSAlgorithm> copy() const override {
                return std::make_unique<VectorSearch>(*this);
            }
        };

        function_set VectorSearchFunction::getFunctionSet() {
            function_set result;
            auto function = std::make_unique<GDSFunction>(name, std::make_unique<VectorSearch>());
            result.push_back(std::move(function));
            return result;
        }

    } // namespace function
} // namespace kuzu

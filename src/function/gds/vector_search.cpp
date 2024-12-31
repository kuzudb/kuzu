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
//                compressedColumn = nodeTable->getColumn(indexHeader->getCompressedPropertyId());

                // TODO: Replace with compressed vector. Should be stored in the
                embeddingVector = std::make_unique<ValueVector>(embeddingColumn->getDataType().copy(),
                                                                context->getMemoryManager());
                embeddingVector->state = DataChunkState::getSingleValueDataChunkState();
//                compressedVector = std::make_unique<ValueVector>(compressedColumn->getDataType().copy(),
//                                                                 context->getMemoryManager());
//                compressedVector->state = DataChunkState::getSingleValueDataChunkState();
                inputNodeIdVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID().copy(),
                                                                  context->getMemoryManager());
                inputNodeIdVector->state = DataChunkState::getSingleValueDataChunkState();
                for (size_t i = 0; i < embeddingColumn->getNumNodeGroups(context->getTx()); i++) {
                    readStates.push_back(std::make_unique<Column::ChunkState>());
                }
            }

            void materialize(graph::Graph *graph, BinaryHeap<NodeDistFarther> &results,
                             FactorizedTable &table, int k) const {
                // TODO: Rerank the results if using some even lower quantization like bitQ

                // Remove elements until we have k elements
                while (results.size() > k) {
                    results.popMin();
                }

                std::priority_queue<NodeDistFarther> reversed;
                while (results.size() > 0) {
                    reversed.emplace(results.top()->id, results.top()->dist);
                    results.popMin();
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

            // TODO: Use in-mem compressed vectors
            // TODO: Maybe try using separate threads to separate io and computation (ideal async io)
//            const uint8_t *getCompressedEmbedding(processor::ExecutionContext *context, vector_id_t id) {
//                auto searchLocalState =
//                        ku_dynamic_cast<GDSLocalState *, VectorSearchLocalState *>(localState.get());
//                auto compressedColumn = searchLocalState->compressedColumn;
//                auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(id);
//
//                // Initialize the read state
//                auto readState = searchLocalState->readStates[nodeGroupIdx].get();
//                compressedColumn->initChunkState(context->clientContext->getTx(), nodeGroupIdx, *readState);
//
//                // Read the embedding
//                auto nodeIdVector = searchLocalState->inputNodeIdVector.get();
//                nodeIdVector->setValue(0, id);
//                auto resultVector = searchLocalState->compressedVector.get();
//                compressedColumn->lookup(context->clientContext->getTx(), *readState, nodeIdVector,
//                                         resultVector);
//                return ListVector::getDataVector(resultVector)->getData();
//            }

            inline void
            computeDistance(processor::ExecutionContext *context, vector_id_t id, CosineDistanceComputer *dc,
                            double *dist) {
                auto searchLocalState =
                        ku_dynamic_cast<GDSLocalState *, VectorSearchLocalState *>(localState.get());
                auto embeddingColumn = searchLocalState->embeddingColumn;
                auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(id);

                // Initialize the read state
                auto readState = searchLocalState->readStates[nodeGroupIdx].get();
                embeddingColumn->initChunkState(context->clientContext->getTx(), nodeGroupIdx, *readState);
                // Fast compute on embedding
                // TODO: Add support for batch computation using io uring
                embeddingColumn->fastLookup(context->clientContext->getTx(), *readState, id,
                                            [dc, dist](const uint8_t *frame, uint16_t posInFrame,
                                                       uint32_t numValuesToRead) {
                                                auto embedding = reinterpret_cast<const float *>(frame);
                                                dc->computeDistance(embedding + posInFrame, dist);
                                            });
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

            inline const uint8_t *getCompressedEmbedding(VectorIndexHeaderPerPartition *header, vector_id_t id) {
                return header->getQuantizedVectors() + id * header->getQuantizer()->codeSize;
            }

            void searchNNOnUpperLevel(ExecutionContext *context, VectorIndexHeaderPerPartition *header,
                                      const float *query, CosineDistanceComputer *dc,
                                      QuantizedDistanceComputer<float, uint8_t> *qdc,
                                      vector_id_t &nearest,
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
//                        auto embedding = getCompressedEmbedding(header, header->getActualId(neighbor));
//                        qdc->compute_distance(query, embedding, &dist);
                        computeDistance(context, header->getActualId(neighbor), dc, &dist);
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

            void findEntrypointUsingUpperLayer(ExecutionContext *context, VectorIndexHeaderPerPartition *header,
                                               const float *query, CosineDistanceComputer *dc,
                                               QuantizedDistanceComputer<float, uint8_t> *qdc, vector_id_t &entrypoint,
                                               double *entrypointDist) {
                uint8_t entrypointLevel;
                header->getEntrypoint(entrypoint, entrypointLevel);
                if (entrypointLevel == 1) {
//                    auto embedding = getCompressedEmbedding(header, header->getActualId(entrypoint));
//                    qdc->compute_distance(query, embedding, entrypointDist);
                    computeDistance(context, header->getActualId(entrypoint), dc, entrypointDist);
                    searchNNOnUpperLevel(context, header, query, dc, qdc, entrypoint, *entrypointDist);
                    entrypoint = header->getActualId(entrypoint);
                } else {
//                    auto embedding = getCompressedEmbedding(header, entrypoint);
//                    qdc->compute_distance(query, embedding, entrypointDist);
                    computeDistance(context, entrypoint, dc, entrypointDist);
                }
            }

            inline bool isMasked(common::offset_t offset, NodeOffsetLevelSemiMask *filterMask) {
                return !filterMask->isEnabled() || filterMask->isMasked(offset);
            }

            void rerankResults(std::priority_queue<NodeDistCloser> &results, int k) {
                std::priority_queue<NodeDistCloser> reranked;
                while (!results.empty()) {
                    reranked.push(results.top());
                    results.pop();
                }
                results = std::move(reranked);
            }

            void unfilteredSearch(processor::ExecutionContext *context, const float *query, const table_id_t tableId,
                                  Graph *graph,
                                  CosineDistanceComputer *dc, QuantizedDistanceComputer<float, uint8_t> *qdc,
                                  GraphScanState &state, const vector_id_t entrypoint, const double entrypointDist,
                                  BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                  VectorIndexHeaderPerPartition *header, const int efSearch) {
                std::priority_queue<NodeDistFarther> candidates;
                candidates.emplace(entrypoint, entrypointDist);
                results.push(NodeDistFarther{entrypoint, entrypointDist});
                visited->set_bit(entrypoint);
                while (!candidates.empty()) {
                    auto candidate = candidates.top();
                    if (candidate.dist > results.top()->dist) {
                        break;
                    }
                    candidates.pop();

                    // Get graph neighbours
                    auto nbrs = graph->scanFwdRandom({candidate.id, tableId}, state);

                    for (auto &neighbor: nbrs) {
                        if (visited->is_bit_set(neighbor.offset)) {
                            continue;
                        }
                        visited->set_bit(neighbor.offset);
                        double dist;
                        computeDistance(context, neighbor.offset, dc, &dist);
                        if (results.size() < efSearch || dist < results.top()->dist) {
                            candidates.emplace(neighbor.offset, dist);
                            results.push(NodeDistFarther{neighbor.offset, dist});
                        }
                    }
                }
            }

            inline void
            oneHopSearch(processor::ExecutionContext *context, std::priority_queue<NodeDistFarther> &candidates,
                         std::vector<common::nodeID_t> &firstHopNbrs, const float *query,
                         const table_id_t tableId,
                         Graph *graph, CosineDistanceComputer *dc,
                         QuantizedDistanceComputer<float, uint8_t> *qdc, NodeOffsetLevelSemiMask *filterMask,
                         GraphScanState &state,
                         BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                         VectorIndexHeaderPerPartition *header, const int efSearch, int &totalDist) {
                for (auto &neighbor: firstHopNbrs) {
                    if (visited->is_bit_set(neighbor.offset)) {
                        continue;
                    }
                    visited->set_bit(neighbor.offset);
                    double dist;
                    computeDistance(context, neighbor.offset, dc, &dist);
                    totalDist += 1;
                    if (results.size() < efSearch || dist < results.top()->dist) {
                        candidates.emplace(neighbor.offset, dist);
                        if (filterMask->isMasked(neighbor.offset)) {
                            results.push(NodeDistFarther(neighbor.offset, dist));
                        }
                    }
                }
            }

            inline void twoHopSearch(processor::ExecutionContext *context,
                                     std::priority_queue<NodeDistFarther> &candidates,
                                     std::vector<common::nodeID_t> &firstHopNbrs,
                                     const float *query, const table_id_t tableId,
                                     Graph *graph,
                                     CosineDistanceComputer *dc,
                                     QuantizedDistanceComputer<float, uint8_t> *qdc,
                                     NodeOffsetLevelSemiMask *filterMask,
                                     GraphScanState &state,
                                     BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                     VectorIndexHeaderPerPartition *header, const int efSearch,
                                     int &totalGetNbrs, int &totalDist) {
                // Get the first hop neighbours
                std::queue<vector_id_t> nbrsToExplore;

                // Try prefetching
                for (auto &neighbor: firstHopNbrs) {
                    filterMask->prefetchMaskValue(neighbor.offset);
                }

                // First hop neighbours
                for (auto &neighbor: firstHopNbrs) {
                    auto isNeighborMasked = filterMask->isMasked(neighbor.offset);
                    if (visited->is_bit_set(neighbor.offset)) {
                        continue;
                    }
                    if (isNeighborMasked) {
                        double dist;
                        computeDistance(context, neighbor.offset, dc, &dist);
                        totalDist++;
                        visited->set_bit(neighbor.offset);
                        if (results.size() < efSearch || dist < results.top()->dist) {
                            candidates.emplace(neighbor.offset, dist);
                            results.push(NodeDistFarther(neighbor.offset, dist));
                        }
                    }
                    nbrsToExplore.push(neighbor.offset);
                }

                // Second hop neighbours
                // TODO: Maybe there's some benefit in doing batch distance computation
                while (!nbrsToExplore.empty()) {
                    auto neighbor = nbrsToExplore.front();
                    nbrsToExplore.pop();

                    if (visited->is_bit_set(neighbor)) {
                        continue;
                    }
                    visited->set_bit(neighbor);
                    auto secondHopNbrs = graph->scanFwdRandom({neighbor, tableId}, state);

                    // Try prefetching
                    for (auto &secondHopNeighbor: secondHopNbrs) {
                        filterMask->prefetchMaskValue(secondHopNeighbor.offset);
                    }

                    totalGetNbrs++;
                    for (auto &secondHopNeighbor: secondHopNbrs) {
                        auto isNeighborMasked = filterMask->isMasked(secondHopNeighbor.offset);
                        if (visited->is_bit_set(secondHopNeighbor.offset)) {
                            continue;
                        }
                        if (isNeighborMasked) {
                            // TODO: Maybe there's some benefit in doing batch distance computation
                            visited->set_bit(secondHopNeighbor.offset);
                            double dist;
                            computeDistance(context, secondHopNeighbor.offset, dc, &dist);
                            totalDist++;
                            if (results.size() < efSearch || dist < results.top()->dist) {
                                candidates.emplace(secondHopNeighbor.offset, dist);
                                results.push(NodeDistFarther(secondHopNeighbor.offset, dist));
                            }
                        }
                    }
                }
            }

            inline void dynamicTwoHopSearch(processor::ExecutionContext *context,
                                            std::priority_queue<NodeDistFarther> &candidates,
                                            NodeDistFarther &candidate, int filterNbrsToFind,
                                            std::unordered_map<vector_id_t, int> &cachedNbrsCount,
                                            std::vector<common::nodeID_t> &firstHopNbrs,
                                            const float *query, const table_id_t tableId,
                                            Graph *graph,
                                            CosineDistanceComputer *dc,
                                            QuantizedDistanceComputer<float, uint8_t> *qdc,
                                            NodeOffsetLevelSemiMask *filterMask,
                                            GraphScanState &state,
                                            BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                            VectorIndexHeaderPerPartition *header, const int efSearch,
                                            int &totalGetNbrs, int &totalDist) {
                // Get the first hop neighbours
                std::priority_queue<NodeDistFarther> nbrsToExplore;

                // Try prefetching
                for (auto &neighbor: firstHopNbrs) {
                    filterMask->prefetchMaskValue(neighbor.offset);
                }

                // First hop neighbours
                int filteredCount = 0;
                for (auto &neighbor: firstHopNbrs) {
                    auto isNeighborMasked = filterMask->isMasked(neighbor.offset);
                    if (isNeighborMasked) {
                        filteredCount++;
                    }
                    if (visited->is_bit_set(neighbor.offset)) {
                        continue;
                    }
                    double dist;
                    computeDistance(context, neighbor.offset, dc, &dist);
                    totalDist++;
                    nbrsToExplore.emplace(neighbor.offset, dist);

                    if (isNeighborMasked) {
                        visited->set_bit(neighbor.offset);
                    }

                    if ((results.size() < efSearch || dist < results.top()->dist) && isNeighborMasked) {
                        candidates.emplace(neighbor.offset, dist);
                        results.push(NodeDistFarther(neighbor.offset, dist));
                    }
                }
                cachedNbrsCount[candidate.id] = filteredCount;
                int exploredFilteredNbrCount = filteredCount;

                // Second hop neighbours
                // TODO: Maybe there's some benefit in doing batch distance computation
                while (!nbrsToExplore.empty()) {
                    auto neighbor = nbrsToExplore.top();
                    nbrsToExplore.pop();

                    if (cachedNbrsCount.contains(neighbor.id)) {
                        exploredFilteredNbrCount += cachedNbrsCount[neighbor.id];
                    }
                    if (exploredFilteredNbrCount >= filterNbrsToFind) {
                        break;
                    }
                    if (visited->is_bit_set(neighbor.id)) {
                        continue;
                    }
                    visited->set_bit(neighbor.id);

                    int secondHopFilteredNbrCount = 0;
                    auto secondHopNbrs = graph->scanFwdRandom({neighbor.id, tableId}, state);
                    totalGetNbrs++;

                    // Try prefetching
                    for (auto &secondHopNeighbor: secondHopNbrs) {
                        filterMask->prefetchMaskValue(secondHopNeighbor.offset);
                    }

                    for (auto &secondHopNeighbor: secondHopNbrs) {
                        auto isNeighborMasked = filterMask->isMasked(secondHopNeighbor.offset);
                        if (isNeighborMasked) {
                            secondHopFilteredNbrCount++;
                        }
                        if (visited->is_bit_set(secondHopNeighbor.offset)) {
                            continue;
                        }
                        if (isNeighborMasked) {
                            // TODO: Maybe there's some benefit in doing batch distance computation
                            visited->set_bit(secondHopNeighbor.offset);
                            double dist;
                            computeDistance(context, secondHopNeighbor.offset, dc, &dist);
                            totalDist++;
                            if (results.size() < efSearch || dist < results.top()->dist) {
                                candidates.emplace(secondHopNeighbor.offset, dist);
                                results.push(NodeDistFarther(secondHopNeighbor.offset, dist));
                            }
                        }
                    }
                    exploredFilteredNbrCount += secondHopFilteredNbrCount;
                    cachedNbrsCount[neighbor.id] = secondHopFilteredNbrCount;
                }
            }

            inline void
            addFilteredNodesToCandidates(processor::ExecutionContext *context, CosineDistanceComputer *dc,
                                         std::priority_queue<NodeDistFarther> &candidates,
                                         BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                         NodeOffsetLevelSemiMask *filterMask, int maxNodesToAdd, int &totalDist) {
                auto startTime = std::chrono::high_resolution_clock::now();
                // Add some initial candidates to handle neg correlation case
                int nbrsAdded = 0;
                for (offset_t i = 0; i < filterMask->getMaxOffset(); i++) {
                    if (filterMask->isMasked(i)) {
                        double dist;
                        computeDistance(context, i, dc, &dist);
                        candidates.emplace(i, dist);
                        results.push(NodeDistFarther(i, dist));
                        visited->set_bit(i);
                        nbrsAdded++;
                        totalDist++;
                    }
                    if (nbrsAdded >= maxNodesToAdd) {
                        break;
                    }
                }
                auto endTime = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
                printf("addFilteredNodesToCandidates (%d, %d): %f ms\n", nbrsAdded, maxNodesToAdd, (float)duration.count() / (1000000.0));
            }

            void filteredSearch(processor::ExecutionContext *context, const float *query,
                                float selectivity,
                                const table_id_t tableId, Graph *graph,
                                CosineDistanceComputer *dc, QuantizedDistanceComputer<float, uint8_t> *qdc,
                                NodeOffsetLevelSemiMask *filterMask,
                                GraphScanState &state, const vector_id_t entrypoint,
                                const double entrypointDist,
                                BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                VectorIndexHeaderPerPartition *header, const int efSearch, const int maxK,
                                NumericMetric *distCompMetric, NumericMetric *listNbrsCallMetric) {
                std::priority_queue<NodeDistFarther> candidates;
                candidates.emplace(entrypoint, entrypointDist);
                if (filterMask->isMasked(entrypoint)) {
                    results.push(NodeDistFarther(entrypoint, entrypointDist));
                }
                visited->set_bit(entrypoint);
                // We will use this metric to skip some part of second hop.
                std::unordered_map<vector_id_t, int> cachedNbrsCount;
                int totalGetNbrs = 0;
                int totalDist = 0;

                // Handle for neg correlation cases
                addFilteredNodesToCandidates(context, dc, candidates, results, visited, filterMask, maxK, totalDist);

                while (!candidates.empty()) {
                    auto candidate = candidates.top();
                    if (candidate.dist > results.top()->dist && results.size() > 0) {
                        break;
                    }
                    candidates.pop();
                    // Get the first hop neighbours
                    auto firstHopNbrs = graph->scanFwdRandom({candidate.id, tableId}, state);
                    totalGetNbrs++;
                    // Reduce density!!
                    auto filterNbrsToFind = firstHopNbrs.size();

                    if (selectivity >= 0.5) {
                        oneHopSearch(context, candidates,
                                     firstHopNbrs, query, tableId,
                                     graph, dc, qdc, filterMask,
                                     state, results, visited, header, efSearch, totalDist);
                    } else if ((filterNbrsToFind * filterNbrsToFind * selectivity) > (filterNbrsToFind * 2)) {
                        // We will use this metric to skip unwanted distance computation in the first hop
                        dynamicTwoHopSearch(context, candidates, candidate, filterNbrsToFind, cachedNbrsCount,
                                            firstHopNbrs, query, tableId,
                                            graph, dc, qdc, filterMask,
                                            state, results, visited, header, efSearch,
                                            totalGetNbrs, totalDist);
                    } else {
                        twoHopSearch(context, candidates, firstHopNbrs, query, tableId,
                                     graph, dc, qdc, filterMask,
                                     state, results, visited, header, efSearch,
                                     totalGetNbrs, totalDist);
                    }
                }
                distCompMetric->increase(totalDist);
                listNbrsCallMetric->increase(totalGetNbrs);
            }

            void exec(ExecutionContext *context) override {
                auto searchLocalState = ku_dynamic_cast<GDSLocalState *, VectorSearchLocalState *>(localState.get());
                auto bindState = ku_dynamic_cast<GDSBindData *, VectorSearchBindData *>(bindData.get());
                auto indexHeader = searchLocalState->indexHeader;
                auto header = indexHeader->getPartitionHeader(0);
                auto graph = sharedState->graph.get();
                auto nodeTableId = indexHeader->getNodeTableId();
                auto efSearch = bindState->efSearch;
                auto filterMaxK = bindState->maxK;
                auto k = bindState->k;
                KU_ASSERT(bindState->queryVector.size() == indexHeader->getDim());
                auto query = bindState->queryVector.data();
                auto state = graph->prepareScan(header->getCSRRelTableId());
                auto visited = std::make_unique<BitVectorVisitedTable>(header->getNumVectors());
                auto filterMask = sharedState->inputNodeOffsetMasks.at(indexHeader->getNodeTableId()).get();
                bool isFilteredSearch = filterMask->isEnabled();
                auto quantizedDc = header->getQuantizer()->get_asym_distance_computer(L2_SQ);
                auto dc = std::make_unique<CosineDistanceComputer>(query, indexHeader->getDim(),
                                                                   header->getNumVectors());
                dc->setQuery(query);
                auto vectorSearchTimeMetric = context->profiler->registerTimeMetricForce("vectorSearchTime");
                auto distCompMetric = context->profiler->registerNumericMetricForce("distanceComputations");
                auto listNbrsMetric = context->profiler->registerNumericMetricForce("listNbrsCalls");
                vectorSearchTimeMetric->start();

                // Find closest entrypoint using the above layer!!
                vector_id_t entrypoint;
                double entrypointDist;
                findEntrypointUsingUpperLayer(context, header, query, dc.get(), quantizedDc.get(), entrypoint,
                                              &entrypointDist);
                BinaryHeap<NodeDistFarther> results(efSearch);

                if (isFilteredSearch) {
                    auto selectivity = static_cast<double>(filterMask->getNumMaskedNodes()) / header->getNumVectors();
                    if (selectivity <= 0.001) {
                        printf("skipping search since selectivity too low\n");
                        return;
                    }
                    filteredSearch(context, query, selectivity, nodeTableId, graph, dc.get(),
                                   quantizedDc.get(),
                                   filterMask, *state.get(), entrypoint, entrypointDist, results,
                                   visited.get(), header, efSearch, filterMaxK, distCompMetric, listNbrsMetric);
                } else {
                    unfilteredSearch(context, query, nodeTableId, graph, dc.get(), quantizedDc.get(), *state.get(),
                                     entrypoint,
                                     entrypointDist, results, visited.get(), header, efSearch);
                }

                searchLocalState->materialize(graph, results, *sharedState->fTable, k);
                vectorSearchTimeMetric->stop();
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

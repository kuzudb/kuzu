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
#include "common/vector_index/distance_computer.h"
#include "common/task_system/task_scheduler.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
    namespace function {
        typedef std::array<vector_id_t, 4096> vector_array_t;

        struct VectorSearchBindData final : public GDSBindData {
            std::shared_ptr<binder::Expression> nodeInput;
            table_id_t nodeTableID = INVALID_TABLE_ID;
            property_id_t embeddingPropertyID = INVALID_PROPERTY_ID;
            int k = 100;
            int efSearch = 100;
            int numFilteredNodesToAdd = 5;
            bool useQuantizedVectors = false;
            bool useKnnSearch = false;
            std::vector<float> queryVector;

            VectorSearchBindData(std::shared_ptr<binder::Expression> nodeInput,
                                 std::shared_ptr<binder::Expression> nodeOutput, table_id_t nodeTableID,
                                 property_id_t embeddingPropertyID, int k, int64_t efSearch, int numFilteredNodesToAdd,
                                 bool useQuantizedVectors, bool useKnnSearch, std::vector<float> queryVector, bool outputAsNode)
                    : GDSBindData{std::move(nodeOutput), outputAsNode}, nodeInput(std::move(nodeInput)),
                      nodeTableID(nodeTableID), embeddingPropertyID(embeddingPropertyID), k(k),
                      efSearch(efSearch), numFilteredNodesToAdd(numFilteredNodesToAdd),
                      useQuantizedVectors(useQuantizedVectors), useKnnSearch(useKnnSearch),
                      queryVector(std::move(queryVector)) {};

            VectorSearchBindData(const VectorSearchBindData &other)
                    : GDSBindData{other}, nodeInput(other.nodeInput), nodeTableID(other.nodeTableID),
                      embeddingPropertyID(other.embeddingPropertyID), k(other.k), efSearch{other.efSearch},
                      numFilteredNodesToAdd(other.numFilteredNodesToAdd),
                      useQuantizedVectors(other.useQuantizedVectors), useKnnSearch(other.useKnnSearch),
                      queryVector(other.queryVector) {}

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
                // TODO: Handle multiple partitions
                auto partitionHeader = indexHeader->getPartitionHeader(0);
                auto startOffset = partitionHeader->getStartNodeGroupId() * StorageConstants::NODE_GROUP_SIZE;
                this->dc = createDistanceComputer(context, nodeTableId, embeddingPropertyId, startOffset,
                                                  indexHeader->getDim(), indexHeader->getConfig().distanceFunc);
                this->qdc = createQuantizedDistanceComputer(context, nodeTableId,
                                                            indexHeader->getCompressedPropertyId(), startOffset,
                                                            indexHeader->getDim(),
                                                            indexHeader->getConfig().distanceFunc,
                                                            partitionHeader->getQuantizer());
            }

            void materialize(std::priority_queue<NodeDistFarther> &results, FactorizedTable &table, int k) const {
                // Remove elements until we have k elements
                int count = 0;
                while (!results.empty()) {
                    auto res = results.top();
                    results.pop();
                    auto nodeID = nodeID_t{res.id, indexHeader->getNodeTableId()};
                    nodeInputVector->setValue<nodeID_t>(0, nodeID);
                    nodeIdVector->setValue<nodeID_t>(0, nodeID);
                    distanceVector->setValue<double>(0, res.dist);
                    table.append(vectors);
                    count++;
                    if (count >= k) {
                        break;
                    }
                }
            }

        public:
            std::unique_ptr<ValueVector> nodeInputVector;
            std::unique_ptr<ValueVector> nodeIdVector;
            std::unique_ptr<ValueVector> distanceVector;
            std::vector<ValueVector *> vectors;

            VectorIndexHeader *indexHeader;
            std::unique_ptr<NodeTableDistanceComputer<float>> dc;
            std::unique_ptr<NodeTableDistanceComputer<uint8_t>> qdc;
        };

        struct VectorSearchStats {
            TimeMetric* vectorSearchTimeMetric;
            NumericMetric* distCompMetric;
            TimeMetric* distanceComputationTime;
            NumericMetric* listNbrsMetric;
            TimeMetric* listNbrsCallTime;
            NumericMetric* oneHopCalls;
            NumericMetric* twoHopCalls;
            NumericMetric* dynamicTwoHopCalls;

            explicit VectorSearchStats(ExecutionContext *context) {
                vectorSearchTimeMetric = context->profiler->registerTimeMetricForce("vectorSearchTime");
                distCompMetric = context->profiler->registerNumericMetricForce("distanceComputations");
                distanceComputationTime = context->profiler->registerTimeMetricForce("distanceComputationTime");
                listNbrsMetric = context->profiler->registerNumericMetricForce("listNbrsCalls");
                listNbrsCallTime = context->profiler->registerTimeMetricForce("listNbrsCallTime");
                oneHopCalls = context->profiler->registerNumericMetricForce("oneHopCalls");
                twoHopCalls = context->profiler->registerNumericMetricForce("twoHopCalls");
                dynamicTwoHopCalls = context->profiler->registerNumericMetricForce("dynamicTwoHopCalls");
            }
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
                        LogicalTypeID::INT64, LogicalTypeID::INT64, LogicalTypeID::INT64, LogicalTypeID::BOOL,
                        LogicalTypeID::BOOL};
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
                auto useQuantizedVectors = ExpressionUtil::getLiteralValue<bool>(*params[7]);
                auto useKnnSearch = ExpressionUtil::getLiteralValue<bool>(*params[8]);
                bindData = std::make_unique<VectorSearchBindData>(nodeInput, nodeOutput, nodeTableId,
                                                                  embeddingPropertyId, k, efSearch, maxK,
                                                                  useQuantizedVectors, useKnnSearch, queryVector, false);
            }

            void initLocalState(main::ClientContext *context) override {
                auto bind = ku_dynamic_cast<GDSBindData *, VectorSearchBindData *>(bindData.get());
                localState = std::make_unique<VectorSearchLocalState>(context, bind->nodeTableID,
                                                                      bind->embeddingPropertyID);
            }

            template<typename T>
            inline void searchNNOnUpperLevel(VectorIndexHeaderPerPartition *header, NodeTableDistanceComputer<T> *dc,
                                             vector_id_t &nearest, double &nearestDist) {
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
                        dc->computeDistance(header->getActualId(neighbor), &dist);
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

            template<typename T>
            inline void findEntrypointUsingUpperLayer(VectorIndexHeaderPerPartition *header,
                                               NodeTableDistanceComputer<T> *dc, vector_id_t &entrypoint,
                                               double *entrypointDist) {
                uint8_t entrypointLevel;
                header->getEntrypoint(entrypoint, entrypointLevel);
                if (entrypointLevel == 1) {
                    dc->computeDistance(header->getActualId(entrypoint), entrypointDist);
                    searchNNOnUpperLevel(header, dc, entrypoint, *entrypointDist);
                    entrypoint = header->getActualId(entrypoint);
                } else {
                    dc->computeDistance(entrypoint, entrypointDist);
                }
            }

            inline bool isMasked(common::offset_t offset, NodeOffsetLevelSemiMask *filterMask) {
                return !filterMask->isEnabled() || filterMask->isMasked(offset);
            }

            inline void reverseAndRerankResults(BinaryHeap<NodeDistFarther> &results,
                                                std::priority_queue<NodeDistFarther> &reversed,
                                                NodeTableDistanceComputer<float> *dc, VectorSearchStats &stats) {
                vector_array_t reranked;
                constexpr int batch_size = 32;
                std::array<double, batch_size> dists;
                int size = 0;
                while (results.size() > 0) {
                    auto res = results.popMin();
                    reranked[size++] = res.id;
                    if (size == batch_size) {
                        stats.distanceComputationTime->start();
                        dc->batchComputeDistance(reranked.data(), size, dists.data());
                        stats.distanceComputationTime->stop();
                        stats.distCompMetric->increase(size);
                        for (int i = 0; i < size; i++) {
                            reversed.emplace(reranked[i], dists[i]);
                        }
                        size = 0;
                    }
                }
                for (int i = 0; i < size; i++) {
                    double dist;
                    stats.distanceComputationTime->start();
                    dc->computeDistance(reranked[i], &dist);
                    stats.distanceComputationTime->stop();
                    stats.distCompMetric->increase(1);
                    reversed.emplace(reranked[i], dist);
                }
            }

            inline void reverseResults(BinaryHeap<NodeDistFarther> &results,
                                       std::priority_queue<NodeDistFarther> &reversed,
                                       NodeTableDistanceComputer<float> *dc) {
                while (results.size() > 0) {
                    auto res = results.popMin();
                    reversed.emplace(res.id, res.dist);
                }
            }

            template<typename T>
            inline void batchComputeDistance(
                    vector_array_t &vectorArray,
                    int &size,
                    NodeTableDistanceComputer<T> *dc,
                    std::priority_queue<NodeDistFarther> &candidates,
                    BinaryHeap<NodeDistFarther> &results,
                    const int efSearch,
                    VectorSearchStats &stats) {
                int i = 0;
                constexpr int batch_size = 4;
                std::array<double, batch_size> dists;
                for (; i + batch_size <= size; i += batch_size) {
                    stats.distanceComputationTime->start();
                    dc->batchComputeDistance(vectorArray.data() + i, batch_size, dists.data());
                    stats.distanceComputationTime->stop();
                    stats.distCompMetric->increase(batch_size);
                    for (int j = 0; j < batch_size; j++) {
                        if (results.size() < efSearch || dists[j] < results.top()->dist) {
                            candidates.emplace(vectorArray[i + j], dists[j]);
                            results.push(NodeDistFarther(vectorArray[i + j], dists[j]));
                        }
                    }
                }

                for (; i < size; i++) {
                    double dist;
                    stats.distanceComputationTime->start();
                    dc->computeDistance(vectorArray[i], &dist);
                    stats.distanceComputationTime->stop();
                    stats.distCompMetric->increase(1);
                    if (results.size() < efSearch || dist < results.top()->dist) {
                        candidates.emplace(vectorArray[i], dist);
                        results.push(NodeDistFarther(vectorArray[i], dist));
                    }
                }

                // reset
                size = 0;
            }

            template<typename T>
            inline void unfilteredSearch(const table_id_t tableId,
                                  Graph *graph, NodeTableDistanceComputer<T> *dc,
                                  GraphScanState &state, const vector_id_t entrypoint, const double entrypointDist,
                                  BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                  const int efSearch, VectorSearchStats &stats) {
                vector_array_t vectorArray;
                int size;
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
                    stats.listNbrsCallTime->start();
                    auto nbrs = graph->scanFwdRandom({candidate.id, tableId}, state);
                    stats.listNbrsCallTime->stop();
                    stats.listNbrsMetric->increase(1);

                    for (auto &neighbor: nbrs) {
                        if (visited->is_bit_set(neighbor.offset)) {
                            continue;
                        }
                        visited->set_bit(neighbor.offset);
                        vectorArray[size++] = neighbor.offset;
                    }
                    batchComputeDistance(vectorArray, size, dc, candidates, results, efSearch, stats);
                }
            }

            template<typename T>
            inline void oneHopSearch(std::priority_queue<NodeDistFarther> &candidates,
                                     std::vector<common::nodeID_t> &firstHopNbrs, NodeTableDistanceComputer<T> *dc,
                                     NodeOffsetLevelSemiMask *filterMask,
                                     BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                     vector_array_t &vectorArray, int &size,
                                     const int efSearch, VectorSearchStats &stats) {
                for (auto &neighbor: firstHopNbrs) {
                    if (!filterMask->isMasked(neighbor.offset)) {
                        continue;
                    }
                    if (visited->is_bit_set(neighbor.offset)) {
                        continue;
                    }
                    visited->set_bit(neighbor.offset);
                    vectorArray[size++] = neighbor.offset;
                }
            }

            template<typename T>
            inline void twoHopSearch(std::priority_queue<NodeDistFarther> &candidates,
                                     std::vector<common::nodeID_t> &firstHopNbrs, const table_id_t tableId,
                                     Graph *graph, NodeTableDistanceComputer<T> *dc,
                                     NodeOffsetLevelSemiMask *filterMask,
                                     GraphScanState &state,
                                     BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                     vector_array_t &vectorArray, int &size,
                                     const int efSearch, VectorSearchStats &stats) {
                // Get the first hop neighbours
                std::queue<vector_id_t> nbrsToExplore;

                // First hop neighbours
                for (auto &neighbor: firstHopNbrs) {
                    auto isNeighborMasked = filterMask->isMasked(neighbor.offset);
                    if (visited->is_bit_set(neighbor.offset)) {
                        continue;
                    }
                    if (isNeighborMasked) {
                        visited->set_bit(neighbor.offset);
                        vectorArray[size++] = neighbor.offset;
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

                    stats.listNbrsCallTime->start();
                    auto secondHopNbrs = graph->scanFwdRandom({neighbor, tableId}, state);
                    stats.listNbrsCallTime->stop();
                    stats.listNbrsMetric->increase(1);

                    // Try prefetching
                    for (auto &secondHopNeighbor: secondHopNbrs) {
                        filterMask->prefetchMaskValue(secondHopNeighbor.offset);
                    }

                    for (auto &secondHopNeighbor: secondHopNbrs) {
                        auto isNeighborMasked = filterMask->isMasked(secondHopNeighbor.offset);
                        if (visited->is_bit_set(secondHopNeighbor.offset)) {
                            continue;
                        }
                        if (isNeighborMasked) {
                            // TODO: Maybe there's some benefit in doing batch distance computation
                            visited->set_bit(secondHopNeighbor.offset);
                            vectorArray[size++] = secondHopNeighbor.offset;
                        }
                    }
                }
            }

            template<typename T>
            inline void batchDynamicComputeDistance(
                    vector_array_t &vectorArray,
                    int &size,
                    NodeTableDistanceComputer<T> *dc,
                    NodeOffsetLevelSemiMask *filterMask,
                    std::priority_queue<NodeDistFarther> &candidates,
                    std::priority_queue<NodeDistFarther> &nbrsToExplore,
                    BinaryHeap<NodeDistFarther> &results,
                    const int efSearch,
                    VectorSearchStats &stats) {
                int i = 0;
                constexpr int batch_size = 4;
                std::array<double, batch_size> dists;
                for (; i + batch_size <= size; i += batch_size) {
                    stats.distanceComputationTime->start();
                    dc->batchComputeDistance(vectorArray.data() + i, batch_size, dists.data());
                    stats.distanceComputationTime->stop();
                    stats.distCompMetric->increase(batch_size);
                    for (int j = 0; j < batch_size; j++) {
                        nbrsToExplore.emplace(vectorArray[i + j], dists[j]);
                        if (filterMask->isMasked(vectorArray[i + j])
                                && (results.size() < efSearch || dists[j] < results.top()->dist)) {
                            candidates.emplace(vectorArray[i + j], dists[j]);
                            results.push(NodeDistFarther(vectorArray[i + j], dists[j]));
                        }
                    }
                }

                for (; i < size; i++) {
                    double dist;
                    stats.distanceComputationTime->start();
                    dc->computeDistance(vectorArray[i], &dist);
                    stats.distanceComputationTime->stop();
                    stats.distCompMetric->increase(1);
                    nbrsToExplore.emplace(vectorArray[i], dist);
                    if (filterMask->isMasked(vectorArray[i])
                            && (results.size() < efSearch || dist < results.top()->dist)) {
                        candidates.emplace(vectorArray[i], dist);
                        results.push(NodeDistFarther(vectorArray[i], dist));
                    }
                }

                // reset
                size = 0;
            }

            template<typename T>
            inline void dynamicTwoHopSearch(std::priority_queue<NodeDistFarther> &candidates,
                                            NodeDistFarther &candidate, int filterNbrsToFind,
                                            std::unordered_map<vector_id_t, int> &cachedNbrsCount,
                                            std::vector<common::nodeID_t> &firstHopNbrs, const table_id_t tableId,
                                            Graph *graph,
                                            NodeTableDistanceComputer<T> *dc,
                                            NodeOffsetLevelSemiMask *filterMask,
                                            GraphScanState &state,
                                            BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                            vector_array_t &vectorArray, int &size,
                                            const int efSearch, VectorSearchStats &stats) {
                // Get the first hop neighbours
                std::priority_queue<NodeDistFarther> nbrsToExplore;

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
                    if (isNeighborMasked) {
                        visited->set_bit(neighbor.offset);
                    }
                    vectorArray[size++] = neighbor.offset;
                }
                batchDynamicComputeDistance(vectorArray, size, dc, filterMask, candidates, nbrsToExplore, results,
                                            efSearch, stats);
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
                    stats.listNbrsCallTime->start();
                    auto secondHopNbrs = graph->scanFwdRandom({neighbor.id, tableId}, state);
                    stats.listNbrsCallTime->stop();
                    stats.listNbrsMetric->increase(1);

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
                            visited->set_bit(secondHopNeighbor.offset);
                            vectorArray[size++] = secondHopNeighbor.offset;
                        }
                    }
                    exploredFilteredNbrCount += secondHopFilteredNbrCount;
                    cachedNbrsCount[neighbor.id] = secondHopFilteredNbrCount;
                }
            }

            template<typename T>
            inline void addFilteredNodesToCandidates(NodeTableDistanceComputer<T> *dc,
                                                     std::priority_queue<NodeDistFarther> &candidates,
                                                     BinaryHeap<NodeDistFarther> &results,
                                                     BitVectorVisitedTable *visited,
                                                     NodeOffsetLevelSemiMask *filterMask, int maxNodesToAdd,
                                                     VectorSearchStats &stats) {
                // Add some initial candidates to handle neg correlation case
                int nbrsAdded = 0;
                for (offset_t i = 0; i < filterMask->getMaxOffset(); i++) {
                    if (filterMask->isMasked(i)) {
                        double dist;
                        dc->computeDistance(i, &dist);
                        candidates.emplace(i, dist);
                        results.push(NodeDistFarther(i, dist));
                        visited->set_bit(i);
                        nbrsAdded++;
                        stats.distCompMetric->increase(1);
                    }
                    if (nbrsAdded >= maxNodesToAdd) {
                        break;
                    }
                }
            }

            template<typename T>
            void filteredSearch(float selectivity, const table_id_t tableId, Graph *graph,
                                NodeTableDistanceComputer<T> *dc, NodeOffsetLevelSemiMask *filterMask,
                                GraphScanState &state, const vector_id_t entrypoint, const double entrypointDist,
                                BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                const int efSearch, const int numFilteredNodesToAdd, VectorSearchStats &stats) {
                vector_array_t vectorArray;
                int size;
                std::priority_queue<NodeDistFarther> candidates;
                candidates.emplace(entrypoint, entrypointDist);
                if (filterMask->isMasked(entrypoint)) {
                    results.push(NodeDistFarther(entrypoint, entrypointDist));
                }
                visited->set_bit(entrypoint);
                // We will use this metric to skip some part of second hop.
                std::unordered_map<vector_id_t, int> cachedNbrsCount;

                // Handle for neg correlation cases
                addFilteredNodesToCandidates(dc, candidates, results, visited, filterMask, numFilteredNodesToAdd, stats);

                while (!candidates.empty()) {
                    auto candidate = candidates.top();
                    if (candidate.dist > results.top()->dist && results.size() > 0) {
                        break;
                    }
                    candidates.pop();

                    // Get the first hop neighbours
                    stats.listNbrsCallTime->start();
                    auto firstHopNbrs = graph->scanFwdRandom({candidate.id, tableId}, state);
                    stats.listNbrsCallTime->stop();
                    stats.listNbrsMetric->increase(1);

                    // Reduce density!!
                    auto filterNbrsToFind = firstHopNbrs.size();

                    // Try prefetching
                    for (auto &neighbor: firstHopNbrs) {
                        filterMask->prefetchMaskValue(neighbor.offset);
                    }

                    // Calculate local selectivity
                    float localSelectivity = 0;
                    for (auto &neighbor: firstHopNbrs) {
                        if (filterMask->isMasked(neighbor.offset)) {
                            localSelectivity += 1;
                        }
                    }
                    localSelectivity /= filterNbrsToFind;

                    if (localSelectivity >= 0.5) {
                        // If the selectivity is high, we will simply do one hop search since we can find the next
                        // closest directly from candidates priority queue.
                        oneHopSearch(candidates, firstHopNbrs, dc, filterMask, results, visited, vectorArray, size,
                                     efSearch, stats);
                        stats.oneHopCalls->increase(1);
                    } else if ((filterNbrsToFind * filterNbrsToFind * localSelectivity) > (filterNbrsToFind * 3)) {
                        // We will use this metric to skip unwanted distance computation in the first hop
                        dynamicTwoHopSearch(candidates, candidate, filterNbrsToFind, cachedNbrsCount,
                                            firstHopNbrs, tableId, graph, dc, filterMask,
                                            state, results, visited, vectorArray, size, efSearch, stats);
                        stats.dynamicTwoHopCalls->increase(1);
                    } else {
                        // If the selectivity is low, we will not do dynamic two hop search since it does some extra
                        // distance computations to reduce listNbrs call which are redundant.
                        twoHopSearch(candidates, firstHopNbrs, tableId, graph, dc, filterMask, state, results, visited,
                                        vectorArray, size, efSearch, stats);
                        stats.twoHopCalls->increase(1);
                    }
                    batchComputeDistance(vectorArray, size, dc, candidates, results, efSearch, stats);
                }
            }

            template<typename T>
            void search(VectorIndexHeaderPerPartition* header, const table_id_t nodeTableId, Graph *graph,
                        NodeTableDistanceComputer<T> *dc, NodeOffsetLevelSemiMask *filterMask, GraphScanState &state,
                        BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited, const int efSearch,
                        const int maxK, VectorSearchStats &stats) {
                // Find closest entrypoint using the above layer!!
                vector_id_t entrypoint;
                double entrypointDist;
                findEntrypointUsingUpperLayer(header, dc, entrypoint, &entrypointDist);

                if (filterMask->isEnabled()) {
                    auto selectivity = static_cast<double>(filterMask->getNumMaskedNodes()) / header->getNumVectors();
                    if (selectivity <= 0.001) {
                        printf("skipping search since selectivity too low\n");
                        return;
                    }
                    filteredSearch(selectivity, nodeTableId, graph, dc,
                                   filterMask, state, entrypoint, entrypointDist, results,
                                   visited, efSearch, maxK, stats);
                } else {
                    unfilteredSearch(nodeTableId, graph, dc, state,
                                     entrypoint,
                                     entrypointDist, results, visited, efSearch, stats);
                }
            }

            inline void knnFilteredSearch(NodeTableDistanceComputer<float>* dc, NodeOffsetLevelSemiMask *filterMask,
                                          BinaryHeap<NodeDistFarther> &results, int k, VectorSearchStats &stats) {
                vector_array_t vectorArray;
                int size = 0;
                constexpr int batch_size = 64;
                std::array<double, batch_size> dists;
                // Find the k nearest neighbors
                for (offset_t i = 0; i < filterMask->getMaxOffset(); i++) {
                    if (!filterMask->isMasked(i)) {
                        continue;
                    }
                    vectorArray[size++] = i;
                    if (size == batch_size) {
                        stats.distanceComputationTime->start();
                        dc->batchComputeDistance(vectorArray.data(), size, dists.data());
                        stats.distanceComputationTime->stop();
                        stats.distCompMetric->increase(size);
                        for (int j = 0; j < size; j++) {
                            if (results.size() < k || dists[j] < results.top()->dist) {
                                results.push(NodeDistFarther(vectorArray[j], dists[j]));
                            }
                        }
                        size = 0;
                    }
                }

                // Process the remaining
                for (int i = 0; i < size; i++) {
                    double dist;
                    stats.distanceComputationTime->start();
                    dc->computeDistance(vectorArray[i], &dist);
                    stats.distanceComputationTime->stop();
                    stats.distCompMetric->increase(1);
                    if (results.size() < k || dist < results.top()->dist) {
                        results.push(NodeDistFarther(vectorArray[i], dist));
                    }
                }
            }

            inline void knnSearch(NodeTableDistanceComputer<float>* dc, offset_t maxOffset,
                                  BinaryHeap<NodeDistFarther> &results, int k, VectorSearchStats &stats) {
                vector_array_t vectorArray;
                int size = 0;
                constexpr int batch_size = 64;
                std::array<double, batch_size> dists;
                // Find the k nearest neighbors
                for (offset_t i = 0; i < maxOffset; i++) {
                    vectorArray[size++] = i;
                    if (size == batch_size) {
                        stats.distanceComputationTime->start();
                        dc->batchComputeDistance(vectorArray.data(), size, dists.data());
                        stats.distanceComputationTime->stop();
                        stats.distCompMetric->increase(size);
                        for (int j = 0; j < size; j++) {
                            if (results.size() < k || dists[j] < results.top()->dist) {
                                results.push(NodeDistFarther(vectorArray[j], dists[j]));
                            }
                        }
                        size = 0;
                    }
                }

                // Handle the remaining
                for (int i = 0; i < size; i++) {
                    double dist;
                    stats.distanceComputationTime->start();
                    dc->computeDistance(vectorArray[i], &dist);
                    stats.distanceComputationTime->stop();
                    stats.distCompMetric->increase(1);
                    if (results.size() < k || dist < results.top()->dist) {
                        results.push(NodeDistFarther(vectorArray[i], dist));
                    }
                }
            }

            void exec(ExecutionContext *context) override {
                auto searchLocalState = ku_dynamic_cast<GDSLocalState *, VectorSearchLocalState *>(localState.get());
                auto bindState = ku_dynamic_cast<GDSBindData *, VectorSearchBindData *>(bindData.get());
                auto indexHeader = searchLocalState->indexHeader;
                auto useQuantizedVectors = bindState->useQuantizedVectors;
                auto header = indexHeader->getPartitionHeader(0);
                auto graph = sharedState->graph.get();
                auto nodeTableId = indexHeader->getNodeTableId();
                auto efSearch = bindState->efSearch;
                auto numFilteredNodesToAdd = bindState->numFilteredNodesToAdd;
                auto k = bindState->k;
                KU_ASSERT(bindState->queryVector.size() == indexHeader->getDim());
                auto query = bindState->queryVector.data();
                auto state = graph->prepareScan(header->getCSRRelTableId());
                auto visited = std::make_unique<BitVectorVisitedTable>(header->getNumVectors());
                auto filterMask = sharedState->inputNodeOffsetMasks.at(indexHeader->getNodeTableId()).get();

                // Profiling
                VectorSearchStats stats(context);
                stats.vectorSearchTimeMetric->start();

                // Distance computers
                auto dc = searchLocalState->dc.get();
                auto qdc = searchLocalState->qdc.get();
                dc->setQuery(query);
                qdc->setQuery(query);

                std::priority_queue<NodeDistFarther> reversed;
                if (bindState->useKnnSearch) {
                    BinaryHeap<NodeDistFarther> results(k);
                    if (filterMask->isEnabled()) {
                        knnFilteredSearch(dc, filterMask, results, k, stats);
                    } else {
                        knnSearch(dc, filterMask->getMaxOffset(), results, k, stats);
                    }
                    reverseResults(results, reversed, dc);
                } else {
                    BinaryHeap<NodeDistFarther> results(efSearch);
                    if (useQuantizedVectors) {
                        search(header, nodeTableId, graph, qdc, filterMask, *state.get(), results, visited.get(),
                               efSearch, numFilteredNodesToAdd, stats);
                        reverseAndRerankResults(results, reversed, dc, stats);
                    } else {
                        search(header, nodeTableId, graph, dc, filterMask, *state.get(), results, visited.get(),
                               efSearch, numFilteredNodesToAdd, stats);
                        reverseResults(results, reversed, dc);
                    }
                }
                searchLocalState->materialize(reversed, *sharedState->fTable, k);
                stats.vectorSearchTimeMetric->stop();
                printf("vector search time: %f ms\n", stats.vectorSearchTimeMetric->getElapsedTimeMS());
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
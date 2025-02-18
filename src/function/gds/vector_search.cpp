#include <queue>

#include "binder/binder.h"
#include "common/exception/binder.h"
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
        // Define enum
        enum class SearchType {
            NAIVE,
            BLIND,
            DIRECTED,
            ADAPTIVE_G,
            ADAPTIVE_L,
            NAVIX
        };

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
            SearchType searchType = SearchType::NAVIX;

            VectorSearchBindData(std::shared_ptr<binder::Expression> nodeInput,
                                 std::shared_ptr<binder::Expression> nodeOutput, table_id_t nodeTableID,
                                 property_id_t embeddingPropertyID, int k, int64_t efSearch, int numFilteredNodesToAdd,
                                 bool useQuantizedVectors, bool useKnnSearch, SearchType searchType,
                                 std::vector<float> queryVector, bool outputAsNode)
                    : GDSBindData{std::move(nodeOutput), outputAsNode}, nodeInput(std::move(nodeInput)),
                      nodeTableID(nodeTableID), embeddingPropertyID(embeddingPropertyID), k(k),
                      efSearch(efSearch), numFilteredNodesToAdd(numFilteredNodesToAdd),
                      useQuantizedVectors(useQuantizedVectors), searchType(searchType), useKnnSearch(useKnnSearch),
                      queryVector(std::move(queryVector)) {};

            VectorSearchBindData(const VectorSearchBindData &other)
                    : GDSBindData{other}, nodeInput(other.nodeInput), nodeTableID(other.nodeTableID),
                      embeddingPropertyID(other.embeddingPropertyID), k(other.k), efSearch{other.efSearch},
                      numFilteredNodesToAdd(other.numFilteredNodesToAdd),
                      useQuantizedVectors(other.useQuantizedVectors), searchType(other.searchType),
                      useKnnSearch(other.useKnnSearch),
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
                totalNodeGroups = ku_dynamic_cast<Table *, NodeTable *>(
                        context->getStorageManager()->getTable(nodeTableId))->getNumCommittedNodeGroups();
                // TODO: Handle multiple partitions
                auto partitionHeader = indexHeader->getPartitionHeader(0);
                auto startOffset = partitionHeader->getStartNodeGroupId() * StorageConstants::NODE_GROUP_SIZE;
                this->dc = createDistanceComputer(context, nodeTableId, embeddingPropertyId, startOffset,
                                                  indexHeader->getDim(), indexHeader->getConfig().distanceFunc);
                this->qdc = createQuantizedDistanceComputer(context, nodeTableId,
                                                            indexHeader->getCompressedPropertyId(), startOffset,
                                                            indexHeader->getDim(),
                                                            indexHeader->getConfig().distanceFunc,
                                                            partitionHeader->getQuantizer(),
                                                            true);
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
            node_group_idx_t totalNodeGroups;
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
                        LogicalTypeID::BOOL, LogicalTypeID::STRING};
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
                auto searchTypeStr = params[9]->toString();
                // lower case
                std::transform(searchTypeStr.begin(), searchTypeStr.end(), searchTypeStr.begin(), ::tolower);

                SearchType searchType;
                if (searchTypeStr == "naive") {
                    searchType = SearchType::NAIVE;
                } else if (searchTypeStr == "blind") {
                    searchType = SearchType::BLIND;
                } else if (searchTypeStr == "directed") {
                    searchType = SearchType::DIRECTED;
                } else if (searchTypeStr == "adaptive_g") {
                    searchType = SearchType::ADAPTIVE_G;
                } else if (searchTypeStr == "adaptive_l") {
                    searchType = SearchType::ADAPTIVE_L;
                } else if (searchTypeStr == "navix") {
                    searchType = SearchType::NAVIX;
                } else {
                    throw BinderException("Invalid search type: " + searchTypeStr);
                }

                bindData = std::make_unique<VectorSearchBindData>(nodeInput, nodeOutput, nodeTableId,
                                                                  embeddingPropertyId, k, efSearch, maxK,
                                                                  useQuantizedVectors, useKnnSearch, searchType,
                                                                  queryVector, false);
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
                int size = 0;
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
                    auto nbrs = graph->scanFwdRandomFast({candidate.id, tableId}, state);
                    stats.listNbrsCallTime->stop();
                    stats.listNbrsMetric->increase(1);

                    auto totalNbrs = nbrs->state->getSelVector().getSelSize();

                    // Try prefetching
                    for (int i = 0; i < totalNbrs; i++) {
                        auto neighbor = nbrs->getValue<nodeID_t>(i);
                        visited->prefetch(neighbor.offset);
                    }

                    for (int i = 0; i < totalNbrs; i++) {
                        auto neighbor = nbrs->getValue<nodeID_t>(i);
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
            inline void batchFilteredComputeDistance(
                    vector_array_t &vectorArray,
                    NodeOffsetLevelSemiMask *filterMask,
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
                            if (filterMask->isMasked(vectorArray[i + j])) {
                                results.push(NodeDistFarther(vectorArray[i + j], dists[j]));
                            }
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
                        if (filterMask->isMasked(vectorArray[i])) {
                            results.push(NodeDistFarther(vectorArray[i], dist));
                        }
                    }
                }

                // reset
                size = 0;
            }

            inline void oneHopSearch(ValueVector* firstHopNbrs, NodeOffsetLevelSemiMask *filterMask,
                                     BitVectorVisitedTable *visited, vector_array_t &vectorArray, int &size) {
                auto totalNbrs = firstHopNbrs->state->getSelVector().getSelSize();
                for (size_t i = 0; i < totalNbrs; i++) {
                    auto neighbor = firstHopNbrs->getValue<nodeID_t>(i);
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

            inline void blindTwoHopSearch(std::vector<common::nodeID_t> &firstHopNbrs,
                                           Graph *graph, NodeOffsetLevelSemiMask *filterMask, GraphScanState &state,
                                           int filterNbrsToFind, BitVectorVisitedTable *visited, vector_array_t &vectorArray,
                                           int &size, VectorSearchStats &stats) {
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

                    if (size >= filterNbrsToFind) {
                        break;
                    }

                    stats.listNbrsCallTime->start();
                    auto secondHopNbrs = graph->scanFwdRandom(neighbor, state);
                    stats.listNbrsCallTime->stop();
                    stats.listNbrsMetric->increase(1);

                    // Try prefetching
                    for (auto &secondHopNeighbor: secondHopNbrs) {
                        visited->prefetch(secondHopNeighbor.offset);
                        filterMask->prefetchMaskValue(secondHopNeighbor.offset);
                    }

                    for (auto &secondHopNeighbor: secondHopNbrs) {
                        auto isNeighborMasked = filterMask->isMasked(secondHopNeighbor.offset);
//                        if (isNeighborMasked) {
//                            visitedSet.insert(secondHopNeighbor.offset);
//                        }
                        if (visited->is_bit_set(secondHopNeighbor.offset)) {
                            continue;
                        }
                        if (isNeighborMasked) {
                            // TODO: Maybe there's some benefit in doing batch distance computation
                            visited->set_bit(secondHopNeighbor.offset);
                            vectorArray[size++] = secondHopNeighbor.offset;
                            if (size >= filterNbrsToFind) {
                                break;
                            }
                        }
                    }
                }
            }

            inline void randomTwoHopSearch(ValueVector* firstHopNbrs, table_id_t tableId, Graph *graph,
                                     NodeOffsetLevelSemiMask *filterMask, GraphScanState &state, int filterNbrsToFind,
                                     BitVectorVisitedTable *visited, vector_array_t &vectorArray, int &size,
                                     VectorSearchStats &stats) {
                auto totalNbrs = firstHopNbrs->state->getSelVector().getSelSize();
                std::queue<vector_id_t> nbrsToExplore;
                int visitedSetSize = 0;

                // First hop neighbours
                for (int i = 0; i < totalNbrs; i++) {
                    auto neighbor = firstHopNbrs->getValue<nodeID_t>(i);
                    auto isNeighborMasked = filterMask->isMasked(neighbor.offset);
                    if (isNeighborMasked) {
                        visitedSetSize++;
                    }
                    if (visited->is_bit_set(neighbor.offset)) {
                        continue;
                    }
                    if (isNeighborMasked) {
                        visited->set_bit(neighbor.offset);
                        vectorArray[size++] = neighbor.offset;
                    }
                    nbrsToExplore.push(neighbor.offset);
                }
                // First hop neighbours
                while (!nbrsToExplore.empty()) {
                    auto neighbor = nbrsToExplore.front();
                    nbrsToExplore.pop();
                    if (visitedSetSize >= filterNbrsToFind) {
                        break;
                    }
                    if (visited->is_bit_set(neighbor)) {
                        continue;
                    }
                    visited->set_bit(neighbor);
                    stats.listNbrsCallTime->start();
                    auto secondHopNbrs = graph->scanFwdRandomFast({neighbor, tableId}, state);
                    stats.listNbrsCallTime->stop();
                    stats.listNbrsMetric->increase(1);

                    auto secondHopNbrsSize = secondHopNbrs->state->getSelVector().getSelSize();

                    // Try prefetching
                    for (int i = 0; i < secondHopNbrsSize; i++) {
                        auto secondHopNeighbor = secondHopNbrs->getValue<nodeID_t>(i);
                        visited->prefetch(secondHopNeighbor.offset);
                        filterMask->prefetchMaskValue(secondHopNeighbor.offset);
                    }

                    for (int i = 0; i < secondHopNbrsSize; i++) {
                        auto secondHopNeighbor = secondHopNbrs->getValue<nodeID_t>(i);
                        auto isNeighborMasked = filterMask->isMasked(secondHopNeighbor.offset);
                        if (isNeighborMasked) {
                            visitedSetSize++;
                        }
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

            inline void fullTwoHopSearch(ValueVector* firstHopNbrs, const table_id_t tableId,
                                     Graph *graph, NodeOffsetLevelSemiMask *filterMask, GraphScanState &state,
                                     BitVectorVisitedTable *visited, vector_array_t &vectorArray, int &size,
                                     VectorSearchStats &stats) {
                auto totalNbrs = firstHopNbrs->state->getSelVector().getSelSize();
                // Get the first hop neighbours
                std::queue<vector_id_t> nbrsToExplore;

                // First hop neighbours
                for (int i = 0; i < totalNbrs; i++) {
                    auto neighbor = firstHopNbrs->getValue<nodeID_t>(i);
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
                while (!nbrsToExplore.empty()) {
                    auto neighbor = nbrsToExplore.front();
                    nbrsToExplore.pop();
                    if (visited->is_bit_set(neighbor)) {
                        continue;
                    }
                    visited->set_bit(neighbor);

                    stats.listNbrsCallTime->start();
                    auto secondHopNbrs = graph->scanFwdRandomFast({neighbor, tableId}, state);
                    stats.listNbrsCallTime->stop();
                    stats.listNbrsMetric->increase(1);

                    auto secondHopNbrsSize = secondHopNbrs->state->getSelVector().getSelSize();
                    // Try prefetching
                    for (int i = 0; i < secondHopNbrsSize; i++) {
                        auto secondHopNeighbor = secondHopNbrs->getValue<nodeID_t>(i);
                        visited->prefetch(secondHopNeighbor.offset);
                        filterMask->prefetchMaskValue(secondHopNeighbor.offset);
                    }

                    for (int i = 0; i < secondHopNbrsSize; i++) {
                        auto secondHopNeighbor = secondHopNbrs->getValue<nodeID_t>(i);
                        auto isNeighborMasked = filterMask->isMasked(secondHopNeighbor.offset);
                        if (visited->is_bit_set(secondHopNeighbor.offset)) {
                            continue;
                        }
                        if (isNeighborMasked) {
                            visited->set_bit(secondHopNeighbor.offset);
                            vectorArray[size++] = secondHopNeighbor.offset;
                        }
                    }
                }
            }

            template<typename T>
            inline void batchDirectedComputeDistance(
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
            inline void directedTwoHopSearch(std::priority_queue<NodeDistFarther> &candidates, int filterNbrsToFind,
                                             ValueVector *firstHopNbrs, const table_id_t tableId,
                                             Graph *graph,
                                             NodeTableDistanceComputer<T> *dc,
                                             NodeOffsetLevelSemiMask *filterMask,
                                             GraphScanState &state,
                                             BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                             vector_array_t &vectorArray, int &size,
                                             const int efSearch, VectorSearchStats &stats) {
                // Get the first hop neighbours
                std::priority_queue<NodeDistFarther> nbrsToExplore;
                auto totalNbrs = firstHopNbrs->state->getSelVector().getSelSize();

                // First hop neighbours
                int visitedSetSize = 0;
                for (int i = 0; i < totalNbrs; i++) {
                    auto neighbor = firstHopNbrs->getValue<nodeID_t>(i);
                    auto isNeighborMasked = filterMask->isMasked(neighbor.offset);
                    if (isNeighborMasked) {
                        visitedSetSize++;
                    }
                    if (visited->is_bit_set(neighbor.offset)) {
                        continue;
                    }
                    if (isNeighborMasked) {
                        visited->set_bit(neighbor.offset);
                    }
                    vectorArray[size++] = neighbor.offset;
                }
                batchDirectedComputeDistance(vectorArray, size, dc, filterMask, candidates, nbrsToExplore, results,
                                            efSearch, stats);

                // Second hop neighbours
                while (!nbrsToExplore.empty()) {
                    auto neighbor = nbrsToExplore.top();
                    nbrsToExplore.pop();
                    if (visitedSetSize >= filterNbrsToFind) {
                        break;
                    }
                    if (visited->is_bit_set(neighbor.id)) {
                        continue;
                    }
                    visited->set_bit(neighbor.id);

                    stats.listNbrsCallTime->start();
                    auto secondHopNbrs = graph->scanFwdRandomFast({neighbor.id, tableId}, state);
                    stats.listNbrsCallTime->stop();
                    stats.listNbrsMetric->increase(1);

                    auto secondHopNbrsSize = secondHopNbrs->state->getSelVector().getSelSize();
                    // Try prefetching
                    for (size_t i = 0; i < secondHopNbrsSize; i++) {
                        auto secondHopNeighbor = secondHopNbrs->getValue<nodeID_t>(i);
                        visited->prefetch(secondHopNeighbor.offset);
                        filterMask->prefetchMaskValue(secondHopNeighbor.offset);
                    }

                    for (int i = 0; i < secondHopNbrsSize; i++) {
                        auto secondHopNeighbor = secondHopNbrs->getValue<nodeID_t>(i);
                        auto isNeighborMasked = filterMask->isMasked(secondHopNeighbor.offset);
                        if (isNeighborMasked) {
                            visitedSetSize++;
                        }
                        if (visited->is_bit_set(secondHopNeighbor.offset)) {
                            continue;
                        }
                        if (isNeighborMasked) {
                            visited->set_bit(secondHopNeighbor.offset);
                            vectorArray[size++] = secondHopNeighbor.offset;
                        }
                    }
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
            inline void naiveFilteredSearch(const table_id_t tableId, NodeOffsetLevelSemiMask *filterMask,
                                            Graph *graph, NodeTableDistanceComputer<T> *dc,
                                            GraphScanState &state, const vector_id_t entrypoint, const double entrypointDist,
                                            BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                            const int efSearch, VectorSearchStats &stats) {
                vector_array_t vectorArray;
                int size = 0;
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
                    auto nbrs = graph->scanFwdRandomFast({candidate.id, tableId}, state);
                    stats.listNbrsCallTime->stop();
                    stats.listNbrsMetric->increase(1);

                    auto totalNbrs = nbrs->state->getSelVector().getSelSize();
                    // Try prefetching
                    for (int i = 0; i < totalNbrs; i++) {
                        auto neighbor = nbrs->getValue<nodeID_t>(i);
                        visited->prefetch(neighbor.offset);
                        filterMask->prefetchMaskValue(neighbor.offset);
                    }

                    for (int i = 0; i < totalNbrs; i++) {
                        auto neighbor = nbrs->getValue<nodeID_t>(i);
                        if (visited->is_bit_set(neighbor.offset)) {
                            continue;
                        }
                        visited->set_bit(neighbor.offset);
                        vectorArray[size++] = neighbor.offset;
                    }
                    batchFilteredComputeDistance(vectorArray, filterMask, size, dc, candidates, results, efSearch, stats);
                }
            }

            template<typename T>
            void blindFilteredSearch(const table_id_t tableId, Graph *graph,
                                     NodeTableDistanceComputer<T> *dc, NodeOffsetLevelSemiMask *filterMask,
                                     GraphScanState &state, const vector_id_t entrypoint, const double entrypointDist,
                                     BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                     const int efSearch, const int numFilteredNodesToAdd, VectorSearchStats &stats) {
                vector_array_t vectorArray;
                int size = 0;
                std::priority_queue<NodeDistFarther> candidates;
                candidates.emplace(entrypoint, entrypointDist);
                if (filterMask->isMasked(entrypoint)) {
                    results.push(NodeDistFarther(entrypoint, entrypointDist));
                }
                visited->set_bit(entrypoint);

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

                    // Try prefetching
                    for (auto &neighbor: firstHopNbrs) {
                        visited->prefetch(neighbor.offset);
                        filterMask->prefetchMaskValue(neighbor.offset);
                    }

                    blindTwoHopSearch(firstHopNbrs, graph, filterMask, state, 64, visited, vectorArray, size,
                                     stats);
                    stats.twoHopCalls->increase(1);
                    batchComputeDistance(vectorArray, size, dc, candidates, results, efSearch, stats);
                }
            }

            template<typename T>
            void directedFilteredSearch(const table_id_t tableId, Graph *graph,
                                     NodeTableDistanceComputer<T> *dc, NodeOffsetLevelSemiMask *filterMask,
                                     GraphScanState &state, const vector_id_t entrypoint, const double entrypointDist,
                                     BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                     const int efSearch, const int numFilteredNodesToAdd, VectorSearchStats &stats) {
                vector_array_t vectorArray;
                int size = 0;
                std::priority_queue<NodeDistFarther> candidates;
                candidates.emplace(entrypoint, entrypointDist);
                if (filterMask->isMasked(entrypoint)) {
                    results.push(NodeDistFarther(entrypoint, entrypointDist));
                }
                visited->set_bit(entrypoint);

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
                    auto firstHopNbrs = graph->scanFwdRandomFast({candidate.id, tableId}, state);
                    stats.listNbrsCallTime->stop();
                    stats.listNbrsMetric->increase(1);

                    // Reduce density!!
                    auto totalNbrs = firstHopNbrs->state->getSelVector().getSelSize();

                    // Try prefetching
                    for (int i = 0; i < totalNbrs; i++) {
                        auto neighbor = firstHopNbrs->getValue<nodeID_t>(i);
                        visited->prefetch(neighbor.offset);
                        filterMask->prefetchMaskValue(neighbor.offset);
                    }

                    directedTwoHopSearch(candidates, totalNbrs,
                                         firstHopNbrs, tableId, graph, dc, filterMask,
                                         state, results, visited, vectorArray, size, efSearch, stats);
                    stats.dynamicTwoHopCalls->increase(1);

                    batchComputeDistance(vectorArray, size, dc, candidates, results, efSearch, stats);
                }
            }

            template<typename T>
            void adaptiveGlobalFilteredSearch(const float selectivity, const table_id_t tableId, Graph *graph,
                                     NodeTableDistanceComputer<T> *dc, NodeOffsetLevelSemiMask *filterMask,
                                     GraphScanState &state, const vector_id_t entrypoint, const double entrypointDist,
                                     BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                     const int efSearch, const int numFilteredNodesToAdd, VectorSearchStats &stats) {
                vector_array_t vectorArray;
                int size = 0;
                std::priority_queue<NodeDistFarther> candidates;
                candidates.emplace(entrypoint, entrypointDist);
                if (filterMask->isMasked(entrypoint)) {
                    results.push(NodeDistFarther(entrypoint, entrypointDist));
                }
                visited->set_bit(entrypoint);

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
                    auto firstHopNbrs = graph->scanFwdRandomFast({candidate.id, tableId}, state);
                    stats.listNbrsCallTime->stop();
                    stats.listNbrsMetric->increase(1);

                    // Reduce density!!
                    auto totalNbrs = firstHopNbrs->state->getSelVector().getSelSize();

                    // Try prefetching
                    for (int i = 0; i < totalNbrs; i++) {
                        auto neighbor = firstHopNbrs->getValue<nodeID_t>(i);
                        visited->prefetch(neighbor.offset);
                        filterMask->prefetchMaskValue(neighbor.offset);
                    }

                    if (selectivity > 0.08) {
                        // We will use this metric to skip
                        // wanted distance computation in the first hop
                        directedTwoHopSearch(candidates, totalNbrs,
                                             firstHopNbrs, tableId, graph, dc, filterMask,
                                             state, results, visited, vectorArray, size, efSearch, stats);
                        stats.dynamicTwoHopCalls->increase(1);
                    } else {
                        // If the selectivity is low, we will not do dynamic two hop search since it does some extra
                        // distance computations to reduce listNbrs call which are redundant.
                        std::vector<common::nodeID_t> firstHopNbrsVec;
                        for (int i = 0; i < totalNbrs; i++) {
                            auto neighbor = firstHopNbrs->getValue<nodeID_t>(i);
                            firstHopNbrsVec.push_back(neighbor);
                        }
                        blindTwoHopSearch(firstHopNbrsVec, graph, filterMask, state, 64, visited, vectorArray, size,
                                          stats);
                        stats.twoHopCalls->increase(1);
                    }
                    batchComputeDistance(vectorArray, size, dc, candidates, results, efSearch, stats);
                }
            }

            template<typename T>
            void navixFilteredSearch(const table_id_t tableId, Graph *graph,
                                NodeTableDistanceComputer<T> *dc, NodeOffsetLevelSemiMask *filterMask,
                                GraphScanState &state, const vector_id_t entrypoint, const double entrypointDist,
                                BinaryHeap<NodeDistFarther> &results, BitVectorVisitedTable *visited,
                                const int efSearch, const int numFilteredNodesToAdd, VectorSearchStats &stats,
                                bool enableHighSelectivityOpt) {
                vector_array_t vectorArray;
                int size = 0;
                std::priority_queue<NodeDistFarther> candidates;
                candidates.emplace(entrypoint, entrypointDist);
                if (filterMask->isMasked(entrypoint)) {
                    results.push(NodeDistFarther(entrypoint, entrypointDist));
                }
                visited->set_bit(entrypoint);

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
                    auto firstHopNbrs = graph->scanFwdRandomFast({candidate.id, tableId}, state);
                    stats.listNbrsCallTime->stop();
                    stats.listNbrsMetric->increase(1);

                    // Reduce density!!
                    auto totalNbrs = firstHopNbrs->state->getSelVector().getSelSize();

                    // Try prefetching
                    for (int i = 0; i < totalNbrs; i++) {
                        auto neighbor = firstHopNbrs->getValue<nodeID_t>(i);
                        visited->prefetch(neighbor.offset);
                        filterMask->prefetchMaskValue(neighbor.offset);
                    }

                    // Calculate local selectivity
                    float filteredNbrs = 0;
                    for (int i = 0; i < totalNbrs; i++) {
                        auto neighbor = firstHopNbrs->getValue<nodeID_t>(i);
                        if (filterMask->isMasked(neighbor.offset)) {
                            filteredNbrs += 1;
                        }
                    }
                    auto localSelectivity = filteredNbrs / totalNbrs;

                    // Multiply by 0.6 due to the overlapping factor
                    auto estimatedFullTwoHopDistanceComp = (totalNbrs * filteredNbrs + filteredNbrs) * 0.4;
                    auto estimatedDirectedDistanceComp = totalNbrs + (totalNbrs - filteredNbrs);
                    if (enableHighSelectivityOpt && localSelectivity >= 0.5) {
                        // If the selectivity is high, we will simply do one hop search since we can find the next
                        // closest directly from candidates priority queue.
                        oneHopSearch(firstHopNbrs, filterMask, visited, vectorArray, size);
                        stats.oneHopCalls->increase(1);
                    } else if (estimatedFullTwoHopDistanceComp > estimatedDirectedDistanceComp) {
                        // We will use this metric to skip
                        // wanted distance computation in the first hop
                        directedTwoHopSearch(candidates, totalNbrs,
                                            firstHopNbrs, tableId, graph, dc, filterMask,
                                            state, results, visited, vectorArray, size, efSearch, stats);
                        stats.dynamicTwoHopCalls->increase(1);
                    } else {
                        // If the selectivity is low, we will not do dynamic two hop search since it does some extra
                        // distance computations to reduce listNbrs call which are redundant.
                        fullTwoHopSearch(firstHopNbrs, tableId, graph, filterMask, state, visited, vectorArray, size,
                                         stats);
                        stats.twoHopCalls->increase(1);
                    }
                    batchComputeDistance(vectorArray, size, dc, candidates, results, efSearch, stats);
                }
            }

            template<typename T>
            void search(SearchType searchType, VectorIndexHeaderPerPartition* header, const table_id_t nodeTableId, Graph *graph,
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

                    if (searchType == SearchType::NAVIX) {
                        navixFilteredSearch(nodeTableId, graph, dc,
                                            filterMask, state, entrypoint, entrypointDist, results,
                                            visited, efSearch, maxK, stats, true);
                    } else if (searchType == SearchType::ADAPTIVE_L) {
                        navixFilteredSearch(nodeTableId, graph, dc,
                                            filterMask, state, entrypoint, entrypointDist, results,
                                            visited, efSearch, maxK, stats, false);
                    } else if (searchType == SearchType::ADAPTIVE_G) {
                        adaptiveGlobalFilteredSearch(selectivity, nodeTableId, graph, dc,
                                                    filterMask, state, entrypoint, entrypointDist, results,
                                                    visited, efSearch, maxK, stats);
                    } else if (searchType == SearchType::NAIVE) {
                        naiveFilteredSearch(nodeTableId, filterMask, graph, dc, state, entrypoint, entrypointDist,
                                        results, visited, efSearch, stats);
                    } else if (searchType == SearchType::BLIND) {
                        blindFilteredSearch(nodeTableId, graph, dc, filterMask, state, entrypoint, entrypointDist,
                                        results, visited, efSearch, maxK, stats);
                    } else if (searchType == SearchType::DIRECTED) {
                        directedFilteredSearch(nodeTableId, graph, dc, filterMask, state, entrypoint, entrypointDist,
                                            results, visited, efSearch, maxK, stats);
                    } else {
                        printf("Invalid search type\n");
                    }
                } else {
                    unfilteredSearch(nodeTableId, graph, dc, state,
                                     entrypoint,
                                     entrypointDist, results, visited, efSearch, stats);
                }
            }

            template<typename T>
            inline void knnFilteredSearch(NodeTableDistanceComputer<T>* dc, NodeOffsetLevelSemiMask *filterMask,
                                          BinaryHeap<NodeDistFarther> &results, int k, VectorSearchStats &stats) {
                vector_array_t vectorArray;
                int size = 0;
                constexpr int batch_size = 64;
                std::array<double, batch_size> dists;
                auto maskedNodes = filterMask->getNumMaskedNodes();
                std::vector<NodeDistCloser> candidates(maskedNodes);
                offset_t l = 0;
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
                            candidates[l++] = NodeDistCloser(vectorArray[j], dists[j]);
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
                    candidates[l++] = NodeDistCloser(vectorArray[i], dist);
                }
                std::sort(candidates.begin(), candidates.end());
                for (int i = 0; i < k; i++) {
                    results.push(NodeDistFarther(candidates[i].id, candidates[i].dist));
                }
            }

            template<typename T>
            inline void knnSearch(NodeTableDistanceComputer<T>* dc, offset_t maxOffset,
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
                auto state = graph->prepareScan(header->getCSRRelTableId(), searchLocalState->totalNodeGroups);
                auto visited = std::make_unique<BitVectorVisitedTable>(header->getNumVectors());
                auto filterMask = sharedState->inputNodeOffsetMasks.at(indexHeader->getNodeTableId()).get();
                auto searchType = bindState->searchType;

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
                        if (useQuantizedVectors) {
                            knnFilteredSearch(qdc, filterMask, results, k, stats);
                        } else {
                            knnFilteredSearch(dc, filterMask, results, k, stats);
                        }
                    } else {
                        knnSearch(dc, filterMask->getMaxOffset(), results, k, stats);
                    }
                    reverseResults(results, reversed, dc);
                } else {
                    BinaryHeap<NodeDistFarther> results(efSearch);
                    if (useQuantizedVectors) {
                        search(searchType, header, nodeTableId, graph, qdc, filterMask, *state.get(), results, visited.get(),
                               efSearch, numFilteredNodesToAdd, stats);
                        reverseAndRerankResults(results, reversed, dc, stats);
                    } else {
                        search(searchType, header, nodeTableId, graph, dc, filterMask, *state.get(), results, visited.get(),
                               efSearch, numFilteredNodesToAdd, stats);
                        reverseResults(results, reversed, dc);
                    }
                }
                searchLocalState->materialize(reversed, *sharedState->fTable, k);
                stats.vectorSearchTimeMetric->stop();
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

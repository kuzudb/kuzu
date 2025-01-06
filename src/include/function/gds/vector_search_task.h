#pragma once

#include "common/task_system/task.h"
#include "function/gds/gds_frontier.h"
#include "common/vector_index/distance_computer.h"
#include "storage/index/vector_index_header.h"
#include "common/vector_index/helpers.h"
#include "queue"
#include "atomic"
#include "processor/execution_context.h"
#include "storage/store/column.h"
#include "processor/operator/mask.h"
#include "graph/graph.h"

using namespace kuzu::storage;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::graph;

namespace kuzu {
    namespace function {
        struct PartSearchInfo {
            ParallelMultiQueue<NodeDistFarther>* parallelResults;
            std::vector<std::vector<NodeDistCloser>> entrypoints;
            BitVectorVisitedTable *visited;
        };

        struct VectorSearchTaskSharedState {
            const int efSearch;
            const int maxK;
            const uint64_t maxNumThreads;
            processor::ExecutionContext *context;
            graph::Graph *graph;
            NodeTableDistanceComputer<float> *distanceComputer;
            NodeOffsetLevelSemiMask *filterMask;
            VectorIndexHeader *header;
            std::vector<PartSearchInfo> partSearchInfo;

            explicit VectorSearchTaskSharedState(const uint64_t maxNumThreads,
                                                 const int efSearch,
                                                 const int maxK,
                                                 processor::ExecutionContext *context,
                                                 graph::Graph *graph,
                                                 NodeTableDistanceComputer<float> *distanceComputer,
                                                 NodeOffsetLevelSemiMask *filterMask,
                                                 VectorIndexHeader *header,
                                                 BitVectorVisitedTable *visited,
                                                 ParallelMultiQueue<NodeDistFarther> *parallelResults,
                                                 std::vector<std::vector<NodeDistCloser>> entrypoints)
                    : efSearch(efSearch), maxK(maxK), maxNumThreads(maxNumThreads), context(context), graph(graph),
                      distanceComputer(distanceComputer), filterMask(filterMask),
                      header(header) {
                numPartitions = header->getNumPartitions();

            };

            inline std::pair<int, int> getWork() {
                if (currentPartition >= numPartitions) {
                    return {-1, -1};
                }

                std::lock_guard<std::mutex> lock(mtx);
                int partition = currentPartition;
                int threadId = currentThreadId;
                currentThreadId++;
                if (currentThreadId == (int) maxNumThreads) {
                    currentThreadId = 0;
                    currentPartition++;
                    if (currentPartition >= numPartitions) {
                        return {-1, -1};
                    }
                }

                return {partition, threadId};
            }

        private:
            std::mutex mtx;
            int numPartitions;
            int currentPartition = 0;
            int currentThreadId = 0;
        };

        struct NodeTableLookupLocalState {
            Column *embeddingColumn;
            std::vector<std::unique_ptr<Column::ChunkState>> readStates;
            std::unique_ptr<ValueVector> inputNodeIdVector;
            std::unique_ptr<ValueVector> embeddingVector;

            explicit NodeTableLookupLocalState(ExecutionContext* context, Column *embeddingColumn,
                                               std::unique_ptr<ValueVector> inputNodeIdVector,
                                               std::unique_ptr<ValueVector> embeddingVector) : embeddingColumn(
                    embeddingColumn), inputNodeIdVector(
                    std::move(inputNodeIdVector)), embeddingVector(std::move(embeddingVector)) {
                auto nodeGroups = embeddingColumn->getNumNodeGroups(context->clientContext->getTx());
                for (size_t i = 0; i < nodeGroups; i++) {
                    readStates.push_back(std::make_unique<Column::ChunkState>());
                }
            };

            inline const float *getEmbedding(processor::ExecutionContext *context, vector_id_t id) {
                auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(id);
                // Initialize the read state
                auto readState = readStates[nodeGroupIdx].get();
                embeddingColumn->initChunkState(context->clientContext->getTx(), nodeGroupIdx, *readState);
                // Read the embedding
                auto nodeIdVector = inputNodeIdVector.get();
                nodeIdVector->setValue(0, id);
                auto resultVector = embeddingVector.get();
                embeddingColumn->lookup(context->clientContext->getTx(), *readState, nodeIdVector,
                                        resultVector);
                return reinterpret_cast<float *>(ListVector::getDataVector(resultVector)->getData());
            }

            std::unique_ptr<NodeTableLookupLocalState> clone(processor::ExecutionContext *context) {
                auto embeddingVec = std::make_unique<ValueVector>(embeddingColumn->getDataType().copy(),
                                                                  context->clientContext->getMemoryManager());
                embeddingVec->state = DataChunkState::getSingleValueDataChunkState();
                auto inputNodeIdVec = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID().copy(),
                                                                    context->clientContext->getMemoryManager());
                inputNodeIdVec->state = DataChunkState::getSingleValueDataChunkState();
                return std::make_unique<NodeTableLookupLocalState>(context, embeddingColumn,
                                                                   std::move(inputNodeIdVec),
                                                                   std::move(embeddingVec));
            }
        };

        class VectorSearchTask : public common::Task {
        public:
            VectorSearchTask(uint64_t maxNumThreads, std::shared_ptr<VectorSearchTaskSharedState> sharedState,
                             std::unique_ptr<NodeTableLookupLocalState> localState)
                    : common::Task{maxNumThreads}, sharedState(std::move(sharedState)),
                      localState(std::move(localState)) {}

            void run() override;

        private:
            inline int findFilteredNextKNeighbours(common::nodeID_t nodeID,
                                                   GraphScanState &state,
                                                   std::vector<common::nodeID_t> &nbrs,
                                                   NodeOffsetLevelSemiMask *filterMask,
                                                   BitVectorVisitedTable *visited,
                                                   int minK,
                                                   int maxNeighboursCheck,
                                                   Graph *graph);

            inline void findNextKNeighbours(common::nodeID_t nodeID,
                                            GraphScanState &state,
                                            std::vector<common::nodeID_t> &nbrs,
                                            BitVectorVisitedTable *visited,
                                            int minK,
                                            int maxK,
                                            int maxNeighboursCheck,
                                            Graph *graph);

            inline bool isMasked(common::offset_t offset, NodeOffsetLevelSemiMask *filterMask);

            inline int calculateMaxK(uint64_t numMaskedNodes, uint64_t totalNodes);

        private:
            std::shared_ptr<VectorSearchTaskSharedState> sharedState;
            std::unique_ptr<NodeTableLookupLocalState> localState;
        };
    } // namespace function
} // namespace kuzu

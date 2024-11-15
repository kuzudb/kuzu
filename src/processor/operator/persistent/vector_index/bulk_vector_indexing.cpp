#include "processor/operator/persistent/vector_index/bulk_vector_indexing.h"

#include <thread>

#include "binder/ddl/bound_create_table.h"
#include "catalog/catalog.h"
#include "common/enums/rel_multiplicity.h"
#include "storage/storage_manager.h"
#include "storage/store/column.h"

namespace kuzu {
    namespace processor {
        BulkVectorIndexing::BulkVectorIndexing(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
                                               std::unique_ptr<BulkVectorIndexingLocalState> localState,
                                               std::shared_ptr<BulkVectorIndexingSharedState> sharedState,
                                               std::unique_ptr<PhysicalOperator> child, uint32_t id,
                                               std::unique_ptr<OPPrintInfo> printInfo)
                : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::UPDATE_VECTOR_INDEX,
                       std::move(child), id, std::move(printInfo)},
                  localState(std::move(localState)), sharedState(sharedState) {}

        void BulkVectorIndexing::initLocalStateInternal(ResultSet *resultSet, ExecutionContext *context) {
            localState->offsetVector = resultSet->getValueVector(localState->offsetPos).get();
            localState->embeddingVector = resultSet->getValueVector(localState->embeddingPos).get();
            localState->dc = std::make_unique<CosineDistanceComputer>(sharedState->tempStorage->vectors,
                                                                      sharedState->header->getDim(),
                                                                      sharedState->tempStorage->numVectors);
            localState->visited = std::make_unique<VisitedTable>(sharedState->tempStorage->numVectors);
        }

        void BulkVectorIndexing::initGlobalStateInternal(ExecutionContext *context) {
            sharedState->headerPerPartition = sharedState->header->getPartitionHeader(sharedState->partitionId);
            auto startNodeGroupId = sharedState->headerPerPartition->getStartNodeGroupId();
            auto endNodeGroupId = sharedState->headerPerPartition->getEndNodeGroupId();

            auto table = ku_dynamic_cast<Table *, NodeTable *>(context->clientContext->getStorageManager()->getTable(
                    sharedState->header->getNodeTableId()));
            auto maxOffsetNodeTable = table->getMaxNodeOffset(context->clientContext->getTx());
            sharedState->startOffsetNodeTable = startNodeGroupId * StorageConstants::NODE_GROUP_SIZE;
            offset_t endOffsetNodeTable = std::min(maxOffsetNodeTable,
                                                   ((endNodeGroupId + 1) *
                                                    StorageConstants::NODE_GROUP_SIZE) - 1);
            auto numVectors = endOffsetNodeTable - sharedState->startOffsetNodeTable + 1;
            sharedState->headerPerPartition->initSampleGraph(numVectors);
            auto maxNbrsAtLowerLevel = sharedState->header->getConfig().maxNbrsAtLowerLevel;
            sharedState->tempStorage =
                    std::make_unique<VectorTempStorage>(sharedState->header->getDim(), numVectors);
            sharedState->graph = std::make_unique<VectorIndexGraph>(numVectors, maxNbrsAtLowerLevel,
                                                                    StorageConstants::NODE_GROUP_SIZE);
            sharedState->builder = std::make_unique<VectorIndexBuilder>(sharedState->header, sharedState->partitionId,
                                                                        sharedState->graph.get(),
                                                                        sharedState->tempStorage.get());
//            sharedState->compressedStorage = std::make_unique<CompressedVectorStorage>(
//                    context->clientContext->getTx(), table->getColumn(table->getPKColumnID())->getMetadataDA(),
//                    sharedState->header->getDim(), startNodeGroupId, endNodeGroupId);
//            sharedState->compressedPropertyColumn =
//                    table->getColumn(sharedState->header->getCompressedPropertyId());
        }

        void BulkVectorIndexing::executeInternal(ExecutionContext *context) {
            std::vector<vector_id_t> vectorIds(DEFAULT_VECTOR_CAPACITY);
            SQ8Bit *quantizer = sharedState->headerPerPartition->getQuantizer();
            printf("Running indexing %d!!\n", sharedState->partitionId);
            while (children[0]->getNextTuple(context)) {
                // print the thread id
                auto numVectors = localState->offsetVector->state->getSelVector().getSelSize();
                auto vectors = reinterpret_cast<float *>(
                        ListVector::getDataVector(localState->embeddingVector)->getData());
                // print the offset and embedding
                for (size_t i = 0; i < numVectors; i++) {
                    auto id = localState->offsetVector->getValue<internalID_t>(i);
                    KU_ASSERT(id.offset >= sharedState->startOffsetNodeTable);
                    vectorIds[i] = id.offset - sharedState->startOffsetNodeTable;
                }
                sharedState->builder->batchInsert(vectors, vectorIds.data(), numVectors,
                                                  localState->visited.get(), localState->dc.get());
                // TODO: FIX this!!!
//                quantizer->batch_train(numVectors, vectors);
            }
            printf("Finished indexing %d!!\n", sharedState->partitionId);
            // Wait for all threads to finish indexing
//            sharedState->compressionLatch.arrive_and_wait();
//            printf("Running quantization!!\n");
//            Column::ChunkState state;
//            // Start compressing the vectors
//            while (true) {
//                auto element = sharedState->compressedStorage->getCompressedChunk();
//                if (element.nodeGroupIdx == INVALID_NODE_GROUP_IDX) {
//                    break;
//                }
//                auto &listChunk = element.columnChunk->cast<ListChunkData>();
//                // Fix the start offset!!
//                // TODO: Cleanup the code. Super Hackyyy!!
//                auto startOffset =
//                        element.startOffset - (sharedState->startOffsetNodeTable * sharedState->header->getDim());
//                quantizer->encode(sharedState->tempStorage->vectors + startOffset,
//                                  listChunk.getDataColumnChunk()->getData(), listChunk.getNumValues());
//                sharedState->compressedPropertyColumn->initChunkState(context->clientContext->getTx(),
//                                                                      element.nodeGroupIdx, state);
//                sharedState->compressedPropertyColumn->append(element.columnChunk.get(), state);
//            }
        }

        void BulkVectorIndexing::finalize(ExecutionContext *context) {
            // TODO: Benchmark and make it parallelized (might be important)
            KU_ASSERT_MSG(sharedState->partitionerSharedState->partitioningBuffers.size() == 1,
                          "Only one partitioning buffer in fwd direction is supported");
//    printf("checking recall... single threaded might take time...\n");
//    testGraph();
            // Populate partition buffer
            // TODO: IMP: Free the memory of the graph as partition is filled!!
            sharedState->graph->populatePartitionBuffer(
                    *sharedState->partitionerSharedState->partitioningBuffers[0]);

            // TODO: Fix this to make it parallel!!
            printf("Running quantization!!\n");
            sharedState->headerPerPartition->getQuantizer()->batch_train(
                    sharedState->tempStorage->vectors, sharedState->tempStorage->numVectors);
            sharedState->headerPerPartition->getQuantizer()->encode(
                    sharedState->tempStorage->vectors, sharedState->headerPerPartition->getQuantizedVectors(),
                    sharedState->tempStorage->numVectors);

            // Free the memory of the graph
            sharedState->graph.reset();
            sharedState->tempStorage.reset();
            sharedState->builder.reset();
            sharedState->compressedStorage.reset();

            context->clientContext->getStorageManager()->getWAL().addToUpdatedTables(
                    sharedState->header->getNodeTableId());
        }

//void BulkVectorIndexing::printGraph() {
////      Print the graph from partitionerSharedState
//    auto& partitionBuffer = *sharedState->partitionerSharedState->partitioningBuffers[0];
//    auto totalElements = 0;
//    for (auto partitionIdx = 0u; partitionIdx < partitionBuffer.partitions.size();
//    partitionIdx++) {
//        auto& partition = partitionBuffer.partitions[partitionIdx];
//        auto& group = partition.getChunkedGroups()[0];
//        auto& from = group->getColumnChunk(0);
//        auto& to = group->getColumnChunk(1);
//        auto& relId = group->getColumnChunk(2);
//        for (auto i = 0u; i < group->getNumRows(); i++) {
//            totalElements++;
//        }
//    }
//    printf("Total elements: %d\n", totalElements);
//}

        void BulkVectorIndexing::testGraph() {
            // For testing purposes
            int k = 100;
            int efSearch = 200;
            auto queryVectorPath = "/home/g3sehgal/projects/def-ssalihog/g3sehgal/gist_50k/query.fvecs";
            size_t queryDimension, queryNumVectors;
            float *queryVecs = readFvecFile(queryVectorPath, &queryDimension, &queryNumVectors);
            auto *gtVecs = new vector_id_t[queryNumVectors * k];

            // Test with ANN
            auto visited = std::make_unique<VisitedTable>(sharedState->tempStorage->numVectors);
            auto distanceComputer = std::make_unique<L2DistanceComputer>(sharedState->tempStorage->vectors,
                                                                         sharedState->header->getDim(),
                                                                         sharedState->tempStorage->numVectors);
            auto dim = sharedState->header->getDim();

            // Generate ground truth
            IndexKNN index(distanceComputer.get(), dim, sharedState->tempStorage->numVectors);
            auto recall = 0.0;
            for (size_t i = 0; i < queryNumVectors; i++) {
                double dists[k];
                index.search(k, queryVecs + i * dim, dists, gtVecs + i * k);
                std::priority_queue<NodeDistCloser> results;
                std::vector<NodeDistFarther> res;
                sharedState->builder->search(queryVecs + (i * queryDimension), k, efSearch, visited.get(), results,
                                             distanceComputer.get());
                while (!results.empty()) {
                    auto top = results.top();
                    res.emplace_back(top.id, top.dist);
                    results.pop();
                }

                auto gt = gtVecs + i * k;
                for (auto &result: res) {
                    if (std::find(gt, gt + k, result.id) != (gt + k)) {
                        recall++;
                    }
                }
            }
            auto recallPerQuery = recall / queryNumVectors;
            printf("Recall: %f\n", (recallPerQuery / k) * 100);
        }

        std::unique_ptr<PhysicalOperator> BulkVectorIndexing::clone() {
            return make_unique<BulkVectorIndexing>(resultSetDescriptor->copy(), localState->copy(),
                                                   sharedState, children[0]->clone(), id, printInfo->copy());
        }

    } // namespace processor
} // namespace kuzu

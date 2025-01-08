#include "processor/operator/persistent/vector_index/bulk_vector_indexing.h"

#include <thread>

#include "common/vector_index/helpers.h"
#include "common/vector_index/vector_index_config.h"
#include "binder/ddl/bound_create_table.h"
#include "catalog/catalog.h"
#include "common/enums/rel_multiplicity.h"
#include "storage/store/column.h"
#include "processor/operator/scan/scan_node_table.h"

namespace kuzu {
    namespace processor {
        BulkVectorIndexing::BulkVectorIndexing(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
                                               std::unique_ptr<BulkVectorIndexingLocalState> localState,
                                               std::shared_ptr<BulkVectorIndexingSharedState> sharedState,
                                               std::unique_ptr<PhysicalOperator> child, uint32_t id,
                                               std::unique_ptr<OPPrintInfo> printInfo)
                : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::UPDATE_VECTOR_INDEX,
                       std::move(child), id, std::move(printInfo)},
                  localState(std::move(localState)), sharedState(sharedState), initialized(false) {}

        void BulkVectorIndexing::initLocalStateInternal(ResultSet *resultSet, ExecutionContext *context) {
            if (initialized) {
                return;
            }
            localState->offsetVector = resultSet->getValueVector(localState->offsetPos).get();
            localState->embeddingVector = resultSet->getValueVector(localState->embeddingPos).get();
            auto distFunc = sharedState->header->getConfig().distanceFunc;
            auto dim = sharedState->header->getDim();
            localState->dc = createDistanceComputer(context->clientContext, sharedState->header->getNodeTableId(),
                                                    sharedState->header->getEmbeddingPropertyId(),
                                                    sharedState->startOffsetNodeTable, dim, distFunc);
            localState->visited = std::make_unique<VisitedTable>(sharedState->numVectors);
            localState->quantizer = std::make_unique<Quantizer>(
                    sharedState->headerPerPartition->getQuantizer(),
                    distFunc,
                    sharedState->compressedStorage.get(),
                    DEFAULT_VECTOR_CAPACITY,
                    dim);
            initialized = true;
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
            sharedState->numVectors = numVectors;
//            sharedState->tempStorage =
//                    std::make_unique<VectorTempStorage>(sharedState->header->getDim(), numVectors);
            sharedState->graph = std::make_unique<VectorIndexGraph>(numVectors, maxNbrsAtLowerLevel,
                                                                    StorageConstants::NODE_GROUP_SIZE);
            sharedState->builder = std::make_unique<VectorIndexBuilder>(sharedState->header, sharedState->partitionId,
                                                                        numVectors,
                                                                        sharedState->graph.get());
            sharedState->compressedStorage = std::make_unique<CompressedVectorStorage>(
                    context->clientContext->getTx(), table->getColumn(sharedState->header->getCompressedPropertyId()),
                    table->getColumn(table->getPKColumnID())->getMetadataDA(),
                    sharedState->headerPerPartition->getQuantizer()->codeSize, startNodeGroupId, endNodeGroupId);
        }

        void BulkVectorIndexing::executeInternal(ExecutionContext *context) {
            std::vector<vector_id_t> vectorIds(DEFAULT_VECTOR_CAPACITY);
            printf("Running indexing %d!!\n", sharedState->partitionId);
            while (children[0]->getNextTuple(context)) {
                auto numVectors = localState->offsetVector->state->getSelVector().getSelSize();
                auto vectors = reinterpret_cast<float *>(
                        ListVector::getDataVector(localState->embeddingVector)->getData());
//                for (size_t i = 0; i < numVectors; i++) {
//                    auto id = localState->offsetVector->getValue<internalID_t>(i);
//                    KU_ASSERT(id.offset >= sharedState->startOffsetNodeTable);
//                    vectorIds[i] = id.offset - sharedState->startOffsetNodeTable;
//                }
//                sharedState->builder->batchInsert(vectors, vectorIds.data(), numVectors,
//                                                  localState->visited.get(), localState->dc.get());

                // Train the quantizer
                localState->quantizer->batchTrain(vectors, numVectors);
            }
            printf("Finished indexing %d!!\n", sharedState->partitionId);

            sharedState->compressionLatch.arrive_and_wait();
            printf("Starting quantization %d!!\n", sharedState->partitionId);
            resetScanTable(context->clientContext->getTx());
            localState->quantizer->finalizeTrain();

            // Run quantization
            vector_id_t startVectorId = 0;
            while (children[0]->getNextTuple(context)) {
                auto numVectors = localState->offsetVector->state->getSelVector().getSelSize();
                if (numVectors == 0) {
                    continue;
                }
                auto vectors = reinterpret_cast<float *>(
                        ListVector::getDataVector(localState->embeddingVector)->getData());
                startVectorId = localState->offsetVector->getValue<internalID_t>(0).offset -
                                sharedState->startOffsetNodeTable;
                // print some vector
                localState->quantizer->encode(vectors, startVectorId, numVectors);
            }
            printf("Finished quantization %d!!\n", sharedState->partitionId);
        }

        inline void BulkVectorIndexing::resetScanTable(transaction::Transaction *transaction) {
            children[0]->ptrCast<ScanNodeTable>()->reset(transaction);
        }

        void BulkVectorIndexing::finalize(ExecutionContext *context) {
            // TODO: Benchmark and make it parallelized (might be important)
            KU_ASSERT_MSG(sharedState->partitionerSharedState->partitioningBuffers.size() == 1,
                          "Only one partitioning buffer in fwd direction is supported");
            // Populate partition buffer
            sharedState->graph->populatePartitionBuffer(
                    *sharedState->partitionerSharedState->partitioningBuffers[0]);

//             TODO: Fix this to make it parallel!!
//            printf("Running quantization!!\n");
            // Skip the quantization for now
//            sharedState->headerPerPartition->getQuantizer()->batch_train(
//                    sharedState->tempStorage->vectors, sharedState->tempStorage->numVectors);
//            sharedState->headerPerPartition->getQuantizer()->encode(
//                    sharedState->tempStorage->vectors, sharedState->headerPerPartition->getQuantizedVectors(),
//                    sharedState->tempStorage->numVectors);

            // Flush quantized vectors
            // TODO: Make it parallel
            printf("Calling Flushing quantized vectors!!\n");
            sharedState->compressedStorage->flush(context->clientContext->getTx());

            // Free the memory of the graph
            sharedState->graph.reset();
//            sharedState->tempStorage.reset();
            sharedState->builder.reset();
            sharedState->compressedStorage.reset();

            // Update the updated tables
            context->clientContext->getStorageManager()->getWAL().addToUpdatedTables(
                    sharedState->header->getNodeTableId());
        }

        std::unique_ptr<PhysicalOperator> BulkVectorIndexing::clone() {
            return make_unique<BulkVectorIndexing>(resultSetDescriptor->copy(), localState->copy(),
                                                   sharedState, children[0]->clone(), id, printInfo->copy());
        }

    } // namespace processor
} // namespace kuzu

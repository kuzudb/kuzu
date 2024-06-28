#include "processor/operator/persistent/vector_index/bulk_vector_indexing.h"

#include "binder/ddl/bound_create_table.h"
#include "catalog/catalog.h"
#include "common/enums/rel_multiplicity.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

BulkVectorIndexing::BulkVectorIndexing(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
    std::unique_ptr<BulkVectorIndexingLocalState> localState,
    std::shared_ptr<BulkVectorIndexingSharedState> sharedState,
    std::unique_ptr<PhysicalOperator> child, uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
    : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::CREATE_VECTOR_INDEX,
          std::move(child), id, std::move(printInfo)},
      localState(std::move(localState)), sharedState(sharedState) {}

void BulkVectorIndexing::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    localState->offsetVector = resultSet->getValueVector(localState->offsetPos).get();
    localState->embeddingVector = resultSet->getValueVector(localState->embeddingPos).get();
    localState->dc = std::make_unique<L2DistanceComputer>(sharedState->tempStorage->vectors,
        sharedState->header->getDim(), sharedState->tempStorage->numVectors);
    localState->visited = std::make_unique<VisitedTable>(sharedState->tempStorage->numVectors);
}

void BulkVectorIndexing::initGlobalStateInternal(ExecutionContext* context) {
    auto numVectors = sharedState->maxOffsetNodeTable + 1;
    auto maxNbrsAtLowerLevel = sharedState->header->getConfig().maxNbrsAtLowerLevel;
    sharedState->tempStorage =
        std::make_unique<VectorTempStorage>(sharedState->header->getDim(), numVectors);
    sharedState->graph = std::make_unique<VectorIndexGraph>(numVectors, maxNbrsAtLowerLevel,
        StorageConstants::NODE_GROUP_SIZE);
    sharedState->builder = std::make_unique<VectorIndexBuilder>(sharedState->header,
        sharedState->graph.get(), sharedState->tempStorage.get());
}

void BulkVectorIndexing::executeInternal(ExecutionContext* context) {
    std::vector<vector_id_t> vectorIds(DEFAULT_VECTOR_CAPACITY);
    while (children[0]->getNextTuple(context)) {
        // print the thread id
        printf("Thread id: %d\n", std::this_thread::get_id());

        auto numVectors = localState->offsetVector->state->getSelVector().getSelSize();
        auto vectors = reinterpret_cast<float*>(
            ListVector::getDataVector(localState->embeddingVector)->getData());
        // print the offset and embedding
        for (size_t i = 0; i < numVectors; i++) {
            auto id = localState->offsetVector->getValue<internalID_t>(i);
            vectorIds[i] = id.offset;
        }
        sharedState->builder->batchInsert(vectors, vectorIds.data(), numVectors,
            localState->visited.get(), localState->dc.get());
    }
}

void BulkVectorIndexing::finalize(ExecutionContext* /*context*/) {
    // TODO: Make it parallelized
    KU_ASSERT_MSG(sharedState->partitionerSharedState->partitioningBuffers.size() == 1,
        "Only one partitioning buffer in fwd direction is supported");
    // Populate partition buffer
    sharedState->graph->populatePartitionBuffer(
        *sharedState->partitionerSharedState->partitioningBuffers[0]);
}

void BulkVectorIndexing::printGraph() {
    // Print the graph from partitionerSharedState
    //    auto& partitionBuffer = *sharedState->partitionerSharedState->partitioningBuffers[0];
    //    for (auto partitionIdx = 0u; partitionIdx < partitionBuffer.partitions.size();
    //    partitionIdx++) {
    //        auto& partition = partitionBuffer.partitions[partitionIdx];
    //        auto& group = partition.getChunkedGroups()[0];
    //        auto& from = group->getColumnChunk(0);
    //        auto& to = group->getColumnChunk(1);
    //        auto& relId = group->getColumnChunk(2);
    //        for (auto i = 0u; i < group->getNumRows(); i++) {
    //            spdlog::info("From: {}, To: {}, RelId: {}", from.getValue<offset_t>(i),
    //                to.getValue<offset_t>(i), relId.getValue<offset_t>(i));
    //        }
    //    }
}

std::unique_ptr<PhysicalOperator> BulkVectorIndexing::clone() {
    return make_unique<BulkVectorIndexing>(resultSetDescriptor->copy(), localState->copy(),
        sharedState, children[0]->clone(), id, printInfo->copy());
}

} // namespace processor
} // namespace kuzu

#include "processor/operator/persistent/vector_index/bulk_vector_indexing.h"

#include <thread>

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
    sharedState->header->initSampleGraph(numVectors);
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
        printf("Thread id in: %d\n", std::this_thread::get_id());
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
//    testGraph();
    // Populate partition buffer
    sharedState->graph->populatePartitionBuffer(
        *sharedState->partitionerSharedState->partitioningBuffers[0]);
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

//void BulkVectorIndexing::testGraph() {
//    // Load Query Fvec file
//    int k = 100;
//    int ef_search = 100;
//    auto queryVectorPath = "/Users/gauravsehgal/work/orangedb/data/gist_10k/query.fvecs";
//    auto groundTruthPath = "/Users/gauravsehgal/work/orangedb/data/gist_10k/gt.bin";
//    size_t queryDimension, queryNumVectors;
//    float *queryVecs = readFvecFile(queryVectorPath, &queryDimension, &queryNumVectors);
//    auto *gtVecs = new vector_id_t[queryNumVectors * k];
//    loadFromFile(groundTruthPath, reinterpret_cast<uint8_t *>(gtVecs), queryNumVectors * k * sizeof(vector_id_t));
//
//    // Test with ANN
//    auto visited = std::make_unique<VisitedTable>(sharedState->tempStorage->numVectors);
//    auto distanceComputer = std::make_unique<L2DistanceComputer>(sharedState->tempStorage->vectors,
//        sharedState->header->getDim(), sharedState->tempStorage->numVectors);
//    auto recall = 0.0;
//
//    for (size_t i = 0; i < queryNumVectors; i++) {
//        std::priority_queue<NodeDistCloser> results;
//        std::vector<NodeDistFarther> res;
//        sharedState->builder->search(queryVecs + (i * queryDimension), k, ef_search, visited.get(), results, distanceComputer.get());
//        while (!results.empty()) {
//            auto top = results.top();
//            res.emplace_back(top.id, top.dist);
//            results.pop();
//        }
////        printf("Results size: %lu\n", res.size());
//        auto gt = gtVecs + i * k;
//        for (auto &result: res) {
//            if (std::find(gt, gt + k, result.id) != (gt + k)) {
//                recall++;
//            }
//        }
//    }
//    auto recallPerQuery = recall / queryNumVectors;
////    printf("Recall: %f\n", (recallPerQuery / k) * 100);
//}

std::unique_ptr<PhysicalOperator> BulkVectorIndexing::clone() {
    return make_unique<BulkVectorIndexing>(resultSetDescriptor->copy(), localState->copy(),
        sharedState, children[0]->clone(), id, printInfo->copy());
}

} // namespace processor
} // namespace kuzu

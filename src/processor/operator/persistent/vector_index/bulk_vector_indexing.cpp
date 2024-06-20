#include "processor/operator/persistent/vector_index/bulk_vector_indexing.h"
#include "spdlog/spdlog.h"

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
}

void BulkVectorIndexing::initGlobalStateInternal(ExecutionContext* context) {
}

void BulkVectorIndexing::executeInternal(kuzu::processor::ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        // print the offset and embedding
        for (size_t i = 0; i < localState->offsetVector->state->getSelVector().getSelSize(); i++) {
            auto id = localState->offsetVector->getValue<internalID_t>(i);
            auto dim = ArrayType::getNumElements(localState->embeddingVector->dataType);
            auto dataVector = ListVector::getDataVector(localState->embeddingVector);
            auto dimSize = ListVector::getDataVectorSize(localState->embeddingVector);
            spdlog::info("offset: {}, table: {}, embedding size: {}", id.offset, id.tableID, dim);
        }
    }
}

std::unique_ptr<PhysicalOperator> BulkVectorIndexing::clone() {
    return make_unique<BulkVectorIndexing>(resultSetDescriptor->copy(), localState->copy(),
        sharedState, children[0]->clone(), id, printInfo->copy());
}

} // namespace processor
} // namespace kuzu

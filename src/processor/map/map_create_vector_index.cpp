#include "planner/operator/persistent/logical_create_vector_index.h"
#include "processor/operator/persistent/vector_index/bulk_vector_indexing.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCreateVectorIndex(
    LogicalOperator* logicalOperator) {
    auto& logical = logicalOperator->constCast<LogicalCreateVectorIndex>();
    auto prevOperator = mapOperator(logical.getChild(0).get());
    auto printInfo = std::make_unique<OPPrintInfo>(logical.getExpressionsForPrinting());
    auto outSchema = logical.getSchema();
    auto internalIdDataPos = DataPos(outSchema->getExpressionPos(*logical.getInternalId()));
    auto embeddingDataPos = DataPos(outSchema->getExpressionPos(*logical.getEmbedding()));
    auto localState =
        std::make_unique<BulkVectorIndexingLocalState>(internalIdDataPos, embeddingDataPos,
            logical.getTableID(), logical.getPropertyID(), logical.getHnswReaderConfig());
    auto sharedState = std::make_shared<BulkVectorIndexingSharedState>();
    return std::make_unique<BulkVectorIndexing>(std::make_unique<ResultSetDescriptor>(outSchema),
        std::move(localState), sharedState, std::move(prevOperator), getOperatorID(),
        std::move(printInfo));
}

} // namespace processor
} // namespace kuzu

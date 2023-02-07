#include "processor/operator/aggregate/base_aggregate.h"

using namespace kuzu::function;

namespace kuzu {
namespace processor {

BaseAggregateSharedState::BaseAggregateSharedState(
    const std::vector<std::unique_ptr<AggregateFunction>>& aggregateFunctions)
    : currentOffset{0} {
    for (auto& aggregateFunction : aggregateFunctions) {
        this->aggregateFunctions.push_back(aggregateFunction->clone());
    }
}

bool BaseAggregate::containDistinctAggregate() const {
    for (auto& aggregateFunction : aggregateFunctions) {
        if (aggregateFunction->isFunctionDistinct()) {
            return true;
        }
    }
    return false;
}

void BaseAggregate::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& dataPos : aggregateVectorsPos) {
        if (dataPos.dataChunkPos == UINT32_MAX) {
            // COUNT(*) aggregate does not take any input vector.
            aggregateVectors.push_back(nullptr);
        } else {
            auto vector = resultSet->getValueVector(dataPos);
            aggregateVectors.push_back(vector.get());
        }
    }
}

} // namespace processor
} // namespace kuzu

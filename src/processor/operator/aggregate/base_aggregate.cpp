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
    for (auto& function : aggregateFunctions) {
        if (function->isFunctionDistinct()) {
            return true;
        }
    }
    return false;
}

void BaseAggregate::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    for (auto& info : aggInfos) {
        auto aggregateInput = AggregateInput();
        if (info.aggVectorPos.dataChunkPos == INVALID_DATA_CHUNK_POS) {
            aggregateInput.aggregateVector = nullptr;
        } else {
            aggregateInput.aggregateVector = resultSet->getValueVector(info.aggVectorPos).get();
        }
        for (auto dataChunkPos : info.multiplicityChunksPos) {
            aggregateInput.multiplicityChunks.push_back(
                resultSet->getDataChunk(dataChunkPos).get());
        }
        aggInputs.push_back(std::move(aggregateInput));
    }
}

std::vector<std::unique_ptr<function::AggregateFunction>> BaseAggregate::cloneAggFunctions() {
    std::vector<std::unique_ptr<AggregateFunction>> result;
    result.reserve(aggregateFunctions.size());
    for (auto& function : aggregateFunctions) {
        result.push_back(function->clone());
    }
    return result;
}

} // namespace processor
} // namespace kuzu

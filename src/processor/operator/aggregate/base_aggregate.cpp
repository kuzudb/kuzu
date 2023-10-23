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
    for (auto& inputInfo : aggregateInputInfos) {
        auto aggregateInput = std::make_unique<AggregateInput>();
        if (inputInfo->aggregateVectorPos.dataChunkPos == INVALID_DATA_CHUNK_POS) {
            aggregateInput->aggregateVector = nullptr;
        } else {
            aggregateInput->aggregateVector =
                resultSet->getValueVector(inputInfo->aggregateVectorPos).get();
        }
        for (auto dataChunkPos : inputInfo->multiplicityChunksPos) {
            aggregateInput->multiplicityChunks.push_back(
                resultSet->getDataChunk(dataChunkPos).get());
        }
        aggregateInputs.push_back(std::move(aggregateInput));
    }
}

std::vector<std::unique_ptr<function::AggregateFunction>> BaseAggregate::cloneAggFunctions() {
    std::vector<std::unique_ptr<AggregateFunction>> result;
    for (auto& function : aggregateFunctions) {
        result.push_back(function->clone());
    }
    return result;
}

std::vector<std::unique_ptr<AggregateInputInfo>> BaseAggregate::cloneAggInputInfos() {
    std::vector<std::unique_ptr<AggregateInputInfo>> result;
    for (auto& info : aggregateInputInfos) {
        result.push_back(info->copy());
    }
    return result;
}

} // namespace processor
} // namespace kuzu

#include "processor/operator/aggregate/simple_aggregate.h"

#include "binder/expression/expression_util.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

std::string SimpleAggregatePrintInfo::toString() const {
    std::string result = "";
    result += "Aggregate: ";
    result += binder::ExpressionUtil::toString(aggregates);
    return result;
}

SimpleAggregateSharedState::SimpleAggregateSharedState(
    const std::vector<std::unique_ptr<AggregateFunction>>& aggregateFunctions)
    : BaseAggregateSharedState{aggregateFunctions} {
    for (auto& aggregateFunction : this->aggregateFunctions) {
        globalAggregateStates.push_back(aggregateFunction->createInitialNullAggregateState());
    }
}

void SimpleAggregateSharedState::combineAggregateStates(
    const std::vector<std::unique_ptr<AggregateState>>& localAggregateStates,
    storage::MemoryManager* memoryManager) {
    KU_ASSERT(localAggregateStates.size() == globalAggregateStates.size());
    std::unique_lock lck{mtx};
    for (auto i = 0u; i < aggregateFunctions.size(); ++i) {
        aggregateFunctions[i]->combineState((uint8_t*)globalAggregateStates[i].get(),
            (uint8_t*)localAggregateStates[i].get(), memoryManager);
    }
}

void SimpleAggregateSharedState::finalizeAggregateStates() {
    std::unique_lock lck{mtx};
    for (auto i = 0u; i < aggregateFunctions.size(); ++i) {
        aggregateFunctions[i]->finalizeState((uint8_t*)globalAggregateStates[i].get());
    }
}

std::pair<uint64_t, uint64_t> SimpleAggregateSharedState::getNextRangeToRead() {
    std::unique_lock lck{mtx};
    if (currentOffset >= 1) {
        return std::make_pair(currentOffset, currentOffset);
    }
    auto startOffset = currentOffset;
    currentOffset++;
    return std::make_pair(startOffset, currentOffset);
}

void SimpleAggregate::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseAggregate::initLocalStateInternal(resultSet, context);
    for (auto i = 0u; i < aggregateFunctions.size(); ++i) {
        auto func = aggregateFunctions[i].get();
        localAggregateStates.push_back(func->createInitialNullAggregateState());
        std::unique_ptr<AggregateHashTable> distinctHT;
        if (func->isDistinct) {
            auto mm = context->clientContext->getMemoryManager();
            distinctHT = AggregateHashTableUtils::createDistinctHashTable(*mm,
                std::vector<LogicalType>{} /* empty group by keys */,
                aggInfos[i].distinctAggKeyType);
        } else {
            distinctHT = nullptr;
        }
        distinctHashTables.push_back(std::move(distinctHT));
    };
}

void SimpleAggregate::executeInternal(ExecutionContext* context) {
    auto memoryManager = context->clientContext->getMemoryManager();
    while (children[0]->getNextTuple(context)) {
        for (auto i = 0u; i < aggregateFunctions.size(); i++) {
            auto aggregateFunction = aggregateFunctions[i].get();
            if (aggregateFunction->isFunctionDistinct()) {
                computeDistinctAggregate(distinctHashTables[i].get(), aggregateFunction,
                    &aggInputs[i], localAggregateStates[i].get(), memoryManager);
            } else {
                computeAggregate(aggregateFunction, &aggInputs[i], localAggregateStates[i].get(),
                    memoryManager);
            }
        }
    }
    sharedState->combineAggregateStates(localAggregateStates, memoryManager);
}

void SimpleAggregate::computeDistinctAggregate(AggregateHashTable* distinctHT,
    function::AggregateFunction* function, AggregateInput* input, function::AggregateState* state,
    storage::MemoryManager* memoryManager) {
    auto multiplicity = 1; // Distinct aggregate should ignore multiplicity.
    if (distinctHT->isAggregateValueDistinctForGroupByKeys(std::vector<ValueVector*>{},
            input->aggregateVector)) {
        auto pos = input->aggregateVector->state->getSelVector()[0];
        if (!input->aggregateVector->isNull(pos)) {
            function->updatePosState((uint8_t*)state, input->aggregateVector, multiplicity, pos,
                memoryManager);
        }
    }
}

void SimpleAggregate::computeAggregate(function::AggregateFunction* function, AggregateInput* input,
    function::AggregateState* state, storage::MemoryManager* memoryManager) {
    auto multiplicity = resultSet->multiplicity;
    for (auto dataChunk : input->multiplicityChunks) {
        multiplicity *= dataChunk->state->getSelVector().getSelSize();
    }
    if (input->aggregateVector && input->aggregateVector->state->isFlat()) {
        auto pos = input->aggregateVector->state->getSelVector()[0];
        if (!input->aggregateVector->isNull(pos)) {
            function->updatePosState((uint8_t*)state, input->aggregateVector, multiplicity, pos,
                memoryManager);
        }
    } else {
        function->updateAllState((uint8_t*)state, input->aggregateVector, multiplicity,
            memoryManager);
    }
}

} // namespace processor
} // namespace kuzu

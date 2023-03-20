#include "processor/operator/aggregate/simple_aggregate.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

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
    assert(localAggregateStates.size() == globalAggregateStates.size());
    auto lck = acquireLock();
    for (auto i = 0u; i < aggregateFunctions.size(); ++i) {
        aggregateFunctions[i]->combineState((uint8_t*)globalAggregateStates[i].get(),
            (uint8_t*)localAggregateStates[i].get(), memoryManager);
    }
}

void SimpleAggregateSharedState::finalizeAggregateStates() {
    auto lck = acquireLock();
    for (auto i = 0u; i < aggregateFunctions.size(); ++i) {
        aggregateFunctions[i]->finalizeState((uint8_t*)globalAggregateStates[i].get());
    }
}

std::pair<uint64_t, uint64_t> SimpleAggregateSharedState::getNextRangeToRead() {
    auto lck = acquireLock();
    if (currentOffset >= 1) {
        return std::make_pair(currentOffset, currentOffset);
    }
    auto startOffset = currentOffset;
    currentOffset++;
    return std::make_pair(startOffset, currentOffset);
}

void SimpleAggregate::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseAggregate::initLocalStateInternal(resultSet, context);
    for (auto& aggregateFunction : this->aggregateFunctions) {
        localAggregateStates.push_back(aggregateFunction->createInitialNullAggregateState());
    }
    distinctHashTables = AggregateHashTableUtils::createDistinctHashTables(
        *context->memoryManager, std::vector<DataType>{}, this->aggregateFunctions);
}

void SimpleAggregate::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        for (auto i = 0u; i < aggregateFunctions.size(); i++) {
            auto aggregateFunction = aggregateFunctions[i].get();
            auto aggVector = aggregateVectors[i];
            if (aggregateFunction->isFunctionDistinct()) {
                auto distinctHT = distinctHashTables[i].get();
                assert(distinctHT != nullptr);
                if (distinctHT->isAggregateValueDistinctForGroupByKeys(
                        std::vector<ValueVector*>{}, aggVector)) {
                    if (!aggVector->isNull(aggVector->state->selVector->selectedPositions[0])) {
                        aggregateFunction->updatePosState((uint8_t*)localAggregateStates[i].get(),
                            aggVector, 1 /* Distinct aggregate should ignore
                                          multiplicity since they are known to be non-distinct. */
                            ,
                            aggVector->state->selVector->selectedPositions[0],
                            context->memoryManager);
                    }
                }
            } else {
                if (aggVector && aggVector->state->isFlat()) {
                    if (!aggVector->isNull(aggVector->state->selVector->selectedPositions[0])) {
                        aggregateFunction->updatePosState((uint8_t*)localAggregateStates[i].get(),
                            aggVector, resultSet->multiplicity,
                            aggVector->state->selVector->selectedPositions[0],
                            context->memoryManager);
                    }
                } else {
                    aggregateFunction->updateAllState((uint8_t*)localAggregateStates[i].get(),
                        aggVector, resultSet->multiplicity, context->memoryManager);
                }
            }
        }
    }
    sharedState->combineAggregateStates(localAggregateStates, context->memoryManager);
}

std::unique_ptr<PhysicalOperator> SimpleAggregate::clone() {
    std::vector<std::unique_ptr<AggregateFunction>> clonedAggregateFunctions;
    for (auto& aggregateFunction : aggregateFunctions) {
        clonedAggregateFunctions.push_back(aggregateFunction->clone());
    }
    return make_unique<SimpleAggregate>(resultSetDescriptor->copy(), sharedState,
        aggregateVectorsPos, std::move(clonedAggregateFunctions), children[0]->clone(), id,
        paramsString);
}

} // namespace processor
} // namespace kuzu

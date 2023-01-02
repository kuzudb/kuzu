#include "processor/operator/aggregate/simple_aggregate.h"

namespace kuzu {
namespace processor {

SimpleAggregateSharedState::SimpleAggregateSharedState(
    const vector<unique_ptr<AggregateFunction>>& aggregateFunctions)
    : BaseAggregateSharedState{aggregateFunctions} {
    for (auto& aggregateFunction : this->aggregateFunctions) {
        globalAggregateStates.push_back(aggregateFunction->createInitialNullAggregateState());
    }
}

void SimpleAggregateSharedState::combineAggregateStates(
    const vector<unique_ptr<AggregateState>>& localAggregateStates) {
    assert(localAggregateStates.size() == globalAggregateStates.size());
    auto lck = acquireLock();
    for (auto i = 0u; i < aggregateFunctions.size(); ++i) {
        aggregateFunctions[i]->combineState(
            (uint8_t*)globalAggregateStates[i].get(), (uint8_t*)localAggregateStates[i].get());
    }
}

void SimpleAggregateSharedState::finalizeAggregateStates() {
    auto lck = acquireLock();
    for (auto i = 0u; i < aggregateFunctions.size(); ++i) {
        aggregateFunctions[i]->finalizeState((uint8_t*)globalAggregateStates[i].get());
    }
}

pair<uint64_t, uint64_t> SimpleAggregateSharedState::getNextRangeToRead() {
    auto lck = acquireLock();
    if (currentOffset >= 1) {
        return make_pair(currentOffset, currentOffset);
    }
    auto startOffset = currentOffset;
    currentOffset++;
    return make_pair(startOffset, currentOffset);
}

void SimpleAggregate::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseAggregate::initLocalStateInternal(resultSet, context);
    for (auto& aggregateFunction : this->aggregateFunctions) {
        localAggregateStates.push_back(aggregateFunction->createInitialNullAggregateState());
    }
    distinctHashTables = AggregateHashTableUtils::createDistinctHashTables(
        *context->memoryManager, vector<DataType>{}, this->aggregateFunctions);
}

void SimpleAggregate::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple()) {
        for (auto i = 0u; i < aggregateFunctions.size(); i++) {
            auto aggregateFunction = aggregateFunctions[i].get();
            auto aggVector = aggregateVectors[i];
            if (aggregateFunction->isFunctionDistinct()) {
                auto distinctHT = distinctHashTables[i].get();
                assert(distinctHT != nullptr);
                if (distinctHT->isAggregateValueDistinctForGroupByKeys(
                        vector<ValueVector*>{}, aggVector)) {
                    if (!aggVector->isNull(aggVector->state->selVector->selectedPositions[0])) {
                        aggregateFunction->updatePosState((uint8_t*)localAggregateStates[i].get(),
                            aggVector, 1 /* Distinct aggregate should ignore
                                          multiplicity since they are known to be non-distinct. */
                            ,
                            aggVector->state->selVector->selectedPositions[0]);
                    }
                }
            } else {
                if (aggVector && aggVector->state->isFlat()) {
                    if (!aggVector->isNull(aggVector->state->selVector->selectedPositions[0])) {
                        aggregateFunction->updatePosState((uint8_t*)localAggregateStates[i].get(),
                            aggVector, resultSet->multiplicity,
                            aggVector->state->selVector->selectedPositions[0]);
                    }
                } else {
                    aggregateFunction->updateAllState((uint8_t*)localAggregateStates[i].get(),
                        aggVector, resultSet->multiplicity);
                }
            }
        }
    }
    sharedState->combineAggregateStates(localAggregateStates);
}

unique_ptr<PhysicalOperator> SimpleAggregate::clone() {
    vector<unique_ptr<AggregateFunction>> clonedAggregateFunctions;
    for (auto& aggregateFunction : aggregateFunctions) {
        clonedAggregateFunctions.push_back(aggregateFunction->clone());
    }
    return make_unique<SimpleAggregate>(resultSetDescriptor->copy(), sharedState,
        aggregateVectorsPos, std::move(clonedAggregateFunctions), children[0]->clone(), id,
        paramsString);
}

} // namespace processor
} // namespace kuzu

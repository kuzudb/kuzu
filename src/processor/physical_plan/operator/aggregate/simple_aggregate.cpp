#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregate.h"

namespace graphflow {
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

SimpleAggregate::SimpleAggregate(shared_ptr<SimpleAggregateSharedState> sharedState,
    vector<DataPos> aggregateVectorsPos, vector<unique_ptr<AggregateFunction>> aggregateFunctions,
    unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
    : BaseAggregate{move(aggregateVectorsPos), move(aggregateFunctions), move(child), context, id},
      sharedState{move(sharedState)} {
    for (auto& aggregateFunction : this->aggregateFunctions) {
        localAggregateStates.push_back(aggregateFunction->createInitialNullAggregateState());
    }
    distinctHashTables = AggregateHashTableUtils::createDistinctHashTables(
        *context.memoryManager, vector<DataType>{}, this->aggregateFunctions);
}

void SimpleAggregate::execute() {
    metrics->executionTime.start();
    BaseAggregate::execute();
    while (children[0]->getNextTuples()) {
        for (auto i = 0u; i < aggregateFunctions.size(); i++) {
            auto aggregateFunction = aggregateFunctions[i].get();
            if (aggregateFunction->isFunctionDistinct()) {
                auto distinctHT = distinctHashTables[i].get();
                assert(distinctHT != nullptr);
                if (distinctHT->isAggregateValueDistinctForGroupByKeys(
                        vector<ValueVector*>{}, aggregateVectors[i])) {
                    aggregateFunction->updateState((uint8_t*)localAggregateStates[i].get(),
                        aggregateVectors[i], resultSet->multiplicity);
                }
            } else {
                aggregateFunction->updateState((uint8_t*)localAggregateStates[i].get(),
                    aggregateVectors[i], resultSet->multiplicity);
            }
        }
    }
    sharedState->combineAggregateStates(localAggregateStates);
    metrics->executionTime.stop();
}

void SimpleAggregate::finalize() {
    sharedState->finalizeAggregateStates();
}

unique_ptr<PhysicalOperator> SimpleAggregate::clone() {
    vector<unique_ptr<AggregateFunction>> clonedAggregateFunctions;
    for (auto& aggregateFunction : aggregateFunctions) {
        clonedAggregateFunctions.push_back(aggregateFunction->clone());
    }
    return make_unique<SimpleAggregate>(sharedState, aggregateVectorsPos,
        move(clonedAggregateFunctions), children[0]->clone(), context, id);
}

} // namespace processor
} // namespace graphflow

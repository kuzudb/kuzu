#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregate.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace processor {

SimpleAggregate::SimpleAggregate(shared_ptr<AggregateSharedState> sharedState,
    vector<DataPos> aggregateVectorsPos, vector<unique_ptr<AggregateFunction>> aggregateFunctions,
    unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
    : Sink{move(child), context, id}, sharedState{move(sharedState)},
      aggregateVectorsPos{move(aggregateVectorsPos)}, aggregateFunctions{move(aggregateFunctions)} {
    for (auto& aggregateFunction : this->aggregateFunctions) {
        aggregateStates.push_back(aggregateFunction->initialize());
    }
}

shared_ptr<ResultSet> SimpleAggregate::initResultSet() {
    resultSet = children[0]->initResultSet();
    for (auto& dataPos : aggregateVectorsPos) {
        if (dataPos.dataChunkPos == UINT32_MAX) {
            aggregateVectors.push_back(nullptr);
            continue;
        }
        auto dataChunk = resultSet->dataChunks[dataPos.dataChunkPos];
        aggregateVectors.push_back(dataChunk->valueVectors[dataPos.valueVectorPos].get());
    }
    return resultSet;
}

void SimpleAggregate::execute() {
    metrics->executionTime.start();
    Sink::execute();
    // Exhaust source to update local state for each aggregate expression by its evaluator.
    while (children[0]->getNextTuples()) {
        for (auto i = 0u; i < aggregateFunctions.size(); i++) {
            aggregateFunctions[i]->update(
                (uint8_t*)aggregateStates[i].get(), aggregateVectors[i], resultSet->multiplicity);
        }
    }
    // Combine global shared state with local states.
    {
        lock_guard<mutex> sharedStateLock(sharedState->aggregateSharedStateLock);
        for (auto i = 0u; i < aggregateFunctions.size(); i++) {
            aggregateFunctions[i]->combine((uint8_t*)sharedState->aggregateStates[i].get(),
                (uint8_t*)aggregateStates[i].get());
        }
    }
    metrics->executionTime.stop();
}

void SimpleAggregate::finalize() {
    {
        lock_guard<mutex> sharedStateLock(sharedState->aggregateSharedStateLock);
        for (auto i = 0u; i < aggregateFunctions.size(); i++) {
            aggregateFunctions[i]->finalize((uint8_t*)sharedState->aggregateStates[i].get());
        }
    }
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

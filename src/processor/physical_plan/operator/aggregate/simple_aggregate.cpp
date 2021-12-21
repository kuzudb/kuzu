#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregate.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace processor {

SimpleAggregate::SimpleAggregate(unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id,
    shared_ptr<AggregationSharedState> aggregationSharedState,
    vector<unique_ptr<AggregateExpressionEvaluator>> aggregationEvaluators)
    : Sink{move(prevOperator), PhysicalOperatorType::AGGREGATE, context, id},
      sharedState{move(aggregationSharedState)}, aggregationEvaluators{
                                                     move(aggregationEvaluators)} {
    for (auto& expressionEvaluator : this->aggregationEvaluators) {
        aggregationStates.push_back(expressionEvaluator->getFunction()->initialize());
    }
}

shared_ptr<ResultSet> SimpleAggregate::initResultSet() {
    resultSet = prevOperator->initResultSet();
    for (auto& aggregationEvaluator : aggregationEvaluators) {
        aggregationEvaluator->initResultSet(*resultSet, *context.memoryManager);
    }
    return resultSet;
}

void SimpleAggregate::execute() {
    metrics->executionTime.start();
    Sink::execute();
    // Exhaust source to update local state for each aggregation expression by its evaluator.
    while (prevOperator->getNextTuples()) {
        for (auto i = 0u; i < aggregationEvaluators.size(); i++) {
            aggregationEvaluators[i]->evaluate();
            aggregationEvaluators[i].get()->getFunction()->update(
                (uint8_t*)aggregationStates[i].get(), aggregationEvaluators[i]->getChildResult(),
                prevOperator->getResultSet()->getNumTuples());
        }
    }
    // Combine global shared state with local states.
    {
        lock_guard<mutex> sharedStateLock(sharedState->aggregationSharedStateLock);
        for (auto i = 0u; i < aggregationEvaluators.size(); i++) {
            aggregationEvaluators[i].get()->getFunction()->combine(
                (uint8_t*)sharedState->aggregationStates[i].get(),
                (uint8_t*)aggregationStates[i].get());
        }
    }
    metrics->executionTime.stop();
}

void SimpleAggregate::finalize() {
    {
        lock_guard<mutex> sharedStateLock(sharedState->aggregationSharedStateLock);
        for (auto i = 0u; i < aggregationEvaluators.size(); i++) {
            aggregationEvaluators[i].get()->getFunction()->finalize(
                (uint8_t*)sharedState->aggregationStates[i].get());
        }
    }
}

unique_ptr<PhysicalOperator> SimpleAggregate::clone() {
    auto prevOperatorClone = prevOperator->clone();
    vector<unique_ptr<AggregateExpressionEvaluator>> aggregationEvaluatorsCloned;
    for (auto& aggregationEvaluator : aggregationEvaluators) {
        aggregationEvaluatorsCloned.push_back(
            static_unique_pointer_cast<ExpressionEvaluator, AggregateExpressionEvaluator>(
                aggregationEvaluator->clone()));
    }
    return make_unique<SimpleAggregate>(
        move(prevOperatorClone), context, id, sharedState, move(aggregationEvaluatorsCloned));
}

} // namespace processor
} // namespace graphflow

#include "src/processor/include/physical_plan/operator/aggregate/aggregate.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace processor {

Aggregate::Aggregate(shared_ptr<ResultSet> resultSet, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id,
    shared_ptr<AggregationSharedState> aggregationSharedState,
    vector<unique_ptr<AggregateExpressionEvaluator>> aggregationEvaluators)
    : Sink{move(resultSet), move(prevOperator), PhysicalOperatorType::AGGREGATE, context, id},
      sharedState{move(aggregationSharedState)}, aggregationEvaluators{
                                                     move(aggregationEvaluators)} {
    for (auto& expressionEvaluator : this->aggregationEvaluators) {
        aggregationStates.push_back(expressionEvaluator->getFunction()->initialize());
    }
}

void Aggregate::init() {
    Sink::init();
    for (auto& aggregationEvaluator : aggregationEvaluators) {
        aggregationEvaluator->initResultSet(*resultSet, *context.memoryManager);
    }
}

void Aggregate::execute() {
    metrics->executionTime.start();
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

void Aggregate::finalize() {
    {
        lock_guard<mutex> sharedStateLock(sharedState->aggregationSharedStateLock);
        for (auto i = 0u; i < aggregationEvaluators.size(); i++) {
            aggregationEvaluators[i].get()->getFunction()->finalize(
                (uint8_t*)sharedState->aggregationStates[i].get());
        }
    }
}

unique_ptr<PhysicalOperator> Aggregate::clone() {
    auto prevOperatorClone = prevOperator->clone();
    vector<unique_ptr<AggregateExpressionEvaluator>> aggregationEvaluatorsCloned;
    for (auto& aggregationEvaluator : aggregationEvaluators) {
        aggregationEvaluatorsCloned.push_back(
            static_unique_pointer_cast<ExpressionEvaluator, AggregateExpressionEvaluator>(
                aggregationEvaluator->clone()));
    }
    auto clonedResultSet = make_shared<ResultSet>(resultSet->dataChunks.size());
    for (auto i = 0u; i < resultSet->dataChunks.size(); ++i) {
        clonedResultSet->insert(
            i, make_shared<DataChunk>(resultSet->dataChunks[i]->valueVectors.size()));
    }
    return make_unique<Aggregate>(move(clonedResultSet), move(prevOperatorClone), context, id,
        move(sharedState), move(aggregationEvaluatorsCloned));
}

} // namespace processor
} // namespace graphflow

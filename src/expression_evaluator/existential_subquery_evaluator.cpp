#include "src/expression_evaluator/include/existential_subquery_evaluator.h"

namespace graphflow {
namespace evaluator {

ExistentialSubqueryEvaluator::ExistentialSubqueryEvaluator(
    MemoryManager& memoryManager, unique_ptr<ResultCollector> subPlanResultCollector)
    : ExpressionEvaluator{EXISTENTIAL_SUBQUERY, BOOL}, subPlanResultCollector{
                                                           move(subPlanResultCollector)} {
    result = make_shared<ValueVector>(&memoryManager, BOOL, true /* isSingleValue */);
    result->state = DataChunkState::getSingleValueDataChunkState();
}

void ExistentialSubqueryEvaluator::executeSubplan() {
    subPlanResultCollector->reInitialize();
    subPlanResultCollector->execute();
}

void ExistentialSubqueryEvaluator::evaluate() {
    executeSubplan();
    result->values[0] = subPlanResultCollector->queryResult->numTuples != 0;
}

uint64_t ExistentialSubqueryEvaluator::select(sel_t* selectedPositions) {
    executeSubplan();
    return subPlanResultCollector->queryResult->numTuples != 0;
}

} // namespace evaluator
} // namespace graphflow

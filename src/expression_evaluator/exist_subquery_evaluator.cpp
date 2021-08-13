#include "src/expression_evaluator/include/exist_subquery_evaluator.h"

namespace graphflow {
namespace evaluator {

ExistSubqueryEvaluator::ExistSubqueryEvaluator(
    MemoryManager& memoryManager, unique_ptr<ResultCollector> subPlanResultCollector)
    : ExpressionEvaluator{EXIST_SUBQUERY, BOOL}, subPlanResultCollector{
                                                     move(subPlanResultCollector)} {
    result = make_shared<ValueVector>(&memoryManager, BOOL, true /* isSingleValue */);
    result->state = DataChunkState::getSingleValueDataChunkState();
}

void ExistSubqueryEvaluator::executeSubplan() {
    subPlanResultCollector->reInitialize();
    subPlanResultCollector->execute();
}

void ExistSubqueryEvaluator::evaluate() {
    executeSubplan();
    result->values[0] = subPlanResultCollector->queryResult->numTuples != 0;
}

uint64_t ExistSubqueryEvaluator::select(sel_t* selectedPositions) {
    executeSubplan();
    return subPlanResultCollector->queryResult->numTuples != 0;
}

} // namespace evaluator
} // namespace graphflow

#include "src/expression_evaluator/include/existential_subquery_evaluator.h"

#include "src/processor/include/physical_plan/operator/result_collector.h"
#include "src/processor/include/physical_plan/operator/select_scan.h"

namespace graphflow {
namespace evaluator {

ExistentialSubqueryEvaluator::ExistentialSubqueryEvaluator(
    unique_ptr<ResultCollector> subPlanResultCollector)
    : ExpressionEvaluator{EXISTENTIAL_SUBQUERY, BOOL}, subPlanResultCollector{
                                                           move(subPlanResultCollector)} {}

void ExistentialSubqueryEvaluator::initResultSet(
    const ResultSet& resultSet, MemoryManager& memoryManager) {
    result = make_shared<ValueVector>(&memoryManager, BOOL, true /* isSingleValue */);
    result->state = DataChunkState::getSingleValueDataChunkState();

    PhysicalOperator* op = subPlanResultCollector.get();
    while (op->prevOperator != nullptr) {
        op = op->prevOperator.get();
    }
    assert(op->operatorType == SELECT_SCAN);
    ((SelectScan*)op)->setInResultSet(&resultSet);
    subPlanResultCollector->init();
}

uint64_t ExistentialSubqueryEvaluator::executeSubplan() {
    subPlanResultCollector->reInitialize();
    subPlanResultCollector->execute();
    return subPlanResultCollector->queryResult->numTuples;
}

void ExistentialSubqueryEvaluator::evaluate() {
    result->values[0] = executeSubplan() != 0;
}

uint64_t ExistentialSubqueryEvaluator::select(sel_t* selectedPositions) {
    return executeSubplan() != 0;
}

unique_ptr<ExpressionEvaluator> ExistentialSubqueryEvaluator::clone() {
    auto subPlanResultCollectorClone = unique_ptr<ResultCollector>{
        dynamic_cast<ResultCollector*>(subPlanResultCollector->clone().release())};
    return make_unique<ExistentialSubqueryEvaluator>(move(subPlanResultCollectorClone));
}

} // namespace evaluator
} // namespace graphflow

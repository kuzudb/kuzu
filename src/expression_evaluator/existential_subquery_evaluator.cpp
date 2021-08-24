#include "src/expression_evaluator/include/existential_subquery_evaluator.h"

#include "src/processor/include/physical_plan/operator/sink/result_collector.h"
#include "src/processor/include/physical_plan/operator/select_scan/select_scan.h"

namespace graphflow {
namespace evaluator {

ExistentialSubqueryEvaluator::ExistentialSubqueryEvaluator(
    MemoryManager& memoryManager, unique_ptr<ResultCollector> subPlanResultCollector)
    : ExpressionEvaluator{EXISTENTIAL_SUBQUERY, BOOL}, subPlanResultCollector{
                                                           move(subPlanResultCollector)} {
    result = make_shared<ValueVector>(&memoryManager, BOOL, true /* isSingleValue */);
    result->state = DataChunkState::getSingleValueDataChunkState();
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

// TODO: figure out what's the right way to clone subPlan (with selectScan)
unique_ptr<ExpressionEvaluator> ExistentialSubqueryEvaluator::clone(
    MemoryManager& memoryManager, const ResultSet& resultSet) {
    auto subPlanResultCollectorClone = unique_ptr<ResultCollector>{
        dynamic_cast<ResultCollector*>(subPlanResultCollector->clone().release())};
    PhysicalOperator* op = subPlanResultCollectorClone.get();
    while (op->prevOperator != nullptr) {
        op = op->prevOperator.get();
    }
    assert(op->operatorType == SELECT_SCAN);
    ((SelectScan*)op)->setInResultSet(&resultSet);
    return make_unique<ExistentialSubqueryEvaluator>(memoryManager, move(subPlanResultCollectorClone));
}

} // namespace evaluator
} // namespace graphflow

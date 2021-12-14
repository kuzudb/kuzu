#include "src/processor/include/physical_plan/operator/exists.h"

#include "src/processor/include/physical_plan/operator/result_collector.h"
#include "src/processor/include/physical_plan/operator/result_scan.h"
#include "src/processor/include/physical_plan/physical_plan.h"

namespace graphflow {
namespace processor {

void Exists::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    auto dataChunkToWrite = resultSet->dataChunks[outDataPos.dataChunkPos].get();
    valueVectorToWrite = make_shared<ValueVector>(context.memoryManager, BOOL);
    dataChunkToWrite->insert(outDataPos.valueVectorPos, valueVectorToWrite);
    // side way information passing: give resultSet reference to subPlan
    auto subPlanResultCollector = (ResultCollector*)subPlan->lastOperator.get();
    auto op = subPlanResultCollector->getLeafOperator();
    assert(op->operatorType == SELECT_SCAN);
    ((ResultScan*)op)->setResultSetToCopyFrom(this->resultSet.get());
    subPlanResultCollector->init();
}

bool Exists::getNextTuples() {
    auto subPlanResultCollector = (ResultCollector*)subPlan->lastOperator.get();
    if (!prevOperator->getNextTuples()) {
        return false;
    }
    subPlanResultCollector->execute();
    assert(valueVectorToWrite->state->currIdx != -1);
    auto hasAtLeastOneTuple = subPlanResultCollector->queryResult->numTuples != 0;
    valueVectorToWrite->values[valueVectorToWrite->state->currIdx] = hasAtLeastOneTuple;
    return true;
}

unique_ptr<PhysicalOperator> Exists::clone() {
    auto subPlanClone = make_unique<PhysicalPlan>(subPlan->lastOperator->clone());
    return make_unique<Exists>(outDataPos, move(subPlanClone), prevOperator->clone(), context, id);
}

} // namespace processor
} // namespace graphflow

#include "src/processor/include/physical_plan/operator/exists.h"

#include "src/processor/include/physical_plan/operator/result_scan.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Exists::initResultSet() {
    resultSet = prevOperator->initResultSet();
    auto dataChunkToWrite = resultSet->dataChunks[outDataPos.dataChunkPos].get();
    valueVectorToWrite = make_shared<ValueVector>(context.memoryManager, BOOL);
    dataChunkToWrite->insert(outDataPos.valueVectorPos, valueVectorToWrite);
    // side way information passing: give resultSet reference to subPlan
    auto op = subPlanLastOperator->getLeafOperator();
    assert(op->operatorType == SELECT_SCAN);
    ((ResultScan*)op)->setResultSetToCopyFrom(resultSet.get());
    subPlanLastOperator->initResultSet();
    return resultSet;
}

void Exists::reInitToRerunSubPlan() {
    prevOperator->reInitToRerunSubPlan();
}

bool Exists::getNextTuples() {
    if (!prevOperator->getNextTuples()) {
        return false;
    }
    subPlanLastOperator->reInitToRerunSubPlan();
    assert(valueVectorToWrite->state->currIdx != -1);
    auto hasAtLeastOneTuple = subPlanLastOperator->getNextTuples();
    valueVectorToWrite->values[valueVectorToWrite->state->currIdx] = hasAtLeastOneTuple;
    return true;
}

unique_ptr<PhysicalOperator> Exists::clone() {
    return make_unique<Exists>(
        outDataPos, subPlanLastOperator->clone(), prevOperator->clone(), context, id);
}

} // namespace processor
} // namespace graphflow

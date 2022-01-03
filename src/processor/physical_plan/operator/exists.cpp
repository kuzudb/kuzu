#include "src/processor/include/physical_plan/operator/exists.h"

#include "src/processor/include/physical_plan/operator/result_scan.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Exists::initResultSet() {
    resultSet = children[0]->initResultSet();
    auto dataChunkToWrite = resultSet->dataChunks[outDataPos.dataChunkPos].get();
    valueVectorToWrite = make_shared<ValueVector>(context.memoryManager, BOOL);
    dataChunkToWrite->insert(outDataPos.valueVectorPos, valueVectorToWrite);
    // side way information passing: give resultSet reference to subPlan
    auto op = children[1]->getLeafOperator();
    assert(op->getOperatorType() == RESULT_SCAN);
    ((ResultScan*)op)->setResultSetToCopyFrom(resultSet.get());
    children[1]->initResultSet();
    return resultSet;
}

void Exists::reInitToRerunSubPlan() {
    children[0]->reInitToRerunSubPlan();
}

bool Exists::getNextTuples() {
    if (!children[0]->getNextTuples()) {
        return false;
    }
    children[1]->reInitToRerunSubPlan();
    assert(valueVectorToWrite->state->currIdx != -1);
    auto hasAtLeastOneTuple = children[1]->getNextTuples();
    valueVectorToWrite->values[valueVectorToWrite->state->currIdx] = hasAtLeastOneTuple;
    return true;
}

} // namespace processor
} // namespace graphflow

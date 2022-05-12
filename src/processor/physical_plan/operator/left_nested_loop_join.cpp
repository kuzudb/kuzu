#include "src/processor/include/physical_plan/operator/left_nested_loop_join.h"

#include "src/processor/include/physical_plan/operator/result_scan.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> LeftNestedLoopJoin::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    // side way information passing: give resultSet reference to subPlan.
    PhysicalOperator* op = children[1]->getLeafOperator();
    assert(op->getOperatorType() == RESULT_SCAN);
    ((ResultScan*)op)->setResultSetToCopyFrom(resultSet.get());
    auto subPlanResultSet = children[1]->init(context);
    // mering sub-plan result set into current result set.
    for (auto& posMapping : subPlanVectorsToRefPosMapping) {
        auto [dataChunkToRefPos, vectorToRefPos] = posMapping.first;
        auto dataChunkToRef = subPlanResultSet->dataChunks[dataChunkToRefPos];
        auto vectorToRef = dataChunkToRef->valueVectors[vectorToRefPos];
        vectorsToRef.push_back(vectorToRef);
        auto [outDataChunkPos, outVectorPos] = posMapping.second;
        auto outDataChunk = this->resultSet->dataChunks[outDataChunkPos];
        outDataChunk->referState(*dataChunkToRef);
        outDataChunk->insert(outVectorPos, vectorToRef);
    }
    return resultSet;
}

void LeftNestedLoopJoin::reInitToRerunSubPlan() {
    children[0]->reInitToRerunSubPlan();
    isFirstExecution = true;
}

bool LeftNestedLoopJoin::getNextTuples() {
    if (isFirstExecution) {
        isFirstExecution = false;
        return pullOnceFromLeftAndRight();
    }
    if (children[1]->getNextTuples()) { // More to pull from subPlan.
        return true;
    }
    // No more to pull from subPlan, so we pull once from outer plan and once from sub plan.
    return pullOnceFromLeftAndRight();
}

bool LeftNestedLoopJoin::pullOnceFromLeftAndRight() {
    if (!children[0]->getNextTuples()) {
        return false;
    }
    children[1]->reInitToRerunSubPlan();
    if (!children[1]->getNextTuples()) { // Nothing is pulled from sub plan.
        for (auto& vector : vectorsToRef) {
            vector->state->initOriginalAndSelectedSize(1);
            vector->setAllNull();
        }
    }
    return true;
}

} // namespace processor
} // namespace graphflow

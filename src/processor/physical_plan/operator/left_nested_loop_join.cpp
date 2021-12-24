#include "src/processor/include/physical_plan/operator/left_nested_loop_join.h"

#include "src/processor/include/physical_plan/operator/result_scan.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> LeftNestedLoopJoin::initResultSet() {
    resultSet = prevOperator->initResultSet();
    // side way information passing: give resultSet reference to subPlan.
    PhysicalOperator* op = subPlanLastOperator->getLeafOperator();
    assert(op->operatorType == SELECT_SCAN);
    ((ResultScan*)op)->setResultSetToCopyFrom(resultSet.get());
    auto subPlanResultSet = subPlanLastOperator->initResultSet();
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

bool LeftNestedLoopJoin::getNextTuples() {
    if (isFirstExecution) {
        isFirstExecution = false;
        return pullOnceFromLeftAndRight();
    }
    if (subPlanLastOperator->getNextTuples()) { // More to pull from subPlan.
        return true;
    }
    // No more to pull from subPlan, so we pull once from outer plan and once from sub plan.
    return pullOnceFromLeftAndRight();
}

bool LeftNestedLoopJoin::pullOnceFromLeftAndRight() {
    if (!prevOperator->getNextTuples()) {
        return false;
    }
    subPlanLastOperator->reInitialize();
    if (!subPlanLastOperator->getNextTuples()) { // Nothing is pulled from sub plan.
        for (auto& vector : vectorsToRef) {
            vector->state->initOriginalAndSelectedSize(1);
            vector->setAllNull();
        }
    }
    return true;
}

unique_ptr<PhysicalOperator> LeftNestedLoopJoin::clone() {
    return make_unique<LeftNestedLoopJoin>(subPlanVectorsToRefPosMapping,
        subPlanLastOperator->clone(), prevOperator->clone(), context, id);
}

} // namespace processor
} // namespace graphflow

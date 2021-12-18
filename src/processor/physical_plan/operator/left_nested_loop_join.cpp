#include "src/processor/include/physical_plan/operator/left_nested_loop_join.h"

#include "src/processor/include/physical_plan/operator/result_collector.h"
#include "src/processor/include/physical_plan/operator/result_scan.h"
#include "src/processor/include/physical_plan/physical_plan.h"

namespace graphflow {
namespace processor {

void LeftNestedLoopJoin::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    // side way information passing: give resultSet reference to subPlan.
    PhysicalOperator* op = subPlanLastOperator->getLeafOperator();
    assert(op->operatorType == SELECT_SCAN);
    ((ResultScan*)op)->setResultSetToCopyFrom(this->resultSet.get());
    subPlanLastOperator->initResultSet(subPlanResultSet);
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
    auto clonedSubPlanResultSet = make_shared<ResultSet>(subPlanResultSet->dataChunks.size());
    for (auto i = 0u; i < subPlanResultSet->dataChunks.size(); ++i) {
        clonedSubPlanResultSet->insert(
            i, make_shared<DataChunk>(subPlanResultSet->dataChunks[i]->valueVectors.size()));
    }
    return make_unique<LeftNestedLoopJoin>(subPlanVectorsToRefPosMapping,
        subPlanLastOperator->clone(), move(clonedSubPlanResultSet), prevOperator->clone(), context,
        id);
}

} // namespace processor
} // namespace graphflow

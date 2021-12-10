#include "src/processor/include/physical_plan/operator/left_nested_loop_join.h"

#include "src/processor/include/physical_plan/operator/result_collector.h"
#include "src/processor/include/physical_plan/operator/result_scan.h"
#include "src/processor/include/physical_plan/physical_plan.h"

namespace graphflow {
namespace processor {

void LeftNestedLoopJoin::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    // side way information passing: give resultSet reference to subPlan.
    auto subPlanResultCollector = (ResultCollector*)subPlan->lastOperator.get();
    auto op = subPlanResultCollector->getPipelineLeafOperator();
    assert(op->operatorType == SELECT_SCAN);
    ((ResultScan*)op)->setResultSetToCopyFrom(this->resultSet.get());
    subPlanResultCollector->init();
    // mering sub-plan result set into current result set.
    auto subPlanResultSet = subPlan->lastOperator->getResultSet();
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
    auto subPlanResultCollector = (ResultCollector*)subPlan->lastOperator.get();
    if (!prevOperator->getNextTuples()) {
        return false;
    }
    subPlanResultCollector->execute();
    auto hasAtLeastOneTuple = subPlanResultCollector->queryResult->numTuples != 0;
    if (!hasAtLeastOneTuple) {
        for (auto& vector : vectorsToRef) {
            vector->state->initOriginalAndSelectedSize(1);
            vector->setAllNull();
        }
    }
    return true;
}

unique_ptr<PhysicalOperator> LeftNestedLoopJoin::clone() {
    auto subPlanClone = make_unique<PhysicalPlan>(subPlan->lastOperator->clone());
    return make_unique<LeftNestedLoopJoin>(
        subPlanVectorsToRefPosMapping, move(subPlanClone), prevOperator->clone(), context, id);
}

} // namespace processor
} // namespace graphflow

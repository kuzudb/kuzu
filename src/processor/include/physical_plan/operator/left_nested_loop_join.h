#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

// Sub-plan last operator is the right child, i.e. children[1]
class LeftNestedLoopJoin : public PhysicalOperator {

public:
    LeftNestedLoopJoin(vector<pair<DataPos, DataPos>> subPlanVectorsToRefPosMapping,
        unique_ptr<PhysicalOperator> child, unique_ptr<PhysicalOperator> subPlanLastOperator,
        ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(child), move(subPlanLastOperator), context, id},
          subPlanVectorsToRefPosMapping{move(subPlanVectorsToRefPosMapping)}, isFirstExecution{
                                                                                  true} {}

    PhysicalOperatorType getOperatorType() override { return LEFT_NESTED_LOOP_JOIN; }

    shared_ptr<ResultSet> initResultSet() override;

    void reInitToRerunSubPlan() override;

    bool getNextTuples() override;

    bool pullOnceFromLeftAndRight();

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<LeftNestedLoopJoin>(
            subPlanVectorsToRefPosMapping, children[0]->clone(), children[1]->clone(), context, id);
    }

private:
    vector<pair<DataPos, DataPos>> subPlanVectorsToRefPosMapping;

    vector<shared_ptr<ValueVector>> vectorsToRef;
    bool isFirstExecution;
};

} // namespace processor
} // namespace graphflow

#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class PhysicalPlan;

class LeftNestedLoopJoin : public PhysicalOperator {

public:
    LeftNestedLoopJoin(vector<pair<DataPos, DataPos>> subPlanVectorsToRefPosMapping,
        unique_ptr<PhysicalPlan> subPlan, unique_ptr<PhysicalOperator> prevOperator,
        ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), LEFT_NESTED_LOOP_JOIN, context, id},
          subPlanVectorsToRefPosMapping{move(subPlanVectorsToRefPosMapping)}, subPlan{
                                                                                  move(subPlan)} {}

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    vector<pair<DataPos, DataPos>> subPlanVectorsToRefPosMapping;
    unique_ptr<PhysicalPlan> subPlan;

    vector<shared_ptr<ValueVector>> vectorsToRef;
};

} // namespace processor
} // namespace graphflow

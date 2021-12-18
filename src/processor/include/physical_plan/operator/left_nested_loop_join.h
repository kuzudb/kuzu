#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class LeftNestedLoopJoin : public PhysicalOperator {

public:
    LeftNestedLoopJoin(vector<pair<DataPos, DataPos>> subPlanVectorsToRefPosMapping,
        unique_ptr<PhysicalOperator> subPlanLastOperator, shared_ptr<ResultSet> subPlanResultSet,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), LEFT_NESTED_LOOP_JOIN, context, id},
          subPlanVectorsToRefPosMapping{move(subPlanVectorsToRefPosMapping)},
          subPlanLastOperator{move(subPlanLastOperator)}, subPlanResultSet{move(subPlanResultSet)},
          isFirstExecution{true} {}

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    bool getNextTuples() override;

    bool pullOnceFromLeftAndRight();

    unique_ptr<PhysicalOperator> clone() override;

private:
    vector<pair<DataPos, DataPos>> subPlanVectorsToRefPosMapping;
    // NOTE: subPlan last operator is not a sink.
    unique_ptr<PhysicalOperator> subPlanLastOperator;
    shared_ptr<ResultSet> subPlanResultSet;

    vector<shared_ptr<ValueVector>> vectorsToRef;
    bool isFirstExecution;
};

} // namespace processor
} // namespace graphflow

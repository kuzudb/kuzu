#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class PhysicalPlan;

class Exists : public PhysicalOperator {

public:
    Exists(const DataPos& outDataPos, unique_ptr<PhysicalPlan> subPlan,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), EXISTS, context, id},
          outDataPos{outDataPos}, subPlan{move(subPlan)} {}

    shared_ptr<ResultSet> initResultSet() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    DataPos outDataPos;
    unique_ptr<PhysicalPlan> subPlan;
    shared_ptr<ValueVector> valueVectorToWrite;
};

} // namespace processor
} // namespace graphflow

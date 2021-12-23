#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class Exists : public PhysicalOperator {

public:
    Exists(const DataPos& outDataPos, unique_ptr<PhysicalOperator> subPlanLastOperator,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), EXISTS, context, id}, outDataPos{outDataPos},
          subPlanLastOperator{move(subPlanLastOperator)} {}

    shared_ptr<ResultSet> initResultSet() override;

    void reInitToRerunSubPlan() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    DataPos outDataPos;
    unique_ptr<PhysicalOperator> subPlanLastOperator;
    shared_ptr<ValueVector> valueVectorToWrite;
};

} // namespace processor
} // namespace graphflow

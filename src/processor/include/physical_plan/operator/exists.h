#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

// Sub-plan last operator is the right child, i.e. children[1]
class Exists : public PhysicalOperator {

public:
    Exists(const DataPos& outDataPos, unique_ptr<PhysicalOperator> child,
        unique_ptr<PhysicalOperator> subPlanLastOperator, uint32_t id)
        : PhysicalOperator{move(child), move(subPlanLastOperator), id}, outDataPos{outDataPos} {}

    PhysicalOperatorType getOperatorType() override { return EXISTS; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    void reInitToRerunSubPlan() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Exists>(outDataPos, children[0]->clone(), children[1]->clone(), id);
    }

private:
    DataPos outDataPos;
    shared_ptr<ValueVector> valueVectorToWrite;
};

} // namespace processor
} // namespace graphflow

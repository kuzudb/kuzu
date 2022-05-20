#pragma once

#include "src/processor/include/physical_plan/operator/filtering_operator.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class Intersect : public PhysicalOperator, public FilteringOperator {

public:
    Intersect(const DataPos& leftDataPos, const DataPos& rightDataPos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, FilteringOperator{},
          leftDataPos{leftDataPos}, rightDataPos{rightDataPos}, leftIdx{0} {}

    PhysicalOperatorType getOperatorType() override { return INTERSECT; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    void reInitToRerunSubPlan() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Intersect>(
            leftDataPos, rightDataPos, children[0]->clone(), id, paramsString);
    }

private:
    DataPos leftDataPos;
    DataPos rightDataPos;

    shared_ptr<DataChunk> leftDataChunk;
    shared_ptr<ValueVector> leftValueVector;
    shared_ptr<DataChunk> rightDataChunk;
    shared_ptr<ValueVector> rightValueVector;

    uint64_t leftIdx;
};

} // namespace processor
} // namespace graphflow

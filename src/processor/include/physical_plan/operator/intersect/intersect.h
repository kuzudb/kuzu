#pragma once

#include "src/processor/include/physical_plan/operator/filtering_operator.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class Intersect : public PhysicalOperator, public FilteringOperator {

public:
    Intersect(const DataPos& leftDataPos, const DataPos& rightDataPos,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), INTERSECT, context, id}, FilteringOperator{},
          leftDataPos{leftDataPos}, rightDataPos{rightDataPos}, leftIdx{0} {}

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Intersect>(
            leftDataPos, rightDataPos, prevOperator->clone(), context, id);
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

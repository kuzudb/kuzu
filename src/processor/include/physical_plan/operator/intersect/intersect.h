#pragma once

#include "src/processor/include/physical_plan/operator/filtering_operator.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class Intersect : public PhysicalOperator, public FilteringOperator {

public:
    Intersect(uint64_t leftDataChunkPos, uint64_t leftValueVectorPos, uint64_t rightDataChunkPos,
        uint64_t rightValueVectorPos, unique_ptr<PhysicalOperator> prevOperator,
        ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Intersect>(leftDataChunkPos, leftValueVectorPos, rightDataChunkPos,
            rightValueVectorPos, prevOperator->clone(), context, id);
    }

private:
    uint64_t leftDataChunkPos;
    uint64_t leftValueVectorPos;
    uint64_t rightDataChunkPos;
    uint64_t rightValueVectorPos;

    shared_ptr<DataChunk> leftDataChunk;
    shared_ptr<NodeIDVector> leftNodeIDVector;
    shared_ptr<DataChunk> rightDataChunk;
    shared_ptr<NodeIDVector> rightNodeIDVector;

    uint64_t leftIdx;
};

} // namespace processor
} // namespace graphflow

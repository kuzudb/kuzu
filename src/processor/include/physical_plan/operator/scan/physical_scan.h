#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class PhysicalScan : public PhysicalOperator {

public:
    PhysicalScan(shared_ptr<MorselDesc>& morsel);

    void getNextTuples() override;

    bool hasNextMorsel() override;

    shared_ptr<NodeIDSequenceVector>& getNodeVector() { return nodeIDVector; }

    unique_ptr<PhysicalOperator> clone() override { return make_unique<PhysicalScan>(morsel); }

protected:
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<NodeIDSequenceVector> nodeIDVector;
    node_offset_t currentMorselStartOffset{-1u};
    uint32_t currentMorselSize{-1u};
    shared_ptr<MorselDesc> morsel;
};

} // namespace processor
} // namespace graphflow

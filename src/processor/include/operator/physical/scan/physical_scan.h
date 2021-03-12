#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/physical/operator.h"

namespace graphflow {
namespace processor {

class PhysicalScan : public Operator {

public:
    PhysicalScan(shared_ptr<MorselDesc>& morsel);

    void getNextTuples() override;

    bool hasNextMorsel() override;

    shared_ptr<NodeIDSequenceVector>& getNodeVector() { return nodeIDVector; }

    unique_ptr<Operator> clone() override { return make_unique<PhysicalScan>(morsel); }

protected:
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<NodeIDSequenceVector> nodeIDVector;
    node_offset_t currentMorselStartOffset{-1u};
    uint32_t currentMorselSize{-1u};
    shared_ptr<MorselDesc> morsel;
};

} // namespace processor
} // namespace graphflow

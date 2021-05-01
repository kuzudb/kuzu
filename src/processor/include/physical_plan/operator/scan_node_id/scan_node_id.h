#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
class ScanNodeID : public PhysicalOperator {

public:
    ScanNodeID(shared_ptr<MorselsDesc>& morselDesc);

    void getNextTuples() override;

    shared_ptr<NodeIDVector>& getNodeVector() { return nodeIDVector; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanNodeID<IS_OUT_DATACHUNK_FILTERED>>(morsel);
    }

protected:
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<NodeIDVector> nodeIDVector;
    shared_ptr<MorselsDesc> morsel;
};

} // namespace processor
} // namespace graphflow

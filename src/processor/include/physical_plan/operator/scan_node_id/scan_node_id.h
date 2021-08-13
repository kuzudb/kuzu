#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/task_system/morsel.h"

namespace graphflow {
namespace processor {

class ScanNodeID : public PhysicalOperator {

public:
    explicit ScanNodeID(
        shared_ptr<MorselsDesc>& morselDesc, ExecutionContext& context, uint32_t id);

    ScanNodeID(shared_ptr<MorselsDesc>& morselsDesc, unique_ptr<PhysicalOperator> prevOperator,
        ExecutionContext& context, uint32_t id);

    void reInitialize() override;

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return prevOperator ? make_unique<ScanNodeID>(morsel, prevOperator->clone(), context, id) :
                              make_unique<ScanNodeID>(morsel, context, id);
    }

protected:
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<NodeIDVector> nodeIDVector;
    shared_ptr<MorselsDesc> morsel;
};

} // namespace processor
} // namespace graphflow

#pragma once

#include "src/processor/include/physical_plan/operator/morsel.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"

namespace graphflow {
namespace processor {

class ScanNodeID : public PhysicalOperator, public SourceOperator {

public:
    ScanNodeID(unique_ptr<ResultSetDescriptor> resultSetDescriptor, label_t nodeLabel,
        const DataPos& outDataPos, shared_ptr<MorselsDesc> morsel, ExecutionContext& context,
        uint32_t id)
        : PhysicalOperator{SCAN, context, id}, SourceOperator{move(resultSetDescriptor)},
          nodeLabel{nodeLabel}, outDataPos{outDataPos}, morsel{move(morsel)} {}

    shared_ptr<ResultSet> initResultSet() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanNodeID>(
            resultSetDescriptor->copy(), nodeLabel, outDataPos, morsel, context, id);
    }

private:
    label_t nodeLabel;
    DataPos outDataPos;
    shared_ptr<MorselsDesc> morsel;

    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow

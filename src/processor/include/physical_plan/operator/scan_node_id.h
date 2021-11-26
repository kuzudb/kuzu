#pragma once

#include "src/processor/include/physical_plan/operator/morsel.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class ScanNodeID : public PhysicalOperator {

public:
    ScanNodeID(label_t nodeLabel, const DataPos& outDataPos, shared_ptr<MorselsDesc> morsel,
        ExecutionContext& context, uint32_t id)
        : PhysicalOperator{SCAN, context, id}, nodeLabel{nodeLabel},
          outDataPos{outDataPos}, morsel{std::move(morsel)} {}

    ScanNodeID(label_t nodeLabel, const DataPos& outDataPos, shared_ptr<MorselsDesc> morsel,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), SCAN, context, id}, nodeLabel{nodeLabel},
          outDataPos{outDataPos}, morsel{std::move(morsel)} {}

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return prevOperator ? make_unique<ScanNodeID>(nodeLabel, outDataPos, morsel,
                                  prevOperator->clone(), context, id) :
                              make_unique<ScanNodeID>(nodeLabel, outDataPos, morsel, context, id);
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

#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ScanAttribute : public PhysicalOperator {

public:
    ScanAttribute(const DataPos& inDataPos, const DataPos& outDataPos,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), SCAN_ATTRIBUTE, context, id}, inDataPos{inDataPos},
          outDataPos{outDataPos} {}

    shared_ptr<ResultSet> initResultSet() override;

    void printMetricsToJson(nlohmann::json& json, Profiler& profiler) override;

protected:
    DataPos inDataPos;
    DataPos outDataPos;

    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<ValueVector> inValueVector;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow

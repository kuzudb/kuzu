#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ScanAttribute : public PhysicalOperator {

public:
    ScanAttribute(uint32_t inAndOutDataChunkPos, uint32_t inValueVectorPos,
        uint32_t outValueVectorPos, unique_ptr<PhysicalOperator> prevOperator,
        ExecutionContext& context, uint32_t id);

    void printMetricsToJson(nlohmann::json& json, Profiler& profiler) override;

protected:
    uint32_t inAndOutDataChunkPos;
    uint32_t inValueVectorPos;
    uint32_t outValueVectorPos;

    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<ValueVector> inValueVector;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow

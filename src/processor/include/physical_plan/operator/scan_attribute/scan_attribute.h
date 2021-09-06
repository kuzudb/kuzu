#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ScanAttribute : public PhysicalOperator {

public:
    ScanAttribute(uint64_t dataChunkPos, uint64_t valueVectorPos,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);

    void printMetricsToJson(nlohmann::json& json, Profiler& profiler) override;

protected:
    uint64_t dataChunkPos;
    uint64_t valueVectorPos;
    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<ValueVector> inValueVector;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow

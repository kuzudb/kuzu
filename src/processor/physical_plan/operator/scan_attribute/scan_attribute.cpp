#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"

namespace graphflow {
namespace processor {

ScanAttribute::ScanAttribute(uint64_t dataChunkPos, uint64_t valueVectorPos,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), SCAN_ATTRIBUTE, context, id}, dataChunkPos{dataChunkPos},
      valueVectorPos{valueVectorPos} {
    resultSet = this->prevOperator->getResultSet();
    inDataChunk = resultSet->dataChunks[dataChunkPos];
    inValueVector = inDataChunk->getValueVector(valueVectorPos);
    assert(inValueVector->dataType == NODE);
}

void ScanAttribute::printMetricsToJson(nlohmann::json& json, Profiler& profiler) {
    PhysicalOperator::printMetricsToJson(json, profiler);
    printBufferManagerMetrics(json, profiler);
}

} // namespace processor
} // namespace graphflow

#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"

namespace graphflow {
namespace processor {

ScanAttribute::ScanAttribute(uint32_t inAndOutDataChunkPos, uint32_t inValueVectorPos,
    uint32_t outValueVectorPos, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), SCAN_ATTRIBUTE, context, id},
      inAndOutDataChunkPos{inAndOutDataChunkPos}, inValueVectorPos{inValueVectorPos},
      outValueVectorPos{outValueVectorPos} {
    resultSet = this->prevOperator->getResultSet();
    inDataChunk = resultSet->dataChunks[inAndOutDataChunkPos];
    inValueVector = inDataChunk->getValueVector(inValueVectorPos);
    assert(inValueVector->dataType == NODE);
}

void ScanAttribute::printMetricsToJson(nlohmann::json& json, Profiler& profiler) {
    PhysicalOperator::printMetricsToJson(json, profiler);
    printBufferManagerMetrics(json, profiler);
}

} // namespace processor
} // namespace graphflow

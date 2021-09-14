#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"

namespace graphflow {
namespace processor {

void ScanAttribute::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    assert(inDataPos.dataChunkPos == outDataPos.dataChunkPos);
    inDataChunk = this->resultSet->dataChunks[inDataPos.dataChunkPos];
    inValueVector = inDataChunk->valueVectors[inDataPos.valueVectorPos];
}

void ScanAttribute::printMetricsToJson(nlohmann::json& json, Profiler& profiler) {
    PhysicalOperator::printMetricsToJson(json, profiler);
    printBufferManagerMetrics(json, profiler);
}

} // namespace processor
} // namespace graphflow

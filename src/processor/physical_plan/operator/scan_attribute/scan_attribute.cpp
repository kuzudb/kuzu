#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ScanAttribute::initResultSet() {
    resultSet = children[0]->initResultSet();
    assert(inDataPos.dataChunkPos == outDataPos.dataChunkPos);
    inDataChunk = resultSet->dataChunks[inDataPos.dataChunkPos];
    inValueVector = inDataChunk->valueVectors[inDataPos.valueVectorPos];
    return resultSet;
}

void ScanAttribute::reInitToRerunSubPlan() {
    children[0]->reInitToRerunSubPlan();
}

void ScanAttribute::printMetricsToJson(nlohmann::json& json, Profiler& profiler) {
    PhysicalOperator::printMetricsToJson(json, profiler);
    printBufferManagerMetrics(json, profiler);
}

} // namespace processor
} // namespace graphflow

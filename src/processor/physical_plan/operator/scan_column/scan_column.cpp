#include "src/processor/include/physical_plan/operator/scan_column/scan_column.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> BaseScanColumn::initResultSet() {
    resultSet = children[0]->initResultSet();
    inputNodeIDDataChunk = resultSet->dataChunks[inputNodeIDVectorPos.dataChunkPos];
    inputNodeIDVector = inputNodeIDDataChunk->valueVectors[inputNodeIDVectorPos.valueVectorPos];
    return resultSet;
}

void BaseScanColumn::reInitToRerunSubPlan() {
    children[0]->reInitToRerunSubPlan();
}

void BaseScanColumn::printMetricsToJson(nlohmann::json& json, Profiler& profiler) {
    PhysicalOperator::printMetricsToJson(json, profiler);
}

} // namespace processor
} // namespace graphflow

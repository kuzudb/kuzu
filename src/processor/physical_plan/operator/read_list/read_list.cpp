#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ReadList::initResultSet() {
    resultSet = children[0]->initResultSet();
    inDataChunk = resultSet->dataChunks[inDataPos.dataChunkPos];
    inValueVector = inDataChunk->valueVectors[inDataPos.valueVectorPos];
    outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
    return resultSet;
}

void ReadList::reInitToRerunSubPlan() {
    children[0]->reInitToRerunSubPlan();
}

void ReadList::printMetricsToJson(nlohmann::json& json, Profiler& profiler) {
    PhysicalOperator::printTimeAndNumOutputMetrics(json, profiler);
    printBufferManagerMetrics(json, profiler);
}

void ReadList::readValuesFromList() {
    auto currentIdx = inDataChunk->state->getPositionOfCurrIdx();
    if (inValueVector->isNull(currentIdx)) {
        outValueVector->state->setSelectedSize(0);
        return;
    }
    auto nodeOffset = inValueVector->readNodeOffset(inDataChunk->state->getPositionOfCurrIdx());
    lists->readValues(nodeOffset, outValueVector, largeListHandle, *metrics->bufferManagerMetrics);
}

} // namespace processor
} // namespace graphflow

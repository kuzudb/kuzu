#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

void ReadList::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    inDataChunk = this->resultSet->dataChunks[inDataPos.dataChunkPos];
    inValueVector = inDataChunk->valueVectors[inDataPos.valueVectorPos];
    outDataChunk = this->resultSet->dataChunks[outDataPos.dataChunkPos];
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
    outValueVector->setAllNonNull();
}

} // namespace processor
} // namespace graphflow

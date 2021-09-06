#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

ReadList::ReadList(const uint64_t& dataChunkPos, const uint64_t& valueVectorPos, Lists* lists,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id,
    bool isAdjList)
    : PhysicalOperator{move(prevOperator), READ_LIST, context, id}, inDataChunkPos{dataChunkPos},
      inValueVectorPos{valueVectorPos}, lists{lists} {
    resultSet = this->prevOperator->getResultSet();
    inDataChunk = resultSet->dataChunks[dataChunkPos];
    inValueVector = inDataChunk->getValueVector(valueVectorPos);
    assert(inValueVector->dataType == NODE);
    largeListHandle = make_unique<LargeListHandle>(isAdjList);
}

void ReadList::printMetricsToJson(nlohmann::json& json, Profiler& profiler) {
    PhysicalOperator::printTimeAndNumOutputMetrics(json, profiler);
    printBufferManagerMetrics(json, profiler);
}

void ReadList::readValuesFromList() {
    auto nodeOffset = inValueVector->readNodeOffset(inDataChunk->state->getPositionOfCurrIdx());
    lists->readValues(nodeOffset, outValueVector, largeListHandle, *metrics->bufferManagerMetrics);
}

} // namespace processor
} // namespace graphflow

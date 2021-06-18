#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

ReadList::ReadList(const uint64_t& dataChunkPos, const uint64_t& valueVectorPos, BaseLists* lists,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), READ_LIST, context, id}, inDataChunkPos{dataChunkPos},
      inValueVectorPos{valueVectorPos}, lists{lists} {
    resultSet = this->prevOperator->getResultSet();
    inDataChunk = resultSet->dataChunks[dataChunkPos];
    inNodeIDVector = static_pointer_cast<NodeIDVector>(inDataChunk->getValueVector(valueVectorPos));
    handle = make_unique<DataStructureHandle>();
}

void ReadList::printMetricsToJson(nlohmann::json& json, Profiler& profiler) {
    PhysicalOperator::printTimeAndNumOutputMetrics(json, profiler);
    printBufferManagerMetrics(json, profiler);
}

void ReadList::readValuesFromList() {
    lists->reclaim(handle);
    auto nodeOffset =
        inNodeIDVector->readNodeOffset(inDataChunk->state->getCurrSelectedValuesPos());
    lists->readValues(nodeOffset, outValueVector, outDataChunk->state->size, handle, MAX_TO_READ,
        *metrics->bufferManagerMetrics);
}

} // namespace processor
} // namespace graphflow

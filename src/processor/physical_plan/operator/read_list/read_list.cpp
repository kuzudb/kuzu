#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

ReadList::ReadList(const uint64_t& dataChunkPos, const uint64_t& valueVectorPos, BaseLists* lists,
    unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator{move(prevOperator), READ_LIST}, inDataChunkPos{dataChunkPos},
      inValueVectorPos{valueVectorPos}, lists{lists} {
    dataChunks = this->prevOperator->getDataChunks();
    inDataChunk = dataChunks->getDataChunk(dataChunkPos);
    inNodeIDVector = static_pointer_cast<NodeIDVector>(inDataChunk->getValueVector(valueVectorPos));
    handle = make_unique<DataStructureHandle>();
}

void ReadList::readValuesFromList() {
    lists->reclaim(handle);
    nodeID_t nodeID;
    inNodeIDVector->readNodeOffset(inDataChunk->state->getCurrSelectedValuesPos(), nodeID);
    lists->readValues(nodeID, outValueVector, outDataChunk->state->size, handle, MAX_TO_READ);
}

} // namespace processor
} // namespace graphflow

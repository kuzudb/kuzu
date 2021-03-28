#include "src/processor/include/physical_plan/operator/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

ListReader::ListReader(const uint64_t& dataChunkPos, const uint64_t& valueVectorPos,
    BaseLists* lists, unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator{move(prevOperator)}, inDataChunkPos{dataChunkPos},
      inValueVectorPos{valueVectorPos}, lists{lists} {
    dataChunks = this->prevOperator->getDataChunks();
    inDataChunk = dataChunks->getDataChunk(dataChunkPos);
    inNodeIDVector = static_pointer_cast<NodeIDVector>(inDataChunk->getValueVector(valueVectorPos));
    handle = make_unique<ColumnOrListsHandle>();
}

void ListReader::readValuesFromList() {
    lists->reclaim(handle);
    nodeID_t nodeID;
    inNodeIDVector->readNodeOffset(inDataChunk->getCurrSelectedValuesPos(), nodeID);
    lists->readValues(nodeID, outValueVector, outDataChunk->size, handle, MAX_TO_READ);
}

} // namespace processor
} // namespace graphflow

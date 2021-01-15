#include "src/processor/include/operator/physical/list_reader/rel_property_list_reader.h"

namespace graphflow {
namespace processor {

RelPropertyListReader::RelPropertyListReader(const uint64_t& inDataChunkPos,
    const uint64_t& inValueVectorPos, const uint64_t& outDataChunkPos, BaseLists* lists,
    unique_ptr<Operator> prevOperator)
    : ListReader{inDataChunkPos, inValueVectorPos, lists, move(prevOperator)},
      outDataChunkPos{outDataChunkPos} {
    outValueVector = make_shared<ValueVector>(lists->getElementSize());
    outDataChunk = dataChunks->getDataChunk(outDataChunkPos);
    outDataChunk->append(outValueVector);
}

void RelPropertyListReader::getNextTuples() {
    prevOperator->getNextTuples();
    if (inDataChunk->size > 0) {
        lists->reclaim(handle);
        nodeID_t nodeID;
        inNodeIDVector->readValue(inDataChunk->curr_idx, nodeID);
        lists->readValues(nodeID, outValueVector, outDataChunk->size, handle);
    }
}

} // namespace processor
} // namespace graphflow

#include "src/processor/include/operator/physical/list_reader/rel_property_list_reader.h"

namespace graphflow {
namespace processor {

RelPropertyListReader::RelPropertyListReader(const uint64_t& inDataChunkIdx,
    const uint64_t& inValueVectorIdx, BaseLists* lists, unique_ptr<Operator> prevOperator)
    : ListReader{inDataChunkIdx, inValueVectorIdx, lists, move(prevOperator)} {
    outValueVector = make_shared<ValueVector>(8 /* placeholder */);
    inDataChunk->append(outValueVector);
}

void RelPropertyListReader::getNextTuples() {
    lists->reclaim(handle);
    prevOperator->getNextTuples();
    if (inDataChunk->size > 0) {
        nodeID_t nodeID;
        inNodeIDVector->readValue(inDataChunk->curr_idx, nodeID);
        lists->readValues(nodeID, outValueVector, inDataChunk->size, handle);
    }
}

} // namespace processor
} // namespace graphflow

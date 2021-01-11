#include "src/processor/include/operator/physical/list_reader/adj_list_extend.h"

namespace graphflow {
namespace processor {

AdjListExtend::AdjListExtend(const uint64_t& inDataChunkIdx, const uint64_t& inValueVectorIdx,
    BaseLists* lists, unique_ptr<Operator> prevOperator)
    : ListReader{inDataChunkIdx, inValueVectorIdx, lists, move(prevOperator)} {
    outNodeIDVector = make_shared<NodeIDVector>(NodeIDCompressionScheme());
    outDataChunk = make_shared<DataChunk>();
    outDataChunk->append(outNodeIDVector);
    dataChunks->append(outDataChunk);
}

bool AdjListExtend::hasNextMorsel() {
    return (inDataChunk->size > 0 && inDataChunk->size > inDataChunk->curr_idx + 1) ||
           prevOperator->hasNextMorsel();
}

void AdjListExtend::getNextTuples() {
    lists->reclaim(handle);
    if (inDataChunk->size == 0 || inDataChunk->size == inDataChunk->curr_idx) {
        inDataChunk->curr_idx = 0;
        prevOperator->getNextTuples();
    } else {
        inDataChunk->curr_idx++;
    }
    if (inDataChunk->size > 0) {
        nodeID_t nodeID;
        inNodeIDVector->readValue(inDataChunk->curr_idx, nodeID);
        lists->readValues(nodeID, outNodeIDVector, outDataChunk->size, handle);
    } else {
        outDataChunk->size = 0;
    }
}

} // namespace processor
} // namespace graphflow

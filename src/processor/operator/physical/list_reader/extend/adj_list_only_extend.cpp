#include "src/processor/include/operator/physical/list_reader/extend/adj_list_only_extend.h"

namespace graphflow {
namespace processor {

void AdjListOnlyExtend::getNextTuples() {
    lists->reclaim(handle);
    prevOperator->getNextTuples();
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

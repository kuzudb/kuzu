#include "src/processor/include/operator/physical/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

ListReader::ListReader(const uint64_t& inDataChunkIdx, const uint64_t& inValueVectorIdx,
    BaseLists* lists, unique_ptr<Operator> prevOperator)
    : Operator{move(prevOperator)}, inDataChunkIdx{inDataChunkIdx},
      inValueVectorIdx{inValueVectorIdx}, lists{lists} {
    dataChunks = this->prevOperator->getOutDataChunks();
    inDataChunk = dataChunks->getDataChunk(inDataChunkIdx);
    inNodeIDVector = static_pointer_cast<NodeIDVector>(
        dataChunks->getValueVector(inDataChunkIdx, inValueVectorIdx));
    handle = make_unique<VectorFrameHandle>();
}

void ListReader::cleanup() {
    lists->reclaim(handle);
    prevOperator->cleanup();
}

} // namespace processor
} // namespace graphflow

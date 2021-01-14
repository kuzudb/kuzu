#include "src/processor/include/operator/physical/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

ListReader::ListReader(const uint64_t& dataChunkPos, const uint64_t& valueVectorPos,
    BaseLists* lists, unique_ptr<Operator> prevOperator)
    : Operator{move(prevOperator)}, dataChunkPos{dataChunkPos},
      valueVectorPos{valueVectorPos}, lists{lists} {
    dataChunks = this->prevOperator->getDataChunks();
    inDataChunk = dataChunks->getDataChunk(dataChunkPos);
    inNodeIDVector =
        static_pointer_cast<NodeIDVector>(dataChunks->getValueVector(dataChunkPos, valueVectorPos));
    handle = make_unique<VectorFrameHandle>();
}

void ListReader::cleanup() {
    lists->reclaim(handle);
    prevOperator->cleanup();
}

} // namespace processor
} // namespace graphflow

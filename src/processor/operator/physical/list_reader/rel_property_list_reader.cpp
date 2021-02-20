#include "src/processor/include/operator/physical/list_reader/rel_property_list_reader.h"

namespace graphflow {
namespace processor {

RelPropertyListReader::RelPropertyListReader(const uint64_t& inDataChunkPos,
    const uint64_t& inValueVectorPos, const uint64_t& outDataChunkPos, BaseLists* lists,
    shared_ptr<ListSyncer> listSyncer, unique_ptr<Operator> prevOperator)
    : ListReader{inDataChunkPos, inValueVectorPos, lists, listSyncer, move(prevOperator)},
      outDataChunkPos{outDataChunkPos} {
    outValueVector = make_shared<ValueVector>(lists->getElementSize());
    outDataChunk = dataChunks->getDataChunk(outDataChunkPos);
    outDataChunk->append(outValueVector);
}

void RelPropertyListReader::getNextTuples() {
    prevOperator->getNextTuples();
    if (handle->hasMoreToRead() || inDataChunk->size > 0) {
        readValuesFromList();
    }
}

} // namespace processor
} // namespace graphflow

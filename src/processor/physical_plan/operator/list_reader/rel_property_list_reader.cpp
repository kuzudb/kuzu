#include "src/processor/include/physical_plan/operator/list_reader/rel_property_list_reader.h"

namespace graphflow {
namespace processor {

RelPropertyListReader::RelPropertyListReader(uint64_t inDataChunkPos, uint64_t inValueVectorPos,
    uint64_t outDataChunkPos, BaseLists* lists, unique_ptr<PhysicalOperator> prevOperator)
    : ListReader{inDataChunkPos, inValueVectorPos, lists, move(prevOperator)},
      outDataChunkPos{outDataChunkPos} {
    outValueVector = make_shared<ValueVector>(lists->getDataType());
    outDataChunk = dataChunks->getDataChunk(outDataChunkPos);
    handle->setListSyncState(dataChunks->getListSyncState(outDataChunkPos));
    outDataChunk->append(outValueVector);
}

void RelPropertyListReader::getNextTuples() {
    prevOperator->getNextTuples();
    if (inDataChunk->numSelectedValues > 0) {
        readValuesFromList();
    }
}

} // namespace processor
} // namespace graphflow

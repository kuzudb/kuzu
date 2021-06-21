#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"

namespace graphflow {
namespace processor {

ReadRelPropertyList::ReadRelPropertyList(uint64_t inDataChunkPos, uint64_t inValueVectorPos,
    uint64_t outDataChunkPos, BaseLists* lists, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id)
    : ReadList{inDataChunkPos, inValueVectorPos, lists, move(prevOperator), context, id},
      outDataChunkPos(outDataChunkPos) {
    outValueVector = make_shared<ValueVector>(context.memoryManager, lists->getDataType());
    outDataChunk = resultSet->dataChunks[outDataChunkPos];
    handle->setListSyncState(resultSet->getListSyncState(outDataChunkPos));
    outDataChunk->append(outValueVector);
}

void ReadRelPropertyList::getNextTuples() {
    metrics->executionTime.start();
    prevOperator->getNextTuples();
    if (inDataChunk->state->size > 0) {
        readValuesFromList();
    }
    outValueVector->fillNullMask();
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow

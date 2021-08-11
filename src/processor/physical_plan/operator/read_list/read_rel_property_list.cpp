#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"

#include "src/common/include/date.h"

namespace graphflow {
namespace processor {

ReadRelPropertyList::ReadRelPropertyList(uint64_t inDataChunkPos, uint64_t inValueVectorPos,
    uint64_t outDataChunkPos, Lists* lists, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id)
    : ReadList{inDataChunkPos, inValueVectorPos, lists, move(prevOperator), context, id,
          false /* is not adj list */},
      outDataChunkPos(outDataChunkPos) {
    outValueVector = make_shared<ValueVector>(context.memoryManager, lists->getDataType());
    outDataChunk = resultSet->dataChunks[outDataChunkPos];
    largeListHandle->setListSyncState(resultSet->getListSyncState(outDataChunkPos));
    outDataChunk->append(outValueVector);
}

void ReadRelPropertyList::getNextTuples() {
    metrics->executionTime.start();
    prevOperator->getNextTuples();
    if (inDataChunk->state->selectedSize > 0) {
        readValuesFromList();
    }
    outValueVector->fillNullMask();
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow

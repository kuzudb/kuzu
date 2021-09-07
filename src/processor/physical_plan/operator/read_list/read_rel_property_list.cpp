#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"

#include "src/common/include/date.h"

namespace graphflow {
namespace processor {

ReadRelPropertyList::ReadRelPropertyList(uint32_t inDataChunkPos, uint32_t inValueVectorPos,
    uint32_t outDataChunkPos, uint32_t outValueVectorPos, Lists* lists,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : ReadList{inDataChunkPos, inValueVectorPos, outDataChunkPos, outValueVectorPos, lists,
          move(prevOperator), context, id, false /* is not adj list */} {
    outValueVector = make_shared<ValueVector>(context.memoryManager, lists->getDataType());
    outDataChunk = resultSet->dataChunks[outDataChunkPos];
    largeListHandle->setListSyncState(resultSet->getListSyncState(outDataChunkPos));
    outDataChunk->insert(outValueVectorPos, outValueVector);
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

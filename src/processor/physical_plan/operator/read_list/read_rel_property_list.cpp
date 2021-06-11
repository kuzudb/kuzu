#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"

#include "src/common/include/vector/operations/vector_comparison_operations.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

ReadRelPropertyList::ReadRelPropertyList(uint64_t inDataChunkPos, uint64_t inValueVectorPos,
    uint64_t outDataChunkPos, BaseLists* lists, unique_ptr<PhysicalOperator> prevOperator)
    : ReadList{inDataChunkPos, inValueVectorPos, lists, move(prevOperator)},
      outDataChunkPos(outDataChunkPos) {
    outValueVector = make_shared<ValueVector>(lists->getDataType());
    outDataChunk = resultSet->dataChunks[outDataChunkPos];
    handle->setListSyncState(resultSet->getListSyncState(outDataChunkPos));
    outDataChunk->append(outValueVector);
}

void ReadRelPropertyList::getNextTuples() {
    prevOperator->getNextTuples();
    if (inDataChunk->state->size > 0) {
        readValuesFromList();
    }
    outValueVector->fillNullMask();
}

} // namespace processor
} // namespace graphflow

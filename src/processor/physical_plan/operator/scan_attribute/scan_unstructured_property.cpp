#include "src/processor/include/physical_plan/operator/scan_attribute/scan_unstructured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

ScanUnstructuredProperty::ScanUnstructuredProperty(uint64_t dataChunkPos, uint64_t valueVectorPos,
    uint32_t propertyKey, BaseLists* lists, unique_ptr<PhysicalOperator> prevOperator)
    : ScanAttribute{dataChunkPos, valueVectorPos, move(prevOperator)},
      propertyKey{propertyKey}, lists{(Lists<UNSTRUCTURED>*)lists} {
    dataChunks = this->prevOperator->getDataChunks();
    inDataChunk = dataChunks->getDataChunk(dataChunkPos);
    inNodeIDVector = static_pointer_cast<NodeIDVector>(inDataChunk->getValueVector(valueVectorPos));
    handle = make_unique<ColumnOrListsHandle>();
    outValueVector = make_shared<ValueVector>(lists->getDataType());
    inDataChunk->appendAndSetVectorState(outValueVector);
}

void ScanUnstructuredProperty::getNextTuples() {
    do {
        prevOperator->getNextTuples();
    } while (inDataChunk->state->size > 0 && inDataChunk->state->numSelectedValues == 0);
    if (inDataChunk->state->size > 0) {
        lists->reclaim(handle);
        lists->readValues(inNodeIDVector, propertyKey, outValueVector, handle);
    }
}

} // namespace processor
} // namespace graphflow

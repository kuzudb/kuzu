#include "src/processor/include/physical_plan/operator/scan_attribute/scan_unstructured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

ScanUnstructuredProperty::ScanUnstructuredProperty(uint64_t dataChunkPos, uint64_t valueVectorPos,
    uint32_t propertyKey, BaseLists* lists, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id)
    : ScanAttribute{dataChunkPos, valueVectorPos, move(prevOperator), context, id},
      propertyKey{propertyKey}, lists{(Lists<UNSTRUCTURED>*)lists} {
    resultSet = this->prevOperator->getResultSet();
    inDataChunk = resultSet->dataChunks[dataChunkPos];
    inNodeIDVector = static_pointer_cast<NodeIDVector>(inDataChunk->getValueVector(valueVectorPos));
    handle = make_unique<DataStructureHandle>();
    outValueVector = make_shared<ValueVector>(lists->getDataType());
    inDataChunk->append(outValueVector);
}

void ScanUnstructuredProperty::getNextTuples() {
    executionTime->start();
    prevOperator->getNextTuples();
    if (inDataChunk->state->size > 0) {
        lists->reclaim(handle);
        lists->readValues(inNodeIDVector, propertyKey, outValueVector, handle);
    }
    executionTime->stop();
}

} // namespace processor
} // namespace graphflow

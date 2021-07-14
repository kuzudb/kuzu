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
    pageHandle = make_unique<PageHandle>();
    outValueVector = make_shared<ValueVector>(context.memoryManager, lists->getDataType());
    inDataChunk->append(outValueVector);
}

void ScanUnstructuredProperty::getNextTuples() {
    metrics->executionTime.start();
    prevOperator->getNextTuples();
    if (inDataChunk->state->size > 0) {
        lists->reclaim(*pageHandle);
        lists->readValues(inNodeIDVector, propertyKey, outValueVector, pageHandle,
            *metrics->bufferManagerMetrics);
    }
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow

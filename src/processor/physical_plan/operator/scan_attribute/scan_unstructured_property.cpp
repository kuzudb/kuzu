#include "src/processor/include/physical_plan/operator/scan_attribute/scan_unstructured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

ScanUnstructuredProperty::ScanUnstructuredProperty(uint64_t dataChunkPos, uint64_t valueVectorPos,
    uint32_t propertyKey, UnstructuredPropertyLists* lists,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : ScanAttribute{dataChunkPos, valueVectorPos, move(prevOperator), context, id},
      propertyKey{propertyKey}, lists{lists} {
    resultSet = this->prevOperator->getResultSet();
    inDataChunk = resultSet->dataChunks[dataChunkPos];
    inValueVector = inDataChunk->getValueVector(valueVectorPos);
    outValueVector = make_shared<ValueVector>(context.memoryManager, lists->getDataType());
    inDataChunk->append(outValueVector);
}

void ScanUnstructuredProperty::getNextTuples() {
    metrics->executionTime.start();
    prevOperator->getNextTuples();
    if (inDataChunk->state->selectedSize > 0) {
        lists->readUnstructuredProperties(
            inValueVector, propertyKey, outValueVector, *metrics->bufferManagerMetrics);
    }
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow

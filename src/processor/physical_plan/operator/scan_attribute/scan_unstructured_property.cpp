#include "src/processor/include/physical_plan/operator/scan_attribute/scan_unstructured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

ScanUnstructuredProperty::ScanUnstructuredProperty(uint32_t inAndOutDataChunkPos,
    uint32_t inValueVectorPos, uint32_t outValueVectorPos, uint32_t propertyKey,
    UnstructuredPropertyLists* lists, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id)
    : ScanAttribute{inAndOutDataChunkPos, inValueVectorPos, outValueVectorPos, move(prevOperator),
          context, id},
      propertyKey{propertyKey}, lists{lists} {
    resultSet = this->prevOperator->getResultSet();
    inDataChunk = resultSet->dataChunks[inAndOutDataChunkPos];
    inValueVector = inDataChunk->getValueVector(inValueVectorPos);
    outValueVector = make_shared<ValueVector>(context.memoryManager, lists->getDataType());
    inDataChunk->insert(outValueVectorPos, outValueVector);
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

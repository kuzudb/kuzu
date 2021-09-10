#include "src/processor/include/physical_plan/operator/scan_attribute/scan_structured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

ScanStructuredProperty::ScanStructuredProperty(uint32_t inAndOutDataChunkPos,
    uint32_t inValueVectorPos, uint32_t outValueVectorPos, Column* column,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : ScanColumn{inAndOutDataChunkPos, inValueVectorPos, outValueVectorPos, column,
          move(prevOperator), context, id} {
    outValueVector = make_shared<ValueVector>(context.memoryManager, column->getDataType());
    inDataChunk->insert(outValueVectorPos, outValueVector);
}

void ScanStructuredProperty::getNextTuples() {
    metrics->executionTime.start();
    ScanColumn::getNextTuples();
    outValueVector->fillNullMask();
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow

#include "src/processor/include/physical_plan/operator/scan_attribute/scan_structured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

ScanStructuredProperty::ScanStructuredProperty(uint64_t dataChunkPos, uint64_t valueVectorPos,
    BaseColumn* column, unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context,
    uint32_t id)
    : ScanColumn{dataChunkPos, valueVectorPos, column, move(prevOperator), context, id} {
    outValueVector = make_shared<ValueVector>(column->getDataType());
    inDataChunk->append(outValueVector);
}

void ScanStructuredProperty::getNextTuples() {
    metrics->executionTime.start();
    ScanColumn::getNextTuples();
    outValueVector->fillNullMask();
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow

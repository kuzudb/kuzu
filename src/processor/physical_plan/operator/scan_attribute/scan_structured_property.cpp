#include "src/processor/include/physical_plan/operator/scan_attribute/scan_structured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

ScanStructuredProperty::ScanStructuredProperty(uint64_t dataChunkPos, uint64_t valueVectorPos,
    BaseColumn* column, unique_ptr<PhysicalOperator> prevOperator)
    : ScanColumn{dataChunkPos, valueVectorPos, column, move(prevOperator)} {
    outValueVector = make_shared<ValueVector>(column->getDataType());
    inDataChunk->append(outValueVector);
}

void ScanStructuredProperty::getNextTuples() {
    ScanColumn::getNextTuples();
    outValueVector->fillNullMask();
}

} // namespace processor
} // namespace graphflow

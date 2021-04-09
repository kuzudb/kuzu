#include "src/processor/include/physical_plan/operator/scan_column/scan_property.h"

#include "src/common/include/vector/operations/vector_comparison_operations.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

ScanProperty::ScanProperty(uint64_t dataChunkPos, uint64_t valueVectorPos, BaseColumn* column,
    unique_ptr<PhysicalOperator> prevOperator)
    : ScanColumn{dataChunkPos, valueVectorPos, column, move(prevOperator)} {
    outValueVector = make_shared<ValueVector>(column->getDataType());
    inDataChunk->append(outValueVector);
}

void ScanProperty::getNextTuples() {
    ScanColumn::getNextTuples();
    outValueVector->fillNullMask();
}

} // namespace processor
} // namespace graphflow

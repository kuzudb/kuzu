#include "src/processor/include/physical_plan/operator/scan_attribute/scan_column.h"

namespace graphflow {
namespace processor {

void ScanColumn::getNextTuples() {
    prevOperator->getNextTuples();
    if (inDataChunk->state->size > 0) {
        column->reclaim(handle);
        column->readValues(inNodeIDVector, outValueVector, handle);
    }
}

} // namespace processor
} // namespace graphflow

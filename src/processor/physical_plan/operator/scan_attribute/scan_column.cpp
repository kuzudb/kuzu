#include "src/processor/include/physical_plan/operator/scan_attribute/scan_column.h"

namespace graphflow {
namespace processor {

void ScanColumn::getNextTuples() {
    do {
        prevOperator->getNextTuples();
    } while (inDataChunk->state->size > 0 && inDataChunk->state->numSelectedValues == 0);
    if (inDataChunk->state->size > 0) {
        column->reclaim(handle);
        column->readValues(inNodeIDVector, outValueVector, handle);
    }
}

} // namespace processor
} // namespace graphflow

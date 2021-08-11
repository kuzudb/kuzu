#include "src/processor/include/physical_plan/operator/scan_attribute/scan_column.h"

namespace graphflow {
namespace processor {

void ScanColumn::getNextTuples() {
    prevOperator->getNextTuples();
    if (inDataChunk->state->selectedSize > 0) {
        column->readValues(inNodeIDVector, outValueVector, *metrics->bufferManagerMetrics);
    }
}

} // namespace processor
} // namespace graphflow

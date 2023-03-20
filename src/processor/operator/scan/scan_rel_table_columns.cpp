#include "processor/operator/scan/scan_rel_table_columns.h"

namespace kuzu {
namespace processor {

bool ScanRelTableColumns::getNextTuplesInternal(ExecutionContext* context) {
    do {
        restoreSelVector(inNodeIDVector->state->selVector);
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        saveSelVector(inNodeIDVector->state->selVector);
        tableData->scan(transaction, *scanState, inNodeIDVector, outputVectors);
    } while (inNodeIDVector->state->selVector->selectedSize == 0);
    metrics->numOutputTuple.increase(inNodeIDVector->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu

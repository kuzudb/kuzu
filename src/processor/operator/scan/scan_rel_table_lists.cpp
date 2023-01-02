#include "processor/operator/scan/scan_rel_table_lists.h"

namespace kuzu {
namespace processor {

bool ScanRelTableLists::getNextTuplesInternal() {
    do {
        if (scanState->syncState->hasMoreAndSwitchSourceIfNecessary()) {
            tableData->scan(transaction, *scanState, inNodeIDVector, outputVectors);
            metrics->numOutputTuple.increase(outputVectors[0]->state->selVector->selectedSize);
            return true;
        }
        if (!children[0]->getNextTuple()) {
            return false;
        }
        scanState->syncState->resetState();
        tableData->scan(transaction, *scanState, inNodeIDVector, outputVectors);
    } while (outputVectors[0]->state->selVector->selectedSize == 0);
    metrics->numOutputTuple.increase(outputVectors[0]->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu

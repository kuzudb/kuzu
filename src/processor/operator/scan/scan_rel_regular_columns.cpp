#include "processor/operator/scan/scan_rel_regular_columns.h"

namespace kuzu {
namespace processor {

bool ScanRelRegularColumns::getNextTuplesInternal(ExecutionContext* context) {
    do {
        restoreSelVector(inVector->state->selVector);
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        saveSelVector(inVector->state->selVector);
        info->table->initializeReadState(
            transaction, info->direction, info->columnIDs, inVector, scanState.get());
        info->table->read(transaction, *scanState, inVector, outVectors);
    } while (inVector->state->selVector->selectedSize == 0);
    return true;
}

} // namespace processor
} // namespace kuzu

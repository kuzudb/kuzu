#include "processor/operator/scan/scan_rel_table.h"

namespace kuzu {
namespace processor {

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (scanState->hasMoreToRead(context->clientContext->getTx())) {
            info->table->read(transaction, *scanState, inVector, outVectors);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        info->table->initializeReadState(
            transaction, info->direction, info->columnIDs, inVector, scanState.get());
    }
}

} // namespace processor
} // namespace kuzu

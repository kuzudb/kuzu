#include "processor/operator/scan/scan_rel_csr_columns.h"

namespace kuzu {
namespace processor {

bool ScanRelCSRColumns::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (scanState->hasMoreToRead(context->clientContext->getActiveTransaction())) {
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

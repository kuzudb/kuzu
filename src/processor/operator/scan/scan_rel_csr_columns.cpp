#include "processor/operator/scan/scan_rel_csr_columns.h"

namespace kuzu {
namespace processor {

bool ScanRelCSRColumns::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (scanState->hasMoreToRead()) {
            info->table->read(
                transaction, *scanState, info->direction, inVector, info->columnIDs, outVectors);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        info->table->initializeReadState(transaction, info->direction, inVector, scanState.get());
    }
}

} // namespace processor
} // namespace kuzu

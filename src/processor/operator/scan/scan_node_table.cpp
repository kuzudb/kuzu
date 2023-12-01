#include "processor/operator/scan/scan_node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool ScanSingleNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& outputVector : outVectors) {
        outputVector->resetAuxiliaryBuffer();
    }
    info->table->initializeReadState(transaction, info->columnIDs, inVector, readState.get());
    info->table->read(transaction, *readState, inVector, outVectors);
    return true;
}

} // namespace processor
} // namespace kuzu

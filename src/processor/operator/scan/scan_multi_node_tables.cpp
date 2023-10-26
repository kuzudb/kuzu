#include "processor/operator/scan/scan_multi_node_tables.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool ScanMultiNodeTables::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto tableID =
        inVector->getValue<nodeID_t>(inVector->state->selVector->selectedPositions[0]).tableID;
    tables.at(tableID)->read(transaction, inVector, tableIDToScanColumnIds.at(tableID), outVectors);
    return true;
}

} // namespace processor
} // namespace kuzu

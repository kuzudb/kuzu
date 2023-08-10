#include "processor/operator/scan/scan_node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool ScanSingleNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    table->read(transaction, inputNodeIDVector, propertyColumnIds, outPropertyVectors);
    return true;
}

bool ScanMultiNodeTables::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto tableID =
        inputNodeIDVector
            ->getValue<nodeID_t>(inputNodeIDVector->state->selVector->selectedPositions[0])
            .tableID;
    tables.at(tableID)->read(
        transaction, inputNodeIDVector, tableIDToScanColumnIds.at(tableID), outPropertyVectors);
    return true;
}

} // namespace processor
} // namespace kuzu

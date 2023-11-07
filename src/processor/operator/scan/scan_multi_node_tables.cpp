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
    KU_ASSERT(readStates.contains(tableID) && tables.contains(tableID));
    auto scanTableInfo = tables.at(tableID).get();
    scanTableInfo->table->initializeReadState(
        transaction, scanTableInfo->columnIDs, inVector, readStates[tableID].get());
    scanTableInfo->table->read(transaction, *readStates.at(tableID), inVector, outVectors);
    return true;
}

} // namespace processor
} // namespace kuzu

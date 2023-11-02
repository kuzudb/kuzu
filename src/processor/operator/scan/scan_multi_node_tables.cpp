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
    auto scanTableInfo = tables.at(tableID).get();
    scanTableInfo->table->read(transaction, inVector, scanTableInfo->columnIDs, outVectors);
    return true;
}

} // namespace processor
} // namespace kuzu

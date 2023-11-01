#include "processor/operator/scan/scan_node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool ScanSingleNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& outputVector : outPropertyVectors) {
        outputVector->resetAuxiliaryBuffer();
    }
    info->table->read(transaction, inputNodeIDVector, info->columnIDs, outPropertyVectors);
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
    auto tableScanInfo = tables.at(tableID).get();
    tableScanInfo->table->read(
        transaction, inputNodeIDVector, tableScanInfo->columnIDs, outPropertyVectors);
    return true;
}

} // namespace processor
} // namespace kuzu

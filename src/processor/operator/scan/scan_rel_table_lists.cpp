#include "processor/operator/scan/scan_rel_table_lists.h"

namespace kuzu {
namespace processor {

void ScanRelTableLists::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanRelTable::initLocalStateInternal(resultSet, context);
    scanState = std::make_unique<storage::RelTableScanState>(
        scanInfo->relStats, scanInfo->propertyIds, storage::RelTableDataType::LISTS);
}

bool ScanRelTableLists::getNextTuplesInternal(ExecutionContext* context) {
    do {
        if (scanState->syncState->hasMoreAndSwitchSourceIfNecessary()) {
            scanInfo->tableData->scan(transaction, *scanState, inNodeVector, outVectors);
            metrics->numOutputTuple.increase(outVectors[0]->state->selVector->selectedSize);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            scanState->syncState->resetState();
            return false;
        }
        scanState->syncState->resetState();
        scanInfo->tableData->scan(transaction, *scanState, inNodeVector, outVectors);
    } while (outVectors[0]->state->selVector->selectedSize == 0);
    metrics->numOutputTuple.increase(outVectors[0]->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu

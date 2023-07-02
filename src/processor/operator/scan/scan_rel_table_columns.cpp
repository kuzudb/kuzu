#include "processor/operator/scan/scan_rel_table_columns.h"

namespace kuzu {
namespace processor {

void ScanRelTableColumns::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanRelTable::initLocalStateInternal(resultSet, context);
    scanState = std::make_unique<storage::RelTableScanState>(
        scanInfo->relStats, scanInfo->propertyIds, storage::RelTableDataType::COLUMNS);
}

bool ScanRelTableColumns::getNextTuplesInternal(ExecutionContext* context) {
    do {
        restoreSelVector(inNodeVector->state->selVector);
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        saveSelVector(inNodeVector->state->selVector);
        scanInfo->tableData->scan(transaction, *scanState, inNodeVector, outVectors);
    } while (inNodeVector->state->selVector->selectedSize == 0);
    metrics->numOutputTuple.increase(inNodeVector->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu

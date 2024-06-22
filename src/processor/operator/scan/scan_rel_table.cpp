#include "processor/operator/scan/scan_rel_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void ScanRelTableInfo::initScanState() {
    std::vector<Column*> columns;
    columns.reserve(columnIDs.size());
    for (const auto columnID : columnIDs) {
        columns.push_back(table->getColumn(columnID, direction));
    }
    localScanState = std::make_unique<RelTableScanState>(table->getTableID(), columnIDs, columns,
        table->getCSROffsetColumn(direction), table->getCSRLengthColumn(direction), direction,
        copyVector(columnPredicates));
}

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    relInfo.initScanState();
    initVectors(*relInfo.localScanState, *resultSet);
}

void ScanRelTable::initVectors(TableScanState& state, const ResultSet& resultSet) const {
    ScanTable::initVectors(state, resultSet);
    state.cast<RelTableScanState>().relIDVector = resultSet.getValueVector(relInfo.relIDPos).get();
}

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    auto transaction = context->clientContext->getTx();
    auto& scanState = *relInfo.localScanState;
    while (true) {
        const auto skipScan =
            transaction->isReadOnly() && scanState.zoneMapResult == ZoneMapCheckResult::SKIP_SCAN;
        if (!skipScan) {
            while (scanState.source != TableScanSource::NONE &&
                   relInfo.table->scan(transaction, scanState)) {
                if (scanState.relIDVector->state->getSelVector().getSelSize() > 0) {
                    return true;
                }
            }
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        relInfo.table->initializeScanState(transaction, scanState);
    }
}

} // namespace processor
} // namespace kuzu

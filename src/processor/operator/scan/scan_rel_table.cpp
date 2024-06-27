#include "processor/operator/scan/scan_rel_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

void ScanRelTableInfo::initScanState() {
    localScanState =
        std::make_unique<RelTableScanState>(columnIDs, direction, copyVector(columnPredicates));
}

void ScanRelTableInfo::scanIfNecessary(Transaction* transaction) const {
    auto& relScanState = localScanState->dataScanState->cast<RelDataReadState>();
    auto& nodeIDSelVector = localScanState->nodeIDVector->state->getSelVectorUnsafe();
    // We need to reinitialize per node group
    if (relScanState.currNodeIdx == relScanState.endNodeIdx) {
        table->initializeScanState(transaction, *localScanState);
    }
    table->scan(transaction, *localScanState);
    relScanState.currNodeIdx = relScanState.batchEndNodeIdx;
    nodeIDSelVector.setToFiltered(relScanState.batchSize);
}

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    relInfo.initScanState();
    initVectors(*relInfo.localScanState, *resultSet);
}

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    auto transaction = context->clientContext->getTx();
    auto& scanState = *relInfo.localScanState;
    auto& relScanState = scanState.dataScanState->cast<RelDataReadState>();
    while (true) {
        auto skipScan =
            transaction->isReadOnly() && scanState.zoneMapResult == ZoneMapCheckResult::SKIP_SCAN;
        if (!skipScan && relInfo.localScanState->hasMoreToRead(transaction)) {
            relInfo.scanIfNecessary(transaction);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        relScanState.resetState();
        relInfo.table->initializeScanState(transaction, scanState);
    }
}

} // namespace processor
} // namespace kuzu

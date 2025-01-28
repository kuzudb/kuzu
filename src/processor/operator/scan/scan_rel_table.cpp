#include "processor/operator/scan/scan_rel_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void ScanRelTableInfo::initScanState() {
    auto numNodeGroups = table->getNumCommittedNodeGroups(direction);
    for (auto i = 0u; i < numNodeGroups; i++) {
        localScanStates.push_back(
                std::make_unique<storage::RelTableScanState>(columnIDs, direction, copyVector(columnPredicates)));
    }

    localScanState =
            std::make_unique<RelTableScanState>(columnIDs, direction, copyVector(columnPredicates));
}

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    relInfo.initScanState();
    for (auto& scanState : relInfo.localScanStates) {
        initVectors(*scanState, *resultSet);
    }
}

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    auto transaction = context->clientContext->getTx();
    while (true) {
        auto relScanState = getRelScanState();
        if (relScanState != nullptr) {
            auto skipScan =
                    transaction->isReadOnly() && relScanState->zoneMapResult == ZoneMapCheckResult::SKIP_SCAN;
            if (!skipScan && relScanState->hasMoreToRead(transaction)) {
                relInfo.table->scan(transaction, *relScanState);
                return true;
            }
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        relScanState = getRelScanState();
        if (relScanState != nullptr) {
            relInfo.table->initializeScanState(transaction, *relScanState);
        }
    }
}

} // namespace processor
} // namespace kuzu

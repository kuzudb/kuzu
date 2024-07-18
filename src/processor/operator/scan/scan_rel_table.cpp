#include "processor/operator/scan/scan_rel_table.h"

#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void ScanRelTableInfo::initScanState(MemoryManager& mm) {
    localScanState =
        std::make_unique<RelTableScanState>(mm, columnIDs, direction, copyVector(columnPredicates));
}

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    relInfo.initScanState(*context->clientContext->getMemoryManager());
    initVectors(*relInfo.localScanState, *resultSet);
}

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    auto transaction = context->clientContext->getTx();
    auto& scanState = *relInfo.localScanState;
    while (true) {
        auto skipScan =
            transaction->isReadOnly() && scanState.zoneMapResult == ZoneMapCheckResult::SKIP_SCAN;
        if (!skipScan && relInfo.localScanState->hasMoreToRead(transaction)) {
            relInfo.table->scan(transaction, scanState);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        relInfo.table->initializeScanState(transaction, scanState);
    }
}

} // namespace processor
} // namespace kuzu

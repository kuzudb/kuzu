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

void ScanRelTableInfo::scanIfNecessary(Transaction* transaction) {
    auto& relScanState = localScanState->dataScanState->cast<RelDataReadState>();
    // We need to reinitialize per node group
    if (relScanState.currNodeIdx == relScanState.endNodeIdx) {
        table->initializeScanState(transaction, *localScanState);
    }
    // We rescan per 2048 tuples
    if (relScanState.currentCSROffset >= DEFAULT_VECTOR_CAPACITY) {
        table->scan(transaction, *localScanState);
    }
    auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(relScanState.nodeGroupIdx);
    auto& outSelVector = localScanState->outputVectors[0]->state->getSelVectorUnsafe();
    auto currCSRSize =
        relScanState.csrHeaderChunks.getCSRLength(relScanState.currentNodeOffset - startNodeOffset);
    if (currCSRSize > DEFAULT_VECTOR_CAPACITY - relScanState.currentCSROffset) {
        currCSRSize = DEFAULT_VECTOR_CAPACITY - relScanState.currentCSROffset;
    } else {
        relScanState.currNodeIdx++;
    }
    for (auto i = 0u; i < currCSRSize; i++) {
        outSelVector.getMultableBuffer()[i] = i + relScanState.currentCSROffset;
    }
    relScanState.currentCSROffset += currCSRSize;
    outSelVector.setToFiltered(currCSRSize);
}

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    relInfo.initScanState();
    initVectors(*relInfo.localScanState, *resultSet);
}

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    auto transaction = context->clientContext->getTx();
    auto& scanState = *relInfo.localScanState;
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
        relInfo.table->initializeScanState(transaction, scanState);
    }
}

} // namespace processor
} // namespace kuzu

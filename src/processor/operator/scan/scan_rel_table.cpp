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
    auto& outSelVector = localScanState->outputVectors[0]->state->getSelVectorUnsafe();
    auto& nodeIDSelVector = localScanState->nodeIDVector->state->getSelVectorUnsafe();
    nodeIDSelVector.setToUnfiltered(relScanState.totalNodeIdxs);
    // We need to reinitialize per node group
    if (relScanState.currNodeIdx == relScanState.endNodeIdx) {
        table->initializeScanState(transaction, *localScanState);
    }
    // We rescan per 2048 tuples
    if (relScanState.currentCSROffset >= DEFAULT_VECTOR_CAPACITY) {
        table->scan(transaction, *localScanState);
    }
    auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(relScanState.nodeGroupIdx);
    relScanState.currentNodeOffset =
        localScanState->nodeIDVector->readNodeOffset(nodeIDSelVector[relScanState.currNodeIdx]);
    // This assumes nodeIDVector is initially unfiltered, which is not safe
    // we should do this using similar logic to Flatten
    nodeIDSelVector.getMultableBuffer()[0] = nodeIDSelVector[relScanState.currNodeIdx];
    nodeIDSelVector.setToFiltered(1);
    auto currCSROffset = relScanState.csrHeaderChunks.getEndCSROffset(
                             relScanState.currentNodeOffset - startNodeOffset) -
                         relScanState.csrHeaderChunks.getStartCSROffset(
                             relScanState.currentNodeOffset - startNodeOffset);
    auto currCSRSize =
        relScanState.csrHeaderChunks.getCSRLength(relScanState.currentNodeOffset - startNodeOffset);
    if (relScanState.currentCSROffset == 0) {
        currCSROffset -= relScanState.posInLastCSR;
        currCSRSize =
            relScanState.posInLastCSR > currCSRSize ? 0 : currCSRSize - relScanState.posInLastCSR;
    }
    auto spaceLeft = DEFAULT_VECTOR_CAPACITY - relScanState.currentCSROffset;
    if (currCSROffset > spaceLeft) {
        currCSROffset = spaceLeft;
        currCSRSize = std::min(currCSRSize, spaceLeft);
    } else {
        relScanState.currNodeIdx++;
    }
    for (auto i = 0u; i < currCSRSize; i++) {
        outSelVector.getMultableBuffer()[i] = i + relScanState.currentCSROffset;
    }
    relScanState.currentCSROffset += currCSROffset;
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

#include "processor/operator/scan/scan_multi_rel_tables.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

bool DirectionInfo::needFlip(RelDataDirection relDataDirection) const {
    if (extendFromSource && relDataDirection == common::RelDataDirection::BWD) {
        return true;
    }
    if (!extendFromSource && relDataDirection == common::RelDataDirection::FWD) {
        return true;
    }
    return false;
}

bool RelTableCollectionScanner::scan(const SelectionVector& selVector, Transaction* transaction) {
    while (true) {
        auto& scanState = *relInfos[currentTableIdx].localScanState;
        auto skipScan =
            transaction->isReadOnly() && scanState.zoneMapResult == ZoneMapCheckResult::SKIP_SCAN;
        if (!skipScan && scanState.hasMoreToRead(transaction)) {
            relInfos[currentTableIdx].table->scan(transaction, scanState);
            if (directionVector != nullptr) {
                KU_ASSERT(selVector.isUnfiltered());
                for (auto i = 0u; i < selVector.getSelSize(); ++i) {
                    directionVector->setValue<bool>(i, directionValues[currentTableIdx]);
                }
            }
            if (selVector.getSelSize() > 0) {
                return true;
            }
        } else {
            currentTableIdx = nextTableIdx;
            if (currentTableIdx == relInfos.size()) {
                return false;
            }
            relInfos[currentTableIdx].table->initializeScanState(transaction,
                *relInfos[currentTableIdx].localScanState);
            nextTableIdx++;
        }
    }
}

void ScanMultiRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto& [_, scanner] : scanners) {
        for (auto& relInfo : scanner.relInfos) {
            relInfo.initScanState();
            initVectors(*relInfo.localScanState, *resultSet);
            if (directionInfo.directionPos.isValid()) {
                scanner.directionVector =
                    resultSet->getValueVector(directionInfo.directionPos).get();
                scanner.directionValues.push_back(directionInfo.needFlip(relInfo.direction));
            }
        }
    }
    currentScanner = nullptr;
}

bool ScanMultiRelTable::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (currentScanner != nullptr &&
            currentScanner->scan(outState->getSelVector(), context->clientContext->getTx())) {
            metrics->numOutputTuple.increase(outState->getSelVector().getSelSize());
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            resetState();
            return false;
        }
        const auto currentIdx = nodeIDVector->state->getSelVector()[0];
        if (nodeIDVector->isNull(currentIdx)) {
            outState->getSelVectorUnsafe().setSelSize(0);
            continue;
        }
        auto nodeID = nodeIDVector->getValue<nodeID_t>(currentIdx);
        initCurrentScanner(nodeID);
    }
}

std::unique_ptr<PhysicalOperator> ScanMultiRelTable::clone() {
    return make_unique<ScanMultiRelTable>(info.copy(), directionInfo.copy(), copyMap(scanners),
        children[0]->clone(), id, printInfo->copy());
}

void ScanMultiRelTable::resetState() {
    currentScanner = nullptr;
    for (auto& [_, scanner] : scanners) {
        scanner.resetState();
    }
}

void ScanMultiRelTable::initCurrentScanner(const nodeID_t& nodeID) {
    if (scanners.contains(nodeID.tableID)) {
        currentScanner = &scanners.at(nodeID.tableID);
        currentScanner->resetState();
    } else {
        currentScanner = nullptr;
    }
}

} // namespace processor
} // namespace kuzu

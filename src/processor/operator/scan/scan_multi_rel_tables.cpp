#include "processor/operator/scan/scan_multi_rel_tables.h"

#include "processor/execution_context.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/local_storage/local_storage.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

bool DirectionInfo::needFlip(RelDataDirection relDataDirection) const {
    if (extendFromSource && relDataDirection == RelDataDirection::BWD) {
        return true;
    }
    if (!extendFromSource && relDataDirection == RelDataDirection::FWD) {
        return true;
    }
    return false;
}

bool RelTableCollectionScanner::scan(Transaction* transaction, RelTableScanState& scanState) {
    while (true) {
        const auto& relInfo = relInfos[currentTableIdx];
        if (relInfo.table->scan(transaction, scanState)) {
            if (directionVector != nullptr) {
                for (auto i = 0u; i < scanState.outState->getSelVector().getSelSize(); ++i) {
                    auto pos = scanState.outState->getSelVector()[i];
                    directionVector->setValue<bool>(pos, directionValues[currentTableIdx]);
                }
            }
            if (scanState.outState->getSelVector().getSelSize() > 0) {
                return true;
            }

        } else {
            currentTableIdx = nextTableIdx;
            if (currentTableIdx == relInfos.size()) {
                return false;
            }
            auto& currentRelInfo = relInfos[currentTableIdx];
            scanState.setToTable(transaction, currentRelInfo.table, currentRelInfo.columnIDs,
                copyVector(currentRelInfo.columnPredicates), currentRelInfo.direction);
            currentRelInfo.table->initScanState(transaction, scanState, currentTableIdx == 0);
            nextTableIdx++;
        }
    }
}

void ScanMultiRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    boundNodeIDVector = resultSet->getValueVector(info.nodeIDPos).get();
    KU_ASSERT(!info.outVectorsPos.empty());
    auto outState = resultSet->getValueVector(info.outVectorsPos[0])->state;
    std::vector<ValueVector*> outVectors;
    for (auto& pos : info.outVectorsPos) {
        outVectors.push_back(resultSet->getValueVector(pos).get());
    }
    scanState = std::make_unique<RelTableScanState>(*context->clientContext->getMemoryManager(),
        boundNodeIDVector, outVectors, outState);
    for (auto& [_, scanner] : scanners) {
        for (auto& relInfo : scanner.relInfos) {
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
            currentScanner->scan(context->clientContext->getTransaction(), *scanState)) {
            metrics->numOutputTuple.increase(scanState->outState->getSelVector().getSelSize());
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            resetState();
            return false;
        }
        const auto currentIdx = boundNodeIDVector->state->getSelVector()[0];
        if (boundNodeIDVector->isNull(currentIdx)) {
            currentScanner = nullptr;
            continue;
        }
        auto nodeID = boundNodeIDVector->getValue<nodeID_t>(currentIdx);
        initCurrentScanner(nodeID);
    }
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

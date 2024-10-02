#include "processor/operator/scan/scan_multi_rel_tables.h"

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

bool RelTableCollectionScanner::scan(Transaction* transaction) {
    while (true) {
        const auto& relInfo = relInfos[currentTableIdx];
        auto& scanState = *relInfo.scanState;
        if (relInfo.table->scan(transaction, scanState)) {
            if (directionVector != nullptr) {
                for (auto i = 0u; i < scanState.outState->getSelVector().getSelSize(); ++i) {
                    directionVector->setValue<bool>(i, directionValues[currentTableIdx]);
                }
            }
            if (scanState.outState->getSelVector().getSelSize() > 0) {
                return true;
            }
        } else {
            currentTableIdx = nextTableIdx;
            if (currentTableIdx == 0) {
                for (auto tableIdx = 0u; tableIdx < relInfos.size(); ++tableIdx) {
                    relInfos[tableIdx].table->initScanState(transaction,
                        *relInfos[tableIdx].scanState);
                }
            }
            if (currentTableIdx == relInfos.size()) {
                return false;
            }
            nextTableIdx++;
        }
    }
}

void ScanMultiRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    boundNodeIDVector = resultSet->getValueVector(info.nodeIDPos).get();
    KU_ASSERT(!info.outVectorsPos.empty());
    outState = resultSet->getValueVector(info.outVectorsPos[0])->state.get();
    for (auto& [_, scanner] : scanners) {
        for (auto& relInfo : scanner.relInfos) {
            relInfo.initScanState(context);
            initVectors(*relInfo.scanState, *resultSet);
            auto& scanState = relInfo.scanState->cast<RelTableScanState>();
            KU_ASSERT(outState == scanState.outState);
            scanState.nodeIDVector = boundNodeIDVector;
            if (const auto localRelTable =
                    context->clientContext->getTx()->getLocalStorage()->getLocalTable(
                        relInfo.table->getTableID(), LocalStorage::NotExistAction::RETURN_NULL)) {
                auto localTableColumnIDs = LocalRelTable::rewriteLocalColumnIDs(relInfo.direction,
                    relInfo.scanState->columnIDs);
                relInfo.scanState->localTableScanState =
                    std::make_unique<LocalRelTableScanState>(*relInfo.scanState,
                        localTableColumnIDs, localRelTable->ptrCast<LocalRelTable>());
            }
            if (directionInfo.directionPos.isValid()) {
                scanner.directionVector =
                    resultSet->getValueVector(directionInfo.directionPos).get();
                scanner.directionValues.push_back(directionInfo.needFlip(relInfo.direction));
            }
        }
    }
    currentScanner = nullptr;
}

void ScanMultiRelTable::initVectors(TableScanState& state, const ResultSet& resultSet) const {
    ScanTable::initVectors(state, resultSet);
    KU_ASSERT(!info.outVectorsPos.empty());
    state.rowIdxVector->state = resultSet.getValueVector(info.outVectorsPos[0])->state;
    state.outState = state.rowIdxVector->state.get();
}

bool ScanMultiRelTable::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (currentScanner != nullptr && currentScanner->scan(context->clientContext->getTx())) {
            metrics->numOutputTuple.increase(outState->getSelVector().getSelSize());
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

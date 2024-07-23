#include "processor/operator/scan/scan_multi_rel_tables.h"

#include "storage/local_storage/local_rel_table.h"

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

bool RelTableCollectionScanner::scan(const SelectionVector& selVector, Transaction* transaction) {
    while (true) {
        const auto& relInfo = relInfos[currentTableIdx];
        auto& scanState = *relInfo.scanState;
        const auto skipScan =
            transaction->isReadOnly() && scanState.zoneMapResult == ZoneMapCheckResult::SKIP_SCAN;
        if (!skipScan && scanState.source != TableScanSource::NONE &&
            relInfo.table->scan(transaction, scanState)) {
            if (directionVector != nullptr) {
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
                *relInfos[currentTableIdx].scanState);
            nextTableIdx++;
        }
    }
}

void ScanMultiRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto& [_, scanner] : scanners) {
        for (auto& relInfo : scanner.relInfos) {
            relInfo.initScanState();
            initVectors(*relInfo.scanState, *resultSet);
            // TODO(Guodong/Xiyang): Temp solution here. Should be moved to `info`.
            boundNodeIDVector = resultSet->getValueVector(relInfo.boundNodeIDPos).get();
            auto& scanState = relInfo.scanState->cast<RelTableScanState>();
            scanState.boundNodeIDVector = resultSet->getValueVector(relInfo.boundNodeIDPos).get();
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
        const auto currentIdx = IDVector->state->getSelVector()[0];
        if (boundNodeIDVector->isNull(currentIdx)) {
            outState->getSelVectorUnsafe().setSelSize(0);
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

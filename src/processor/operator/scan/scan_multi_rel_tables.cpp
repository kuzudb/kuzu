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
        if (readStates[currentTableIdx]->hasMoreToRead(transaction)) {
            const auto scanInfo = scanInfos[currentTableIdx].get();
            scanInfo->table->scan(transaction, *readStates[currentTableIdx]);
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
            if (currentTableIdx == readStates.size()) {
                return false;
            }
            const auto scanInfo = scanInfos[currentTableIdx].get();
            readStates[currentTableIdx]->direction = scanInfo->direction;
            scanInfo->table->initializeScanState(transaction, *readStates[currentTableIdx]);
            nextTableIdx++;
        }
    }
}

std::unique_ptr<RelTableCollectionScanner> RelTableCollectionScanner::clone() const {
    std::vector<std::unique_ptr<ScanRelTableInfo>> scanInfosCopy;
    for (auto& scanInfo : scanInfos) {
        scanInfosCopy.push_back(std::make_unique<ScanRelTableInfo>(*scanInfo));
    }
    return make_unique<RelTableCollectionScanner>(std::move(scanInfosCopy));
}

void ScanMultiRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto& [_, scanner] : scannerPerNodeTable) {
        scanner->readStates.resize(scanner->scanInfos.size());
        for (auto i = 0u; i < scanner->scanInfos.size(); i++) {
            const auto scanInfo = scanner->scanInfos[i].get();
            scanner->readStates[i] =
                std::make_unique<RelTableScanState>(scanInfo->columnIDs, scanInfo->direction);
            initVectors(*scanner->readStates[i], *resultSet);
            if (directionInfo.directionPos.isValid()) {
                scanner->directionVector =
                    resultSet->getValueVector(directionInfo.directionPos).get();
                scanner->directionValues.push_back(directionInfo.needFlip(scanInfo->direction));
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
    node_table_id_scanner_map_t clonedScanners;
    for (auto& [tableID, scanner] : scannerPerNodeTable) {
        clonedScanners.insert({tableID, scanner->clone()});
    }
    return make_unique<ScanMultiRelTable>(info.copy(), directionInfo.copy(),
        std::move(clonedScanners), children[0]->clone(), id, paramsString);
}

void ScanMultiRelTable::resetState() {
    currentScanner = nullptr;
    for (auto& [_, scanner] : scannerPerNodeTable) {
        scanner->resetState();
    }
}

void ScanMultiRelTable::initCurrentScanner(const nodeID_t& nodeID) {
    if (scannerPerNodeTable.contains(nodeID.tableID)) {
        currentScanner = scannerPerNodeTable.at(nodeID.tableID).get();
        currentScanner->resetState();
    } else {
        currentScanner = nullptr;
    }
}

} // namespace processor
} // namespace kuzu

#include "processor/operator/scan/generic_scan_rel_tables.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

void RelTableCollectionScanner::init() {
    for (auto& scanInfo : scanInfos) {
        scanStates.push_back(std::make_unique<RelTableScanState>(
            scanInfo->relStats, scanInfo->propertyIds, scanInfo->relTableDataType));
    }
}

bool RelTableCollectionScanner::scan(ValueVector* inVector,
    const std::vector<ValueVector*>& outputVectors, Transaction* transaction) {
    do {
        if (scanStates[currentTableIdx]->hasMoreAndSwitchSourceIfNecessary()) {
            assert(
                scanStates[currentTableIdx]->relTableDataType == storage::RelTableDataType::LISTS);
            scanInfos[currentTableIdx]->tableData->scan(
                transaction, *scanStates[currentTableIdx], inVector, outputVectors);
        } else {
            currentTableIdx = nextTableIdx;
            if (currentTableIdx == scanStates.size()) {
                return false;
            }
            if (scanStates[currentTableIdx]->relTableDataType ==
                storage::RelTableDataType::COLUMNS) {
                outputVectors[0]->state->selVector->resetSelectorToValuePosBufferWithSize(1);
                outputVectors[0]->state->selVector->selectedPositions[0] =
                    inVector->state->selVector->selectedPositions[0];
            } else {
                scanStates[currentTableIdx]->syncState->resetState();
            }
            scanInfos[currentTableIdx]->tableData->scan(
                transaction, *scanStates[currentTableIdx], inVector, outputVectors);
            nextTableIdx++;
        }
    } while (outputVectors[0]->state->selVector->selectedSize == 0);
    return true;
}

std::unique_ptr<RelTableCollectionScanner> RelTableCollectionScanner::clone() const {
    std::vector<std::unique_ptr<RelTableScanInfo>> clonedScanInfos;
    for (auto& scanInfo : scanInfos) {
        clonedScanInfos.push_back(scanInfo->copy());
    }
    return make_unique<RelTableCollectionScanner>(std::move(clonedScanInfos));
}

void ScanMultiRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanRelTable::initLocalStateInternal(resultSet, context);
    for (auto& [_, scanner] : scannerPerNodeTable) {
        scanner->init();
    }
    currentScanner = nullptr;
}

bool ScanMultiRelTable::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (currentScanner != nullptr &&
            currentScanner->scan(inNodeVector, outVectors, transaction)) {
            metrics->numOutputTuple.increase(outVectors[0]->state->selVector->selectedSize);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            resetState();
            return false;
        }
        auto currentIdx = inNodeVector->state->selVector->selectedPositions[0];
        if (inNodeVector->isNull(currentIdx)) {
            outVectors[0]->state->selVector->selectedSize = 0;
            continue;
        }
        auto nodeID = inNodeVector->getValue<nodeID_t>(currentIdx);
        initCurrentScanner(nodeID);
    }
}

std::unique_ptr<PhysicalOperator> ScanMultiRelTable::clone() {
    node_table_id_scanner_map_t clonedScanners;
    for (auto& [tableID, scanner] : scannerPerNodeTable) {
        clonedScanners.insert({tableID, scanner->clone()});
    }
    return make_unique<ScanMultiRelTable>(
        posInfo->copy(), std::move(clonedScanners), children[0]->clone(), id, paramsString);
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

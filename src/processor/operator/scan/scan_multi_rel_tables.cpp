#include "processor/operator/scan/scan_multi_rel_tables.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

void RelTableCollectionScanner::init() {
    for (auto& scanInfo : scanInfos) {
        readStates.push_back(std::make_unique<RelDataReadState>(
            scanInfo->table->getTableDataFormat(scanInfo->direction)));
    }
}

bool RelTableCollectionScanner::scan(ValueVector* inVector,
    const std::vector<ValueVector*>& outputVectors, Transaction* transaction) {
    while (true) {
        if (readStates[currentTableIdx]->hasMoreToRead(transaction)) {
            KU_ASSERT(readStates[currentTableIdx]->dataFormat == ColumnDataFormat::CSR);
            auto scanInfo = scanInfos[currentTableIdx].get();
            scanInfo->table->read(
                transaction, *readStates[currentTableIdx], inVector, outputVectors);
            if (outputVectors[0]->state->selVector->selectedSize > 0) {
                return true;
            }
        } else {
            currentTableIdx = nextTableIdx;
            if (currentTableIdx == readStates.size()) {
                return false;
            }
            auto scanInfo = scanInfos[currentTableIdx].get();
            scanInfo->table->initializeReadState(transaction, scanInfo->direction,
                scanInfo->columnIDs, inVector, readStates[currentTableIdx].get());
            nextTableIdx++;
            if (readStates[currentTableIdx]->dataFormat == ColumnDataFormat::REGULAR) {
                outputVectors[0]->state->selVector->resetSelectorToValuePosBufferWithSize(1);
                outputVectors[0]->state->selVector->selectedPositions[0] =
                    inVector->state->selVector->selectedPositions[0];
                scanInfo->table->read(
                    transaction, *readStates[currentTableIdx], inVector, outputVectors);
                if (outputVectors[0]->state->selVector->selectedSize > 0) {
                    return true;
                }
            }
        }
    }
}

std::unique_ptr<RelTableCollectionScanner> RelTableCollectionScanner::clone() const {
    std::vector<std::unique_ptr<ScanRelTableInfo>> clonedScanInfos;
    clonedScanInfos.reserve(scanInfos.size());
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
        if (currentScanner != nullptr && currentScanner->scan(inVector, outVectors, transaction)) {
            metrics->numOutputTuple.increase(outVectors[0]->state->selVector->selectedSize);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            resetState();
            return false;
        }
        auto currentIdx = inVector->state->selVector->selectedPositions[0];
        if (inVector->isNull(currentIdx)) {
            outVectors[0]->state->selVector->selectedSize = 0;
            continue;
        }
        auto nodeID = inVector->getValue<nodeID_t>(currentIdx);
        initCurrentScanner(nodeID);
    }
}

std::unique_ptr<PhysicalOperator> ScanMultiRelTable::clone() {
    node_table_id_scanner_map_t clonedScanners;
    for (auto& [tableID, scanner] : scannerPerNodeTable) {
        clonedScanners.insert({tableID, scanner->clone()});
    }
    return make_unique<ScanMultiRelTable>(std::move(clonedScanners), inVectorPos, outVectorsPos,
        children[0]->clone(), id, paramsString);
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

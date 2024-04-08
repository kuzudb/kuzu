#include "processor/operator/scan/scan_multi_rel_tables.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

void RelTableCollectionScanner::init(ValueVector* inVector,
    const std::vector<ValueVector*>& outputVectors) {
    readStates.resize(scanInfos.size());
    for (auto i = 0u; i < scanInfos.size(); i++) {
        auto scanInfo = scanInfos[i].get();
        readStates[i] = std::make_unique<RelTableReadState>(*inVector, scanInfo->columnIDs,
            outputVectors, scanInfo->direction);
    }
}

bool RelTableCollectionScanner::scan(ValueVector* inVector,
    const std::vector<ValueVector*>& outputVectors, Transaction* transaction) {
    while (true) {
        if (readStates[currentTableIdx]->hasMoreToRead(transaction)) {
            auto scanInfo = scanInfos[currentTableIdx].get();
            scanInfo->table->read(transaction, *readStates[currentTableIdx]);
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
                scanInfo->columnIDs, *inVector, *readStates[currentTableIdx]);
            nextTableIdx++;
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
        scanner->init(inVector, outVectors);
    }
    currentScanner = nullptr;
}

bool ScanMultiRelTable::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (currentScanner != nullptr &&
            currentScanner->scan(inVector, outVectors, context->clientContext->getTx())) {
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

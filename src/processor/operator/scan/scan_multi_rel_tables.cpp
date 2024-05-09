#include "processor/operator/scan/scan_multi_rel_tables.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

bool RelTableCollectionScanner::scan(const common::SelectionVector& selVector,
    Transaction* transaction) {
    while (true) {
        if (readStates[currentTableIdx]->hasMoreToRead(transaction)) {
            auto scanInfo = scanInfos[currentTableIdx].get();
            scanInfo->table->read(transaction, *readStates[currentTableIdx]);
            if (selVector.getSelSize() > 0) {
                return true;
            }
        } else {
            currentTableIdx = nextTableIdx;
            if (currentTableIdx == readStates.size()) {
                return false;
            }
            auto scanInfo = scanInfos[currentTableIdx].get();
            scanInfo->table->initializeReadState(transaction, scanInfo->direction,
                scanInfo->columnIDs, *readStates[currentTableIdx]);
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
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto& [_, scanner] : scannerPerNodeTable) {
        scanner->readStates.resize(scanner->scanInfos.size());
        for (auto i = 0u; i < scanner->scanInfos.size(); i++) {
            auto scanInfo = scanner->scanInfos[i].get();
            scanner->readStates[i] =
                std::make_unique<RelTableReadState>(scanInfo->columnIDs, scanInfo->direction);
            ScanTable::initVectors(*scanner->readStates[i], *resultSet);
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
        auto currentIdx = nodeIDVector->state->getSelVector()[0];
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
    return make_unique<ScanMultiRelTable>(std::move(clonedScanners), nodeIDPos, outVectorsPos,
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

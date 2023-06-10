#include "processor/operator/scan/generic_scan_rel_tables.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

void RelTableDataCollection::resetState() {
    currentRelTableIdxToScan = 0;
    nextRelTableIdxToScan = 0;
}

bool RelTableDataCollection::scan(ValueVector* inVector,
    const std::vector<ValueVector*>& outputVectors, Transaction* transaction) {
    do {
        if (tableScanStates[currentRelTableIdxToScan]->hasMoreAndSwitchSourceIfNecessary()) {
            assert(tableScanStates[currentRelTableIdxToScan]->relTableDataType ==
                   storage::RelTableDataType::LISTS);
            relTableDatas[currentRelTableIdxToScan]->scan(
                transaction, *tableScanStates[currentRelTableIdxToScan], inVector, outputVectors);
        } else {
            currentRelTableIdxToScan = nextRelTableIdxToScan;
            if (currentRelTableIdxToScan == tableScanStates.size()) {
                return false;
            }
            if (tableScanStates[currentRelTableIdxToScan]->relTableDataType ==
                storage::RelTableDataType::COLUMNS) {
                outputVectors[0]->state->selVector->resetSelectorToValuePosBufferWithSize(1);
                outputVectors[0]->state->selVector->selectedPositions[0] =
                    inVector->state->selVector->selectedPositions[0];
            } else {
                tableScanStates[currentRelTableIdxToScan]->syncState->resetState();
            }
            relTableDatas[currentRelTableIdxToScan]->scan(
                transaction, *tableScanStates[currentRelTableIdxToScan], inVector, outputVectors);
            nextRelTableIdxToScan++;
        }
    } while (outputVectors[0]->state->selVector->selectedSize == 0);
    return true;
}

std::unique_ptr<RelTableDataCollection> RelTableDataCollection::clone() const {
    std::vector<std::unique_ptr<RelTableScanState>> clonedScanStates;
    for (auto& scanState : tableScanStates) {
        clonedScanStates.push_back(make_unique<RelTableScanState>(
            scanState->relStats, scanState->propertyIds, scanState->relTableDataType));
    }
    return make_unique<RelTableDataCollection>(relTableDatas, std::move(clonedScanStates));
}

void GenericScanRelTables::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanRelTable::initLocalStateInternal(resultSet, context);
    currentRelTableDataCollection = nullptr;
}

bool GenericScanRelTables::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (scanCurrentRelTableDataCollection()) {
            metrics->numOutputTuple.increase(outputVectors[0]->state->selVector->selectedSize);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            currentRelTableDataCollection = nullptr;
            for (auto& [_, relTableDataCollection] : relTableCollectionPerNodeTable) {
                relTableDataCollection->resetState();
            }
            return false;
        }
        auto currentIdx = inNodeIDVector->state->selVector->selectedPositions[0];
        if (inNodeIDVector->isNull(currentIdx)) {
            outputVectors[0]->state->selVector->selectedSize = 0;
            continue;
        }
        auto nodeID = inNodeIDVector->getValue<nodeID_t>(currentIdx);
        initCurrentRelTableDataCollection(nodeID);
    }
}

bool GenericScanRelTables::scanCurrentRelTableDataCollection() {
    if (currentRelTableDataCollection == nullptr) {
        return false;
    }
    return currentRelTableDataCollection->scan(inNodeIDVector, outputVectors, transaction);
}

void GenericScanRelTables::initCurrentRelTableDataCollection(const nodeID_t& nodeID) {
    if (relTableCollectionPerNodeTable.contains(nodeID.tableID)) {
        currentRelTableDataCollection = relTableCollectionPerNodeTable.at(nodeID.tableID).get();
        currentRelTableDataCollection->resetState();
    } else {
        currentRelTableDataCollection = nullptr;
    }
}

} // namespace processor
} // namespace kuzu

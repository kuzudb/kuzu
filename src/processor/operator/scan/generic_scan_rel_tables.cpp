#include "processor/operator/scan/generic_scan_rel_tables.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

void RelTableCollection::resetState() {
    currentRelTableIdxToScan = 0;
    nextRelTableIdxToScan = 0;
}

bool RelTableCollection::scan(ValueVector* inVector, const std::vector<ValueVector*>& outputVectors,
    Transaction* transaction) {
    do {
        if (tableScanStates[currentRelTableIdxToScan]->hasMoreAndSwitchSourceIfNecessary()) {
            assert(tableScanStates[currentRelTableIdxToScan]->relTableDataType ==
                   storage::RelTableDataType::LISTS);
            tables[currentRelTableIdxToScan]->scan(
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
            tables[currentRelTableIdxToScan]->scan(
                transaction, *tableScanStates[currentRelTableIdxToScan], inVector, outputVectors);
            nextRelTableIdxToScan++;
        }
    } while (outputVectors[0]->state->selVector->selectedSize == 0);
    return true;
}

std::unique_ptr<RelTableCollection> RelTableCollection::clone() const {
    std::vector<std::unique_ptr<RelTableScanState>> clonedScanStates;
    for (auto& scanState : tableScanStates) {
        clonedScanStates.push_back(
            make_unique<RelTableScanState>(scanState->propertyIds, scanState->relTableDataType));
    }
    return make_unique<RelTableCollection>(tables, std::move(clonedScanStates));
}

void GenericScanRelTables::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanRelTable::initLocalStateInternal(resultSet, context);
    currentRelTableCollection = nullptr;
}

bool GenericScanRelTables::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (scanCurrentRelTableCollection()) {
            metrics->numOutputTuple.increase(outputVectors[0]->state->selVector->selectedSize);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        auto currentIdx = inNodeIDVector->state->selVector->selectedPositions[0];
        if (inNodeIDVector->isNull(currentIdx)) {
            outputVectors[0]->state->selVector->selectedSize = 0;
            continue;
        }
        auto nodeID = inNodeIDVector->getValue<nodeID_t>(currentIdx);
        initCurrentRelTableCollection(nodeID);
    }
}

bool GenericScanRelTables::scanCurrentRelTableCollection() {
    if (currentRelTableCollection == nullptr) {
        return false;
    }
    return currentRelTableCollection->scan(inNodeIDVector, outputVectors, transaction);
}

void GenericScanRelTables::initCurrentRelTableCollection(const nodeID_t& nodeID) {
    if (relTableCollectionPerNodeTable.contains(nodeID.tableID)) {
        currentRelTableCollection = relTableCollectionPerNodeTable.at(nodeID.tableID).get();
        currentRelTableCollection->resetState();
    } else {
        currentRelTableCollection = nullptr;
    }
}

} // namespace processor
} // namespace kuzu

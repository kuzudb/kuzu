#include "processor/operator/generic_extend.h"

namespace kuzu {
namespace processor {

void ColumnAndListCollection::populateListHandles(ListSyncState& syncState) {
    assert(listHandles.empty());
    for (auto _ : lists) {
        listHandles.push_back(make_shared<ListHandle>(syncState));
    }
}

void AdjAndPropertyCollection::populateListHandles() {
    listSyncState = make_unique<ListSyncState>();
    adjCollection->populateListHandles(*listSyncState);
    for (auto& propertyCollection : propertyCollections) {
        assert(propertyCollection->lists.size() == adjCollection->lists.size());
        propertyCollection->populateListHandles(*listSyncState);
    }
}

void AdjAndPropertyCollection::resetState(node_offset_t nodeOffset) {
    nextColumnIdx = 0;
    nextListIdx = 0;
    currentNodeOffset = nodeOffset;
    currentListIdx = UINT32_MAX;
}

bool AdjAndPropertyCollection::scan(const shared_ptr<ValueVector>& inVector,
    const shared_ptr<ValueVector>& outNodeVector,
    const vector<shared_ptr<ValueVector>>& outPropertyVectors, Transaction* transaction) {
    if (scanColumns(inVector, outNodeVector, outPropertyVectors, transaction)) {
        return true;
    }
    if (scanLists(inVector, outNodeVector, outPropertyVectors, transaction)) {
        return true;
    }
    return false;
}

bool AdjAndPropertyCollection::scanColumns(const shared_ptr<ValueVector>& inVector,
    const shared_ptr<ValueVector>& outNodeVector,
    const vector<shared_ptr<ValueVector>>& outPropertyVectors, Transaction* transaction) {
    while (hasColumnToScan()) {
        if (scanColumn(nextColumnIdx, inVector, outNodeVector, outPropertyVectors, transaction)) {
            nextColumnIdx++;
            return true;
        }
        nextColumnIdx++;
    }
    return false;
}

bool AdjAndPropertyCollection::scanLists(const shared_ptr<ValueVector>& inVector,
    const shared_ptr<ValueVector>& outNodeVector,
    const vector<shared_ptr<ValueVector>>& outPropertyVectors, Transaction* transaction) {
    if (currentListIdx != UINT32_MAX) { // check current list
        auto currentAdjList = adjCollection->lists[currentListIdx];
        auto currentAdjListHandle = adjCollection->listHandles[currentListIdx].get();
        if (currentAdjListHandle->listSyncState.hasMoreToRead()) {
            // scan current adjList
            currentAdjList->readValues(outNodeVector, *currentAdjListHandle);
            // scan current propertyLists
            for (auto i = 0u; i < propertyCollections.size(); ++i) {
                auto currentPropertyList = propertyCollections[i]->lists[currentListIdx];
                auto currentPropertyListHandle =
                    propertyCollections[i]->listHandles[currentListIdx].get();
                if (currentPropertyList == nullptr) {
                    outPropertyVectors[i]->setAllNull();
                } else {
                    currentPropertyList->readValues(
                        outPropertyVectors[i], *currentPropertyListHandle);
                }
            }
            return true;
        } else {
            // no more to scan on current list, move to next list.
            nextListIdx++;
            currentListIdx = UINT32_MAX;
        }
    }
    while (hasListToScan()) {
        if (scanList(nextListIdx, inVector, outNodeVector, outPropertyVectors, transaction)) {
            return true;
        }
        nextListIdx++;
    }
    return false;
}

bool AdjAndPropertyCollection::scanColumn(uint32_t idx, const shared_ptr<ValueVector>& inVector,
    const shared_ptr<ValueVector>& outNodeVector,
    const vector<shared_ptr<ValueVector>>& outPropertyVectors, Transaction* transaction) {
    auto selVector = outNodeVector->state->selVector.get();
    if (selVector->isUnfiltered()) {
        selVector->resetSelectorToValuePosBuffer();
    }
    // We need to sync output vector state with input vector because we always write output to a new
    // data chunk and thus they don't share state.
    auto inVectorCurrentIdx = inVector->state->selVector->selectedPositions[0];
    selVector->selectedPositions[0] = inVectorCurrentIdx;
    selVector->selectedSize = 1;
    auto adjColumn = adjCollection->columns[idx];
    // scan adjColumn
    adjColumn->read(transaction, inVector, outNodeVector);
    if (outNodeVector->isNull(selVector->selectedPositions[0])) {
        return false;
    }
    // scan propertyColumns
    for (auto i = 0u; i < propertyCollections.size(); ++i) {
        auto propertyColumn = propertyCollections[i]->columns[idx];
        if (propertyColumn == nullptr) {
            outPropertyVectors[i]->setAllNull();
        } else {
            propertyColumn->read(transaction, inVector, outPropertyVectors[i]);
        }
    }
    return true;
}

bool AdjAndPropertyCollection::scanList(uint32_t idx, const shared_ptr<ValueVector>& inVector,
    const shared_ptr<ValueVector>& outNodeVector,
    const vector<shared_ptr<ValueVector>>& outPropertyVectors, Transaction* transaction) {
    auto selVector = outNodeVector->state->selVector.get();
    if (!selVector->isUnfiltered()) {
        selVector->resetSelectorToUnselected();
    }
    auto adjList = (AdjLists*)adjCollection->lists[idx];
    auto adjListHandle = adjCollection->listHandles[idx].get();
    // scan adjList
    adjList->initListReadingState(currentNodeOffset, *adjListHandle, transaction->getType());
    adjList->readValues(outNodeVector, *adjListHandle);
    if (selVector->selectedSize == 0) {
        return false;
    }
    currentListIdx = idx;
    // scan propertyLists
    for (auto i = 0u; i < propertyCollections.size(); ++i) {
        auto propertyList = propertyCollections[i]->lists[idx];
        auto propertyListHandle = propertyCollections[i]->listHandles[idx].get();
        if (propertyList == nullptr) {
            outPropertyVectors[i]->setAllNull();
        } else {
            propertyList->readValues(outPropertyVectors[i], *propertyListHandle);
            propertyList->setDeletedRelsIfNecessary(
                transaction, propertyListHandle->listSyncState, outPropertyVectors[i]);
        }
    }
    return selVector->selectedSize != 0;
}

unique_ptr<AdjAndPropertyCollection> AdjAndPropertyCollection::clone() const {
    auto clonedAdjCollection =
        make_unique<ColumnAndListCollection>(adjCollection->columns, adjCollection->lists);
    vector<unique_ptr<ColumnAndListCollection>> clonedPropertyCollections;
    for (auto& propertyCollection : propertyCollections) {
        clonedPropertyCollections.push_back(make_unique<ColumnAndListCollection>(
            propertyCollection->columns, propertyCollection->lists));
    }
    return make_unique<AdjAndPropertyCollection>(
        std::move(clonedAdjCollection), std::move(clonedPropertyCollections));
}

void GenericExtend::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    inVector = resultSet->getValueVector(inVectorPos);
    outNodeVector = resultSet->getValueVector(outNodeVectorPos);
    for (auto& dataPos : outPropertyVectorsPos) {
        auto vector = resultSet->getValueVector(dataPos);
        outPropertyVectors.push_back(vector);
    }
    for (auto& [_, adjAndPropertyCollection] : adjAndPropertyCollectionPerNodeTable) {
        adjAndPropertyCollection->populateListHandles();
    }
    // config local state
    currentAdjAndPropertyCollection = nullptr;
}

bool GenericExtend::getNextTuplesInternal() {
    while (true) {
        if (scanCurrentAdjAndPropertyCollection()) {
            metrics->numOutputTuple.increase(outNodeVector->state->selVector->selectedSize);
            return true;
        }
        if (!children[0]->getNextTuple()) {
            return false;
        }
        auto currentIdx = inVector->state->selVector->selectedPositions[0];
        if (inVector->isNull(currentIdx)) {
            outNodeVector->state->selVector->selectedSize = 0;
            continue;
        }
        auto nodeID = inVector->getValue<nodeID_t>(currentIdx);
        initCurrentAdjAndPropertyCollection(nodeID);
    }
}

bool GenericExtend::scanCurrentAdjAndPropertyCollection() {
    if (currentAdjAndPropertyCollection == nullptr) {
        return false;
    }
    return currentAdjAndPropertyCollection->scan(
        inVector, outNodeVector, outPropertyVectors, transaction);
}

void GenericExtend::initCurrentAdjAndPropertyCollection(const nodeID_t& nodeID) {
    if (adjAndPropertyCollectionPerNodeTable.contains(nodeID.tableID)) {
        currentAdjAndPropertyCollection =
            adjAndPropertyCollectionPerNodeTable.at(nodeID.tableID).get();
        currentAdjAndPropertyCollection->resetState(nodeID.offset);
    } else {
        currentAdjAndPropertyCollection = nullptr;
    }
}

} // namespace processor
} // namespace kuzu

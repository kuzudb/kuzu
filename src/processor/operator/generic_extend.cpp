#include "processor/operator/generic_extend.h"

namespace kuzu {
namespace processor {

void GenericExtend::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    inVector = resultSet->getValueVector(inVectorPos);
    listSyncState = make_unique<ListSyncState>();
    outNodeVector = resultSet->getValueVector(outNodeVectorPos);
    for (auto& _ : adjColumnAndListCollection.lists) {
        adjColumnAndListCollection.listHandles.push_back(make_shared<ListHandle>(*listSyncState));
    }
    for (auto i = 0u; i < outPropertyVectorsPos.size(); ++i) {
        auto vector = resultSet->getValueVector(outPropertyVectorsPos[i]);
        outPropertyVectors.push_back(vector);
        auto& propertyColumnAndListCollection = propertyColumnAndListCollections[i];
        assert(propertyColumnAndListCollection.lists.size() ==
               adjColumnAndListCollection.lists.size());
        for (auto& _ : propertyColumnAndListCollection.lists) {
            propertyColumnAndListCollection.listHandles.push_back(
                make_shared<ListHandle>(*listSyncState));
        }
    }
    // config local state
    nextListIdx = adjColumnAndListCollection.lists.size();
    nextColumnIdx = adjColumnAndListCollection.columns.size();
    currentAdjList = nullptr;
    currentPropertyLists.resize(outPropertyVectors.size());
    currentPropertyListHandles.resize(outPropertyVectors.size());
}

bool GenericExtend::getNextTuplesInternal() {
    while (true) {
        if (scan()) {
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
        nextColumnIdx = 0;
        nextListIdx = 0;
        currentAdjList = nullptr;
        currentNodeOffset = inVector->getValue<nodeID_t>(currentIdx).offset;
    }
}

bool GenericExtend::scan() {
    if (scanColumns()) {
        return true;
    }
    if (scanLists()) {
        return true;
    }
    return false;
}

bool GenericExtend::scanColumns() {
    while (hasColumnToScan()) {
        if (scanColumn(nextColumnIdx)) {
            nextColumnIdx++;
            return true;
        }
        nextColumnIdx++;
    }
    return false;
}

bool GenericExtend::scanLists() {
    if (currentAdjList != nullptr) { // check current list
        if (currentAdjListHandle->listSyncState.hasMoreToRead()) {
            currentAdjList->readValues(outNodeVector, *currentAdjListHandle);
            for (auto i = 0u; i < currentPropertyLists.size(); ++i) {
                if (currentPropertyLists[i] == nullptr) {
                    outPropertyVectors[i]->setAllNull();
                } else {
                    currentPropertyLists[i]->readValues(
                        outPropertyVectors[i], *currentPropertyListHandles[i]);
                }
            }
            return true;
        } else {
            nextListIdx++;
            currentAdjList = nullptr;
        }
    }
    while (hasListToScan()) {
        if (scanList(nextListIdx)) {
            return true;
        }
        nextListIdx++;
    }
    return false;
}

bool GenericExtend::scanColumn(uint32_t idx) {
    auto selVector = outNodeVector->state->selVector.get();
    if (selVector->isUnfiltered()) {
        selVector->resetSelectorToValuePosBuffer();
    }
    // We need to sync output vector state with input vector because we always write output to a new
    // data chunk and thus they don't share state.
    auto inVectorCurrentIdx = inVector->state->selVector->selectedPositions[0];
    selVector->selectedPositions[0] = inVectorCurrentIdx;
    selVector->selectedSize = 1;
    auto adjColumn = adjColumnAndListCollection.columns[idx];
    // scan adjColumn
    adjColumn->read(transaction, inVector, outNodeVector);
    if (outNodeVector->isNull(selVector->selectedPositions[0])) {
        return false;
    }
    // scan propertyColumns
    for (auto i = 0u; i < propertyColumnAndListCollections.size(); ++i) {
        auto propertyColumn = propertyColumnAndListCollections[i].columns[idx];
        if (propertyColumn == nullptr) {
            outPropertyVectors[i]->setAllNull();
        } else {
            propertyColumn->read(transaction, inVector, outPropertyVectors[i]);
        }
    }
    return true;
}

bool GenericExtend::scanList(uint32_t idx) {
    auto selVector = outNodeVector->state->selVector.get();
    if (!selVector->isUnfiltered()) {
        selVector->resetSelectorToUnselected();
    }
    auto adjList = (AdjLists*)adjColumnAndListCollection.lists[idx];
    auto adjListHandle = adjColumnAndListCollection.listHandles[idx].get();
    // scan adjList
    adjList->initListReadingState(currentNodeOffset, *adjListHandle, transaction->getType());
    adjList->readValues(outNodeVector, *adjListHandle);
    if (selVector->selectedSize == 0) {
        return false;
    }
    currentAdjList = adjList;
    currentAdjListHandle = adjListHandle;
    // scan propertyLists
    for (auto i = 0u; i < propertyColumnAndListCollections.size(); ++i) {
        auto propertyList = propertyColumnAndListCollections[i].lists[idx];
        auto propertyListHandle = propertyColumnAndListCollections[i].listHandles[idx].get();
        if (propertyList == nullptr) {
            outPropertyVectors[i]->setAllNull();
        } else {
            propertyList->readValues(outPropertyVectors[i], *propertyListHandle);
            propertyList->setDeletedRelsIfNecessary(
                transaction, propertyListHandle->listSyncState, outPropertyVectors[i]);
        }
        currentPropertyLists[i] = propertyList;
        currentPropertyListHandles[i] = propertyListHandle;
    }
    return selVector->selectedSize != 0;
}

} // namespace processor
} // namespace kuzu

#include "processor/operator/generic_extend.h"

namespace kuzu {
namespace processor {

void GenericExtend::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    inVector = resultSet->getValueVector(inVectorPos);
    outVector = resultSet->getValueVector(outVectorPos);
    resultSet->initListSyncState(outVectorPos.dataChunkPos);
    for (auto& list : lists) {
        listHandles.push_back(
            make_shared<ListHandle>(*resultSet->getListSyncState(outVectorPos.dataChunkPos)));
    }
    nextListIdx = lists.size();
    nextColumnIdx = columns.size();
    currentList = nullptr;
}

bool GenericExtend::getNextTuplesInternal() {
    while (true) {
        if (findOutput()) {
            metrics->numOutputTuple.increase(outVector->state->selVector->selectedSize);
            return true;
        }
        if (!children[0]->getNextTuple()) {
            return false;
        }
        auto currentIdx = inVector->state->selVector->selectedPositions[0];
        if (inVector->isNull(currentIdx)) {
            outVector->state->selVector->selectedSize = 0;
            continue;
        }
        nextColumnIdx = 0;
        nextListIdx = 0;
        currentList = nullptr;
        currentNodeOffset = inVector->getValue<nodeID_t>(currentIdx).offset;
    }
}

bool GenericExtend::findOutput() {
    if (findInColumns()) {
        return true;
    }
    if (findInLists()) {
        return true;
    }
    return false;
}

bool GenericExtend::findInColumns() {
    while (hasColumnToScan()) {
        if (scanColumn(nextColumnIdx)) {
            nextColumnIdx++;
            return true;
        }
        nextColumnIdx++;
    }
    return false;
}

bool GenericExtend::findInLists() {
    if (currentList != nullptr) { // check current list
        if (currentListHandle->listSyncState.hasMoreToRead()) {
            currentList->readValues(outVector, *currentListHandle);
            return true;
        } else {
            nextListIdx++;
            currentList = nullptr;
            currentListHandle = nullptr;
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
    if (outVector->state->selVector->isUnfiltered()) {
        outVector->state->selVector->resetSelectorToValuePosBuffer();
    }
    // We need to sync output vector state with input vector because we always write output to a new
    // data chunk and thus they don't share state.
    auto inVectorCurrentIdx = inVector->state->selVector->selectedPositions[0];
    outVector->state->selVector->selectedPositions[0] = inVectorCurrentIdx;
    outVector->state->selVector->selectedSize = 1;
    columns[idx]->read(transaction, inVector, outVector);
    return !outVector->isNull(outVector->state->selVector->selectedPositions[0]);
}

bool GenericExtend::scanList(uint32_t idx) {
    if (!outVector->state->selVector->isUnfiltered()) {
        outVector->state->selVector->resetSelectorToUnselected();
    }
    auto adjLists = (AdjLists*)lists[idx];
    auto listHandle = listHandles[idx].get();
    adjLists->initListReadingState(currentNodeOffset, *listHandle, transaction->getType());
    adjLists->readValues(outVector, *listHandle);
    currentList = lists[idx];
    currentListHandle = listHandles[idx].get();
    return outVector->state->selVector->selectedSize != 0;
}

} // namespace processor
} // namespace kuzu

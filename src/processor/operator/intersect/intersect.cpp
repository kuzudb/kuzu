#include "processor/operator/intersect/intersect.h"

#include <algorithm>

using namespace kuzu::common;
using namespace kuzu::function::operation;

namespace kuzu {
namespace processor {

void Intersect::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    outKeyVector = resultSet->getValueVector(outputDataPos);
    for (auto& dataInfo : intersectDataInfos) {
        probeKeyVectors.push_back(resultSet->getValueVector(dataInfo.keyDataPos));
        std::vector<uint32_t> columnIdxesToScanFrom;
        std::vector<std::shared_ptr<ValueVector>> vectorsToReadInto;
        for (auto i = 0u; i < dataInfo.payloadsDataPos.size(); i++) {
            auto vector = resultSet->getValueVector(dataInfo.payloadsDataPos[i]);
            // Always skip the first two columns in the fTable: build key and intersect key.
            columnIdxesToScanFrom.push_back(i + 2);
            vectorsToReadInto.push_back(vector);
        }
        payloadColumnIdxesToScanFrom.push_back(columnIdxesToScanFrom);
        payloadVectorsToScanInto.push_back(std::move(vectorsToReadInto));
    }
    for (auto& sharedHT : sharedHTs) {
        intersectSelVectors.push_back(std::make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY));
        isIntersectListAFlatValue.push_back(
            sharedHT->getHashTable()->getTableSchema()->getColumn(1)->isFlat());
    }
}

std::vector<uint8_t*> Intersect::probeHTs(const std::vector<nodeID_t>& keys) {
    std::vector<uint8_t*> tuples(keys.size());
    hash_t tmpHash;
    for (auto i = 0u; i < keys.size(); i++) {
        Hash::operation<nodeID_t>(keys[i], false, tmpHash);
        tuples[i] = sharedHTs[i]->getHashTable()->getTupleForHash(tmpHash);
        while (tuples[i]) {
            if (*(nodeID_t*)tuples[i] == keys[i]) {
                break; // The build side should guarantee each key only has one matching tuple.
            }
            tuples[i] = *sharedHTs[i]->getHashTable()->getPrevTuple(tuples[i]);
        }
    }
    return tuples;
}

void Intersect::twoWayIntersect(nodeID_t* leftNodeIDs, SelectionVector& lSelVector,
    nodeID_t* rightNodeIDs, SelectionVector& rSelVector) {
    assert(lSelVector.selectedSize <= rSelVector.selectedSize);
    sel_t leftPosition = 0, rightPosition = 0;
    uint64_t outputValuePosition = 0;
    while (leftPosition < lSelVector.selectedSize && rightPosition < rSelVector.selectedSize) {
        auto leftNodeID = leftNodeIDs[leftPosition];
        auto rightNodeID = rightNodeIDs[rightPosition];
        if (leftNodeID.offset < rightNodeID.offset) {
            leftPosition++;
        } else if (leftNodeID.offset > rightNodeID.offset) {
            rightPosition++;
        } else {
            lSelVector.getSelectedPositionsBuffer()[outputValuePosition] = leftPosition;
            rSelVector.getSelectedPositionsBuffer()[outputValuePosition] = rightPosition;
            leftNodeIDs[outputValuePosition] = leftNodeID;
            leftPosition++;
            rightPosition++;
            outputValuePosition++;
        }
    }
    lSelVector.resetSelectorToValuePosBufferWithSize(outputValuePosition);
    rSelVector.resetSelectorToValuePosBufferWithSize(outputValuePosition);
}

std::vector<nodeID_t> Intersect::getProbeKeys() {
    std::vector<nodeID_t> keys(probeKeyVectors.size());
    for (auto i = 0u; i < keys.size(); i++) {
        assert(probeKeyVectors[i]->state->isFlat());
        keys[i] = probeKeyVectors[i]->getValue<nodeID_t>(
            probeKeyVectors[i]->state->selVector->selectedPositions[0]);
    }
    return keys;
}

static std::vector<overflow_value_t> fetchListsToIntersectFromTuples(
    const std::vector<uint8_t*>& tuples, const std::vector<bool>& isFlatValue) {
    std::vector<overflow_value_t> listsToIntersect(tuples.size());
    for (auto i = 0u; i < tuples.size(); i++) {
        if (!tuples[i]) {
            continue; // overflow_value will be initialized with size 0 for non-matching tuples.
        }
        listsToIntersect[i] =
            isFlatValue[i] ? overflow_value_t{1 /* numElements */, tuples[i] + sizeof(nodeID_t)} :
                             *(overflow_value_t*)(tuples[i] + sizeof(nodeID_t));
    }
    return listsToIntersect;
}

static std::vector<uint32_t> swapSmallestListToFront(std::vector<overflow_value_t>& lists) {
    assert(lists.size() >= 2);
    std::vector<uint32_t> listIdxes(lists.size());
    iota(listIdxes.begin(), listIdxes.end(), 0);
    uint32_t smallestListIdx = 0;
    for (auto i = 1u; i < lists.size(); i++) {
        if (lists[i].numElements < lists[smallestListIdx].numElements) {
            smallestListIdx = i;
        }
    }
    if (smallestListIdx != 0) {
        std::swap(lists[smallestListIdx], lists[0]);
        std::swap(listIdxes[smallestListIdx], listIdxes[0]);
    }
    return listIdxes;
}

static void sliceSelVectors(
    const std::vector<SelectionVector*>& selVectorsToSlice, SelectionVector& slicer) {
    for (auto selVec : selVectorsToSlice) {
        for (auto i = 0u; i < slicer.selectedSize; i++) {
            auto pos = slicer.selectedPositions[i];
            selVec->getSelectedPositionsBuffer()[i] = selVec->selectedPositions[pos];
        }
        selVec->resetSelectorToValuePosBufferWithSize(slicer.selectedSize);
    }
}

void Intersect::intersectLists(const std::vector<overflow_value_t>& listsToIntersect) {
    if (listsToIntersect[0].numElements == 0) {
        outKeyVector->state->selVector->selectedSize = 0;
        return;
    }
    assert(listsToIntersect[0].numElements <= DEFAULT_VECTOR_CAPACITY);
    memcpy(outKeyVector->getData(), listsToIntersect[0].value,
        listsToIntersect[0].numElements * sizeof(nodeID_t));
    SelectionVector lSelVector(listsToIntersect[0].numElements);
    lSelVector.selectedSize = listsToIntersect[0].numElements;
    std::vector<SelectionVector*> selVectorsForIntersectedLists;
    intersectSelVectors[0]->resetSelectorToUnselectedWithSize(listsToIntersect[0].numElements);
    selVectorsForIntersectedLists.push_back(intersectSelVectors[0].get());
    for (auto i = 0u; i < listsToIntersect.size() - 1; i++) {
        intersectSelVectors[i + 1]->resetSelectorToUnselectedWithSize(
            listsToIntersect[i + 1].numElements);
        twoWayIntersect((nodeID_t*)outKeyVector->getData(), lSelVector,
            (nodeID_t*)listsToIntersect[i + 1].value, *intersectSelVectors[i + 1]);
        // Here we need to slice all selVectors that have been previously intersected, as all these
        // lists need to be selected synchronously to read payloads correctly.
        sliceSelVectors(selVectorsForIntersectedLists, lSelVector);
        lSelVector.resetSelectorToUnselected();
        selVectorsForIntersectedLists.push_back(intersectSelVectors[i + 1].get());
    }
    outKeyVector->state->selVector->selectedSize = lSelVector.selectedSize;
}

void Intersect::populatePayloads(
    const std::vector<uint8_t*>& tuples, const std::vector<uint32_t>& listIdxes) {
    for (auto i = 0u; i < listIdxes.size(); i++) {
        auto listIdx = listIdxes[i];
        sharedHTs[i]->getHashTable()->getFactorizedTable()->lookup(
            payloadVectorsToScanInto[listIdx], intersectSelVectors[i].get(),
            payloadColumnIdxesToScanFrom[listIdx], tuples[listIdx]);
    }
}

bool Intersect::getNextTuplesInternal() {
    do {
        if (!children[0]->getNextTuple()) {
            return false;
        }
        auto tuples = probeHTs(getProbeKeys());
        auto listsToIntersect = fetchListsToIntersectFromTuples(tuples, isIntersectListAFlatValue);
        auto listIdxes = swapSmallestListToFront(listsToIntersect);
        intersectLists(listsToIntersect);
        if (outKeyVector->state->selVector->selectedSize != 0) {
            populatePayloads(tuples, listIdxes);
        }
    } while (outKeyVector->state->selVector->selectedSize == 0);
    return true;
}

} // namespace processor
} // namespace kuzu

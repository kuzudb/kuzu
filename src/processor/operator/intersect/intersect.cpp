#include "processor/operator/intersect/intersect.h"

#include <algorithm>

#include "function/hash/hash_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void Intersect::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    outKeyVector = resultSet->getValueVector(outputDataPos);
    for (auto& dataInfo : intersectDataInfos) {
        probeKeyVectors.push_back(resultSet->getValueVector(dataInfo.keyDataPos));
        std::vector<uint32_t> columnIdxesToScanFrom;
        std::vector<ValueVector*> vectorsToReadInto;
        for (auto i = 0u; i < dataInfo.payloadsDataPos.size(); i++) {
            auto vector = resultSet->getValueVector(dataInfo.payloadsDataPos[i]);
            // Always skip the first two columns in the fTable: build key and intersect key.
            // TODO(Guodong): Remove this assumption so that keys can be stored in any order. Change
            // mapping logic too so that we don't need to maintain this order explicitly.
            columnIdxesToScanFrom.push_back(i + 2);
            vectorsToReadInto.push_back(vector.get());
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

void Intersect::probeHTs() {
    std::vector<std::vector<overflow_value_t>> flatTuples(probeKeyVectors.size());
    hash_t hashVal;
    for (auto i = 0u; i < probeKeyVectors.size(); i++) {
        KU_ASSERT(probeKeyVectors[i]->state->isFlat());
        probedFlatTuples[i].clear();
        auto key = probeKeyVectors[i]->getValue<nodeID_t>(
            probeKeyVectors[i]->state->selVector->selectedPositions[0]);
        function::Hash::operation<nodeID_t>(key, false, hashVal);
        auto flatTuple = sharedHTs[i]->getHashTable()->getTupleForHash(hashVal);
        while (flatTuple) {
            if (*(nodeID_t*)flatTuple == key) {
                probedFlatTuples[i].push_back(flatTuple);
            }
            flatTuple = *sharedHTs[i]->getHashTable()->getPrevTuple(flatTuple);
        }
    }
}

void Intersect::twoWayIntersect(nodeID_t* leftNodeIDs, SelectionVector& lSelVector,
    nodeID_t* rightNodeIDs, SelectionVector& rSelVector) {
    KU_ASSERT(lSelVector.selectedSize <= rSelVector.selectedSize);
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

static std::vector<overflow_value_t> fetchListsToIntersectFromTuples(
    const std::vector<uint8_t*>& tuples, const std::vector<bool>& isFlatValue) {
    std::vector<overflow_value_t> listsToIntersect(tuples.size());
    for (auto i = 0u; i < tuples.size(); i++) {
        listsToIntersect[i] =
            isFlatValue[i] ? overflow_value_t{1 /* numElements */, tuples[i] + sizeof(nodeID_t)} :
                             *(overflow_value_t*)(tuples[i] + sizeof(nodeID_t));
    }
    return listsToIntersect;
}

static std::vector<uint32_t> swapSmallestListToFront(std::vector<overflow_value_t>& lists) {
    KU_ASSERT(lists.size() >= 2);
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
    KU_ASSERT(listsToIntersect[0].numElements <= DEFAULT_VECTOR_CAPACITY);
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
        sharedHTs[listIdx]->getHashTable()->getFactorizedTable()->lookup(
            payloadVectorsToScanInto[listIdx], intersectSelVectors[i].get(),
            payloadColumnIdxesToScanFrom[listIdx], tuples[listIdx]);
    }
}

bool Intersect::hasNextTuplesToIntersect() {
    tupleIdxPerBuildSide[carryBuildSideIdx]++;
    if (tupleIdxPerBuildSide[carryBuildSideIdx] == probedFlatTuples[carryBuildSideIdx].size()) {
        if (carryBuildSideIdx == 0) {
            return false;
        }
        tupleIdxPerBuildSide[carryBuildSideIdx] = 0;
        carryBuildSideIdx--;
        if (!hasNextTuplesToIntersect()) {
            return false;
        }
        carryBuildSideIdx++;
    }
    return true;
}

bool Intersect::getNextTuplesInternal(ExecutionContext* context) {
    do {
        while (carryBuildSideIdx == -1u) {
            if (!children[0]->getNextTuple(context)) {
                return false;
            }
            // For each build side, probe its HT and return a vector of matched flat tuples.
            probeHTs();
            auto maxNumTuplesToIntersect = 1u;
            for (auto i = 0u; i < getNumBuilds(); i++) {
                maxNumTuplesToIntersect *= probedFlatTuples[i].size();
            }
            if (maxNumTuplesToIntersect == 0) {
                // Skip if any build side has no matches.
                continue;
            }
            carryBuildSideIdx = getNumBuilds() - 1;
            std::fill(tupleIdxPerBuildSide.begin(), tupleIdxPerBuildSide.end(), 0);
        }
        // Cartesian product of all flat tuples probed from all build sides.
        // Notice: when there are large adjacency lists in the build side, which means the list is
        // too large to fit in a single ValueVector, we end up chunking the list as multiple tuples
        // in FTable. Thus, when performing the intersection, we need to perform cartesian product
        // between all flat tuples probed from all build sides.
        std::vector<uint8_t*> flatTuplesToIntersect(getNumBuilds());
        for (auto i = 0u; i < getNumBuilds(); i++) {
            flatTuplesToIntersect[i] = probedFlatTuples[i][tupleIdxPerBuildSide[i]];
        }
        auto listsToIntersect =
            fetchListsToIntersectFromTuples(flatTuplesToIntersect, isIntersectListAFlatValue);
        auto listIdxes = swapSmallestListToFront(listsToIntersect);
        intersectLists(listsToIntersect);
        if (outKeyVector->state->selVector->selectedSize != 0) {
            populatePayloads(flatTuplesToIntersect, listIdxes);
        }
        if (!hasNextTuplesToIntersect()) {
            carryBuildSideIdx = -1u;
        }
    } while (outKeyVector->state->selVector->selectedSize == 0);
    return true;
}

} // namespace processor
} // namespace kuzu

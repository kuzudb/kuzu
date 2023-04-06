#include "processor/operator/hash_join/hash_join_probe.h"

#include "function/hash/hash_operations.h"

using namespace kuzu::common;
using namespace kuzu::function::operation;

namespace kuzu {
namespace processor {

void HashJoinProbe::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    probeState = std::make_unique<ProbeState>();
    for (auto& keyDataPos : probeDataInfo.keysDataPos) {
        keyVectors.push_back(resultSet->getValueVector(keyDataPos).get());
    }
    if (joinType == JoinType::MARK) {
        markVector = resultSet->getValueVector(probeDataInfo.markDataPos);
    }
    for (auto& dataPos : probeDataInfo.payloadsOutPos) {
        vectorsToReadInto.push_back(resultSet->getValueVector(dataPos).get());
    }
    // We only need to read nonKeys from the factorizedTable. Key columns are always kept as first k
    // columns in the factorizedTable, so we skip the first k columns.
    assert(probeDataInfo.keysDataPos.size() + probeDataInfo.getNumPayloads() + 1 ==
           sharedState->getHashTable()->getTableSchema()->getNumColumns());
    columnIdxsToReadFrom.resize(probeDataInfo.getNumPayloads());
    iota(
        columnIdxsToReadFrom.begin(), columnIdxsToReadFrom.end(), probeDataInfo.keysDataPos.size());
    hashVector = std::make_unique<common::ValueVector>(common::INT64, context->memoryManager);
    if (keyVectors.size() > 1) {
        tmpHashVector =
            std::make_unique<common::ValueVector>(common::INT64, context->memoryManager);
    }
}

bool HashJoinProbe::hasMoreLeft() {
    if (keyVectors[0]->state->isFlat() && probeState->probedTuples[0] != nullptr) {
        return true;
    }
    return false;
}

bool HashJoinProbe::getNextBatchOfMatchedTuples(ExecutionContext* context) {
    if (probeState->nextMatchedTupleIdx < probeState->matchedSelVector->selectedSize) {
        return true;
    }
    if (!hasMoreLeft()) {
        restoreSelVector(keyVectors[0]->state->selVector);
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        saveSelVector(keyVectors[0]->state->selVector);
        sharedState->getHashTable()->probe(
            keyVectors, hashVector.get(), tmpHashVector.get(), probeState->probedTuples.get());
    }
    auto numMatchedTuples = 0;
    auto keyState = keyVectors[0]->state.get();
    if (keyState->isFlat()) {
        // probe side is flat.
        while (probeState->probedTuples[0]) {
            if (numMatchedTuples == DEFAULT_VECTOR_CAPACITY) {
                break;
            }
            auto currentTuple = probeState->probedTuples[0];
            probeState->matchedTuples[numMatchedTuples] = currentTuple;
            bool isKeysEqual = true;
            for (auto i = 0u; i < keyVectors.size(); i++) {
                auto pos = keyVectors[i]->state->selVector->selectedPositions[0];
                if (((nodeID_t*)currentTuple)[i] != keyVectors[i]->getValue<nodeID_t>(pos)) {
                    isKeysEqual = false;
                    break;
                }
            }
            numMatchedTuples += isKeysEqual;
            probeState->probedTuples[0] = *sharedState->getHashTable()->getPrevTuple(currentTuple);
        }
    } else {
        assert(keyVectors.size() == 1);
        for (auto i = 0u; i < keyState->selVector->selectedSize; i++) {
            auto pos = keyState->selVector->selectedPositions[i];
            while (probeState->probedTuples[i]) {
                assert(numMatchedTuples <= DEFAULT_VECTOR_CAPACITY);
                auto currentTuple = probeState->probedTuples[i];
                probeState->matchedTuples[numMatchedTuples] = currentTuple;
                probeState->matchedSelVector->selectedPositions[numMatchedTuples] = pos;
                numMatchedTuples +=
                    *(nodeID_t*)currentTuple == keyVectors[0]->getValue<nodeID_t>(pos);
                probeState->probedTuples[i] =
                    *sharedState->getHashTable()->getPrevTuple(currentTuple);
            }
        }
    }
    probeState->matchedSelVector->selectedSize = numMatchedTuples;
    probeState->nextMatchedTupleIdx = 0;
    return true;
}

void HashJoinProbe::setVectorsToNull() {
    for (auto& vector : vectorsToReadInto) {
        if (vector->state->isFlat()) {
            vector->setNull(vector->state->selVector->selectedPositions[0], true);
        } else {
            assert(vector->state != keyVectors[0]->state);
            auto pos = vector->state->selVector->selectedPositions[0];
            vector->setNull(pos, true);
            vector->state->selVector->selectedSize = 1;
        }
    }
}

uint64_t HashJoinProbe::getNextInnerJoinResult() {
    if (probeState->matchedSelVector->selectedSize == 0) {
        return 0;
    }
    auto numTuplesToRead =
        keyVectors[0]->state->isFlat() ? 1 : probeState->matchedSelVector->selectedSize;
    if (!keyVectors[0]->state->isFlat() &&
        keyVectors[0]->state->selVector->selectedSize != numTuplesToRead) {
        // Update probeSideKeyVector's selectedPositions when the probe side is unflat and its
        // selected positions need to change (i.e., some keys has no matched tuples).
        auto keySelectedBuffer = keyVectors[0]->state->selVector->getSelectedPositionsBuffer();
        for (auto i = 0u; i < numTuplesToRead; i++) {
            keySelectedBuffer[i] = probeState->matchedSelVector->selectedPositions[i];
        }
        keyVectors[0]->state->selVector->selectedSize = numTuplesToRead;
        keyVectors[0]->state->selVector->resetSelectorToValuePosBuffer();
    }
    sharedState->getHashTable()->lookup(vectorsToReadInto, columnIdxsToReadFrom,
        probeState->matchedTuples.get(), probeState->nextMatchedTupleIdx, numTuplesToRead);
    probeState->nextMatchedTupleIdx += numTuplesToRead;
    return numTuplesToRead;
}

uint64_t HashJoinProbe::getNextLeftJoinResult() {
    if (getNextInnerJoinResult() == 0) {
        setVectorsToNull();
    }
    return 1;
}

uint64_t HashJoinProbe::getNextMarkJoinResult() {
    auto markValues = (bool*)markVector->getData();
    if (markVector->state->isFlat()) {
        markValues[markVector->state->selVector->selectedPositions[0]] =
            probeState->matchedSelVector->selectedSize != 0;
    } else {
        std::fill(markValues, markValues + DEFAULT_VECTOR_CAPACITY, false);
        for (auto i = 0u; i < probeState->matchedSelVector->selectedSize; i++) {
            markValues[probeState->matchedSelVector->selectedPositions[i]] = true;
        }
    }
    probeState->nextMatchedTupleIdx = probeState->matchedSelVector->selectedSize;
    return 1;
}

uint64_t HashJoinProbe::getNextJoinResult() {
    switch (joinType) {
    case JoinType::LEFT: {
        return getNextLeftJoinResult();
    }
    case JoinType::MARK: {
        return getNextMarkJoinResult();
    }
    case JoinType::INNER: {
        return getNextInnerJoinResult();
    }
    default: {
        assert(false);
    }
    }
}

// The general flow of a hash join probe:
// 1) find matched tuples of probe side key from ht.
// 2) populate values from matched tuples into resultKeyDataChunk , buildSideFlatResultDataChunk
// (all flat data chunks from the build side are merged into one) and buildSideVectorPtrs (each
// VectorPtr corresponds to one unFlat build side data chunk that is appended to the resultSet).
bool HashJoinProbe::getNextTuplesInternal(ExecutionContext* context) {
    uint64_t numPopulatedTuples;
    do {
        if (!getNextBatchOfMatchedTuples(context)) {
            return false;
        }
        numPopulatedTuples = getNextJoinResult();
    } while (numPopulatedTuples == 0);
    metrics->numOutputTuple.increase(numPopulatedTuples);
    return true;
}

} // namespace processor
} // namespace kuzu

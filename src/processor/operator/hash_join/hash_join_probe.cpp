#include "processor/operator/hash_join/hash_join_probe.h"

using namespace kuzu::common;

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
    KU_ASSERT(probeDataInfo.keysDataPos.size() + probeDataInfo.getNumPayloads() + 1 ==
              sharedState->getHashTable()->getTableSchema()->getNumColumns());
    columnIdxsToReadFrom.resize(probeDataInfo.getNumPayloads());
    iota(
        columnIdxsToReadFrom.begin(), columnIdxsToReadFrom.end(), probeDataInfo.keysDataPos.size());
    hashVector = std::make_unique<ValueVector>(LogicalTypeID::INT64, context->memoryManager);
    if (keyVectors.size() > 1) {
        tmpHashVector = std::make_unique<ValueVector>(LogicalTypeID::INT64, context->memoryManager);
    }
}

bool HashJoinProbe::getMatchedTuplesForFlatKey(ExecutionContext* context) {
    if (probeState->nextMatchedTupleIdx < probeState->matchedSelVector->selectedSize) {
        // Not all matched tuples have been shipped. Continue shipping.
        return true;
    }
    if (probeState->probedTuples[0] == nullptr) { // No more matched tuples on the chain.
        // We still need to save and restore for flat input because we are discarding NULL join keys
        // which changes the selected position.
        // TODO(Guodong): we have potential bugs here because all keys' states should be restored.
        restoreSelVector(keyVectors[0]->state->selVector);
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        saveSelVector(keyVectors[0]->state->selVector);
        sharedState->getHashTable()->probe(
            keyVectors, hashVector.get(), tmpHashVector.get(), probeState->probedTuples.get());
    }
    auto numMatchedTuples = sharedState->getHashTable()->matchFlatKeys(
        keyVectors, probeState->probedTuples.get(), probeState->matchedTuples.get());
    probeState->matchedSelVector->selectedSize = numMatchedTuples;
    probeState->nextMatchedTupleIdx = 0;
    return true;
}

bool HashJoinProbe::getMatchedTuplesForUnFlatKey(ExecutionContext* context) {
    KU_ASSERT(keyVectors.size() == 1);
    auto keyVector = keyVectors[0];
    restoreSelVector(keyVector->state->selVector);
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    saveSelVector(keyVector->state->selVector);
    sharedState->getHashTable()->probe(
        keyVectors, hashVector.get(), tmpHashVector.get(), probeState->probedTuples.get());
    auto numMatchedTuples =
        sharedState->getHashTable()->matchUnFlatKey(keyVector, probeState->probedTuples.get(),
            probeState->matchedTuples.get(), probeState->matchedSelVector.get());
    probeState->matchedSelVector->selectedSize = numMatchedTuples;
    probeState->nextMatchedTupleIdx = 0;
    return true;
}

uint64_t HashJoinProbe::getInnerJoinResultForFlatKey() {
    if (probeState->matchedSelVector->selectedSize == 0) {
        return 0;
    }
    auto numTuplesToRead = 1;
    sharedState->getHashTable()->lookup(vectorsToReadInto, columnIdxsToReadFrom,
        probeState->matchedTuples.get(), probeState->nextMatchedTupleIdx, numTuplesToRead);
    probeState->nextMatchedTupleIdx += numTuplesToRead;
    return numTuplesToRead;
}

uint64_t HashJoinProbe::getInnerJoinResultForUnFlatKey() {
    auto numTuplesToRead = probeState->matchedSelVector->selectedSize;
    if (numTuplesToRead == 0) {
        return 0;
    }
    auto keySelVector = keyVectors[0]->state->selVector.get();
    if (keySelVector->selectedSize != numTuplesToRead) {
        // Some keys have no matched tuple. So we modify selected position.
        auto keySelectedBuffer = keySelVector->getSelectedPositionsBuffer();
        for (auto i = 0u; i < numTuplesToRead; i++) {
            keySelectedBuffer[i] = probeState->matchedSelVector->selectedPositions[i];
        }
        keySelVector->selectedSize = numTuplesToRead;
        keySelVector->resetSelectorToValuePosBuffer();
    }
    sharedState->getHashTable()->lookup(vectorsToReadInto, columnIdxsToReadFrom,
        probeState->matchedTuples.get(), probeState->nextMatchedTupleIdx, numTuplesToRead);
    probeState->nextMatchedTupleIdx += numTuplesToRead;
    return numTuplesToRead;
}

uint64_t HashJoinProbe::getLeftJoinResult() {
    if (getInnerJoinResult() == 0) {
        for (auto& vector : vectorsToReadInto) {
            vector->setAsSingleNullEntry();
        }
        // TODO(Xiyang): We have a bug in LEFT JOIN which should not discard NULL keys. To be more
        // clear, NULL keys should only be discarded for probe but should not reflect on the vector.
        // The following for loop is a temporary hack.
        for (auto& vector : keyVectors) {
            KU_ASSERT(vector->state->isFlat());
            vector->state->selVector->selectedSize = 1;
        }
        probeState->probedTuples[0] = nullptr;
    }
    return 1;
}

uint64_t HashJoinProbe::getCountJoinResult() {
    KU_ASSERT(vectorsToReadInto.size() == 1);
    if (getInnerJoinResult() == 0) {
        auto pos = vectorsToReadInto[0]->state->selVector->selectedPositions[0];
        vectorsToReadInto[0]->setValue<int64_t>(pos, 0);
        probeState->probedTuples[0] = nullptr;
    }
    return 1;
}

uint64_t HashJoinProbe::getMarkJoinResult() {
    auto markValues = (bool*)markVector->getData();
    if (markVector->state->isFlat()) {
        auto pos = markVector->state->selVector->selectedPositions[0];
        markValues[pos] = probeState->matchedSelVector->selectedSize != 0;
    } else {
        std::fill(markValues, markValues + DEFAULT_VECTOR_CAPACITY, false);
        for (auto i = 0u; i < probeState->matchedSelVector->selectedSize; i++) {
            auto pos = probeState->matchedSelVector->selectedPositions[i];
            markValues[pos] = true;
        }
    }
    probeState->probedTuples[0] = nullptr;
    probeState->nextMatchedTupleIdx = probeState->matchedSelVector->selectedSize;
    return 1;
}

uint64_t HashJoinProbe::getJoinResult() {
    switch (joinType) {
    case JoinType::LEFT: {
        return getLeftJoinResult();
    }
    case JoinType::COUNT: {
        return getCountJoinResult();
    }
    case JoinType::MARK: {
        return getMarkJoinResult();
    }
    case JoinType::INNER: {
        return getInnerJoinResult();
    }
    default:
        throw InternalException("Unimplemented join type for HashJoinProbe::getJoinResult()");
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
        if (!getMatchedTuples(context)) {
            return false;
        }
        numPopulatedTuples = getJoinResult();
    } while (numPopulatedTuples == 0);
    metrics->numOutputTuple.increase(numPopulatedTuples);
    return true;
}

} // namespace processor
} // namespace kuzu

#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

#include "src/function/hash/include/vector_hash_operations.h"
#include "src/function/hash/operations/include/hash_operations.h"

using namespace graphflow::function::operation;

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> HashJoinProbe::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    probeState = make_unique<ProbeState>();
    probeSideKeyDataChunk = resultSet->dataChunks[probeDataInfo.getKeyIDDataChunkPos()];
    probeSideKeyVector = probeSideKeyDataChunk->valueVectors[probeDataInfo.getKeyIDVectorPos()];
    for (auto pos : flatDataChunkPositions) {
        auto dataChunk = resultSet->dataChunks[pos];
        dataChunk->state = DataChunkState::getSingleValueDataChunkState();
    }
    for (auto i = 0u; i < probeDataInfo.nonKeyOutputDataPos.size(); ++i) {
        auto probeSideNonKeyVector = make_shared<ValueVector>(
            context->memoryManager, sharedState->getNonKeyDataPosesDataTypes()[i]);
        auto [dataChunkPos, valueVectorPos] = probeDataInfo.nonKeyOutputDataPos[i];
        resultSet->dataChunks[dataChunkPos]->insert(valueVectorPos, probeSideNonKeyVector);
        vectorsToReadInto.push_back(probeSideNonKeyVector);
    }
    // We only need to read nonKeys from the factorizedTable. The first column in the
    // factorizedTable is the key column, so we skip the key column.
    columnIdxsToReadFrom.resize(probeDataInfo.nonKeyOutputDataPos.size());
    iota(columnIdxsToReadFrom.begin(), columnIdxsToReadFrom.end(), 1);
    return resultSet;
}

bool HashJoinProbe::getNextBatchOfMatchedTuples() {
    if (probeState->nextMatchedTupleIdx < probeState->numMatchedTuples) {
        return true;
    }
    auto keys = (nodeID_t*)probeSideKeyVector->values;
    do {
        if (probeState->numMatchedTuples == 0) {
            restoreSelVector(probeSideKeyDataChunk->state->selVector.get());
            if (!children[0]->getNextTuples()) {
                return false;
            }
            saveSelVector(probeSideKeyDataChunk->state->selVector.get());
            sharedState->getHashTable()->probe(
                *probeSideKeyVector, probeState->probedTuples.get(), *probeState->probeSelVector);
        }
        auto numMatchedTuples = 0;
        while (true) {
            if (numMatchedTuples == DEFAULT_VECTOR_CAPACITY ||
                probeState->probedTuples[0] == nullptr) {
                // The logic behind checking on probedTuples[0]: when the probe side is flat, we
                // always put the probed tuple into probedTuples[0], and chase the pointer until it
                // reaches the end (nullptr); when the probe side is unflat, it is guaranteed that
                // all probed tuples has no prev chained to it, so for probedTuples[0]. Thus, we
                // only check on probedTuples[0] to see if we hit the end or not.
                break;
            }
            for (auto i = 0u; i < probeState->probeSelVector->selectedSize; i++) {
                auto pos = probeState->probeSelVector->selectedPositions[i];
                auto currentTuple = probeState->probedTuples[i];
                probeState->matchedTuples[numMatchedTuples] = currentTuple;
                probeState->probeSelVector->selectedPositions[numMatchedTuples] = pos;
                numMatchedTuples += *(nodeID_t*)currentTuple == keys[pos];
                probeState->probedTuples[i] =
                    *sharedState->getHashTable()->getPrevTuple(currentTuple);
            }
        }
        probeState->numMatchedTuples = numMatchedTuples;
        probeState->nextMatchedTupleIdx = 0;
    } while (probeState->numMatchedTuples == 0);
    return true;
}

uint64_t HashJoinProbe::populateResultSet() {
    // Copy the matched value from the build side key data chunk into the resultKeyDataChunk.
    auto numTuplesToRead = probeSideKeyVector->state->isFlat() ? 1 : probeState->numMatchedTuples;
    if (!probeSideKeyVector->state->isFlat() &&
        probeSideKeyVector->state->selVector->selectedSize != numTuplesToRead) {
        // Update probeSideKeyVector's selectedPositions when the probe side is unflat and its
        // selected positions need to change (i.e., some keys has no matched tuples).
        auto keySelectedBuffer = probeSideKeyVector->state->selVector->getSelectedPositionsBuffer();
        for (auto i = 0u; i < numTuplesToRead; i++) {
            keySelectedBuffer[i] = probeState->probeSelVector->selectedPositions[i];
        }
        probeSideKeyVector->state->selVector->selectedSize = numTuplesToRead;
        probeSideKeyVector->state->selVector->resetSelectorToValuePosBuffer();
    }
    sharedState->getHashTable()->lookup(vectorsToReadInto, columnIdxsToReadFrom,
        probeState->matchedTuples.get(), probeState->nextMatchedTupleIdx, numTuplesToRead);
    probeState->nextMatchedTupleIdx += numTuplesToRead;
    return numTuplesToRead;
}

// The general flow of a hash join probe:
// 1) find matched tuples of probe side key from ht.
// 2) populate values from matched tuples into resultKeyDataChunk , buildSideFlatResultDataChunk
// (all flat data chunks from the build side are merged into one) and buildSideVectorPtrs (each
// VectorPtr corresponds to one unFlat build side data chunk that is appended to the resultSet).
bool HashJoinProbe::getNextTuples() {
    metrics->executionTime.start();
    if (sharedState->getHashTable()->getNumTuples() == 0) {
        metrics->executionTime.stop();
        return false;
    }
    if (!getNextBatchOfMatchedTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    auto numPopulatedTuples = populateResultSet();
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(numPopulatedTuples);
    return true;
}
} // namespace processor
} // namespace graphflow

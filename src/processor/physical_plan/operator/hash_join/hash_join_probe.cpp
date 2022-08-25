#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

#include "src/function/hash/include/vector_hash_operations.h"
#include "src/function/hash/operations/include/hash_operations.h"

using namespace graphflow::function::operation;

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> HashJoinProbe::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    probeState = make_unique<ProbeState>();
    probeSideKeyVector = resultSet->dataChunks[probeDataInfo.getKeyIDDataChunkPos()]
                             ->valueVectors[probeDataInfo.getKeyIDVectorPos()];
    for (auto pos : flatDataChunkPositions) {
        auto dataChunk = resultSet->dataChunks[pos];
        dataChunk->state = DataChunkState::getSingleValueDataChunkState();
    }
    for (auto i = 0u; i < probeDataInfo.nonKeyOutputDataPos.size(); ++i) {
        auto probeSideNonKeyVector = make_shared<ValueVector>(
            sharedState->getNonKeyDataPosesDataTypes()[i], context->memoryManager);
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
    if (probeState->nextMatchedTupleIdx < probeState->matchedSelVector->selectedSize) {
        return true;
    }
    auto keys = (nodeID_t*)probeSideKeyVector->values;
    do {
        if (probeState->probeSelVector->selectedSize == 0) {
            restoreSelVector(probeSideKeyVector->state->selVector.get());
            if (!children[0]->getNextTuples()) {
                return false;
            }
            saveSelVector(probeSideKeyVector->state->selVector.get());
            sharedState->getHashTable()->probe(
                *probeSideKeyVector, probeState->probedTuples.get(), *probeState->probeSelVector);
        }
        auto numMatchedTuples = 0;
        while (true) {
            if (numMatchedTuples == DEFAULT_VECTOR_CAPACITY ||
                probeState->probeSelVector->selectedSize == 0) {
                break;
            }
            auto numNonNullProbedTuples = 0;
            for (auto i = 0u; i < probeState->probeSelVector->selectedSize; i++) {
                auto pos = probeState->probeSelVector->selectedPositions[i];
                auto currentTuple = probeState->probedTuples[i];
                probeState->matchedTuples[numMatchedTuples] = currentTuple;
                probeState->matchedSelVector->selectedPositions[numMatchedTuples] = pos;
                numMatchedTuples += *(nodeID_t*)currentTuple == keys[pos];
                probeState->probedTuples[numNonNullProbedTuples] =
                    *sharedState->getHashTable()->getPrevTuple(currentTuple);
                probeState->probeSelVector->selectedPositions[numNonNullProbedTuples] = pos;
                numNonNullProbedTuples +=
                    probeState->probedTuples[numNonNullProbedTuples] != nullptr;
            }
            probeState->probeSelVector->selectedSize = numNonNullProbedTuples;
        }
        probeState->matchedSelVector->selectedSize = numMatchedTuples;
        probeState->nextMatchedTupleIdx = 0;
    } while (probeState->matchedSelVector->selectedSize == 0);
    return true;
}

uint64_t HashJoinProbe::populateResultSet() {
    // Note: When the probe side is flat, and the build side: a) has no unflat columns; b) has no
    // payloads in the key data chunk; c) has payloads required to read from hash table, we apply an
    // optimization to unflat the probe side payloads in the resultSet.
    auto numTuplesToRead = isScanOneRow ? 1 : probeState->matchedSelVector->selectedSize;
    if (!probeSideKeyVector->state->isFlat() &&
        probeSideKeyVector->state->selVector->selectedSize != numTuplesToRead) {
        // Update probeSideKeyVector's selectedPositions when the probe side is unflat and its
        // selected positions need to change (i.e., some keys has no matched tuples).
        auto keySelectedBuffer = probeSideKeyVector->state->selVector->getSelectedPositionsBuffer();
        for (auto i = 0u; i < numTuplesToRead; i++) {
            keySelectedBuffer[i] = probeState->matchedSelVector->selectedPositions[i];
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

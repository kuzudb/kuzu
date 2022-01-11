#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

#include "src/common/include/operations/hash_operations.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> HashJoinProbe::initResultSet() {
    resultSet = children[0]->initResultSet();
    probeState = make_unique<ProbeState>(DEFAULT_VECTOR_CAPACITY);
    constructResultVectorsPosAndFieldsToRead();
    initializeResultSet();
    return resultSet;
}

void HashJoinProbe::constructResultVectorsPosAndFieldsToRead() {
    // Skip the first key field.
    auto fieldId = 1;
    for (auto& dataPos : probeDataInfo.nonKeyOutputDataPos) {
        resultVectorsPos.push_back(dataPos);
        fieldsToRead.push_back(fieldId++);
    }
}

void HashJoinProbe::initializeResultSet() {
    probeSideKeyVector = resultSet->dataChunks[probeDataInfo.getKeyIDDataChunkPos()]
                             ->valueVectors[probeDataInfo.getKeyIDVectorPos()];
    auto rowCollectionLayout = sharedState->rowCollection->getLayout();
    // Skip the first key field.
    auto fieldIdx = 1;
    for (auto i = 0u; i < probeDataInfo.nonKeyOutputDataPos.size(); ++i) {
        auto probeSideVector = make_shared<ValueVector>(
            context.memoryManager, rowCollectionLayout.fields[fieldIdx + i].dataType);
        auto [dataChunkPos, valueVectorPos] = probeDataInfo.nonKeyOutputDataPos[i];
        resultSet->dataChunks[dataChunkPos]->insert(valueVectorPos, probeSideVector);
    }
}

void HashJoinProbe::getNextBatchOfMatchedTuples() {
    probeState->numMatchedTuples = 0;
    tuplePosToReadInProbedState = 0;
    do {
        if (probeState->numMatchedTuples == 0) {
            if (!children[0]->getNextTuples()) {
                probeState->numMatchedTuples = 0;
                return;
            }
            auto currentIdx = probeSideKeyVector->state->getPositionOfCurrIdx();
            if (probeSideKeyVector->isNull(currentIdx)) {
                continue;
            }
            probeSideKeyVector->readNodeID(currentIdx, probeState->probeSideKeyNodeID);
            auto directory = (uint8_t**)sharedState->htDirectory->data;
            hash_t hash;
            Hash::operation<nodeID_t>(probeState->probeSideKeyNodeID, false /* isNull */, hash);
            hash = hash & sharedState->hashBitMask;
            probeState->probedTuple = (uint8_t*)(directory[hash]);
        }
        while (probeState->probedTuple) {
            if (DEFAULT_VECTOR_CAPACITY == probeState->numMatchedTuples) {
                break;
            }
            auto nodeID = *(nodeID_t*)probeState->probedTuple;
            probeState->matchedTuples[probeState->numMatchedTuples] = probeState->probedTuple;
            probeState->numMatchedTuples += nodeID == probeState->probeSideKeyNodeID;
            probeState->probedTuple =
                *(uint8_t**)(probeState->probedTuple +
                             sharedState->rowCollection->getLayout().getNullMapOffset() -
                             sizeof(uint8_t*));
        }
    } while (probeState->numMatchedTuples == 0);
}

void HashJoinProbe::populateResultSet() {
    // Copy the matched value from the build side key data chunk into the resultKeyDataChunk.
    assert(tuplePosToReadInProbedState < probeState->numMatchedTuples);
    // Consider the following plan with a HashJoin on c.
    // Build side: Scan(a) -> Extend(b) -> Extend(c) -> Projection(c)
    // Probe side: Scan(e) -> Extend(d) -> Extend(c)
    // Build side only has one hash column but no payload column. In such case, a hash probe can
    // only read rows one by one (or updating multiplicity) because there is no payload unFlat
    // vector to represent multiple rows.
    auto numRowsToRead =
        fieldsToRead.empty() ? 1 : probeState->numMatchedTuples - tuplePosToReadInProbedState;
    tuplePosToReadInProbedState +=
        sharedState->rowCollection->lookup(fieldsToRead, resultVectorsPos, *resultSet,
            probeState->matchedTuples.get(), tuplePosToReadInProbedState, numRowsToRead);
}

// The general flow of a hash join probe:
// 1) find matched tuples of probe side key from ht.
// 2) populate values from matched tuples into resultKeyDataChunk , buildSideFlatResultDataChunk
// (all flat data chunks from the build side are merged into one) and buildSideVectorPtrs (each
// VectorPtr corresponds to one unFlat build side data chunk that is appended to the resultSet).
// 3) flat buildSideFlatResultDataChunk, updating its currIdx, and also populates appended unFlat
// data chunks. If there is no appended unFlat data chunks, which means buildSideVectorPtrs is
// empty, directly unFlat buildSideFlatResultDataChunk (if it exists).
bool HashJoinProbe::getNextTuples() {
    metrics->executionTime.start();
    if (tuplePosToReadInProbedState < probeState->numMatchedTuples) {
        populateResultSet();
        metrics->executionTime.stop();
        metrics->numOutputTuple.increase(probeState->numMatchedTuples);
        return true;
    }
    getNextBatchOfMatchedTuples();
    if (probeState->numMatchedTuples == 0) {
        metrics->executionTime.stop();
        return false;
    }
    populateResultSet();
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(probeState->numMatchedTuples);
    return true;
}
} // namespace processor
} // namespace graphflow

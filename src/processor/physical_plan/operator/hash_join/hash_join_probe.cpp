#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

#include "src/common/include/operations/hash_operations.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::HashJoinProbe(uint64_t buildSideKeyDataChunkPos,
    uint64_t buildSideKeyVectorPos, const vector<bool>& buildSideDataChunkPosToIsFlat,
    uint64_t probeSideKeyDataChunkPos, uint64_t probeSideKeyVectorPos,
    unique_ptr<PhysicalOperator> buildSidePrevOp, unique_ptr<PhysicalOperator> probeSidePrevOp,
    ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(probeSidePrevOp), HASH_JOIN_PROBE, IS_OUT_DATACHUNK_FILTERED, context,
          id},
      buildSidePrevOp{move(buildSidePrevOp)}, buildSideKeyDataChunkPos{buildSideKeyDataChunkPos},
      buildSideKeyVectorPos{buildSideKeyVectorPos},
      buildSideDataChunkPosToIsFlat{buildSideDataChunkPosToIsFlat},
      probeSideKeyDataChunkPos{probeSideKeyDataChunkPos}, probeSideKeyVectorPos{
                                                              probeSideKeyVectorPos} {
    // The buildSidePrevOp (HashJoinBuild) yields no actual resultSet, we get it from it's prevOp.
    buildSideResultSet = this->buildSidePrevOp->prevOperator->getResultSet();
    // The prevOperator is the probe side prev operator passed in to the PhysicalOperator
    resultSet = prevOperator->getResultSet();
    numProbeSidePrevDataChunks = resultSet->dataChunks.size();
    probeSideKeyDataChunk = resultSet->dataChunks[probeSideKeyDataChunkPos];
    // The probe side key data chunk is also the output keyDataChunk in the resultSet. Non-key
    // vectors from the build side key data chunk are appended into the the probeSideKeyDataChunk.
    resultKeyDataChunk = probeSideKeyDataChunk;
    numProbeSidePrevKeyValueVectors = probeSideKeyDataChunk->valueVectors.size();
    probeSideKeyVector = static_pointer_cast<NodeIDVector>(
        probeSideKeyDataChunk->getValueVector(probeSideKeyVectorPos));
    probeState = make_unique<ProbeState>(DEFAULT_VECTOR_CAPACITY);
    initializeResultSetAndVectorPtrs();
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::createVectorsFromExistingOnesAndAppend(
    DataChunk& inDataChunk, DataChunk& resultDataChunk, vector<uint64_t>& vectorPositions) {
    for (auto pos : vectorPositions) {
        auto inVector = inDataChunk.valueVectors[pos];
        shared_ptr<ValueVector> resultVector;
        if (inVector->dataType == NODE) {
            auto nodeIDVector = static_pointer_cast<NodeIDVector>(inVector);
            resultVector = make_shared<NodeIDVector>(nodeIDVector->representation.commonLabel,
                nodeIDVector->representation.compressionScheme, false);
        } else {
            resultVector = make_shared<ValueVector>(context.memoryManager, inVector->dataType);
        }
        resultDataChunk.append(resultVector);
    }
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::createVectorPtrs(DataChunk& buildSideDataChunk) {
    for (auto j = 0u; j < buildSideDataChunk.valueVectors.size(); j++) {
        auto vectorPtrs = make_unique<overflow_value_t[]>(DEFAULT_VECTOR_CAPACITY);
        BuildSideVectorInfo vectorInfo(buildSideDataChunk.valueVectors[j]->getNumBytesPerValue(),
            resultSet->dataChunks.size(), j, buildSideVectorPtrs.size());
        buildSideVectorInfos.push_back(vectorInfo);
        buildSideVectorPtrs.push_back(move(vectorPtrs));
    }
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::initializeResultSetAndVectorPtrs() {
    // Initialize vectors for build side key data chunk (except for the key vector, which is already
    // included in the probe side key data chunk) and append them into the resultKeyDataChunk.
    auto buildSideKeyDataChunk = buildSideResultSet->dataChunks[buildSideKeyDataChunkPos];
    vector<uint64_t> buildSideKeyVectorPositions;
    for (auto i = 0u; i < buildSideKeyDataChunk->valueVectors.size(); i++) {
        if (i == buildSideKeyVectorPos) {
            continue;
        }
        buildSideKeyVectorPositions.push_back(i);
    }
    createVectorsFromExistingOnesAndAppend(
        *buildSideKeyDataChunk, *resultKeyDataChunk, buildSideKeyVectorPositions);
    // Create vectors for build side non-key data chunks and append them into the resultSet:
    // 1. Merge all (if any) flat non-key data chunks into buildSideFlatResultDataChunk. Notice that
    // if build side has no flat data chunks, the buildSideFlatResultDataChunk can be empty, and not
    // appended into the resultSet.
    // 2. For each unFlat data chunk, create a new one, and allocate vectorPtrs.
    buildSideFlatResultDataChunk = make_shared<DataChunk>();
    for (auto i = 0u; i < buildSideResultSet->dataChunks.size(); i++) {
        if (i == buildSideKeyDataChunkPos) {
            continue;
        }
        auto& dataChunk = buildSideResultSet->dataChunks[i];
        vector<uint64_t> dataChunkVectorPositions;
        for (auto j = 0u; j < dataChunk->valueVectors.size(); j++) {
            dataChunkVectorPositions.push_back(j);
        }
        if (buildSideDataChunkPosToIsFlat[i]) {
            if (buildSideFlatResultDataChunk->valueVectors.empty()) {
                buildSideFlatResultDataChunk = make_shared<DataChunk>();
                resultSet->append(buildSideFlatResultDataChunk);
            }
            createVectorsFromExistingOnesAndAppend(
                *dataChunk, *buildSideFlatResultDataChunk, dataChunkVectorPositions);
        } else {
            auto unFlatOutDataChunk = make_shared<DataChunk>();
            createVectorsFromExistingOnesAndAppend(
                *dataChunk, *unFlatOutDataChunk, dataChunkVectorPositions);
            createVectorPtrs(*dataChunk);
            resultSet->append(unFlatOutDataChunk);
        }
    }
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::getNextBatchOfMatchedTuples() {
    do {
        if (probeState->matchedTuplesSize == 0) {
            prevOperator->getNextTuples();
            if (probeSideKeyDataChunk->state->size == 0) {
                return;
            }
            probeSideKeyVector->readNodeID(probeSideKeyVector->state->getCurrSelectedValuesPos(),
                probeState->probeSideKeyNodeID);
            auto directory = (uint8_t**)sharedState->htDirectory->data;
            auto hash = Hash::operation<nodeID_t>(probeState->probeSideKeyNodeID) &
                        sharedState->hashBitMask;
            probeState->probedTuple = (uint8_t*)(directory[hash]);
        }
        nodeID_t nodeIDFromHT;
        while (probeState->probedTuple) {
            if (DEFAULT_VECTOR_CAPACITY == probeState->matchedTuplesSize) {
                break;
            }
            memcpy(&nodeIDFromHT, probeState->probedTuple, NUM_BYTES_PER_NODE_ID);
            probeState->matchedTuples[probeState->matchedTuplesSize] = probeState->probedTuple;
            probeState->matchedTuplesSize +=
                (nodeIDFromHT.label == probeState->probeSideKeyNodeID.label &&
                    nodeIDFromHT.offset == probeState->probeSideKeyNodeID.offset);
            probeState->probedTuple =
                *(uint8_t**)(probeState->probedTuple + sharedState->numBytesForFixedTuplePart -
                             sizeof(uint8_t*));
        }
    } while (probeState->matchedTuplesSize == 0);
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::copyTuplesFromHT(DataChunk& resultDataChunk,
    uint64_t numResultVectors, uint64_t resultVectorStartPosition, uint64_t& tupleReadOffset,
    uint64_t startOffsetInResultVector, uint64_t numTuples) {
    for (auto i = 0u; i < numResultVectors; i++) {
        auto resultVector = resultDataChunk.getValueVector(resultVectorStartPosition++);
        auto numBytesPerValue = resultVector->getNumBytesPerValue();
        for (auto j = 0u; j < numTuples; j++) {
            memcpy(resultVector->values + (startOffsetInResultVector * numBytesPerValue),
                probeState->matchedTuples[j] + tupleReadOffset, numBytesPerValue);
            startOffsetInResultVector++;
        }
        tupleReadOffset += numBytesPerValue;
    }
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::populateResultFlatDataChunksAndVectorPtrs() {
    // Copy the matched value from the build side key data chunk into the resultKeyDataChunk.
    auto tupleReadOffset = NUM_BYTES_PER_NODE_ID;
    copyTuplesFromHT(*resultKeyDataChunk,
        buildSideResultSet->dataChunks[buildSideKeyDataChunkPos]->valueVectors.size() - 1,
        numProbeSidePrevKeyValueVectors, tupleReadOffset,
        resultKeyDataChunk->state->getCurrSelectedValuesPos(), 1);
    // Copy the matched values from the build side non-key data chunks.
    auto buildSideVectorPtrsPos = 0;
    auto mergedFlatDataChunkVectorPos = 0;
    for (auto i = 0u; i < buildSideResultSet->dataChunks.size(); i++) {
        if (i == buildSideKeyDataChunkPos) {
            continue;
        }
        auto dataChunk = buildSideResultSet->dataChunks[i];
        if (buildSideDataChunkPosToIsFlat[i]) {
            copyTuplesFromHT(*buildSideFlatResultDataChunk, dataChunk->valueVectors.size(),
                mergedFlatDataChunkVectorPos, tupleReadOffset, 0, probeState->matchedTuplesSize);
            mergedFlatDataChunkVectorPos += dataChunk->valueVectors.size();
        } else {
            for (auto& vector : dataChunk->valueVectors) {
                for (auto j = 0u; j < probeState->matchedTuplesSize; j++) {
                    buildSideVectorPtrs[buildSideVectorPtrsPos][j] =
                        *(overflow_value_t*)(probeState->matchedTuples[j] + tupleReadOffset);
                }
                tupleReadOffset += sizeof(overflow_value_t);
                buildSideVectorPtrsPos++;
            }
        }
    }
    buildSideFlatResultDataChunk->state->size = probeState->matchedTuplesSize;
    probeState->matchedTuplesSize = 0;
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::updateAppendedUnFlatDataChunks() {
    for (auto& buildVectorInfo : buildSideVectorInfos) {
        auto outDataChunk = resultSet->dataChunks[buildVectorInfo.resultDataChunkPos];
        auto appendVectorData =
            outDataChunk->getValueVector(buildVectorInfo.resultVectorPos)->values;
        auto overflowVal = buildSideVectorPtrs[buildVectorInfo.vectorPtrsPos].operator[](
            buildSideFlatResultDataChunk->state->currPos);
        memcpy(appendVectorData, overflowVal.value, overflowVal.len);
        outDataChunk->state->size = overflowVal.len / buildVectorInfo.numBytesPerValue;
    }
}

// The general flow of a hash join probe:
// 1) find matched tuples of probe side key from ht.
// 2) populate values from matched tuples into resultKeyDataChunk , buildSideFlatResultDataChunk
// (all flat data chunks from the build side are merged into one) and buildSideVectorPtrs (each
// VectorPtr corresponds to one unFlat build side data chunk that is appended to the resultSet).
// 3) flat buildSideFlatResultDataChunk, updating its currPos, and also populates appended unFlat
// data chunks. If there is no appended unFlat data chunks, which means buildSideVectorPtrs is
// empty, directly unFlat buildSideFlatResultDataChunk (if it exists).
template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::getNextTuples() {
    metrics->executionTime.start();
    if (!buildSideVectorPtrs.empty() &&
        buildSideFlatResultDataChunk->state->currPos <
            (int64_t)(buildSideFlatResultDataChunk->state->size - 1)) {
        buildSideFlatResultDataChunk->state->currPos += 1;
        updateAppendedUnFlatDataChunks();
        if constexpr (IS_OUT_DATACHUNK_FILTERED) {
            for (uint64_t i = (numProbeSidePrevDataChunks); i < resultSet->dataChunks.size(); i++) {
                resultSet->dataChunks[i]->state->resetSelector();
            }
        }
        metrics->executionTime.stop();
        metrics->numOutputTuple.increase(resultSet->getNumTuples());
        return;
    }
    getNextBatchOfMatchedTuples();
    if (probeState->matchedTuplesSize == 0) {
        // No matching tuples, set size of appended data chunks to 0.
        buildSideFlatResultDataChunk->state->size = 0;
        for (auto& buildVectorInfo : buildSideVectorInfos) {
            auto resultDataChunk = resultSet->dataChunks[buildVectorInfo.resultDataChunkPos];
            resultDataChunk->state->size = 0;
        }
        return;
    }
    populateResultFlatDataChunksAndVectorPtrs();
    // UnFlat the buildSideFlatResultDataChunk if there is no build side unFlat non-key data chunks.
    buildSideFlatResultDataChunk->state->currPos = buildSideVectorPtrs.empty() ? -1 : 0;
    updateAppendedUnFlatDataChunks();
    if constexpr (IS_OUT_DATACHUNK_FILTERED) {
        for (uint64_t i = (numProbeSidePrevDataChunks); i < resultSet->dataChunks.size(); i++) {
            resultSet->dataChunks[i]->state->resetSelector();
        }
    }
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(resultSet->getNumTuples());
}

template class HashJoinProbe<true>;
template class HashJoinProbe<false>;
} // namespace processor
} // namespace graphflow

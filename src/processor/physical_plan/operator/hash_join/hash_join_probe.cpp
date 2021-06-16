#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::HashJoinProbe(uint64_t buildSideKeyDataChunkPos,
    uint64_t buildSideKeyVectorPos, vector<bool> buildSideDataChunkPosToIsFlat,
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
    buildSideKeyDataChunk = buildSideResultSet->dataChunks[buildSideKeyDataChunkPos];
    // The prevOperator is the probe side prev operator passed in to the PhysicalOperator
    probeSideResultSet = prevOperator->getResultSet();
    probeSideKeyDataChunk = probeSideResultSet->dataChunks[probeSideKeyDataChunkPos];
    probeSideKeyVector = static_pointer_cast<NodeIDVector>(
        probeSideKeyDataChunk->getValueVector(probeSideKeyVectorPos));

    decompressedProbeKeyVector = make_shared<NodeIDVector>(0, NodeIDCompressionScheme(8, 8), false);
    decompressedProbeKeyVector->state = probeSideKeyVector->state;
    hashedProbeKeyVector = make_shared<ValueVector>(INT64);
    hashedProbeKeyVector->state = probeSideKeyVector->state;

    probeState = make_unique<ProbeState>(DEFAULT_VECTOR_CAPACITY);

    initializeOutResultSetAndVectorPtrs();
}

static void createVectorsFromExistingOnesAndAppend(
    DataChunk& inDataChunk, DataChunk& outDataChunk, vector<uint64_t>& vectorPositions) {
    for (auto pos : vectorPositions) {
        auto inVector = inDataChunk.valueVectors[pos];
        shared_ptr<ValueVector> outVector;
        if (inVector->dataType == NODE) {
            auto nodeIDVector = static_pointer_cast<NodeIDVector>(inVector);
            outVector = make_shared<NodeIDVector>(nodeIDVector->representation.commonLabel,
                nodeIDVector->representation.compressionScheme, false);
        } else {
            outVector = make_shared<ValueVector>(inVector->dataType);
        }
        outDataChunk.append(outVector);
    }
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::initializeOutResultSetAndVectorPtrs() {
    resultSet = make_shared<ResultSet>();
    outKeyDataChunk = make_unique<DataChunk>();
    resultSet->append(outKeyDataChunk);
    vector<uint64_t> probeSideKeyVectorPositions(probeSideKeyDataChunk->valueVectors.size());
    for (auto i = 0u; i < probeSideKeyDataChunk->valueVectors.size(); i++) {
        probeSideKeyVectorPositions.push_back(i);
    }
    createVectorsFromExistingOnesAndAppend(
        *probeSideKeyDataChunk, *outKeyDataChunk, probeSideKeyVectorPositions);
    for (auto i = 0u; i < probeSideResultSet->dataChunks.size(); i++) {
        if (i != probeSideKeyDataChunkPos) {
            resultSet->append(probeSideResultSet->dataChunks[i]);
        }
    }
    vector<uint64_t> buildSideKeyVectorPositions(buildSideKeyDataChunk->valueVectors.size() - 1);
    for (auto i = 0u; i < buildSideKeyDataChunk->valueVectors.size(); i++) {
        if (i == buildSideKeyVectorPos) {
            continue;
        }
        buildSideKeyVectorPositions.push_back(i);
    }
    createVectorsFromExistingOnesAndAppend(
        *buildSideKeyDataChunk, *outKeyDataChunk, buildSideKeyVectorPositions);

    for (auto i = 0u; i < buildSideResultSet->dataChunks.size(); i++) {
        if (i == buildSideKeyDataChunkPos) {
            continue;
        }
        auto dataChunk = buildSideResultSet->dataChunks[i];
        vector<uint64_t> dataChunkVectorPositions(dataChunk->valueVectors.size());
        for (auto j = 0u; j < dataChunk->valueVectors.size(); j++) {
            dataChunkVectorPositions.push_back(j);
        }
        if (buildSideDataChunkPosToIsFlat[i]) {
            createVectorsFromExistingOnesAndAppend(
                *dataChunk, *outKeyDataChunk, dataChunkVectorPositions);
        } else {
            auto unFlatOutDataChunk = make_shared<DataChunk>();
            createVectorsFromExistingOnesAndAppend(
                *dataChunk, *unFlatOutDataChunk, dataChunkVectorPositions);
            for (auto j = 0u; j < dataChunk->valueVectors.size(); j++) {
                auto vectorPtrs = make_unique<overflow_value_t[]>(NODE_SEQUENCE_VECTOR_CAPACITY);
                BuildSideVectorInfo vectorInfo(dataChunk->valueVectors[j]->getNumBytesPerValue(),
                    resultSet->dataChunks.size(), j, buildSideVectorPtrs.size());
                buildSideVectorInfos.push_back(vectorInfo);
                buildSideVectorPtrs.push_back(move(vectorPtrs));
            }
            resultSet->append(unFlatOutDataChunk);
        }
    }
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::probeHTDirectory() {
    auto keyCount = hashedProbeKeyVector->state->isFlat() ? 1 : hashedProbeKeyVector->state->size;
    VectorNodeIDOperations::Hash(*probeSideKeyVector, *hashedProbeKeyVector);
    auto hashes = (uint64_t*)hashedProbeKeyVector->values;
    auto directory = (uint8_t**)sharedState->htDirectory->blockPtr;
    if (hashedProbeKeyVector->state->isFlat()) {
        auto hash = hashes[hashedProbeKeyVector->state->getCurrSelectedValuesPos()] &
                    sharedState->hashBitMask;
        probeState->probedTuples[0] = (uint8_t*)(directory[hash]);
    } else {
        for (auto i = 0u; i < keyCount; i++) {
            auto pos = hashedProbeKeyVector->state->selectedValuesPos[i];
            auto hash = hashes[pos] & sharedState->hashBitMask;
            probeState->probedTuples[i] = (uint8_t*)(directory[hash]);
        }
    }
    probeState->probedTuplesSize = keyCount;
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::getNextBatchOfMatchedTuples() {
    do {
        if (probeState->probedTuplesSize == 0 ||
            probeState->probeKeyPos == probeState->probedTuplesSize) {
            prevOperator->getNextTuples();
            if (probeSideKeyDataChunk->state->size == 0) {
                probeState->probedTuplesSize = 0;
                return;
            }
            probeHTDirectory();
            VectorNodeIDOperations::Decompress(*probeSideKeyVector, *decompressedProbeKeyVector);
            probeState->probeKeyPos = 0;
        }
        nodeID_t nodeId;
        auto decompressedProbeKeys = (nodeID_t*)decompressedProbeKeyVector->values;
        uint64_t decompressedProbeKeyPos, probeSelPos;
        for (uint64_t i = probeState->probeKeyPos; i < probeState->probedTuplesSize; i++) {
            while (probeState->probedTuples[i]) {
                if (NODE_SEQUENCE_VECTOR_CAPACITY == probeState->matchedTuplesSize) {
                    break;
                }
                memcpy(&nodeId, probeState->probedTuples[i], NUM_BYTES_PER_NODE_ID);
                probeState->matchedTuples[probeState->matchedTuplesSize] =
                    probeState->probedTuples[i];
                if (probeSideKeyDataChunk->state->isFlat()) {
                    probeSelPos = probeSideKeyDataChunk->state->getCurrSelectedValuesPos();
                    decompressedProbeKeyPos =
                        decompressedProbeKeyVector->state->getCurrSelectedValuesPos();
                } else {
                    probeSelPos =
                        probeSideKeyDataChunk->state->selectedValuesPos[probeState->probeKeyPos];
                    decompressedProbeKeyPos =
                        decompressedProbeKeyVector->state->selectedValuesPos[i];
                }
                probeState->probeSelVector[probeState->matchedTuplesSize] = probeSelPos;
                probeState->matchedTuplesSize +=
                    (nodeId.label == decompressedProbeKeys[decompressedProbeKeyPos].label &&
                        nodeId.offset == decompressedProbeKeys[decompressedProbeKeyPos].offset);
                probeState->probedTuples[i] =
                    *(uint8_t**)(probeState->probedTuples[i] +
                                 sharedState->numBytesForFixedTuplePart - sizeof(uint8_t*));
            }
            probeState->probeKeyPos++;
        }
    } while (probeState->matchedTuplesSize == 0);
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::populateOutKeyDataChunkAndVectorPtrs() {
    for (auto i = 0u; i < probeSideKeyDataChunk->valueVectors.size(); i++) {
        auto probeVector = probeSideKeyDataChunk->getValueVector(i);
        auto probeVectorValues = probeVector->values;
        auto outKeyVector = outKeyDataChunk->getValueVector(i);
        auto outKeyVectorValues = outKeyVector->values;
        auto numBytesPerValue = outKeyVector->getNumBytesPerValue();
        if (probeVector->dataType == NODE &&
            static_pointer_cast<NodeIDVector>(probeVector)->isSequence()) {
            auto outKeyVectorNodeOffsets = (node_offset_t*)outKeyVectorValues;
            auto probeKeyStartOffset = *(node_offset_t*)(probeVectorValues);
            for (auto j = 0u; j < probeState->matchedTuplesSize; j++) {
                outKeyVectorNodeOffsets[j] = probeKeyStartOffset + probeState->probeSelVector[j];
            }
            static_pointer_cast<NodeIDVector>(outKeyVector)->representation.commonLabel =
                static_pointer_cast<NodeIDVector>(probeVector)->representation.commonLabel;
        } else {
            for (auto j = 0u; j < probeState->matchedTuplesSize; j++) {
                memcpy(outKeyVectorValues + (j * numBytesPerValue),
                    probeVectorValues + (probeState->probeSelVector[j] * numBytesPerValue),
                    numBytesPerValue);
            }
        }
    }
    auto outKeyDataChunkVectorPos = probeSideKeyDataChunk->valueVectors.size();
    auto tupleReadOffset = NUM_BYTES_PER_NODE_ID;
    for (auto i = 0u; i < buildSideKeyDataChunk->valueVectors.size(); i++) {
        if (i == buildSideKeyVectorPos) {
            continue;
        }
        auto outVectorValues = outKeyDataChunk->getValueVector(outKeyDataChunkVectorPos)->values;
        auto numBytesPerValue = buildSideKeyDataChunk->getValueVector(i)->getNumBytesPerValue();
        for (auto j = 0u; j < probeState->matchedTuplesSize; j++) {
            memcpy(outVectorValues + (j * numBytesPerValue),
                probeState->matchedTuples[j] + tupleReadOffset, numBytesPerValue);
        }
        tupleReadOffset += numBytesPerValue;
        outKeyDataChunkVectorPos++;
    }
    auto buildSideVectorPtrsPos = 0;
    for (auto i = 0u; i < buildSideResultSet->dataChunks.size(); i++) {
        if (i == buildSideKeyDataChunkPos) {
            continue;
        }
        auto dataChunk = buildSideResultSet->dataChunks[i];
        if (buildSideDataChunkPosToIsFlat[i]) {
            for (auto& vector : dataChunk->valueVectors) {
                auto outVectorValues =
                    outKeyDataChunk->getValueVector(outKeyDataChunkVectorPos)->values;
                auto numBytesPerValue = vector->getNumBytesPerValue();
                for (auto j = 0u; j < probeState->matchedTuplesSize; j++) {
                    memcpy(outVectorValues + (j * numBytesPerValue),
                        probeState->matchedTuples[j] + tupleReadOffset, numBytesPerValue);
                }
                tupleReadOffset += numBytesPerValue;
                outKeyDataChunkVectorPos++;
            }
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

    outKeyDataChunk->state->size = probeState->matchedTuplesSize;
    probeState->matchedTuplesSize = 0;
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::updateAppendedUnFlatOutResultSet() {
    if (outKeyDataChunk->state->size == 0) {
        for (auto& buildVectorInfo : buildSideVectorInfos) {
            auto outDataChunk = resultSet->dataChunks[buildVectorInfo.outDataChunkPos];
            outDataChunk->state->size = 0;
        }
    } else {
        for (auto& buildVectorInfo : buildSideVectorInfos) {
            auto outDataChunk = resultSet->dataChunks[buildVectorInfo.outDataChunkPos];
            auto appendVectorData =
                outDataChunk->getValueVector(buildVectorInfo.outVectorPos)->values;
            auto overflowVal = buildSideVectorPtrs[buildVectorInfo.vectorPtrsPos].operator[](
                outKeyDataChunk->state->currPos);
            memcpy(appendVectorData, overflowVal.value, overflowVal.len);
            outDataChunk->state->size = overflowVal.len / buildVectorInfo.numBytesPerValue;
        }
    }
}

// The general flow of a hash join probe:
// 1) find matched tuples of probe side keys from ht.
// 2) populate values from matched tuples into outKeyDataChunk and buildSideVectorPtrs.
// 3) if build side doesn't contain any unflat non-key data chunks, which means buildSideVectorPtrs
// is empty, directly unflat outKeyDataChunk. else flat outKeyDataChunk and update currPos of
// outKeyDataChunk, and update appended unflat data chunks based on buildSideVectorPtrs and
// outKeyDataChunk.currPos.
template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::getNextTuples() {
    metrics->executionTime.start();
    if (!buildSideVectorPtrs.empty() &&
        outKeyDataChunk->state->currPos < (int64_t)(outKeyDataChunk->state->size - 1)) {
        outKeyDataChunk->state->currPos += 1;
        updateAppendedUnFlatOutResultSet();
        if constexpr (IS_OUT_DATACHUNK_FILTERED) {
            for (uint64_t i = (probeSideResultSet->dataChunks.size());
                 i < resultSet->dataChunks.size(); i++) {
                resultSet->dataChunks[i]->state->initializeSelector();
            }
        }
        metrics->executionTime.stop();
        metrics->numOutputTuple.increase(resultSet->getNumTuples());
        return;
    }
    getNextBatchOfMatchedTuples();
    populateOutKeyDataChunkAndVectorPtrs();
    if (buildSideVectorPtrs.empty()) {
        outKeyDataChunk->state->currPos = -1;
    } else {
        outKeyDataChunk->state->currPos = 0;
        updateAppendedUnFlatOutResultSet();
    }
    if constexpr (IS_OUT_DATACHUNK_FILTERED) {
        outKeyDataChunk->state->initializeSelector();
        for (uint64_t i = (probeSideResultSet->dataChunks.size()); i < resultSet->dataChunks.size();
             i++) {
            resultSet->dataChunks[i]->state->initializeSelector();
        }
    }
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(resultSet->getNumTuples());
}

template class HashJoinProbe<true>;
template class HashJoinProbe<false>;
} // namespace processor
} // namespace graphflow

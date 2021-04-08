#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::HashJoinProbe(uint64_t buildSideKeyDataChunkPos,
    uint64_t buildSideKeyVectorPos, uint64_t probeSideKeyDataChunkPos,
    uint64_t probeSideKeyVectorPos, unique_ptr<PhysicalOperator> buildSidePrevOp,
    unique_ptr<PhysicalOperator> probeSidePrevOp)
    : PhysicalOperator(move(probeSidePrevOp), HASH_JOIN_PROBE, IS_OUT_DATACHUNK_FILTERED),
      buildSidePrevOp(move(buildSidePrevOp)), buildSideKeyDataChunkPos(buildSideKeyDataChunkPos),
      buildSideKeyVectorPos(buildSideKeyVectorPos),
      probeSideKeyDataChunkPos(probeSideKeyDataChunkPos),
      probeSideKeyVectorPos(probeSideKeyVectorPos) {
    auto buildSideDataChunks = this->buildSidePrevOp->getDataChunks();
    buildSideKeyDataChunk = buildSideDataChunks->getDataChunk(buildSideKeyDataChunkPos);
    buildSideNonKeyDataChunks = make_shared<DataChunks>();
    for (uint64_t i = 0; i < buildSideDataChunks->getNumDataChunks(); i++) {
        if (i != buildSideKeyDataChunkPos) {
            buildSideNonKeyDataChunks->append(buildSideDataChunks->getDataChunk(i));
        }
    }
    // The prevOperator is the probe side prev operator passed in to the PhysicalOperator
    auto probeSideDataChunks = prevOperator->getDataChunks();
    probeSideKeyDataChunk = probeSideDataChunks->getDataChunk(probeSideKeyDataChunkPos);
    probeSideKeyVector = static_pointer_cast<NodeIDVector>(
        probeSideKeyDataChunk->getValueVector(probeSideKeyVectorPos));
    probeSideNonKeyDataChunks = make_shared<DataChunks>();
    for (uint64_t i = 0; i < probeSideDataChunks->getNumDataChunks(); i++) {
        if (i != probeSideKeyDataChunkPos) {
            probeSideNonKeyDataChunks->append(probeSideDataChunks->getDataChunk(i));
        }
    }

    vectorDecompressOp = ValueVector::getUnaryOperation(DECOMPRESS_NODE_ID);
    vectorHashOp = ValueVector::getUnaryOperation(HASH_NODE_ID);
    decompressedProbeKeyVector = make_unique<NodeIDVector>(NodeIDCompressionScheme(8, 8));
    hashedProbeKeyVector = make_unique<ValueVector>(INT64);

    probeState = make_unique<ProbeState>(MAX_VECTOR_SIZE);

    initializeOutDataChunksAndVectorPtrs();
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::initializeOutDataChunksAndVectorPtrs() {
    dataChunks = make_shared<DataChunks>();
    outKeyDataChunk = make_unique<DataChunk>();
    dataChunks->append(outKeyDataChunk);
    for (uint64_t i = 0; i < probeSideNonKeyDataChunks->getNumDataChunks(); i++) {
        dataChunks->append(probeSideNonKeyDataChunks->getDataChunk(i));
    }

    for (uint64_t i = 0; i < probeSideKeyDataChunk->getNumAttributes(); i++) {
        auto probeSideVector = probeSideKeyDataChunk->getValueVector(i);
        shared_ptr<ValueVector> outVector;
        if (probeSideVector->getDataType() == NODE) {
            auto probeSideNodeIDVector = static_pointer_cast<NodeIDVector>(probeSideVector);
            outVector = make_shared<NodeIDVector>(probeSideNodeIDVector->getCommonLabel(),
                probeSideNodeIDVector->getCompressionScheme());
        } else {
            outVector = make_shared<ValueVector>(probeSideVector->getDataType());
        }
        outKeyDataChunk->append(outVector);
        outVector->setDataChunkOwner(outKeyDataChunk);
    }
    for (uint64_t i = 0; i < buildSideKeyDataChunk->getNumAttributes(); i++) {
        if (i == buildSideKeyVectorPos) {
            continue;
        }
        auto buildSideVector = buildSideKeyDataChunk->getValueVector(i);
        shared_ptr<ValueVector> outVector;
        if (buildSideVector->getDataType() == NODE) {
            auto buildSideNodeIDVector = static_pointer_cast<NodeIDVector>(buildSideVector);
            outVector = make_shared<NodeIDVector>(buildSideNodeIDVector->getCommonLabel(),
                buildSideNodeIDVector->getCompressionScheme());
        } else {
            outVector = make_shared<ValueVector>(buildSideVector->getDataType());
        }
        outKeyDataChunk->append(outVector);
        outVector->setDataChunkOwner(outKeyDataChunk);
    }

    for (uint64_t i = 0; i < buildSideNonKeyDataChunks->getNumDataChunks(); i++) {
        auto dataChunk = buildSideNonKeyDataChunks->getDataChunk(i);
        if (dataChunk->isFlat()) {
            for (auto& vector : dataChunk->valueVectors) {
                shared_ptr<ValueVector> outVector;
                if (vector->getDataType() == NODE) {
                    auto nodeIDVector = static_pointer_cast<NodeIDVector>(vector);
                    outVector = make_shared<NodeIDVector>(
                        nodeIDVector->getCommonLabel(), nodeIDVector->getCompressionScheme());
                } else {
                    outVector = make_shared<ValueVector>(vector->getDataType());
                }
                outKeyDataChunk->append(outVector);
                outVector->setDataChunkOwner(outKeyDataChunk);
            }
        } else {
            auto unFlatOutDataChunk = make_shared<DataChunk>();
            for (auto& vector : dataChunk->valueVectors) {
                shared_ptr<ValueVector> outVector;
                if (vector->getDataType() == NODE) {
                    auto nodeIDVector = static_pointer_cast<NodeIDVector>(vector);
                    outVector = make_shared<NodeIDVector>(
                        nodeIDVector->getCommonLabel(), nodeIDVector->getCompressionScheme());
                } else {
                    outVector = make_shared<ValueVector>(vector->getDataType());
                }
                auto vectorPtrs = make_unique<overflow_value_t[]>(NODE_SEQUENCE_VECTOR_SIZE);
                BuildSideVectorInfo vectorInfo(vector->getNumBytesPerValue(),
                    dataChunks->getNumDataChunks(), unFlatOutDataChunk->getNumAttributes(),
                    buildSideVectorPtrs.size());
                buildSideVectorInfos.push_back(vectorInfo);
                buildSideVectorPtrs.push_back(move(vectorPtrs));
                unFlatOutDataChunk->append(outVector);
                outVector->setDataChunkOwner(unFlatOutDataChunk);
            }
            dataChunks->append(unFlatOutDataChunk);
        }
    }
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::probeHTDirectory() {
    auto keyCount = probeSideKeyVector->getNumSelectedValues();
    vectorHashOp(*probeSideKeyVector, *hashedProbeKeyVector);
    auto hashes = (uint64_t*)hashedProbeKeyVector->getValues();
    for (uint64_t i = 0; i < keyCount; i++) {
        hashes[i] = hashes[i] & sharedState->hashBitMask;
    }
    auto directory = (uint8_t**)sharedState->htDirectory->blockPtr;
    for (uint64_t i = 0; i < keyCount; i++) {
        auto hash = hashes[i];
        probeState->probedTuples[i] = (uint8_t*)(directory[hash]);
    }
    probeState->probedTuplesSize = keyCount;
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::getNextBatchOfMatchedTuples() {
    if (probeState->probedTuplesSize == 0 ||
        probeState->probeKeyPos == probeState->probedTuplesSize) {
        if (!prevOperator->hasNextMorsel()) {
            probeState->probedTuplesSize = 0;
            return;
        }
        prevOperator->getNextTuples();
        if (probeSideKeyDataChunk->numSelectedValues == 0) {
            probeState->probedTuplesSize = 0;
            return;
        }
        probeHTDirectory();
        vectorDecompressOp(*probeSideKeyVector, *decompressedProbeKeyVector);
        probeState->probeKeyPos = 0;
    }
    nodeID_t nodeId;
    auto decompressedProbeKeys = (nodeID_t*)decompressedProbeKeyVector->getValues();
    for (uint64_t i = probeState->probeKeyPos; i < probeState->probedTuplesSize; i++) {
        while (probeState->probedTuples[i]) {
            if (NODE_SEQUENCE_VECTOR_SIZE == probeState->matchedTuplesSize) {
                break;
            }
            memcpy(&nodeId, probeState->probedTuples[i], NUM_BYTES_PER_NODE_ID);
            probeState->matchedTuples[probeState->matchedTuplesSize] = probeState->probedTuples[i];
            probeState->probeSelVector[probeState->matchedTuplesSize] =
                probeSideKeyDataChunk->isFlat() ? probeSideKeyDataChunk->currPos :
                                                  probeState->probeKeyPos;
            probeState->matchedTuplesSize += (nodeId.label == decompressedProbeKeys[i].label &&
                                              nodeId.offset == decompressedProbeKeys[i].offset);
            probeState->probedTuples[i] =
                *(uint8_t**)(probeState->probedTuples[i] + sharedState->numBytesForFixedTuplePart -
                             sizeof(uint8_t*));
        }
        probeState->probeKeyPos++;
    }
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::populateOutKeyDataChunkAndVectorPtrs() {
    for (uint64_t i = 0; i < probeSideKeyDataChunk->getNumAttributes(); i++) {
        auto probeVector =
            probeSideKeyDataChunk->getValueVector(i); // todo: bug, vector can be nodeIDSequence
        auto probeVectorValues = probeVector->getValues();
        auto outKeyVector = outKeyDataChunk->getValueVector(i);
        auto outKeyVectorValues = outKeyVector->getValues();
        auto numBytesPerValue = outKeyVector->getNumBytesPerValue();
        if (probeVector->getDataType() == NODE &&
            static_pointer_cast<NodeIDVector>(probeVector)->getIsSequence()) {
            auto outKeyVectorNodeOffsets = (node_offset_t*)outKeyVectorValues;
            auto probeKeyStartOffset = *(node_offset_t*)(probeVectorValues);
            for (uint64_t j = 0; j < probeState->matchedTuplesSize; j++) {
                outKeyVectorNodeOffsets[j] = probeKeyStartOffset + probeState->probeSelVector[j];
            }
            static_pointer_cast<NodeIDVector>(outKeyVector)
                ->setCommonLabel(static_pointer_cast<NodeIDVector>(probeVector)->getCommonLabel());
        } else {
            for (uint64_t j = 0; j < probeState->matchedTuplesSize; j++) {
                memcpy(outKeyVectorValues + (j * numBytesPerValue),
                    probeVectorValues + (probeState->probeSelVector[j] * numBytesPerValue),
                    numBytesPerValue);
            }
        }
    }
    auto outKeyDataChunkVectorPos = probeSideKeyDataChunk->getNumAttributes();
    auto tupleReadOffset = NUM_BYTES_PER_NODE_ID;
    for (uint64_t i = 0; i < buildSideKeyDataChunk->getNumAttributes(); i++) {
        if (i == buildSideKeyVectorPos) {
            continue;
        }
        auto outVectorValues =
            outKeyDataChunk->getValueVector(outKeyDataChunkVectorPos)->getValues();
        auto numBytesPerValue = buildSideKeyDataChunk->getValueVector(i)->getNumBytesPerValue();
        for (uint64_t j = 0; j < probeState->matchedTuplesSize; j++) {
            memcpy(outVectorValues + (j * numBytesPerValue),
                probeState->matchedTuples[j] + tupleReadOffset, numBytesPerValue);
        }
        tupleReadOffset += numBytesPerValue;
        outKeyDataChunkVectorPos++;
    }
    auto buildSideVectorPtrsPos = 0;
    for (uint64_t i = 0; i < buildSideNonKeyDataChunks->getNumDataChunks(); i++) {
        auto dataChunk = buildSideNonKeyDataChunks->getDataChunk(i);
        if (dataChunk->isFlat()) {
            for (auto& vector : dataChunk->valueVectors) {
                auto outVectorValues =
                    outKeyDataChunk->getValueVector(outKeyDataChunkVectorPos)->getValues();
                auto numBytesPerValue = vector->getNumBytesPerValue();
                for (uint64_t j = 0; j < probeState->matchedTuplesSize; j++) {
                    memcpy(outVectorValues + (j * numBytesPerValue),
                        probeState->matchedTuples[j] + tupleReadOffset, numBytesPerValue);
                }
                tupleReadOffset += numBytesPerValue;
                outKeyDataChunkVectorPos++;
            }
        } else {
            for (auto& vector : dataChunk->valueVectors) {
                for (uint64_t j = 0; j < probeState->matchedTuplesSize; j++) {
                    buildSideVectorPtrs[buildSideVectorPtrsPos][j] =
                        *(overflow_value_t*)(probeState->matchedTuples[j] + tupleReadOffset);
                }
                tupleReadOffset += sizeof(overflow_value_t);
                buildSideVectorPtrsPos++;
            }
        }
    }

    outKeyDataChunk->size = probeState->matchedTuplesSize;
    outKeyDataChunk->numSelectedValues = probeState->matchedTuplesSize;
    probeState->matchedTuplesSize = 0;
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void HashJoinProbe<IS_OUT_DATACHUNK_FILTERED>::updateAppendedUnFlatOutDataChunks() {
    for (auto& buildVectorInfo : buildSideVectorInfos) {
        auto outDataChunk = dataChunks->getDataChunk(buildVectorInfo.outDataChunkPos);
        auto appendVectorData =
            outDataChunk->getValueVector(buildVectorInfo.outVectorPos)->getValues();
        auto overflowVal =
            buildSideVectorPtrs[buildVectorInfo.vectorPtrsPos].operator[](outKeyDataChunk->currPos);
        memcpy(appendVectorData, overflowVal.value, overflowVal.len);
        outDataChunk->size = overflowVal.len / buildVectorInfo.numBytesPerValue;
        outDataChunk->numSelectedValues = outDataChunk->size;
    }
}

static void initializeSelectedValuesPos(uint64_t* selectedValuesPos, uint64_t numSelectedValues) {
    for (uint64_t i = 0; i < numSelectedValues; i++) {
        selectedValuesPos[i] = i;
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
    if (!buildSideVectorPtrs.empty() && outKeyDataChunk->currPos < (outKeyDataChunk->size - 1)) {
        outKeyDataChunk->currPos += 1;
        updateAppendedUnFlatOutDataChunks();
        if constexpr (IS_OUT_DATACHUNK_FILTERED) {
            for (uint64_t i = (probeSideNonKeyDataChunks->getNumDataChunks() + 1);
                 i < dataChunks->getNumDataChunks(); i++) {
                initializeSelectedValuesPos(dataChunks->getDataChunk(i)->selectedValuesPos.get(),
                    dataChunks->getDataChunk(i)->numSelectedValues);
            }
        }
        return;
    }
    getNextBatchOfMatchedTuples();
    populateOutKeyDataChunkAndVectorPtrs();
    if (buildSideVectorPtrs.empty()) {
        outKeyDataChunk->currPos = -1;
    } else {
        outKeyDataChunk->currPos = 0;
        updateAppendedUnFlatOutDataChunks();
    }
    if constexpr (IS_OUT_DATACHUNK_FILTERED) {
        initializeSelectedValuesPos(
            outKeyDataChunk->selectedValuesPos.get(), outKeyDataChunk->numSelectedValues);
        for (uint64_t i = (probeSideNonKeyDataChunks->getNumDataChunks() + 1);
             i < dataChunks->getNumDataChunks(); i++) {
            initializeSelectedValuesPos(dataChunks->getDataChunk(i)->selectedValuesPos.get(),
                dataChunks->getDataChunk(i)->numSelectedValues);
        }
    }
}

template class HashJoinProbe<true>;
template class HashJoinProbe<false>;
} // namespace processor
} // namespace graphflow

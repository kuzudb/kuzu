#include "src/processor/include/physical_plan/operator/hash_join/hash_join.h"

namespace graphflow {
namespace processor {

HashJoin::HashJoin(MemoryManager& memManager, uint64_t buildSideKeyDataChunkIdx,
    uint64_t buildSideKeyVectorIdx, uint64_t probeSideKeyDataChunkIdx,
    uint64_t probeSideKeyVectorIdx, unique_ptr<PhysicalOperator> buildSidePrevOp,
    unique_ptr<PhysicalOperator> probeSidePrevOp)
    : PhysicalOperator(move(probeSidePrevOp)), memManager(memManager),
      buildSideKeyDataChunkIdx(buildSideKeyDataChunkIdx),
      buildSideKeyVectorIdx(buildSideKeyVectorIdx),
      probeSideKeyDataChunkIdx(probeSideKeyDataChunkIdx),
      probeSideKeyVectorIdx(probeSideKeyVectorIdx), buildSidePrevOp(move(buildSidePrevOp)),
      isHTInitialized(false) {
    auto buildSideDataChunks = this->buildSidePrevOp->getDataChunks();
    buildSideKeyDataChunk = buildSideDataChunks->getDataChunk(buildSideKeyDataChunkIdx);
    buildSideNonKeyDataChunks = make_shared<DataChunks>();
    for (uint64_t i = 0; i < buildSideDataChunks->getNumDataChunks(); i++) {
        if (i != buildSideKeyDataChunkIdx) {
            buildSideNonKeyDataChunks->append(buildSideDataChunks->getDataChunk(i));
        }
    }

    // The prevOperator is the probe side prev operator passed in to the PhysicalOperator
    auto probeSideDataChunks = prevOperator->getDataChunks();
    probeSideKeyDataChunk = probeSideDataChunks->getDataChunk(probeSideKeyDataChunkIdx);
    probeSideKeyVector = static_pointer_cast<NodeIDVector>(
        probeSideKeyDataChunk->getValueVector(probeSideKeyVectorIdx));
    probeSideNonKeyDataChunks = make_shared<DataChunks>();
    for (uint64_t i = 0; i < probeSideDataChunks->getNumDataChunks(); i++) {
        if (i != probeSideKeyDataChunkIdx) {
            probeSideNonKeyDataChunks->append(probeSideDataChunks->getDataChunk(i));
        }
    }

    vectorDecompressOp = ValueVector::getUnaryOperation(DECOMPRESS_NODE_ID);
    decompressedProbeKeyVector = make_unique<NodeIDVector>(NodeIDCompressionScheme(8, 8));

    probeState = make_unique<ProbeState>(ValueVector::MAX_VECTOR_SIZE);

    initializeHashTable();
    initializeOutDataChunksAndVectorPtrs();
}

void HashJoin::initializeHashTable() {
    vector<PayloadInfo> htPayloadInfos;
    for (uint64_t i = 0; i < buildSideKeyDataChunk->getNumAttributes(); i++) {
        if (i == buildSideKeyVectorIdx) {
            continue;
        }
        PayloadInfo info(buildSideKeyDataChunk->getValueVector(i)->getNumBytesPerValue(), false);
        htPayloadInfos.push_back(info);
    }
    for (uint64_t i = 0; i < buildSideNonKeyDataChunks->getNumDataChunks(); i++) {
        auto dataChunk = buildSideNonKeyDataChunks->getDataChunk(i);
        for (auto& vector : dataChunk->valueVectors) {
            PayloadInfo info(
                (dataChunk->isFlat() ? vector->getNumBytesPerValue() : sizeof(overflow_value_t)),
                !dataChunk->isFlat());
            htPayloadInfos.push_back(info);
        }
    }
    hashTable = make_unique<HashTable>(memManager, htPayloadInfos);
}

void HashJoin::initializeOutDataChunksAndVectorPtrs() {
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
    }
    for (uint64_t i = 0; i < buildSideKeyDataChunk->getNumAttributes(); i++) {
        if (i == buildSideKeyVectorIdx) {
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
                auto vectorPtrs = make_unique<overflow_value_t[]>(ValueVector::DEFAULT_VECTOR_SIZE);
                BuildSideVectorInfo vectorInfo(vector->getNumBytesPerValue(),
                    dataChunks->getNumDataChunks(), unFlatOutDataChunk->getNumAttributes(),
                    buildSideVectorPtrs.size());
                buildSideVectorInfos.push_back(vectorInfo);
                buildSideVectorPtrs.push_back(move(vectorPtrs));
                unFlatOutDataChunk->append(outVector);
            }
            dataChunks->append(unFlatOutDataChunk);
        }
    }
}

void HashJoin::getNextBatchOfMatchedTuples() {
    if (probeState->probedTuplesSize == 0 ||
        probeState->probeKeyIndex == probeState->probedTuplesSize) {
        if (!prevOperator->hasNextMorsel()) {
            probeState->probedTuplesSize = 0;
            return;
        }
        prevOperator->getNextTuples();
        if (probeSideKeyDataChunk->size == 0) {
            probeState->probedTuplesSize = 0;
            return;
        }
        probeState->probedTuplesSize =
            hashTable->probeDirectory(*probeSideKeyVector, probeState->probedTuples.get());
        vectorDecompressOp(*probeSideKeyVector, *decompressedProbeKeyVector);
        probeState->probeKeyIndex = 0;
    }
    nodeID_t nodeId;
    auto decompressedProbeKeys = (nodeID_t*)decompressedProbeKeyVector->getValues();
    for (uint64_t i = probeState->probeKeyIndex; i < probeState->probedTuplesSize; i++) {
        while (probeState->probedTuples[i]) {
            if (ValueVector::DEFAULT_VECTOR_SIZE == probeState->matchedTuplesSize) {
                break;
            }
            memcpy(&nodeId, probeState->probedTuples[i], NUM_BYTES_PER_NODE_ID);
            probeState->matchedTuples[probeState->matchedTuplesSize] = probeState->probedTuples[i];
            probeState->probeSelVector[probeState->matchedTuplesSize] =
                probeSideKeyDataChunk->isFlat() ? probeSideKeyDataChunk->currPos :
                                                  probeState->probeKeyIndex;
            probeState->matchedTuplesSize += (nodeId.label == decompressedProbeKeys[i].label &&
                                              nodeId.offset == decompressedProbeKeys[i].offset);
            probeState->probedTuples[i] =
                *(uint8_t**)(probeState->probedTuples[i] + hashTable->numBytesForFixedTuplePart -
                             sizeof(uint8_t*));
        }
        probeState->probeKeyIndex++;
    }
}

void HashJoin::populateOutKeyDataChunkAndVectorPtrs() {
    for (uint64_t i = 0; i < probeSideKeyDataChunk->getNumAttributes(); i++) {
        auto probeVector = probeSideKeyDataChunk->getValueVector(i);
        auto probeVectorValues = probeVector->getValues();
        auto outKeyVector = outKeyDataChunk->getValueVector(i);
        auto outKeyVectorValues = outKeyVector->getValues();
        auto numBytesPerValue = outKeyVector->getNumBytesPerValue();
        for (uint64_t j = 0; j < probeState->matchedTuplesSize; j++) {
            memcpy(outKeyVectorValues + (j * numBytesPerValue),
                probeVectorValues + (probeState->probeSelVector[j] * numBytesPerValue),
                numBytesPerValue);
        }
    }
    auto outKeyDataChunkVectorIdx = probeSideKeyDataChunk->getNumAttributes();
    auto tupleReadOffset = NUM_BYTES_PER_NODE_ID;
    for (uint64_t i = 0; i < buildSideKeyDataChunk->getNumAttributes(); i++) {
        if (i == buildSideKeyVectorIdx) {
            continue;
        }
        auto outVectorValues =
            outKeyDataChunk->getValueVector(outKeyDataChunkVectorIdx)->getValues();
        auto numBytesPerValue = buildSideKeyDataChunk->getValueVector(i)->getNumBytesPerValue();
        for (uint64_t j = 0; j < probeState->matchedTuplesSize; j++) {
            memcpy(outVectorValues + (j * numBytesPerValue),
                probeState->matchedTuples[j] + tupleReadOffset, numBytesPerValue);
        }
        tupleReadOffset += numBytesPerValue;
        outKeyDataChunkVectorIdx++;
    }
    auto buildSideVectorPtrsIdx = 0;
    for (uint64_t i = 0; i < buildSideNonKeyDataChunks->getNumDataChunks(); i++) {
        auto dataChunk = buildSideNonKeyDataChunks->getDataChunk(i);
        if (dataChunk->isFlat()) {
            for (auto& vector : dataChunk->valueVectors) {
                auto outVectorValues =
                    outKeyDataChunk->getValueVector(outKeyDataChunkVectorIdx)->getValues();
                auto numBytesPerValue = vector->getNumBytesPerValue();
                for (uint64_t j = 0; j < probeState->matchedTuplesSize; j++) {
                    memcpy(outVectorValues + (j * numBytesPerValue),
                        probeState->matchedTuples[j] + tupleReadOffset, numBytesPerValue);
                }
                tupleReadOffset += numBytesPerValue;
                outKeyDataChunkVectorIdx++;
            }
        } else {
            for (auto& vector : dataChunk->valueVectors) {
                for (uint64_t j = 0; j < probeState->matchedTuplesSize; j++) {
                    buildSideVectorPtrs[buildSideVectorPtrsIdx][j] =
                        *(overflow_value_t*)(probeState->matchedTuples[j] + tupleReadOffset);
                }
                tupleReadOffset += sizeof(overflow_value_t);
                buildSideVectorPtrsIdx++;
            }
        }
    }

    outKeyDataChunk->size = probeState->matchedTuplesSize;
    probeState->matchedTuplesSize = 0;
}

void HashJoin::updateAppendedUnFlatOutDataChunks() {
    for (auto& buildVectorInfo : buildSideVectorInfos) {
        auto outDataChunk = dataChunks->getDataChunk(buildVectorInfo.outDataChunkIdx);
        auto appendVectorData =
            outDataChunk->getValueVector(buildVectorInfo.outVectorIdx)->getValues();
        auto overflowVal =
            buildSideVectorPtrs[buildVectorInfo.vectorPtrsIdx].operator[](outKeyDataChunk->currPos);
        memcpy(appendVectorData, overflowVal.value, overflowVal.len);
        outDataChunk->size = overflowVal.len / buildVectorInfo.numBytesPerValue;
    }
}

// The general flow of a hash join:
// 1) append all data chunks from the build side into ht.
// 2) for probing, first find matched tuples of probe side keys from ht, then populate values from
// matched tuples into outKeyDataChunk and buildSideVectorPtrs, finally, if build side doesn't
// contain any unflat non-key data chunks, which means buildSideVectorPtrs is empty, directly unflat
// outKeyDataChunk. else flat outKeyDataChunk and update currPos of outKeyDataChunk, and update
// appended unflat data chunks based on buildSideVectorPtrs and outKeyDataChunk.currPos.
void HashJoin::getNextTuples() {
    if (!isHTInitialized) {
        while (buildSidePrevOp->hasNextMorsel()) {
            buildSidePrevOp->getNextTuples();
            if (0 == buildSideKeyDataChunk->size) {
                break;
            }
            hashTable->addDataChunks(
                *buildSideKeyDataChunk, buildSideKeyVectorIdx, *buildSideNonKeyDataChunks);
        }
        hashTable->buildDirectory();
        isHTInitialized = true;
    }
    if (!buildSideVectorPtrs.empty() && outKeyDataChunk->currPos < (outKeyDataChunk->size - 1)) {
        outKeyDataChunk->currPos += 1;
        updateAppendedUnFlatOutDataChunks();
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
}

} // namespace processor
} // namespace graphflow

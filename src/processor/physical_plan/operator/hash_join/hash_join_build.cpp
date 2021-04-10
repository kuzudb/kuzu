#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"

namespace graphflow {
namespace processor {

static uint64_t nextPowerOfTwo(uint64_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
}

HashJoinBuild::HashJoinBuild(
    uint64_t keyDataChunkPos, uint64_t keyVectorPos, unique_ptr<PhysicalOperator> prevOperator)
    : Sink(move(prevOperator), HASH_JOIN_BUILD), keyDataChunkPos(keyDataChunkPos),
      keyVectorPos(keyVectorPos), numEntries(0) {
    dataChunks = this->prevOperator->getDataChunks();
    keyDataChunk = dataChunks->getDataChunk(keyDataChunkPos);
    nonKeyDataChunks = make_shared<DataChunks>();
    for (uint64_t i = 0; i < dataChunks->getNumDataChunks(); i++) {
        if (i != keyDataChunkPos) {
            nonKeyDataChunks->append(dataChunks->getDataChunk(i));
        }
    }
    // key field (node_offset_t + label_t) + prev field (uint8_t*)
    numBytesForFixedTuplePart = sizeof(node_offset_t) + sizeof(label_t) + sizeof(uint8_t*);
    for (uint64_t i = 0; i < keyDataChunk->getNumAttributes(); i++) {
        if (i == keyVectorPos) {
            continue;
        }
        numBytesForFixedTuplePart += keyDataChunk->getValueVector(i)->getNumBytesPerValue();
    }
    for (uint64_t i = 0; i < nonKeyDataChunks->getNumDataChunks(); i++) {
        auto dataChunk = nonKeyDataChunks->getDataChunk(i);
        for (auto& vector : dataChunk->valueVectors) {
            numBytesForFixedTuplePart +=
                dataChunk->isFlat() ? vector->getNumBytesPerValue() : sizeof(overflow_value_t);
        }
    }
    htBlockCapacity = DEFAULT_HT_BLOCK_SIZE / numBytesForFixedTuplePart;
}

void HashJoinBuild::finalize() {
    auto directory_capacity = nextPowerOfTwo(
        max(sharedState->numEntries * 2, (DEFAULT_HT_BLOCK_SIZE / sizeof(uint8_t*)) + 1));
    sharedState->hashBitMask = directory_capacity - 1;

    sharedState->htDirectory = memManager->allocateBlock(directory_capacity * sizeof(uint8_t*));
    memset(sharedState->htDirectory->blockPtr, 0, directory_capacity * sizeof(uint8_t*));

    nodeID_t nodeId;
    uint64_t hash;
    auto directory = (uint8_t**)sharedState->htDirectory->blockPtr;
    for (auto& block : sharedState->htBlocks) {
        uint8_t* basePtr = block->blockPtr;
        uint64_t entryPos = 0;
        while (entryPos < block->numEntries) {
            memcpy(&nodeId, basePtr, NUM_BYTES_PER_NODE_ID);
            hash = murmurhash64(nodeId.offset) ^ murmurhash64(nodeId.label);
            auto slotId = hash & sharedState->hashBitMask;
            auto prevPtr = (uint8_t**)(basePtr + numBytesForFixedTuplePart - sizeof(uint8_t*));
            memcpy((uint8_t*)prevPtr, (void*)&(directory[slotId]), sizeof(uint8_t*));
            directory[slotId] = basePtr;
            basePtr += numBytesForFixedTuplePart;
            entryPos++;
        }
    }
}

void HashJoinBuild::appendPayloadVectorAsFixSizedValues(ValueVector& vector, uint8_t* appendBuffer,
    uint64_t valueOffsetInVector, uint64_t appendCount, bool isSingleValue) {
    auto typeSize = vector.getNumBytesPerValue();
    auto selectedValuesPos = vector.getSelectedValuesPos();
    if (vector.getDataType() == NODE) {
        auto& nodeIDVec = dynamic_cast<NodeIDVector&>(vector);
        if (nodeIDVec.getIsSequence()) {
            auto startNodeOffset = ((node_offset_t*)vector.getValues())[0];
            for (uint64_t i = 0; i < appendCount; i++) {
                auto valuePos = valueOffsetInVector + (i * (1 - isSingleValue));
                auto nodeOffset = startNodeOffset + selectedValuesPos[valuePos];
                memcpy(appendBuffer + (i * numBytesForFixedTuplePart), &nodeOffset, typeSize);
            }
            return;
        }
    }
    for (uint64_t i = 0; i < appendCount; i++) {
        auto valuePos = valueOffsetInVector + (i * (1 - isSingleValue));
        memcpy(appendBuffer + (i * numBytesForFixedTuplePart),
            vector.getValues() + (selectedValuesPos[valuePos] * typeSize), typeSize);
    }
}

overflow_value_t HashJoinBuild::addVectorInOverflowBlocks(ValueVector& vector) {
    auto numBytesPerValue = vector.getNumBytesPerValue();
    auto valuesLength = vector.getNumBytesPerValue() * vector.getNumSelectedValues();
    auto vectorValues = vector.getValues();
    auto vectorSelectedValuesPos = vector.getSelectedValuesPos();
    if (valuesLength > DEFAULT_OVERFLOW_BLOCK_SIZE) {
        throw invalid_argument("Value length is larger than a overflow block.");
    }
    uint8_t* blockAppendPos = nullptr;
    // Find free space in existing memory blocks
    for (auto& blockHandle : overflowBlocks) {
        if (blockHandle->freeSize >= valuesLength) {
            blockAppendPos =
                blockHandle->blockPtr + DEFAULT_OVERFLOW_BLOCK_SIZE - blockHandle->freeSize;
            blockHandle->freeSize -= valuesLength;
        }
    }
    if (blockAppendPos == nullptr) {
        // If no free space found in existing memory blocks, allocate a new one
        auto blockHandle = memManager->allocateBlock(DEFAULT_OVERFLOW_BLOCK_SIZE);
        memset(blockHandle->blockPtr, 0, DEFAULT_OVERFLOW_BLOCK_SIZE);
        blockAppendPos = blockHandle->blockPtr;
        blockHandle->freeSize -= valuesLength;
        overflowBlocks.push_back(move(blockHandle));
    }

    for (uint64_t i = 0; i < vector.getNumSelectedValues(); i++) {
        memcpy(blockAppendPos + (i * numBytesPerValue),
            vectorValues + (vectorSelectedValuesPos[i] * numBytesPerValue), numBytesPerValue);
    }
    return overflow_value_t{valuesLength, blockAppendPos};
}

// First, add the vector into overflowBlocks, then, append the returned value into htBlocks
// appendCount times. (a sequence nodeID vector should never be passed in this function)
void HashJoinBuild::appendPayloadVectorAsOverflowValue(
    ValueVector& vector, uint8_t* appendBuffer, uint64_t appendCount) {
    assert(
        !((vector.getDataType() == NODE) && (dynamic_cast<NodeIDVector&>(vector).getIsSequence())));
    auto overflowValue = addVectorInOverflowBlocks(vector);
    for (uint64_t i = 0; i < appendCount; i++) {
        memcpy(appendBuffer + (i * numBytesForFixedTuplePart), (void*)&overflowValue,
            sizeof(overflow_value_t));
    }
}

void HashJoinBuild::appendKeyVector(NodeIDVector& vector, uint8_t* appendBuffer,
    uint64_t valueOffsetInVector, uint64_t appendCount) const {
    auto compressionScheme = vector.getCompressionScheme();
    auto offsetSize = compressionScheme.getNumBytesForOffset();
    auto nodeSize = compressionScheme.getNumTotalBytes();
    auto isLabelCommon = compressionScheme.getNumBytesForLabel() == 0;
    auto labelSize = isLabelCommon ? sizeof(label_t) : compressionScheme.getNumBytesForLabel();
    auto commonLabel = vector.getCommonLabel();
    auto startNodeOffset = ((node_offset_t*)vector.getValues())[0];
    auto selectedValuesPos = vector.getSelectedValuesPos();
    if (vector.getIsSequence()) {
        for (uint64_t i = 0; i < appendCount; i++) {
            auto nodeOffset = startNodeOffset + selectedValuesPos[valueOffsetInVector + i];
            memcpy(appendBuffer, &nodeOffset, offsetSize);
            memcpy(appendBuffer + sizeof(node_offset_t), (uint8_t*)&commonLabel, labelSize);
            appendBuffer += numBytesForFixedTuplePart;
        }
        return;
    }

    for (uint64_t i = 0; i < appendCount; i++) {
        auto valuePos = selectedValuesPos[valueOffsetInVector + i];
        memcpy(appendBuffer, vector.getValues() + (valuePos * nodeSize), offsetSize);
        memcpy(appendBuffer + sizeof(node_offset_t),
            (isLabelCommon ? (uint8_t*)&commonLabel :
                             vector.getValues() + (valuePos * nodeSize) + offsetSize),
            labelSize);
        appendBuffer += numBytesForFixedTuplePart;
    }
}

void HashJoinBuild::allocateHTBlocks(
    uint64_t remaining, vector<BlockAppendInfo>& blockAppendInfos) {
    if (htBlockCapacity == 0) {
        // We need ensure htBlockCapacity is not 0, otherwise, we will fall into a dead for-loop and
        // keep allocating memory blocks.
        throw logic_error("The size of fixed tuple is larger than a ht block. Not supported yet!");
    }
    if (!htBlocks.empty()) {
        auto& lastBlock = htBlocks.back();
        if (lastBlock->numEntries < htBlockCapacity) {
            // Find free space in existing blocks
            auto appendCount = min(remaining, htBlockCapacity - lastBlock->numEntries);
            auto currentPos =
                lastBlock->blockPtr + (lastBlock->numEntries * numBytesForFixedTuplePart);
            blockAppendInfos.emplace_back(currentPos, appendCount);
            lastBlock->numEntries += appendCount;
            remaining -= appendCount;
        }
    }
    while (remaining > 0) {
        // Need allocate new blocks for tuples
        auto appendCount = min(remaining, htBlockCapacity);
        auto newBlock = memManager->allocateBlock(DEFAULT_HT_BLOCK_SIZE, appendCount);
        blockAppendInfos.emplace_back(newBlock->blockPtr, appendCount);
        htBlocks.push_back(move(newBlock));
        remaining -= appendCount;
    }
}

void HashJoinBuild::appendDataChunks() {
    if (keyDataChunk->numSelectedValues == 0) {
        return;
    }

    // Allocate space for tuples
    auto numTuplesToAppend = keyDataChunk->isFlat() ? 1 : keyDataChunk->numSelectedValues;
    vector<BlockAppendInfo> blockAppendInfos;
    allocateHTBlocks(numTuplesToAppend, blockAppendInfos);

    // Append tuples
    auto tupleAppendOffset = 0; // The start offset of each field inside the tuple.
    // Append key vector
    auto keyVector = static_pointer_cast<NodeIDVector>(keyDataChunk->getValueVector(keyVectorPos));
    auto keyValOffsetInVec = keyDataChunk->isFlat() ? keyDataChunk->currPos : 0;
    for (auto& blockAppendInfo : blockAppendInfos) {
        auto blockAppendPtr = blockAppendInfo.buffer + tupleAppendOffset;
        auto blockAppendCount = blockAppendInfo.numEntries;
        appendKeyVector(*keyVector, blockAppendPtr, keyValOffsetInVec, blockAppendCount);
        keyValOffsetInVec += blockAppendCount;
    }
    tupleAppendOffset += NUM_BYTES_PER_NODE_ID;

    // Append payloads in key data chunk
    for (uint64_t i = 0; i < keyDataChunk->getNumAttributes(); i++) {
        if (i == keyVectorPos) {
            continue;
        }
        // Append payload vectors in the keyDataChunk
        auto payloadVector = keyDataChunk->getValueVector(i);
        auto valOffsetInVec = keyDataChunk->isFlat() ? keyDataChunk->currPos : 0;
        for (auto& blockAppendInfo : blockAppendInfos) {
            auto blockAppendPtr = blockAppendInfo.buffer + tupleAppendOffset;
            auto blockAppendCount = blockAppendInfo.numEntries;
            appendPayloadVectorAsFixSizedValues(*payloadVector, blockAppendPtr, valOffsetInVec,
                blockAppendCount, keyDataChunk->isFlat());
            valOffsetInVec += blockAppendCount;
        }
        tupleAppendOffset += payloadVector->getNumBytesPerValue();
    }

    // Append payload in non-key data chunks
    for (uint64_t chunkPos = 0; chunkPos < nonKeyDataChunks->getNumDataChunks(); chunkPos++) {
        auto payloadDataChunk = nonKeyDataChunks->getDataChunk(chunkPos);
        if (payloadDataChunk->isFlat()) {
            for (uint64_t i = 0; i < payloadDataChunk->getNumAttributes(); i++) {
                auto payloadVector = payloadDataChunk->getValueVector(i);
                for (auto& blockAppendInfo : blockAppendInfos) {
                    auto appendCount = blockAppendInfo.numEntries;
                    auto appendBuffer = blockAppendInfo.buffer + tupleAppendOffset;
                    appendPayloadVectorAsFixSizedValues(*payloadVector, appendBuffer,
                        payloadVector->getCurrPos(), appendCount, true);
                }
                tupleAppendOffset += payloadVector->getNumBytesPerValue();
            }
        } else {
            for (uint64_t i = 0; i < payloadDataChunk->getNumAttributes(); i++) {
                auto payloadVector = payloadDataChunk->getValueVector(i);
                for (auto& blockAppendInfo : blockAppendInfos) {
                    auto appendCount = blockAppendInfo.numEntries;
                    auto appendBuffer = blockAppendInfo.buffer + tupleAppendOffset;
                    appendPayloadVectorAsOverflowValue(*payloadVector, appendBuffer, appendCount);
                }
                tupleAppendOffset += sizeof(overflow_value_t);
            }
        }
    }

    numEntries += numTuplesToAppend;
}

void HashJoinBuild::getNextTuples() {
    // Append thread-local tuples
    do {
        prevOperator->getNextTuples();
        appendDataChunks();
    } while (keyDataChunk->size > 0);

    // Merge thread-local state (numEntries, htBlocks, overflowBlocks) with the shared one
    {
        lock_guard<mutex> sharedStateLock(sharedState->hashJoinSharedStateLock);
        sharedState->numEntries += numEntries;
        move(begin(htBlocks), end(htBlocks), back_inserter(sharedState->htBlocks));
        move(
            begin(overflowBlocks), end(overflowBlocks), back_inserter(sharedState->overflowBlocks));
    }

    keyDataChunk->size = 0;
    keyDataChunk->numSelectedValues = 0;
}
} // namespace processor
} // namespace graphflow

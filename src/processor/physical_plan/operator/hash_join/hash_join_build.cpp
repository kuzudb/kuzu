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

HashJoinBuild::HashJoinBuild(uint64_t keyDataChunkPos, uint64_t keyVectorPos,
    const vector<bool>& dataChunkPosToIsFlat, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id)
    : Sink{move(prevOperator), HASH_JOIN_BUILD, context, id}, keyDataChunkPos{keyDataChunkPos},
      keyVectorPos{keyVectorPos}, dataChunkPosToIsFlat{dataChunkPosToIsFlat}, numEntries{0} {
    auto prevResultSet = this->prevOperator->getResultSet();
    keyDataChunk = prevResultSet->dataChunks[keyDataChunkPos];
    resultSetWithoutKeyDataChunk = make_shared<ResultSet>();
    // key field (node_offset_t + label_t) + prev field (uint8_t*)
    numBytesForFixedTuplePart = sizeof(node_offset_t) + sizeof(label_t) + sizeof(uint8_t*);
    for (auto vectorPos = 0u; vectorPos < keyDataChunk->valueVectors.size(); vectorPos++) {
        if (vectorPos == keyVectorPos) {
            continue;
        }
        numBytesForFixedTuplePart += keyDataChunk->getValueVector(vectorPos)->getNumBytesPerValue();
    }
    for (auto dataChunkPos = 0u; dataChunkPos < prevResultSet->dataChunks.size(); dataChunkPos++) {
        if (dataChunkPos == keyDataChunkPos) {
            continue;
        }
        resultSetWithoutKeyDataChunk->append(prevResultSet->dataChunks[dataChunkPos]);
        for (auto& vector : prevResultSet->dataChunks[dataChunkPos]->valueVectors) {
            numBytesForFixedTuplePart += this->dataChunkPosToIsFlat[dataChunkPos] ?
                                             vector->getNumBytesPerValue() :
                                             sizeof(overflow_value_t);
        }
    }
    htBlockCapacity = DEFAULT_HT_BLOCK_SIZE / numBytesForFixedTuplePart;
}

void HashJoinBuild::finalize() {
    auto directory_capacity = nextPowerOfTwo(
        max(sharedState->numEntries * 2, (DEFAULT_HT_BLOCK_SIZE / sizeof(uint8_t*)) + 1));
    sharedState->hashBitMask = directory_capacity - 1;
    sharedState->htDirectory = context.memoryManager->allocateBlock(
        directory_capacity * sizeof(uint8_t*), true /* initialize */);

    nodeID_t nodeId;
    uint64_t hash;
    auto directory = (uint8_t**)sharedState->htDirectory->data;
    for (auto& block : sharedState->htBlocks) {
        uint8_t* basePtr = block.data;
        uint64_t entryPos = 0;
        while (entryPos < block.numEntries) {
            memcpy(&nodeId, basePtr, NUM_BYTES_PER_NODE_ID);
            Hash::operation<nodeID_t>(nodeId, hash);
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
    uint64_t valueOffsetInVector, uint64_t appendCount, bool isSingleValue) const {
    auto typeSize = vector.getNumBytesPerValue();
    if (vector.dataType == NODE && vector.isSequence) {
        nodeID_t nodeID(0, 0);
        for (auto i = 0u; i < appendCount; i++) {
            auto valuePos = valueOffsetInVector + (i * (1 - isSingleValue));
            vector.readNodeID(valuePos, nodeID);
            memcpy(appendBuffer + (i * numBytesForFixedTuplePart), &nodeID, typeSize);
        }
    } else {
        for (auto i = 0u; i < appendCount; i++) {
            auto valuePos = valueOffsetInVector + (i * (1 - isSingleValue));
            memcpy(appendBuffer + (i * numBytesForFixedTuplePart),
                vector.values + (vector.state->selectedPositions[valuePos] * typeSize), typeSize);
        }
    }
}

overflow_value_t HashJoinBuild::addVectorInOverflowBlocks(ValueVector& vector) {
    auto numBytesPerValue = vector.getNumBytesPerValue();
    auto valuesLength = vector.getNumBytesPerValue() * vector.state->selectedSize;
    auto vectorValues = vector.values;
    if (valuesLength > DEFAULT_OVERFLOW_BLOCK_SIZE) {
        throw invalid_argument("Value length is larger than a overflow block.");
    }
    uint8_t* blockAppendPos = nullptr;
    // Find free space in existing memory blocks
    for (auto& blockHandle : overflowBlocks) {
        if (blockHandle.freeSize >= valuesLength) {
            blockAppendPos = blockHandle.data + DEFAULT_OVERFLOW_BLOCK_SIZE - blockHandle.freeSize;
            blockHandle.freeSize -= valuesLength;
        }
    }
    if (blockAppendPos == nullptr) {
        // If no free space found in existing memory blocks, allocate a new one
        BlockHandle blockHandle(
            context.memoryManager->allocateBlock(DEFAULT_OVERFLOW_BLOCK_SIZE, true), 0);
        blockAppendPos = blockHandle.data;
        blockHandle.freeSize -= valuesLength;
        overflowBlocks.push_back(move(blockHandle));
    }
    if (vector.dataType == NODE && vector.isSequence) {
        nodeID_t nodeID(0, 0);
        for (auto i = 0u; i < vector.state->selectedSize; i++) {
            vector.readNodeID(vector.state->selectedPositions[i], nodeID);
            memcpy(blockAppendPos + (i * numBytesPerValue), &nodeID, numBytesPerValue);
        }
    } else {
        for (auto i = 0u; i < vector.state->selectedSize; i++) {
            memcpy(blockAppendPos + (i * numBytesPerValue),
                vectorValues + (vector.state->selectedPositions[i] * numBytesPerValue),
                numBytesPerValue);
        }
    }
    return overflow_value_t{valuesLength, blockAppendPos};
}

// First, add the vector into overflowBlocks, then, append the returned value into htBlocks
// appendCount times. (a sequence nodeID vector should never be passed in this function)
void HashJoinBuild::appendPayloadVectorAsOverflowValue(
    ValueVector& vector, uint8_t* appendBuffer, uint64_t appendCount) {
    auto overflowValue = addVectorInOverflowBlocks(vector);
    for (auto i = 0u; i < appendCount; i++) {
        memcpy(appendBuffer + (i * numBytesForFixedTuplePart), (void*)&overflowValue,
            sizeof(overflow_value_t));
    }
}

void HashJoinBuild::appendKeyVector(ValueVector& vector, uint8_t* appendBuffer,
    uint64_t valueOffsetInVector, uint64_t appendCount) const {
    nodeID_t nodeID(0, 0);
    for (auto i = 0u; i < appendCount; i++) {
        vector.readNodeID(vector.state->selectedPositions[valueOffsetInVector + i], nodeID);
        memcpy(appendBuffer, &nodeID, sizeof(nodeID_t));
        appendBuffer += numBytesForFixedTuplePart;
    }
}

vector<BlockAppendInfo> HashJoinBuild::allocateHTBlocks(uint64_t numTuplesToAppend) {
    auto remaining = numTuplesToAppend;
    vector<BlockAppendInfo> blockAppendInfos;
    if (htBlockCapacity == 0) {
        // We need ensure htBlockCapacity is not 0, otherwise, we will fall into a dead for-loop and
        // keep allocating memory blocks.
        throw invalid_argument(
            "The size of fixed tuple is larger than a ht block. Not supported yet!");
    }
    if (!htBlocks.empty()) {
        auto& lastBlock = htBlocks.back();
        if (lastBlock.numEntries < htBlockCapacity) {
            // Find free space in existing blocks
            auto appendCount = min(remaining, htBlockCapacity - lastBlock.numEntries);
            auto currentPos = lastBlock.data + (lastBlock.numEntries * numBytesForFixedTuplePart);
            blockAppendInfos.emplace_back(currentPos, appendCount);
            lastBlock.numEntries += appendCount;
            remaining -= appendCount;
        }
    }
    while (remaining > 0) {
        // Need allocate new blocks for tuples
        auto appendCount = min(remaining, htBlockCapacity);
        auto newBlock =
            context.memoryManager->allocateBlock(DEFAULT_HT_BLOCK_SIZE, true /* initialize */);
        blockAppendInfos.emplace_back(newBlock->data, appendCount);
        BlockHandle blockHandle(move(newBlock), appendCount);
        htBlocks.push_back(move(blockHandle));
        remaining -= appendCount;
    }
    return blockAppendInfos;
}

void HashJoinBuild::appendKeyDataChunk(
    const vector<BlockAppendInfo>& blockAppendInfos, uint64_t& tupleAppendOffset) {
    // Append key vector, which must be node ids
    auto keyVector = keyDataChunk->getValueVector(keyVectorPos);
    auto keyValOffsetInVec = keyDataChunk->state->isFlat() ? keyDataChunk->state->currIdx : 0;
    for (auto& blockAppendInfo : blockAppendInfos) {
        appendKeyVector(*keyVector, blockAppendInfo.buffer + tupleAppendOffset, keyValOffsetInVec,
            blockAppendInfo.numEntries);
        keyValOffsetInVec += blockAppendInfo.numEntries;
    }
    tupleAppendOffset += NUM_BYTES_PER_NODE_ID;
    // Append payloads in key data chunk
    for (auto i = 0u; i < keyDataChunk->valueVectors.size(); i++) {
        if (i == keyVectorPos) {
            continue;
        }
        // Append payload vectors in the keyDataChunk
        auto payloadVector = keyDataChunk->getValueVector(i);
        auto valOffsetInVec = keyDataChunk->state->isFlat() ? keyDataChunk->state->currIdx : 0;
        for (auto& blockAppendInfo : blockAppendInfos) {
            appendPayloadVectorAsFixSizedValues(*payloadVector,
                blockAppendInfo.buffer + tupleAppendOffset, valOffsetInVec,
                blockAppendInfo.numEntries, keyDataChunk->state->isFlat());
            valOffsetInVec += blockAppendInfo.numEntries;
        }
        tupleAppendOffset += payloadVector->getNumBytesPerValue();
    }
}

// Append payload in non-key data chunks
void HashJoinBuild::appendPayloadDataChunks(
    const vector<BlockAppendInfo>& blockAppendInfos, uint64_t& tupleAppendOffset) {
    for (auto& payloadDataChunk : resultSetWithoutKeyDataChunk->dataChunks) {
        if (payloadDataChunk->state->isFlat()) {
            for (auto i = 0u; i < payloadDataChunk->valueVectors.size(); i++) {
                auto payloadVector = payloadDataChunk->getValueVector(i);
                for (auto& blockAppendInfo : blockAppendInfos) {
                    appendPayloadVectorAsFixSizedValues(*payloadVector,
                        blockAppendInfo.buffer + tupleAppendOffset,
                        payloadVector->state->getPositionOfCurrIdx(), blockAppendInfo.numEntries,
                        true);
                }
                tupleAppendOffset += payloadVector->getNumBytesPerValue();
            }
        } else {
            for (auto i = 0u; i < payloadDataChunk->valueVectors.size(); i++) {
                auto payloadVector = payloadDataChunk->getValueVector(i);
                for (auto& blockAppendInfo : blockAppendInfos) {
                    appendPayloadVectorAsOverflowValue(*payloadVector,
                        blockAppendInfo.buffer + tupleAppendOffset, blockAppendInfo.numEntries);
                }
                tupleAppendOffset += sizeof(overflow_value_t);
            }
        }
    }
}

void HashJoinBuild::appendResultSet() {
    if (keyDataChunk->state->selectedSize == 0) {
        return;
    }
    // Allocate htBlocks for tuples
    auto numTuplesToAppend = keyDataChunk->state->isFlat() ? 1 : keyDataChunk->state->selectedSize;
    auto blockAppendInfos = allocateHTBlocks(numTuplesToAppend);
    // Append tuples
    uint64_t tupleAppendOffset = 0; // The start offset of each field inside the tuple.
    appendKeyDataChunk(blockAppendInfos, tupleAppendOffset);
    appendPayloadDataChunks(blockAppendInfos, tupleAppendOffset);
    numEntries += numTuplesToAppend;
}

void HashJoinBuild::execute() {
    metrics->executionTime.start();
    // Append thread-local tuples
    do {
        prevOperator->getNextTuples();
        appendResultSet();
    } while (keyDataChunk->state->selectedSize > 0);

    // Merge thread-local state (numEntries, htBlocks, overflowBlocks) with the shared one
    {
        lock_guard<mutex> sharedStateLock(sharedState->hashJoinSharedStateLock);
        sharedState->numEntries += numEntries;
        move(begin(htBlocks), end(htBlocks), back_inserter(sharedState->htBlocks));
        move(
            begin(overflowBlocks), end(overflowBlocks), back_inserter(sharedState->overflowBlocks));
    }

    keyDataChunk->state->selectedSize = 0;
    metrics->executionTime.stop();
}
} // namespace processor
} // namespace graphflow

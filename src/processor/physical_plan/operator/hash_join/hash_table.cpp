#include "src/processor/include/physical_plan/operator/hash_join/hash_table.h"

#include <cassert>
#include <iostream>

#include "src/common/include/operations/hash_operations.h"

using namespace graphflow::common::operation;

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

HashTable::HashTable(MemoryManager* memManager, vector<PayloadInfo>& payloadInfos)
    : hashBitMask(0), htDirectory(nullptr), memManager(memManager), numEntries(0),
      overflowBlocks(memManager) {
    // key field (node_offset_t + label_t) + prev field (uint8_t*)
    numBytesForFixedTuplePart = sizeof(node_offset_t) + sizeof(label_t) + sizeof(uint8_t*);
    for (auto& payloadInfo : payloadInfos) {
        numBytesForFixedTuplePart +=
            (payloadInfo.isStoredAsOverflow ? sizeof(overflow_value_t) :
                                              payloadInfo.numBytesPerValue);
    }
    htBlockCapacity = HT_BLOCK_SIZE / numBytesForFixedTuplePart;
}

void HashTable::appendPayloadVectorAsFixSizedValues(ValueVector& vector, uint8_t* appendBuffer,
    uint64_t valueOffsetInVector, uint64_t appendCount, bool isSingleValue) const {
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

// First, add the vector into overflowBlocks, then, append the returned value into htBlocks
// appendCount times. (a sequence nodeID vector should never be passed in this function)
void HashTable::appendPayloadVectorAsOverflowValue(
    ValueVector& vector, uint8_t* appendBuffer, uint64_t appendCount) {
    assert(
        !((vector.getDataType() == NODE) && (dynamic_cast<NodeIDVector&>(vector).getIsSequence())));
    auto overflowValue = overflowBlocks.addVector(vector);
    for (uint64_t i = 0; i < appendCount; i++) {
        memcpy(appendBuffer + (i * numBytesForFixedTuplePart), (void*)&overflowValue,
            sizeof(overflow_value_t));
    }
}

void HashTable::appendKeyVector(NodeIDVector& vector, uint8_t* appendBuffer,
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

void HashTable::allocateHTBlocks(uint64_t remaining, vector<BlockAppendInfo>& blockAppendInfos) {
    lock_guard htLockGuard(htLock);
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
        auto newBlock = memManager->allocateBlock(HT_BLOCK_SIZE, appendCount);
        blockAppendInfos.emplace_back(newBlock->blockPtr, appendCount);
        htBlocks.push_back(move(newBlock));
        remaining -= appendCount;
    }
}

void HashTable::addDataChunks(
    DataChunk& keyDataChunk, uint64_t keyVectorPos, DataChunks& nonKeyDataChunks) {
    if (keyDataChunk.numSelectedValues == 0) {
        return;
    }

    // Allocate space for tuples
    auto numTuplesToAppend = keyDataChunk.isFlat() ? 1 : keyDataChunk.numSelectedValues;
    vector<BlockAppendInfo> blockAppendInfos;
    allocateHTBlocks(numTuplesToAppend, blockAppendInfos);

    // Append tuples
    auto tupleAppendOffset = 0; // The start offset of each field inside the tuple.
    // Append key vector
    auto keyVector = static_pointer_cast<NodeIDVector>(keyDataChunk.getValueVector(keyVectorPos));
    auto keyValOffsetInVec = keyDataChunk.isFlat() ? keyDataChunk.currPos : 0;
    for (auto& blockAppendInfo : blockAppendInfos) {
        auto blockAppendPtr = blockAppendInfo.buffer + tupleAppendOffset;
        auto blockAppendCount = blockAppendInfo.numEntries;
        appendKeyVector(*keyVector, blockAppendPtr, keyValOffsetInVec, blockAppendCount);
        keyValOffsetInVec += blockAppendCount;
    }
    tupleAppendOffset += NUM_BYTES_PER_NODE_ID;

    // Append payloads in key data chunk
    for (uint64_t i = 0; i < keyDataChunk.getNumAttributes(); i++) {
        if (i == keyVectorPos) {
            continue;
        }
        // Append payload vectors in the keyDataChunk
        auto payloadVector = keyDataChunk.getValueVector(i);
        auto valOffsetInVec = keyDataChunk.isFlat() ? keyDataChunk.currPos : 0;
        for (auto& blockAppendInfo : blockAppendInfos) {
            auto blockAppendPtr = blockAppendInfo.buffer + tupleAppendOffset;
            auto blockAppendCount = blockAppendInfo.numEntries;
            appendPayloadVectorAsFixSizedValues(*payloadVector, blockAppendPtr, valOffsetInVec,
                blockAppendCount, keyDataChunk.isFlat());
            valOffsetInVec += blockAppendCount;
        }
        tupleAppendOffset += payloadVector->getNumBytesPerValue();
    }

    // Append payload in non-key data chunks
    for (uint64_t chunkPos = 0; chunkPos < nonKeyDataChunks.getNumDataChunks(); chunkPos++) {
        auto payloadDataChunk = nonKeyDataChunks.getDataChunk(chunkPos);
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

    numEntries.fetch_add(numTuplesToAppend);
}

void HashTable::buildDirectory() {
    auto directory_capacity =
        nextPowerOfTwo(max(numEntries * 2, (HT_BLOCK_SIZE / sizeof(uint8_t)) + 1));
    hashBitMask = directory_capacity - 1;

    htDirectory = memManager->allocateBlock(directory_capacity * sizeof(uint8_t*));
    memset(htDirectory->blockPtr, 0, directory_capacity * sizeof(uint8_t*));

    nodeID_t nodeId;
    uint64_t hash;
    auto directory = (uint8_t**)htDirectory->blockPtr;
    for (auto& block : htBlocks) {
        uint8_t* basePtr = block->blockPtr;
        uint64_t entryPos = 0;
        while (entryPos < block->numEntries) {
            memcpy(&nodeId, basePtr, NUM_BYTES_PER_NODE_ID);
            hash = murmurhash64(nodeId.offset) ^ murmurhash64(nodeId.label);
            auto slotId = hash & hashBitMask;
            auto prevPtr = (uint8_t**)(basePtr + numBytesForFixedTuplePart - sizeof(uint8_t*));
            memcpy((uint8_t*)prevPtr, (void*)&(directory[slotId]), sizeof(uint8_t*));
            directory[slotId] = basePtr;
            basePtr += numBytesForFixedTuplePart;
            entryPos++;
        }
    }
}

} // namespace processor
} // namespace graphflow

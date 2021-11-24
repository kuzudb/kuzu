#include "src/processor/include/physical_plan/result/row_collection.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

bool RowLayout::operator==(const RowLayout& other) const {
    if (fields.size() != other.fields.size()) {
        return false;
    }
    for (auto i = 0u; i < fields.size(); i++) {
        if (fields[i] != other.fields[i]) {
            return false;
        }
    }
    return numBytesPerRow == other.numBytesPerRow;
}

RowCollection::RowCollection(MemoryManager& memoryManager, const RowLayout& layout)
    : memoryManager{memoryManager}, layout{layout}, numRows{0}, numRowsPerBlock{0} {
    assert(layout.numBytesPerRow <= DEFAULT_MEMORY_BLOCK_SIZE);
    numRowsPerBlock = DEFAULT_MEMORY_BLOCK_SIZE / layout.numBytesPerRow;
}

vector<BlockAppendingInfo> RowCollection::allocateDataBlocks(vector<DataBlock>& dataBlocks,
    uint64_t numBytesPerEntry, uint64_t numEntriesToAppend, bool allocateOnlyFromLastBlock) {
    assert(numBytesPerEntry < DEFAULT_MEMORY_BLOCK_SIZE);
    vector<BlockAppendingInfo> appendingInfos;
    int64_t blockPos = allocateOnlyFromLastBlock ? dataBlocks.size() - 1 : 0;
    blockPos = max(blockPos, (int64_t)0); // Set blockPos to 0 if dataBlocks is empty.
    for (; blockPos < dataBlocks.size(); blockPos++) {
        // Find free space in existing blocks
        if (numEntriesToAppend == 0) {
            break;
        }
        auto& block = dataBlocks[blockPos];
        auto numEntriesRemainingInBlock = block.freeSize / numBytesPerEntry;
        auto numEntriesAppending = min(numEntriesRemainingInBlock, numEntriesToAppend);
        if (numEntriesAppending > 0) {
            appendingInfos.emplace_back(
                block.data + DEFAULT_MEMORY_BLOCK_SIZE - block.freeSize, numEntriesAppending);
            block.freeSize -= numEntriesAppending * numBytesPerEntry;
            block.numEntries += numEntriesAppending;
        }
        numEntriesToAppend -= numEntriesAppending;
    }
    while (numEntriesToAppend > 0) {
        // Need allocate new blocks for rows
        auto numEntriesAppending = min(numRowsPerBlock, numEntriesToAppend);
        auto memBlock =
            memoryManager.allocateBlock(DEFAULT_MEMORY_BLOCK_SIZE, true /* initializeToZero */);
        appendingInfos.emplace_back(memBlock->data, numEntriesAppending);
        DataBlock block(move(memBlock));
        block.freeSize -= numEntriesAppending * numBytesPerEntry;
        block.numEntries = numEntriesAppending;
        dataBlocks.push_back(move(block));
        numEntriesToAppend -= numEntriesAppending;
    }
    return appendingInfos;
}

void RowCollection::copyVectorDataToBuffer(const ValueVector& vector, uint64_t valuePosInVec,
    uint64_t posStride, uint8_t* buffer, uint64_t offsetInBuffer, uint64_t offsetStride,
    uint64_t numValues) {
    auto numBytesPerValue = vector.getNumBytesPerValue();
    if (vector.dataType == NODE && vector.isSequence) {
        nodeID_t baseNodeID{UINT64_MAX, UINT64_MAX}, nodeID{UINT64_MAX, UINT64_MAX};
        vector.readNodeID(0, baseNodeID);
        nodeID.label = baseNodeID.label;
        if (vector.state->isUnfiltered()) {
            for (auto i = 0u; i < numValues; i++) {
                nodeID.offset = baseNodeID.offset + valuePosInVec + i * posStride;
                memcpy(buffer + offsetInBuffer + (i * offsetStride), (uint8_t*)&nodeID,
                    numBytesPerValue);
            }
        } else {
            for (auto i = 0u; i < numValues; i++) {
                auto pos = vector.state->selectedPositions[valuePosInVec + i * posStride];
                nodeID.offset = baseNodeID.offset + pos;
                memcpy(buffer + offsetInBuffer + (i * offsetStride), (uint8_t*)&nodeID,
                    numBytesPerValue);
            }
        }
    } else {
        if (vector.state->isUnfiltered()) {
            for (auto i = 0u; i < numValues; i++) {
                memcpy(buffer + offsetInBuffer + (i * offsetStride),
                    vector.values + (valuePosInVec + i * posStride) * numBytesPerValue,
                    numBytesPerValue);
            }
        } else {
            for (auto i = 0u; i < numValues; i++) {
                auto pos = vector.state->selectedPositions[valuePosInVec + i * posStride];
                memcpy(buffer + offsetInBuffer + (i * offsetStride),
                    vector.values + pos * numBytesPerValue, numBytesPerValue);
            }
        }
    }
}

overflow_value_t RowCollection::appendUnFlatVectorToOverflowBlocks(const ValueVector& vector) {
    assert(vector.state->isFlat() == false);
    auto numBytesForVector = vector.getNumBytesPerValue() * vector.state->selectedSize;
    auto appendInfos = allocateDataBlocks(vectorOverflowBlocks, numBytesForVector,
        1 /* numEntriesToAppend */, false /* allocateOnlyFromLastBlock */);
    assert(appendInfos.size() == 1);
    auto blockAppendPos = appendInfos[0].data;
    copyVectorDataToBuffer(vector, 0 /* valuePosInVec */, 1 /* posStride */, blockAppendPos,
        0 /* offsetInBuffer */, vector.getNumBytesPerValue(), vector.state->selectedSize);
    return overflow_value_t{numBytesForVector, blockAppendPos};
}

void RowCollection::copyVectorToBlock(const ValueVector& vector,
    const BlockAppendingInfo& blockAppendInfo, const FieldInLayout& field, uint64_t posInVector,
    uint64_t offsetInRow) {
    if (field.isVectorOverflow) {
        auto overflowValue = appendUnFlatVectorToOverflowBlocks(vector);
        for (auto i = 0u; i < blockAppendInfo.numEntriesToAppend; i++) {
            memcpy(blockAppendInfo.data + offsetInRow + (i * layout.numBytesPerRow),
                (uint8_t*)&overflowValue, sizeof(overflow_value_t));
        }
    } else {
        posInVector = vector.state->isFlat() ? vector.state->getPositionOfCurrIdx() : posInVector;
        // If the vector is flat, we always read the same value, thus the posStride is 0.
        auto posStride = vector.state->isFlat() ? 0 : 1;
        copyVectorDataToBuffer(vector, posInVector, posStride, blockAppendInfo.data, offsetInRow,
            layout.numBytesPerRow, blockAppendInfo.numEntriesToAppend);
    }
}

void RowCollection::appendVector(ValueVector& vector,
    const std::vector<BlockAppendingInfo>& blockAppendInfos, const FieldInLayout& field,
    uint64_t numRowsToAppend, uint64_t offsetInRow) {
    auto posInVector = 0;
    for (auto& blockAppendInfo : blockAppendInfos) {
        copyVectorToBlock(vector, blockAppendInfo, field, posInVector, offsetInRow);
        posInVector += blockAppendInfo.numEntriesToAppend;
    }
    assert(posInVector == numRowsToAppend);
}

void RowCollection::append(
    const vector<shared_ptr<ValueVector>>& vectors, uint64_t numRowsToAppend) {
    auto appendInfos = allocateDataBlocks(rowDataBlocks, layout.numBytesPerRow, numRowsToAppend,
        true /* allocateOnlyFromLastBlock */);
    uint64_t offsetInRow = 0;
    for (auto i = 0u; i < vectors.size(); i++) {
        appendVector(*vectors[i], appendInfos, layout.fields[i], numRowsToAppend, offsetInRow);
        offsetInRow += layout.fields[i].size;
    }
    numRows += numRowsToAppend;
}

void RowCollection::copyBufferDataToVector(uint8_t** locations, uint64_t offset, uint64_t length,
    ValueVector& vector, uint64_t valuePosInVec, uint64_t numValues) {
    if (vector.state->isUnfiltered()) {
        for (auto i = 0u; i < numValues; i++) {
            auto pos = valuePosInVec + i;
            memcpy(vector.values + pos * length, locations[i] + offset, length);
        }
    } else {
        for (auto i = 0u; i < numValues; i++) {
            auto pos = vector.state->selectedPositions[valuePosInVec + i];
            memcpy(vector.values + pos * length, locations[i] + offset, length);
        }
    }
}

void RowCollection::readVector(const FieldInLayout& field, uint8_t** rows, uint64_t offsetInRow,
    uint64_t startRowPos, ValueVector& vector, uint64_t numValues) const {
    if (field.isVectorOverflow) {
        assert(vector.state->isFlat() == false && numValues == 1);
        auto vectorOverflowValue = *(overflow_value_t*)(rows[startRowPos] + offsetInRow);
        auto numValuesInVector = vectorOverflowValue.len / vector.getNumBytesPerValue();
        copyBufferDataToVector(&vectorOverflowValue.value, 0 /* offset */, vectorOverflowValue.len,
            vector, 0 /* valuePosInVec */, numValues);
        vector.state->selectedSize = numValuesInVector;
    } else {
        assert((vector.state->isFlat() && numValues == 1) || !vector.state->isFlat());
        auto valuePosInVec = vector.state->isFlat() ? vector.state->getPositionOfCurrIdx() : 0;
        copyBufferDataToVector(
            rows, offsetInRow, vector.getNumBytesPerValue(), vector, valuePosInVec, numValues);
        vector.state->selectedSize = numValues;
    }
}

uint64_t RowCollection::scan(const vector<uint64_t>& fieldsToScan,
    const vector<DataPos>& resultDataPos, ResultSet& resultSet, uint64_t startRowId,
    uint64_t numRowsToScan) const {
    assert(resultDataPos.size() == fieldsToScan.size());
    if (startRowId >= numRows) {
        return 0;
    }
    numRowsToScan = min(numRowsToScan, numRows - startRowId);
    unique_ptr<uint8_t*[]> rowsToScan = make_unique<uint8_t*[]>(numRowsToScan);
    for (auto i = 0u; i < numRowsToScan; i++) {
        rowsToScan[i] = getRow(startRowId + i);
    }
    return lookup(
        fieldsToScan, resultDataPos, resultSet, rowsToScan.get(), 0 /* startPos */, numRowsToScan);
}

uint64_t RowCollection::lookup(const vector<uint64_t>& fieldsToRead,
    const vector<DataPos>& resultDataPos, ResultSet& resultSet, uint8_t** rowsToLookup,
    uint64_t startPos, uint64_t numRowsToRead) const {
    assert(resultDataPos.size() == fieldsToRead.size());
    auto hasVectorOverflow = false;
    for (auto& fieldId : fieldsToRead) {
        if (layout.fields[fieldId].isVectorOverflow) {
            hasVectorOverflow = true;
        }
    }
    numRowsToRead = min(hasVectorOverflow ? 1 : DEFAULT_VECTOR_CAPACITY, numRowsToRead);
    vector<uint64_t> fieldOffsets;
    for (auto& fieldId : fieldsToRead) {
        fieldOffsets.push_back(getFieldOffsetInRow(fieldId));
    }
    for (auto i = 0u; i < fieldsToRead.size(); i++) {
        auto dataPos = resultDataPos[i];
        auto resultVector =
            resultSet.dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
        readVector(layout.fields[fieldsToRead[i]], rowsToLookup, fieldOffsets[i], startPos,
            *resultVector, numRowsToRead);
    }
    return numRowsToRead;
}

void RowCollection::merge(unique_ptr<RowCollection> other) {
    assert(layout == other->layout);
    move(begin(other->rowDataBlocks), end(other->rowDataBlocks), back_inserter(rowDataBlocks));
    move(begin(other->vectorOverflowBlocks), end(other->vectorOverflowBlocks),
        back_inserter(vectorOverflowBlocks));
    this->numRows += other->numRows;
}

uint64_t RowCollection::getFieldOffsetInRow(uint64_t fieldId) const {
    uint64_t offset = 0;
    for (auto i = 0u; i < fieldId; i++) {
        offset += layout.fields[i].size;
    }
    return offset;
}
} // namespace processor
} // namespace graphflow

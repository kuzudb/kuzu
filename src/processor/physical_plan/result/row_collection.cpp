#include "src/processor/include/physical_plan/result/row_collection.h"

#include "src/common/include/exception.h"

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
    assert(layout.isInitialized);
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
    uint64_t numValues, uint64_t colIdx, bool isVectorOverflow) {
    auto numBytesPerValue = vector.getNumBytesPerValue();
    if (vector.dataType == NODE && vector.isSequence) {
        nodeID_t baseNodeID{UINT64_MAX, UINT64_MAX}, nodeID{UINT64_MAX, UINT64_MAX};
        vector.readNodeID(0, baseNodeID);
        nodeID.label = baseNodeID.label;
        for (auto i = 0u; i < numValues; i++) {
            nodeID.offset = baseNodeID.offset + vector.state->isUnfiltered() ?
                                (valuePosInVec + i * posStride) :
                                (vector.state->selectedPositions[valuePosInVec + i * posStride]);
            memcpy(
                buffer + offsetInBuffer + (i * offsetStride), (uint8_t*)&nodeID, numBytesPerValue);
            if (vector.isNull(i)) {
                if (isVectorOverflow) {
                    // for overflow column, the nullMap is at the end of the overflow memory
                    setNullMap(buffer + numValues * offsetStride, i);
                } else {
                    // for other columns, the nullMap is at the end of each row
                    setNullMap(buffer + i * offsetStride + layout.getNullMapOffset(), colIdx);
                }
            }
        }
    } else {
        for (auto i = 0u; i < numValues; i++) {
            // update the null map
            const auto pos = vector.state->isUnfiltered() ?
                                 (valuePosInVec + i * posStride) :
                                 vector.state->selectedPositions[valuePosInVec + i * posStride];
            memcpy(buffer + offsetInBuffer + (i * offsetStride),
                vector.values + pos * numBytesPerValue, numBytesPerValue);
            if (vector.isNull(pos)) {
                if (isVectorOverflow) {
                    setNullMap(buffer + numValues * offsetStride, i);
                } else {
                    setNullMap(buffer + i * offsetStride + layout.getNullMapOffset(), colIdx);
                }
            }
        }
    }
}

overflow_value_t RowCollection::appendUnFlatVectorToOverflowBlocks(
    const ValueVector& vector, uint64_t colIdx) {
    if (vector.state->isFlat()) {
        throw RowCollectionException(
            "Append an unflat vector to an overflow column is not allowed!");
    }
    auto selectedSize = vector.state->selectedSize;
    // For the overflow column, the nullMap size = ceil(selectedSize / 4)
    auto numBytesForVector =
        vector.getNumBytesPerValue() * selectedSize +
        (((selectedSize >> 2) + ((selectedSize & 3) != 0)) << 2); // &3 is the same as %4
    auto appendInfos = allocateDataBlocks(vectorOverflowBlocks, numBytesForVector,
        1 /* numEntriesToAppend */, false /* allocateOnlyFromLastBlock */);
    assert(appendInfos.size() == 1);
    auto blockAppendPos = appendInfos[0].data;

    copyVectorDataToBuffer(vector, 0 /* valuePosInVec */, 1 /* posStride */, blockAppendPos,
        0 /* offsetInBuffer */, vector.getNumBytesPerValue(), vector.state->selectedSize, colIdx,
        true /* isVectorOverflow */);
    return overflow_value_t{selectedSize, blockAppendPos};
}

void RowCollection::copyVectorToBlock(const ValueVector& vector,
    const BlockAppendingInfo& blockAppendInfo, const FieldInLayout& field, uint64_t posInVector,
    uint64_t offsetInRow, uint64_t colIdx) {
    if (field.isVectorOverflow) {
        auto overflowValue = appendUnFlatVectorToOverflowBlocks(vector, colIdx);
        for (auto i = 0u; i < blockAppendInfo.numEntriesToAppend; i++) {
            memcpy(blockAppendInfo.data + offsetInRow + (i * layout.numBytesPerRow),
                (uint8_t*)&overflowValue, sizeof(overflow_value_t));
        }
    } else {
        posInVector = vector.state->isFlat() ? vector.state->getPositionOfCurrIdx() : posInVector;
        // If the vector is flat, we always read the same value, thus the posStride is 0.
        auto posStride = vector.state->isFlat() ? 0 : 1;
        copyVectorDataToBuffer(vector, posInVector, posStride, blockAppendInfo.data, offsetInRow,
            layout.numBytesPerRow, blockAppendInfo.numEntriesToAppend, colIdx,
            false /* isVectorOverflow */);
    }
}

void RowCollection::appendVector(ValueVector& vector,
    const std::vector<BlockAppendingInfo>& blockAppendInfos, const FieldInLayout& field,
    uint64_t numRowsToAppend, uint64_t offsetInRow, uint64_t colIdx) {
    auto posInVector = 0;
    for (auto& blockAppendInfo : blockAppendInfos) {
        copyVectorToBlock(vector, blockAppendInfo, field, posInVector, offsetInRow, colIdx);
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
        appendVector(*vectors[i], appendInfos, layout.fields[i], numRowsToAppend, offsetInRow, i);
        offsetInRow += layout.fields[i].size;
    }
    numRows += numRowsToAppend;
}

void RowCollection::readOverflowVector(
    uint8_t** rows, uint64_t offsetInRow, uint64_t startRowPos, ValueVector& vector) const {
    if (vector.state->isFlat()) {
        throw RowCollectionException(
            "Read an overflow column to a flat valueVector is not allowed!");
    }
    auto vectorOverflowValue = *(overflow_value_t*)(rows[startRowPos] + offsetInRow);
    auto overflowBufferDataLen = vectorOverflowValue.numElements * vector.getNumBytesPerValue();
    auto pos = vector.state->isUnfiltered() ? 0 : vector.state->selectedPositions[0];
    // copy the whole overflowBuffer(except the nullMap) back to the vector
    memcpy(vector.values + pos * overflowBufferDataLen, vectorOverflowValue.value,
        overflowBufferDataLen);
    // we need to check the null bit for each element in the overflow column, and update the
    // null mask in the valueVector. Note: we always put the nullMap at the end of the overflow
    // buffer
    auto nullMapBuffer = vectorOverflowValue.value + overflowBufferDataLen;
    for (auto i = 0u; i < vectorOverflowValue.numElements; i++) {
        vector.setNull(pos + i, isNull(nullMapBuffer, i));
    }
    vector.state->selectedSize = vectorOverflowValue.numElements;
}

void RowCollection::readNonOverflowVector(uint8_t** rows, uint64_t offsetInRow, ValueVector& vector,
    uint64_t numRowsToRead, uint64_t colIdx) const {
    assert((vector.state->isFlat() && numRowsToRead == 1) || !vector.state->isFlat());
    auto valuePosInVec = vector.state->isFlat() ? vector.state->getPositionOfCurrIdx() : 0;
    for (auto i = 0u; i < numRowsToRead; i++) {
        auto nullMapBuffer = rows[i] + layout.getNullMapOffset();
        auto pos = vector.state->isUnfiltered() ?
                       valuePosInVec + i :
                       vector.state->selectedPositions[valuePosInVec + i];
        memcpy(vector.values + pos * vector.getNumBytesPerValue(), rows[i] + offsetInRow,
            vector.getNumBytesPerValue());
        vector.setNull(pos, isNull(nullMapBuffer, colIdx));
    }
    vector.state->selectedSize = numRowsToRead;
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
        auto fieldToRead = layout.fields[fieldsToRead[i]];
        if (fieldToRead.isVectorOverflow) {
            // for overflow vectors, we can only read one row at a time and the valueVector must be
            // unflat
            readOverflowVector(rowsToLookup, fieldOffsets[i], startPos, *resultVector);
        } else {
            readNonOverflowVector(rowsToLookup, fieldOffsets[i], *resultVector, numRowsToRead, i);
        }
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

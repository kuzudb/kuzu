#include "src/processor/include/physical_plan/result/factorized_table.h"

#include "src/common/include/exception.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

bool TupleSchema::operator==(const TupleSchema& other) const {
    if (columns.size() != other.columns.size()) {
        return false;
    }
    for (auto i = 0u; i < columns.size(); i++) {
        if (columns[i] != other.columns[i]) {
            return false;
        }
    }
    return numBytesPerTuple == other.numBytesPerTuple;
}

FactorizedTable::FactorizedTable(MemoryManager& memoryManager, const TupleSchema& tupleSchema)
    : memoryManager{memoryManager}, tupleSchema{tupleSchema}, numTuples{0}, numTuplesPerBlock{0},
      totalNumFlatTuples{0} {
    assert(tupleSchema.numBytesPerTuple <= DEFAULT_MEMORY_BLOCK_SIZE);
    assert(tupleSchema.isInitialized);

    stringBuffer = make_unique<StringBuffer>(memoryManager);
    numTuplesPerBlock = DEFAULT_MEMORY_BLOCK_SIZE / tupleSchema.numBytesPerTuple;
    consecutiveIndicesOfAllColumns.resize(tupleSchema.columns.size());
    iota(consecutiveIndicesOfAllColumns.begin(), consecutiveIndicesOfAllColumns.end(), 0);
}

vector<BlockAppendingInfo> FactorizedTable::allocateDataBlocks(vector<DataBlock>& dataBlocks,
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
        // Need allocate new blocks for tuples.
        auto numEntriesAppending = min(numTuplesPerBlock, numEntriesToAppend);
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

void FactorizedTable::copyVectorDataToBuffer(ValueVector& vector, uint64_t valuePosInVecIfUnflat,
    uint8_t* buffer, uint64_t offsetInBuffer, uint64_t offsetStride, uint64_t numValues,
    uint64_t colIdx, bool isUnflat) {
    for (auto i = 0u; i < numValues; i++) {
        // update the null map
        const auto pos = vector.state->isFlat() ?
                             vector.state->getPositionOfCurrIdx() :
                             (vector.state->isUnfiltered() ?
                                     (valuePosInVecIfUnflat + i) :
                                     vector.state->selectedPositions[valuePosInVecIfUnflat + i]);
        if (vector.isNull(pos)) {
            if (isUnflat) {
                setNullMap(buffer + numValues * offsetStride, i);
            } else {
                setNullMap(buffer + i * offsetStride + tupleSchema.getNullMapOffset(), colIdx);
            }
        } else {
            vector.copyNonNullDataWithSameTypeOutFromPos(
                pos, buffer + offsetInBuffer + (i * offsetStride), *stringBuffer);
        }
    }
}

overflow_value_t FactorizedTable::appendUnFlatVectorToOverflowBlocks(
    ValueVector& vector, uint64_t colIdx) {
    if (vector.state->isFlat()) {
        throw FactorizedTableException("Append a flat vector to an unflat column is not allowed!");
    }
    auto selectedSize = vector.state->selectedSize;
    // For unflat columns, the nullMap size = ceil(selectedSize / 4).
    auto numBytesForVector =
        vector.getNumBytesPerValue() * selectedSize +
        (((selectedSize >> 2) + ((selectedSize & 3) != 0)) << 2); // &3 is the same as %4
    auto appendInfos = allocateDataBlocks(vectorOverflowBlocks, numBytesForVector,
        1 /* numEntriesToAppend */, false /* allocateOnlyFromLastBlock */);
    assert(appendInfos.size() == 1);
    auto blockAppendPos = appendInfos[0].data;

    copyVectorDataToBuffer(vector, 0 /* valuePosInVec */, blockAppendPos, 0 /* offsetInBuffer */,
        vector.getNumBytesPerValue(), vector.state->selectedSize, colIdx, true /* isUnflat */);
    return overflow_value_t{selectedSize, blockAppendPos};
}

void FactorizedTable::copyVectorToBlock(ValueVector& vector,
    const BlockAppendingInfo& blockAppendInfo, const ColumnInTupleSchema& column,
    uint64_t posInVector, uint64_t offsetInTuple, uint64_t colIdx) {
    if (column.isUnflat) {
        auto overflowValue = appendUnFlatVectorToOverflowBlocks(vector, colIdx);
        for (auto i = 0u; i < blockAppendInfo.numEntriesToAppend; i++) {
            memcpy(blockAppendInfo.data + offsetInTuple + (i * tupleSchema.numBytesPerTuple),
                (uint8_t*)&overflowValue, sizeof(overflow_value_t));
        }
    } else {
        // posInVector argument where we possibly pass -1 will be ignored if the vector is flat.
        copyVectorDataToBuffer(vector, vector.state->isFlat() ? -1 : posInVector,
            blockAppendInfo.data, offsetInTuple, tupleSchema.numBytesPerTuple,
            blockAppendInfo.numEntriesToAppend, colIdx, false /* isUnflat */);
    }
}

void FactorizedTable::appendVector(ValueVector& vector,
    const std::vector<BlockAppendingInfo>& blockAppendInfos, const ColumnInTupleSchema& column,
    uint64_t numTuples, uint64_t offsetInTuple, uint64_t colIdx) {
    auto posInVector = 0;
    for (auto& blockAppendInfo : blockAppendInfos) {
        copyVectorToBlock(vector, blockAppendInfo, column, posInVector, offsetInTuple, colIdx);
        posInVector += blockAppendInfo.numEntriesToAppend;
    }
    assert(posInVector == numTuples);
}

void FactorizedTable::append(
    const vector<shared_ptr<ValueVector>>& vectors, uint64_t numTuplesToAppend) {
    auto appendInfos = allocateDataBlocks(tupleDataBlocks, tupleSchema.numBytesPerTuple,
        numTuplesToAppend, true /* allocateOnlyFromLastBlock */);
    uint64_t offsetInTuple = 0;
    for (auto i = 0u; i < vectors.size(); i++) {
        appendVector(
            *vectors[i], appendInfos, tupleSchema.columns[i], numTuplesToAppend, offsetInTuple, i);
        offsetInTuple += tupleSchema.columns[i].getColumnSize();
    }
    numTuples += numTuplesToAppend;
    for (auto i = 1; i <= numTuplesToAppend; i++) {
        totalNumFlatTuples += getNumFlatTuples(numTuples - i);
    }
}

void FactorizedTable::readUnflatVector(
    uint8_t** tuples, uint64_t offsetInTuple, uint64_t startTuplePos, ValueVector& vector) const {
    if (vector.state->isFlat()) {
        throw FactorizedTableException(
            "Reading an unflat column to a flat valueVector is not allowed!");
    }
    auto vectorOverflowValue = *(overflow_value_t*)(tuples[startTuplePos] + offsetInTuple);
    auto overflowBufferDataLen = vectorOverflowValue.numElements * vector.getNumBytesPerValue();
    if (!vector.state->isUnfiltered()) {
        throw FactorizedTableException(
            "Reading an unflat column into a filtered valueVector is not allowed!");
    }
    auto nullMapBuffer = vectorOverflowValue.value + overflowBufferDataLen;
    for (auto i = 0u; i < vectorOverflowValue.numElements; i++) {
        auto isPosNull = isNull(nullMapBuffer, i);
        vector.setNull(i, isPosNull);
        if (!isPosNull) {
            vector.copyNonNullDataWithSameTypeIntoPos(
                i, vectorOverflowValue.value + i * vector.getNumBytesPerValue());
        }
    }
    vector.state->selectedSize = vectorOverflowValue.numElements;
}

void FactorizedTable::readFlatVector(uint8_t** tuples, uint64_t offsetInTuple, ValueVector& vector,
    uint64_t numTuplesToRead, uint64_t colIdx, uint64_t startPos, uint64_t valuePosInVec) const {
    assert((vector.state->isFlat() && numTuplesToRead == 1) || !vector.state->isFlat());
    for (auto i = 0u; i < numTuplesToRead; i++) {
        auto nullMapBuffer = tuples[i + startPos] + tupleSchema.getNullMapOffset();
        auto pos = vector.state->isUnfiltered() ?
                       valuePosInVec + i :
                       vector.state->selectedPositions[valuePosInVec + i];
        auto isPosNull = isNull(nullMapBuffer, colIdx);
        vector.setNull(pos, isPosNull);
        if (!isPosNull) {
            vector.copyNonNullDataWithSameTypeIntoPos(pos, tuples[i + startPos] + offsetInTuple);
        }
    }
    // If the vector is a flalt valueVector, the selectedSize must be 1.
    // If the vector is an unflat valueVector, the selectedSize = original size + numTuplesToRead.
    vector.state->selectedSize = vector.state->isFlat() ? 1 : valuePosInVec + numTuplesToRead;
}

vector<uint64_t> FactorizedTable::computeColumnOffsets(
    const vector<uint64_t>& columnsToRead) const {
    vector<uint64_t> columnOffsets;
    for (auto& columnIdx : columnsToRead) {
        columnOffsets.push_back(getColumnOffsetInTuple(columnIdx));
    }
    return columnOffsets;
}

bool FactorizedTable::hasUnflatColToRead(const vector<uint64_t>& columnsToRead) const {
    for (auto& columnIdx : columnsToRead) {
        if (tupleSchema.columns[columnIdx].isUnflat) {
            return true;
        }
    }
    return false;
}

void FactorizedTable::readFlatTupleToUnflatVector(const vector<uint64_t>& columnsToScan,
    const vector<DataPos>& resultDataPos, ResultSet& resultSet, uint64_t tupleIdx,
    uint64_t valuePosInVec) const {
    assert(resultDataPos.size() == columnsToScan.size());
    assert(tupleIdx < numTuples);
    uint8_t* tuplesToRead[1] = {getTuple(tupleIdx)};
    auto columnOffsets = computeColumnOffsets(columnsToScan);
    for (auto i = 0u; i < columnsToScan.size(); i++) {
        auto dataPos = resultDataPos[i];
        auto resultVector =
            resultSet.dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
        readFlatVector(tuplesToRead, columnOffsets[i], *resultVector, 1 /* numTuplesToRead */, i,
            0 /* startPos */, valuePosInVec);
    }
}

uint64_t FactorizedTable::scan(const vector<uint64_t>& columnsToScan,
    const vector<DataPos>& resultDataPos, ResultSet& resultSet, uint64_t startTupleIdx,
    uint64_t numTuplesToScan) const {
    assert(resultDataPos.size() == columnsToScan.size());
    if (startTupleIdx >= numTuples) {
        return 0;
    }
    numTuplesToScan = min(numTuplesToScan, numTuples - startTupleIdx);
    unique_ptr<uint8_t*[]> tuplesToScan = make_unique<uint8_t*[]>(numTuplesToScan);
    for (auto i = 0u; i < numTuplesToScan; i++) {
        tuplesToScan[i] = getTuple(startTupleIdx + i);
    }
    return lookup(columnsToScan, resultDataPos, resultSet, tuplesToScan.get(), 0 /* startPos */,
        numTuplesToScan);
}

// This scan function scans all columns in the factorizedTable and outputs to resultSet.
uint64_t FactorizedTable::scan(const vector<DataPos>& resultDataPos, ResultSet& resultSet,
    uint64_t startTupleIdx, uint64_t numTuplesToScan) const {
    scan(consecutiveIndicesOfAllColumns, resultDataPos, resultSet, startTupleIdx, numTuplesToScan);
}

uint64_t FactorizedTable::lookup(const vector<uint64_t>& columnsToRead,
    const vector<DataPos>& resultDataPos, ResultSet& resultSet, uint8_t** tuplesToRead,
    uint64_t startPos, uint64_t numTuplesToRead) const {
    assert(resultDataPos.size() == columnsToRead.size());
    auto hasUnflatColumn = hasUnflatColToRead(columnsToRead);
    numTuplesToRead = min(hasUnflatColumn ? 1 : DEFAULT_VECTOR_CAPACITY, numTuplesToRead);
    auto columnOffsets = computeColumnOffsets(columnsToRead);
    for (auto i = 0u; i < columnsToRead.size(); i++) {
        auto dataPos = resultDataPos[i];
        auto resultVector =
            resultSet.dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
        auto columnToRead = tupleSchema.columns[columnsToRead[i]];
        if (columnToRead.isUnflat) {
            // For unflat vectors, we can only read one tuple at a time and the valueVector must be
            // unflat.
            readUnflatVector(tuplesToRead, columnOffsets[i], startPos, *resultVector);
        } else {
            readFlatVector(tuplesToRead, columnOffsets[i], *resultVector, numTuplesToRead, i,
                startPos,
                resultVector->state->isFlat() ? resultVector->state->getPositionOfCurrIdx() :
                                                0 /* valuePosInVec */);
        }
    }
    return numTuplesToRead;
}

void FactorizedTable::merge(FactorizedTable& other) {
    assert(tupleSchema == other.tupleSchema);
    move(begin(other.vectorOverflowBlocks), end(other.vectorOverflowBlocks),
        back_inserter(vectorOverflowBlocks));

    auto appendInfos = allocateDataBlocks(tupleDataBlocks, tupleSchema.numBytesPerTuple,
        other.numTuples, true /* allocateOnlyFromLastBlock */);
    auto otherTupleIdx = 0u;
    for (auto& appendInfo : appendInfos) {
        for (auto i = 0u; i < appendInfo.numEntriesToAppend; i++) {
            memcpy(appendInfo.data + i * tupleSchema.numBytesPerTuple,
                other.getTuple(otherTupleIdx), tupleSchema.numBytesPerTuple);
            otherTupleIdx++;
        }
    }

    this->stringBuffer->merge(*other.stringBuffer);
    this->numTuples += other.numTuples;
    this->totalNumFlatTuples += other.totalNumFlatTuples;
}

uint64_t FactorizedTable::getColumnOffsetInTuple(uint64_t columnIdx) const {
    uint64_t offset = 0;
    for (auto i = 0u; i < columnIdx; i++) {
        offset += tupleSchema.columns[i].getColumnSize();
    }
    return offset;
}

FlatTupleIterator FactorizedTable::getFlatTuples() {
    return FlatTupleIterator(*this);
}

FlatTupleIterator::FlatTupleIterator(FactorizedTable& factorizedTable)
    : factorizedTable{factorizedTable}, nextFlatTupleIdx{0}, nextTupleIdx{1} {
    // Don't initialize currentTupleBuffer, numFlatTuples if there are no tuples in the
    // factorizedTable.
    if (factorizedTable.getNumTuples()) {
        currentTupleBuffer = factorizedTable.getTuple(0);
        numFlatTuples = factorizedTable.getNumFlatTuples(0);
        updateNumElementsInDataChunk();
        updateInvalidEntriesInFlatTuplePositionsInDataChunk();
        for (auto& column : factorizedTable.getTupleSchema().columns) {
            dataTypes.emplace_back(column.dataType);
        }
    }
}

void FlatTupleIterator::updateInvalidEntriesInFlatTuplePositionsInDataChunk() {
    for (auto i = 0u; i < flatTuplePositionsInDataChunk.size(); i++) {
        bool isValidEntry = false;
        for (auto& column : factorizedTable.getTupleSchema().columns) {
            if (column.dataChunkPos == i) {
                isValidEntry = true;
                break;
            }
        }
        if (!isValidEntry) {
            flatTuplePositionsInDataChunk[i] = make_pair(UINT64_MAX, UINT64_MAX);
        }
    }
}

void FlatTupleIterator::updateFlatTuplePositionsInDataChunk() {
    for (auto i = 0u; i < flatTuplePositionsInDataChunk.size(); i++) {
        if (!isValidDataChunkPos(i)) {
            continue;
        }
        flatTuplePositionsInDataChunk.at(i).first++;
        auto tuplePosition = flatTuplePositionsInDataChunk.at(i);
        // If we have output all elements in the current column, we reset the
        // nextIdxToReadInDataChunk in the current column to 0.
        if (tuplePosition.first >= tuplePosition.second - 1) {
            tuplePosition.first = 0;
        } else {
            // If the current dataChunk is not full, then we don't need to update the next
            // dataChunk.
            break;
        }
    }
}

void FlatTupleIterator::readValueBufferToFlatTuple(
    DataType dataType, FlatTuple& flatTuple, uint64_t flatTupleValIdx, uint8_t* valueBuffer) {
    switch (dataType) {
    case INT64: {
        flatTuple.getValue(flatTupleValIdx)->val.int64Val = *((int64_t*)valueBuffer);
    } break;
    case BOOL: {
        flatTuple.getValue(flatTupleValIdx)->val.booleanVal = *((bool*)valueBuffer);
    } break;
    case DOUBLE: {
        flatTuple.getValue(flatTupleValIdx)->val.doubleVal = *((double_t*)valueBuffer);
    } break;
    case STRING: {
        flatTuple.getValue(flatTupleValIdx)->val.strVal = *((gf_string_t*)valueBuffer);
    } break;
    case NODE: {
        flatTuple.getValue(flatTupleValIdx)->val.nodeID = *((nodeID_t*)valueBuffer);
    } break;
    case UNSTRUCTURED: {
        *flatTuple.getValue(flatTupleValIdx) = *((Value*)valueBuffer);
    } break;
    case DATE: {
        flatTuple.getValue(flatTupleValIdx)->val.dateVal = *((date_t*)valueBuffer);
    } break;
    case TIMESTAMP: {
        flatTuple.getValue(flatTupleValIdx)->val.timestampVal = *((timestamp_t*)valueBuffer);
    } break;
    case INTERVAL: {
        flatTuple.getValue(flatTupleValIdx)->val.intervalVal = *((interval_t*)valueBuffer);
    } break;
    default:
        assert(false);
    }
}

void FlatTupleIterator::readUnflatColToFlatTuple(FlatTuple& flatTuple, uint64_t flatTupleValIdx,
    const ColumnInTupleSchema& column, uint8_t* valueBuffer) {
    auto overflowValue = (overflow_value_t*)valueBuffer;
    valueBuffer =
        overflowValue->value + TypeUtils::getDataTypeSize(column.dataType) *
                                   flatTuplePositionsInDataChunk[column.dataChunkPos].first;
    auto nullMapBuffer = overflowValue->value +
                         overflowValue->numElements * TypeUtils::getDataTypeSize(column.dataType);
    flatTuple.nullMask[flatTupleValIdx] = FactorizedTable::isNull(
        nullMapBuffer, flatTuplePositionsInDataChunk[column.dataChunkPos].first);
    if (!flatTuple.nullMask[flatTupleValIdx]) {
        readValueBufferToFlatTuple(column.dataType, flatTuple, flatTupleValIdx, valueBuffer);
    }
}

void FlatTupleIterator::readFlatColToFlatTuple(FlatTuple& flatTuple, uint64_t flatTupleValIdx,
    const ColumnInTupleSchema& column, uint8_t* valueBuffer) {
    auto nullMapBuffer = currentTupleBuffer + factorizedTable.getTupleSchema().getNullMapOffset();
    flatTuple.nullMask[flatTupleValIdx] = FactorizedTable::isNull(nullMapBuffer, flatTupleValIdx);
    if (!flatTuple.nullMask[flatTupleValIdx]) {
        readValueBufferToFlatTuple(column.dataType, flatTuple, flatTupleValIdx, valueBuffer);
    }
}

void FlatTupleIterator::updateNumElementsInDataChunk() {
    auto colOffsetInTupleBuffer = 0ul;
    for (auto column : factorizedTable.getTupleSchema().columns) {
        // If this is an unflat column, the number of elements is stored in the
        // overflow_value_t struct. Otherwise the number of elements is 1.
        auto numElementsInDataChunk =
            column.isUnflat ?
                ((overflow_value_t*)(currentTupleBuffer + colOffsetInTupleBuffer))->numElements :
                1;
        if (column.dataChunkPos >= flatTuplePositionsInDataChunk.size()) {
            flatTuplePositionsInDataChunk.resize(column.dataChunkPos + 1);
        }
        flatTuplePositionsInDataChunk[column.dataChunkPos] =
            make_pair(0 /* nextIdxToReadInDataChunk */, numElementsInDataChunk);
        colOffsetInTupleBuffer += column.getColumnSize();
    }
}

FlatTuple FlatTupleIterator::getNextFlatTuple() {
    // Go to the next tuple if we have iterated all the flat tuples of the current tuple.
    if (nextFlatTupleIdx >= numFlatTuples) {
        currentTupleBuffer = factorizedTable.getTuple(nextTupleIdx);
        numFlatTuples = factorizedTable.getNumFlatTuples(nextTupleIdx);
        nextFlatTupleIdx = 0;
        updateNumElementsInDataChunk();
        nextTupleIdx++;
    }
    auto colOffsetInTupleBuffer = 0ul;
    FlatTuple flatTuple(dataTypes);
    for (auto i = 0ul; i < factorizedTable.getTupleSchema().columns.size(); i++) {
        auto column = factorizedTable.getTupleSchema().columns[i];
        auto valueBuffer = currentTupleBuffer + colOffsetInTupleBuffer;
        if (column.isUnflat) {
            readUnflatColToFlatTuple(flatTuple, i, column, valueBuffer);
        } else {
            readFlatColToFlatTuple(flatTuple, i, column, valueBuffer);
        }
        colOffsetInTupleBuffer += column.getColumnSize();
    }
    updateFlatTuplePositionsInDataChunk();
    nextFlatTupleIdx++;
    return flatTuple;
}

} // namespace processor
} // namespace graphflow

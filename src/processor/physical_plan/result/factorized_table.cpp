#include "src/processor/include/physical_plan/result/factorized_table.h"

#include "src/common/include/exception.h"
#include "src/common/include/vector/value_vector_utils.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace processor {

ColumnSchema::ColumnSchema(const ColumnSchema& other) {
    isUnflat = other.isUnflat;
    dataChunkPos = other.dataChunkPos;
    numBytes = other.numBytes;
    mayContainNulls = other.mayContainNulls;
}

TableSchema::TableSchema(const TableSchema& other) {
    for (auto& column : other.columns) {
        appendColumn(make_unique<ColumnSchema>(*column));
    }
}

void TableSchema::appendColumn(unique_ptr<ColumnSchema> column) {
    numBytesForDataPerTuple += column->getNumBytes();
    columns.push_back(move(column));
    colOffsets.push_back(
        colOffsets.empty() ? 0 : colOffsets.back() + getColumn(columns.size() - 2)->getNumBytes());
    numBytesForNullMapPerTuple = getNumBytesForNullBuffer(getNumColumns());
    numBytesPerTuple = numBytesForDataPerTuple + numBytesForNullMapPerTuple;
}

bool TableSchema::operator==(const TableSchema& other) const {
    if (columns.size() != other.columns.size()) {
        return false;
    }
    for (auto i = 0u; i < columns.size(); i++) {
        if (*columns[i] != *other.columns[i]) {
            return false;
        }
    }
    return numBytesForDataPerTuple == other.numBytesForDataPerTuple && numBytesForNullMapPerTuple &&
           other.numBytesForNullMapPerTuple;
}

FactorizedTable::FactorizedTable(MemoryManager* memoryManager, unique_ptr<TableSchema> tableSchema)
    : memoryManager{memoryManager}, tableSchema{move(tableSchema)}, numTuples{0}, numTuplesPerBlock{
                                                                                      0} {
    assert(this->tableSchema->getNumBytesPerTuple() <= LARGE_PAGE_SIZE);
    if (!this->tableSchema->isEmpty()) {
        overflowBuffer = make_unique<OverflowBuffer>(memoryManager);
        numTuplesPerBlock = LARGE_PAGE_SIZE / this->tableSchema->getNumBytesPerTuple();
    }
}

void FactorizedTable::append(const vector<shared_ptr<ValueVector>>& vectors) {
    auto numTuplesToAppend = computeNumTuplesToAppend(vectors);
    auto appendInfos = allocateTupleBlocks(numTuplesToAppend);
    for (auto i = 0u; i < vectors.size(); i++) {
        auto numAppendedTuples = 0ul;
        for (auto& blockAppendInfo : appendInfos) {
            copyVectorToDataBlock(*vectors[i], blockAppendInfo, numAppendedTuples, i);
            numAppendedTuples += blockAppendInfo.numTuplesToAppend;
        }
        assert(numAppendedTuples == numTuplesToAppend);
    }
    numTuples += numTuplesToAppend;
}

uint8_t* FactorizedTable::appendEmptyTuple() {
    auto dataBuffer = allocateTupleBlocks(1 /* numTuplesToAppend */)[0].data;
    numTuples++;
    return dataBuffer;
}

void FactorizedTable::scan(vector<shared_ptr<ValueVector>>& vectors, uint64_t tupleIdx,
    uint64_t numTuplesToScan, vector<uint32_t>& colIdxesToScan) const {
    assert(tupleIdx + numTuplesToScan <= numTuples);
    assert(vectors.size() == colIdxesToScan.size());
    unique_ptr<uint8_t*[]> tuplesToRead = make_unique<uint8_t*[]>(numTuplesToScan);
    for (auto i = 0u; i < numTuplesToScan; i++) {
        tuplesToRead[i] = getTuple(tupleIdx + i);
    }
    return lookup(vectors, colIdxesToScan, tuplesToRead.get(), 0 /* startPos */, numTuplesToScan);
}

void FactorizedTable::lookup(vector<shared_ptr<ValueVector>>& vectors,
    vector<uint32_t>& colIdxesToScan, uint8_t** tuplesToRead, uint64_t startPos,
    uint64_t numTuplesToRead) const {
    assert(vectors.size() == colIdxesToScan.size());
    for (auto i = 0u; i < colIdxesToScan.size(); i++) {
        uint64_t colIdx = colIdxesToScan[i];
        if (tableSchema->getColumn(colIdx)->getIsUnflat()) {
            // If the caller wants to read an unflat column from factorizedTable, the vector
            // must be unflat and the numTuplesToScan should be 1.
            assert(!vectors[i]->state->isFlat() && numTuplesToRead == 1);
            readUnflatCol(tuplesToRead + startPos, colIdx, *vectors[i]);
        } else {
            assert(!(vectors[i]->state->isFlat() && numTuplesToRead > 1));
            readFlatCol(tuplesToRead + startPos, colIdx, *vectors[i], numTuplesToRead);
        }
    }
}

void FactorizedTable::mergeMayContainNulls(FactorizedTable& other) {
    for (auto i = 0u; i < other.tableSchema->getNumColumns(); i++) {
        if (!other.hasNoNullGuarantee(i)) {
            tableSchema->setMayContainsNullsToTrue(i);
        }
    }
}

void FactorizedTable::merge(FactorizedTable& other) {
    assert(*tableSchema == *other.tableSchema);
    mergeMayContainNulls(other);
    move(begin(other.vectorOverflowBlocks), end(other.vectorOverflowBlocks),
        back_inserter(vectorOverflowBlocks));
    auto appendInfos = allocateTupleBlocks(other.numTuples);
    auto otherTupleIdx = 0u;
    for (auto& appendInfo : appendInfos) {
        for (auto i = 0u; i < appendInfo.numTuplesToAppend; i++) {
            memcpy(appendInfo.data + i * tableSchema->getNumBytesPerTuple(),
                other.getTuple(otherTupleIdx), tableSchema->getNumBytesPerTuple());
            otherTupleIdx++;
        }
    }
    this->overflowBuffer->merge(*other.overflowBuffer);
    this->numTuples += other.numTuples;
}

bool FactorizedTable::hasUnflatCol() const {
    vector<uint32_t> colIdxes(tableSchema->getNumColumns());
    iota(colIdxes.begin(), colIdxes.end(), 0);
    return hasUnflatCol(colIdxes);
}

uint64_t FactorizedTable::getTotalNumFlatTuples() const {
    auto totalNumFlatTuples = 0ul;
    for (auto i = 0u; i < getNumTuples(); i++) {
        totalNumFlatTuples += getNumFlatTuples(i);
    }
    return totalNumFlatTuples;
}

uint64_t FactorizedTable::getNumFlatTuples(uint64_t tupleIdx) const {
    unordered_map<uint32_t, bool> calculatedDataChunkPoses;
    uint64_t numFlatTuples = 1;
    auto tupleBuffer = getTuple(tupleIdx);
    for (auto i = 0u; i < tableSchema->getNumColumns(); i++) {
        auto column = tableSchema->getColumn(i);
        if (!calculatedDataChunkPoses.contains(column->getDataChunkPos())) {
            calculatedDataChunkPoses[column->getDataChunkPos()] = true;
            numFlatTuples *=
                (column->getIsUnflat() ? ((overflow_value_t*)tupleBuffer)->numElements : 1);
        }
        tupleBuffer += column->getNumBytes();
    }
    return numFlatTuples;
}

uint8_t* FactorizedTable::getTuple(uint64_t tupleIdx) const {
    assert(tupleIdx < numTuples);
    auto blockIdxAndTupleIdxInBlock = getBlockIdxAndTupleIdxInBlock(tupleIdx);
    return tupleDataBlocks[blockIdxAndTupleIdxInBlock.first]->getData() +
           blockIdxAndTupleIdxInBlock.second * tableSchema->getNumBytesPerTuple();
}

void FactorizedTable::updateFlatCell(
    uint8_t* tuplePtr, uint32_t colIdx, ValueVector* valueVector, uint32_t pos) {
    if (valueVector->isNull(pos)) {
        setNonOverflowColNull(tuplePtr + tableSchema->getNullMapOffset(), colIdx);
    } else {
        ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(
            *valueVector, pos, tuplePtr + tableSchema->getColOffset(colIdx), *overflowBuffer);
    }
}

bool FactorizedTable::isOverflowColNull(
    const uint8_t* nullBuffer, uint32_t tupleIdx, uint32_t colIdx) const {
    assert(colIdx < tableSchema->getNumColumns());
    if (tableSchema->getColumn(colIdx)->hasNoNullGuarantee()) {
        return false;
    }
    return isNull(nullBuffer, tupleIdx);
}

bool FactorizedTable::isNonOverflowColNull(const uint8_t* nullBuffer, uint32_t colIdx) const {
    assert(colIdx < tableSchema->getNumColumns());
    if (tableSchema->getColumn(colIdx)->hasNoNullGuarantee()) {
        return false;
    }
    return isNull(nullBuffer, colIdx);
}

bool FactorizedTable::isNull(const uint8_t* nullMapBuffer, uint32_t idx) {
    uint32_t nullMapIdx = idx >> 3;
    uint8_t nullMapMask = 0x1 << (idx & 7); // note: &7 is the same as %8
    return nullMapBuffer[nullMapIdx] & nullMapMask;
}

void FactorizedTable::setNull(uint8_t* nullBuffer, uint32_t idx) {
    uint64_t nullMapIdx = idx >> 3;
    uint8_t nullMapMask = 0x1 << (idx & 7); // note: &7 is the same as %8
    nullBuffer[nullMapIdx] |= nullMapMask;
}

void FactorizedTable::setOverflowColNull(uint8_t* nullBuffer, uint32_t colIdx, uint32_t tupleIdx) {
    setNull(nullBuffer, tupleIdx);
    tableSchema->setMayContainsNullsToTrue(colIdx);
}

void FactorizedTable::setNonOverflowColNull(uint8_t* nullBuffer, uint32_t colIdx) {
    setNull(nullBuffer, colIdx);
    tableSchema->setMayContainsNullsToTrue(colIdx);
}

uint64_t FactorizedTable::computeNumTuplesToAppend(
    const vector<shared_ptr<ValueVector>>& vectorsToAppend) const {
    auto unflatDataChunkPos = -1ul;
    auto numTuplesToAppend = 1ul;

    for (auto i = 0u; i < vectorsToAppend.size(); i++) {
        // If the caller tries to append an unflat column to a flat column in the factorizedTable,
        // the factorizedTable needs to flatten that vector.
        if (!tableSchema->getColumn(i)->getIsUnflat() && !vectorsToAppend[i]->state->isFlat()) {
            // The caller is not allowed to append multiple unflat columns from different datachunks
            // to multiple flat columns in the factorizedTable.
            if (unflatDataChunkPos != -1 &&
                tableSchema->getColumn(i)->getDataChunkPos() != unflatDataChunkPos) {
                assert(false);
            }
            unflatDataChunkPos = tableSchema->getColumn(i)->getDataChunkPos();
            numTuplesToAppend = vectorsToAppend[i]->state->selectedSize;
        }
    }
    return numTuplesToAppend;
}

vector<BlockAppendingInfo> FactorizedTable::allocateTupleBlocks(uint64_t numTuplesToAppend) {
    auto numBytesPerTuple = tableSchema->getNumBytesPerTuple();
    assert(numBytesPerTuple < LARGE_PAGE_SIZE);
    vector<BlockAppendingInfo> appendingInfos;
    while (numTuplesToAppend > 0) {
        if (tupleDataBlocks.empty() || tupleDataBlocks.back()->freeSize < numBytesPerTuple) {
            tupleDataBlocks.emplace_back(make_unique<DataBlock>(memoryManager));
        }
        auto& block = tupleDataBlocks.back();
        auto numTuplesToAppendInCurBlock =
            min(numTuplesToAppend, block->freeSize / numBytesPerTuple);
        appendingInfos.emplace_back(
            block->getData() + LARGE_PAGE_SIZE - block->freeSize, numTuplesToAppendInCurBlock);
        block->freeSize -= numTuplesToAppendInCurBlock * numBytesPerTuple;
        block->numTuples += numTuplesToAppendInCurBlock;
        numTuplesToAppend -= numTuplesToAppendInCurBlock;
    }
    return appendingInfos;
}

uint8_t* FactorizedTable::allocateOverflowBlocks(uint32_t numBytes) {
    assert(numBytes < LARGE_PAGE_SIZE);
    for (auto& dataBlock : vectorOverflowBlocks) {
        if (dataBlock->freeSize > numBytes) {
            dataBlock->freeSize -= numBytes;
            return dataBlock->getData() + LARGE_PAGE_SIZE - dataBlock->freeSize - numBytes;
        }
    }
    vectorOverflowBlocks.push_back(make_unique<DataBlock>(memoryManager));
    vectorOverflowBlocks.back()->freeSize -= numBytes;
    return vectorOverflowBlocks.back()->getData();
}

void FactorizedTable::copyFlatVectorToFlatColumn(
    const ValueVector& vector, const BlockAppendingInfo& blockAppendInfo, uint32_t colIdx) {
    auto valuePositionInVectorToAppend = vector.state->getPositionOfCurrIdx();
    auto colOffsetInDataBlock = tableSchema->getColOffset(colIdx);
    auto dstDataPtr = blockAppendInfo.data;
    for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
        if (vector.isNull(valuePositionInVectorToAppend)) {
            setNonOverflowColNull(dstDataPtr + tableSchema->getNullMapOffset(), colIdx);
        } else {
            ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(vector,
                valuePositionInVectorToAppend, dstDataPtr + colOffsetInDataBlock, *overflowBuffer);
        }
        dstDataPtr += tableSchema->getNumBytesPerTuple();
    }
}

void FactorizedTable::copyUnflatVectorToFlatColumn(const ValueVector& vector,
    const BlockAppendingInfo& blockAppendInfo, uint64_t numAppendedTuples, uint32_t colIdx) {
    auto colOffsetInDataBlock = tableSchema->getColOffset(colIdx);
    auto dstDataPtr = blockAppendInfo.data;
    if (vector.state->isUnfiltered()) {
        if (vector.hasNoNullsGuarantee()) {
            for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
                ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(vector,
                    numAppendedTuples + i, dstDataPtr + colOffsetInDataBlock, *overflowBuffer);
                dstDataPtr += tableSchema->getNumBytesPerTuple();
            }
        } else {
            for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
                if (vector.isNull(numAppendedTuples + i)) {
                    setNonOverflowColNull(dstDataPtr + tableSchema->getNullMapOffset(), colIdx);
                } else {
                    ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(vector,
                        numAppendedTuples + i, dstDataPtr + colOffsetInDataBlock, *overflowBuffer);
                }
                dstDataPtr += tableSchema->getNumBytesPerTuple();
            }
        }
    } else {
        if (vector.hasNoNullsGuarantee()) {
            for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
                ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(vector,
                    vector.state->selectedPositions[numAppendedTuples + i],
                    dstDataPtr + colOffsetInDataBlock, *overflowBuffer);
                dstDataPtr += tableSchema->getNumBytesPerTuple();
            }
        } else {
            for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
                auto pos = vector.state->selectedPositions[numAppendedTuples + i];
                if (vector.isNull(pos)) {
                    setNonOverflowColNull(dstDataPtr + tableSchema->getNullMapOffset(), colIdx);
                } else {
                    ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(
                        vector, pos, dstDataPtr + colOffsetInDataBlock, *overflowBuffer);
                }
                dstDataPtr += tableSchema->getNumBytesPerTuple();
            }
        }
    }
}

void FactorizedTable::copyVectorToDataBlock(const ValueVector& vector,
    const BlockAppendingInfo& blockAppendInfo, uint64_t numAppendedTuples, uint32_t colIdx) {
    auto colOffsetInDataBlock = tableSchema->getColOffset(colIdx);
    if (tableSchema->getColumn(colIdx)->getIsUnflat()) {
        // For unflat column, we only store a overflow_value_t, which contains a pointer to the
        // overflow dataBlock, in the factorizedTable. The nullMasks are stored in the overflow
        // buffer.
        auto overflowValue = appendUnFlatVectorToOverflowBlocks(vector, colIdx);
        auto blockPtr = blockAppendInfo.data + colOffsetInDataBlock;
        for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
            memcpy(blockPtr, (uint8_t*)&overflowValue, sizeof(overflow_value_t));
            blockPtr += tableSchema->getNumBytesPerTuple();
        }
    } else {
        copyVectorToFlatColumn(vector, blockAppendInfo, numAppendedTuples, colIdx);
    }
}

overflow_value_t FactorizedTable::appendUnFlatVectorToOverflowBlocks(
    const ValueVector& vector, uint32_t colIdx) {
    assert(!vector.state->isFlat());
    auto numFlatTuplesInVector = vector.state->selectedSize;
    auto numBytesForData = vector.getNumBytesPerValue() * numFlatTuplesInVector;
    auto overflowBlockBuffer = allocateOverflowBlocks(
        numBytesForData + TableSchema::getNumBytesForNullBuffer(numFlatTuplesInVector));
    if (vector.state->isUnfiltered()) {
        if (vector.hasNoNullsGuarantee()) {
            auto dstDataBuffer = overflowBlockBuffer;
            for (auto i = 0u; i < numFlatTuplesInVector; i++) {
                ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(
                    vector, i, dstDataBuffer, *overflowBuffer);
                dstDataBuffer += vector.getNumBytesPerValue();
            }
        } else {
            auto dstDataBuffer = overflowBlockBuffer;
            for (auto i = 0u; i < numFlatTuplesInVector; i++) {
                if (vector.isNull(i)) {
                    setOverflowColNull(overflowBlockBuffer + numBytesForData, colIdx, i);
                } else {
                    ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(
                        vector, i, dstDataBuffer, *overflowBuffer);
                }
                dstDataBuffer += vector.getNumBytesPerValue();
            }
        }
    } else {
        if (vector.hasNoNullsGuarantee()) {
            auto dstDataBuffer = overflowBlockBuffer;
            for (auto i = 0u; i < numFlatTuplesInVector; i++) {
                ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(
                    vector, vector.state->selectedPositions[i], dstDataBuffer, *overflowBuffer);
                dstDataBuffer += vector.getNumBytesPerValue();
            }
        } else {
            auto dstDataBuffer = overflowBlockBuffer;
            for (auto i = 0u; i < numFlatTuplesInVector; i++) {
                auto pos = vector.state->selectedPositions[i];
                if (vector.isNull(pos)) {
                    setOverflowColNull(overflowBlockBuffer + numBytesForData, colIdx, i);
                } else {
                    ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(
                        vector, pos, dstDataBuffer, *overflowBuffer);
                }
                dstDataBuffer += vector.getNumBytesPerValue();
            }
        }
    }
    return overflow_value_t{numFlatTuplesInVector, overflowBlockBuffer};
}

void FactorizedTable::readUnflatCol(
    uint8_t** tuplesToRead, uint32_t colIdx, ValueVector& vector) const {
    auto vectorOverflowValue =
        *(overflow_value_t*)(tuplesToRead[0] + tableSchema->getColOffset(colIdx));
    assert(vector.state->isUnfiltered());
    if (hasNoNullGuarantee(colIdx)) {
        vector.setAllNonNull();
        auto val = vectorOverflowValue.value;
        for (auto i = 0u; i < vectorOverflowValue.numElements; i++) {
            ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(vector, i, val);
            val += vector.getNumBytesPerValue();
        }
    } else {
        for (auto i = 0u; i < vectorOverflowValue.numElements; i++) {
            if (isOverflowColNull(vectorOverflowValue.value + vectorOverflowValue.numElements *
                                                                  vector.getNumBytesPerValue(),
                    i, colIdx)) {
                vector.setNull(i, true);
            } else {
                vector.setNull(i, false);
                ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(
                    vector, i, vectorOverflowValue.value + i * vector.getNumBytesPerValue());
            }
        }
    }
    vector.state->selectedSize = vectorOverflowValue.numElements;
}

void FactorizedTable::readFlatColToFlatVector(
    uint8_t** tuplesToRead, uint32_t colIdx, ValueVector& vector) const {
    assert(vector.state->isFlat());
    vector.state->selectedSize = 1;
    auto pos = vector.state->getPositionOfCurrIdx();
    if (isNonOverflowColNull(tuplesToRead[0] + tableSchema->getNullMapOffset(), colIdx)) {
        vector.setNull(pos, true);
    } else {
        vector.setNull(pos, false);
        ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(
            vector, pos, tuplesToRead[0] + tableSchema->getColOffset(colIdx));
    }
}

void FactorizedTable::readFlatColToUnflatVector(
    uint8_t** tuplesToRead, uint32_t colIdx, ValueVector& vector, uint64_t numTuplesToRead) const {
    vector.state->selectedSize = numTuplesToRead;
    if (hasNoNullGuarantee(colIdx)) {
        vector.setAllNonNull();
        for (auto i = 0u; i < numTuplesToRead; i++) {
            auto positionInVectorToRead = vector.state->selectedPositions[i];
            auto srcData = tuplesToRead[i] + tableSchema->getColOffset(colIdx);
            ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(
                vector, positionInVectorToRead, srcData);
        }
    } else {
        for (auto i = 0u; i < numTuplesToRead; i++) {
            auto positionInVectorToRead = vector.state->selectedPositions[i];
            auto dataBuffer = tuplesToRead[i];
            if (isNonOverflowColNull(dataBuffer + tableSchema->getNullMapOffset(), colIdx)) {
                vector.setNull(positionInVectorToRead, true);
            } else {
                vector.setNull(positionInVectorToRead, false);
                ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(
                    vector, positionInVectorToRead, dataBuffer + tableSchema->getColOffset(colIdx));
            }
        }
    }
}

FlatTupleIterator::FlatTupleIterator(
    FactorizedTable& factorizedTable, const vector<DataType>& columnDataTypes)
    : factorizedTable{factorizedTable}, numFlatTuples{0}, nextFlatTupleIdx{0}, nextTupleIdx{1},
      columnDataTypes{columnDataTypes} {
    if (factorizedTable.getNumTuples()) {
        currentTupleBuffer = factorizedTable.getTuple(0);
        numFlatTuples = factorizedTable.getNumFlatTuples(0);
        updateNumElementsInDataChunk();
        updateInvalidEntriesInFlatTuplePositionsInDataChunk();
    }
    // Note: there is difference between column data types and value types in iteratorFlatTuple.
    // Column data type could be UNSTRUCTURED but value type must be a structured type.
    iteratorFlatTuple = make_shared<FlatTuple>(columnDataTypes);
    assert(columnDataTypes.size() == factorizedTable.tableSchema->getNumColumns());
}

shared_ptr<FlatTuple> FlatTupleIterator::getNextFlatTuple() {
    // Go to the next tuple if we have iterated all the flat tuples of the current tuple.
    if (nextFlatTupleIdx >= numFlatTuples) {
        currentTupleBuffer = factorizedTable.getTuple(nextTupleIdx);
        numFlatTuples = factorizedTable.getNumFlatTuples(nextTupleIdx);
        nextFlatTupleIdx = 0;
        updateNumElementsInDataChunk();
        nextTupleIdx++;
    }
    for (auto i = 0ul; i < factorizedTable.getTableSchema()->getNumColumns(); i++) {
        auto column = factorizedTable.getTableSchema()->getColumn(i);
        if (column->getIsUnflat()) {
            readUnflatColToFlatTuple(i, currentTupleBuffer);
        } else {
            readFlatColToFlatTuple(i, currentTupleBuffer);
        }
    }
    updateFlatTuplePositionsInDataChunk();
    nextFlatTupleIdx++;
    return iteratorFlatTuple;
}

void FlatTupleIterator::readValueBufferToFlatTuple(
    uint64_t flatTupleValIdx, const uint8_t* valueBuffer) {
    switch (columnDataTypes[flatTupleValIdx].typeID) {
    case INT64: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.int64Val = *((int64_t*)valueBuffer);
    } break;
    case BOOL: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.booleanVal = *((bool*)valueBuffer);
    } break;
    case DOUBLE: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.doubleVal = *((double_t*)valueBuffer);
    } break;
    case STRING: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.strVal = *((gf_string_t*)valueBuffer);
    } break;
    case NODE_ID: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.nodeID = *((nodeID_t*)valueBuffer);
    } break;
    case UNSTRUCTURED: {
        *iteratorFlatTuple->getValue(flatTupleValIdx) = *((Value*)valueBuffer);
    } break;
    case DATE: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.dateVal = *((date_t*)valueBuffer);
    } break;
    case TIMESTAMP: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.timestampVal =
            *((timestamp_t*)valueBuffer);
    } break;
    case INTERVAL: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.intervalVal = *((interval_t*)valueBuffer);
    } break;
    case LIST: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.listVal = *((gf_list_t*)valueBuffer);
    } break;
    default:
        assert(false);
    }
}

void FlatTupleIterator::readUnflatColToFlatTuple(uint64_t colIdx, uint8_t* valueBuffer) {
    auto overflowValue =
        (overflow_value_t*)(valueBuffer + factorizedTable.getTableSchema()->getColOffset(colIdx));
    auto columnInFactorizedTable = factorizedTable.getTableSchema()->getColumn(colIdx);
    auto tupleSizeInOverflowBuffer = Types::getDataTypeSize(columnDataTypes[colIdx]);
    valueBuffer =
        overflowValue->value +
        tupleSizeInOverflowBuffer *
            flatTuplePositionsInDataChunk[columnInFactorizedTable->getDataChunkPos()].first;
    iteratorFlatTuple->nullMask[colIdx] = factorizedTable.isOverflowColNull(
        overflowValue->value + tupleSizeInOverflowBuffer * overflowValue->numElements,
        flatTuplePositionsInDataChunk[columnInFactorizedTable->getDataChunkPos()].first, colIdx);
    if (!iteratorFlatTuple->nullMask[colIdx]) {
        readValueBufferToFlatTuple(colIdx, valueBuffer);
    }
}

void FlatTupleIterator::readFlatColToFlatTuple(uint32_t colIdx, uint8_t* valueBuffer) {
    iteratorFlatTuple->nullMask[colIdx] = factorizedTable.isNonOverflowColNull(
        valueBuffer + factorizedTable.getTableSchema()->getNullMapOffset(), colIdx);
    if (!iteratorFlatTuple->nullMask[colIdx]) {
        readValueBufferToFlatTuple(
            colIdx, valueBuffer + factorizedTable.getTableSchema()->getColOffset(colIdx));
    }
}

void FlatTupleIterator::updateInvalidEntriesInFlatTuplePositionsInDataChunk() {
    for (auto i = 0u; i < flatTuplePositionsInDataChunk.size(); i++) {
        bool isValidEntry = false;
        for (auto j = 0u; j < factorizedTable.getTableSchema()->getNumColumns(); j++) {
            if (factorizedTable.getTableSchema()->getColumn(j)->getDataChunkPos() == i) {
                isValidEntry = true;
                break;
            }
        }
        if (!isValidEntry) {
            flatTuplePositionsInDataChunk[i] = make_pair(UINT64_MAX, UINT64_MAX);
        }
    }
}

void FlatTupleIterator::updateNumElementsInDataChunk() {
    auto colOffsetInTupleBuffer = 0ul;
    for (auto i = 0u; i < factorizedTable.getTableSchema()->getNumColumns(); i++) {
        auto column = factorizedTable.getTableSchema()->getColumn(i);
        // If this is an unflat column, the number of elements is stored in the
        // overflow_value_t struct. Otherwise the number of elements is 1.
        auto numElementsInDataChunk =
            column->getIsUnflat() ?
                ((overflow_value_t*)(currentTupleBuffer + colOffsetInTupleBuffer))->numElements :
                1;
        if (column->getDataChunkPos() >= flatTuplePositionsInDataChunk.size()) {
            flatTuplePositionsInDataChunk.resize(column->getDataChunkPos() + 1);
        }
        flatTuplePositionsInDataChunk[column->getDataChunkPos()] =
            make_pair(0 /* nextIdxToReadInDataChunk */, numElementsInDataChunk);
        colOffsetInTupleBuffer += column->getNumBytes();
    }
}

void FlatTupleIterator::updateFlatTuplePositionsInDataChunk() {
    for (auto i = 0u; i < flatTuplePositionsInDataChunk.size(); i++) {
        if (!isValidDataChunkPos(i)) {
            continue;
        }
        flatTuplePositionsInDataChunk.at(i).first++;
        // If we have output all elements in the current column, we reset the
        // nextIdxToReadInDataChunk in the current column to 0.
        if (flatTuplePositionsInDataChunk.at(i).first >=
            flatTuplePositionsInDataChunk.at(i).second) {
            flatTuplePositionsInDataChunk.at(i).first = 0;
        } else {
            // If the current dataChunk is not full, then we don't need to update the next
            // dataChunk.
            break;
        }
    }
}

} // namespace processor
} // namespace graphflow

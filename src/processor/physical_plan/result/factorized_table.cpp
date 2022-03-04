#include "src/processor/include/physical_plan/result/factorized_table.h"

#include "src/common/include/exception.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace processor {

TableSchema::TableSchema(vector<ColumnSchema> columns) {
    for (auto& column : columns) {
        appendColumn(column);
    }
}

void TableSchema::appendColumn(const ColumnSchema& column) {
    columns.push_back(column);
    colOffsets.push_back(
        colOffsets.empty() ? 0 : colOffsets.back() + getColumn(columns.size() - 2).getNumBytes());
    numBytesForDataPerTuple += column.getNumBytes();
    numBytesForNullMapPerTuple = getNumBytesForNullBuffer(getNumColumns());
}

bool TableSchema::operator==(const TableSchema& other) const {
    if (columns.size() != other.columns.size()) {
        return false;
    }
    for (auto i = 0u; i < columns.size(); i++) {
        if (columns[i] != other.columns[i]) {
            return false;
        }
    }
    return numBytesForDataPerTuple == other.numBytesForDataPerTuple && numBytesForNullMapPerTuple &&
           other.numBytesForNullMapPerTuple;
}

FactorizedTable::FactorizedTable(MemoryManager* memoryManager, const TableSchema& tableSchema)
    : memoryManager{memoryManager}, tableSchema{tableSchema}, numTuples{0}, numTuplesPerBlock{0} {
    assert(tableSchema.getNumBytesPerTuple() <= LARGE_PAGE_SIZE);
    stringBuffer = make_unique<StringBuffer>(memoryManager);
    numTuplesPerBlock = LARGE_PAGE_SIZE / tableSchema.getNumBytesPerTuple();
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
    uint64_t numTuplesToScan, vector<uint64_t>& colIdxesToScan) const {
    assert(tupleIdx + numTuplesToScan <= numTuples);
    assert(vectors.size() == colIdxesToScan.size());
    unique_ptr<uint8_t*[]> tuplesToRead = make_unique<uint8_t*[]>(numTuplesToScan);
    for (auto i = 0u; i < numTuplesToScan; i++) {
        tuplesToRead[i] = getTuple(tupleIdx + i);
    }
    return lookup(vectors, colIdxesToScan, tuplesToRead.get(), 0 /* startPos */, numTuplesToScan);
}

void FactorizedTable::lookup(vector<shared_ptr<ValueVector>>& vectors,
    vector<uint64_t>& colIdxesToScan, uint8_t** tuplesToRead, uint64_t startPos,
    uint64_t numTuplesToRead) const {
    assert(vectors.size() == colIdxesToScan.size());
    for (auto i = 0u; i < colIdxesToScan.size(); i++) {
        uint64_t colIdx = colIdxesToScan[i];
        if (tableSchema.getColumn(colIdx).getIsUnflat()) {
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

void FactorizedTable::scanTupleToVectorPos(
    vector<shared_ptr<ValueVector>> vectors, uint64_t tupleIdx, uint64_t valuePosInVec) const {
    assert(vectors.size() == tableSchema.getNumColumns());
    assert(tupleIdx < numTuples);
    for (auto i = 0u; i < vectors.size(); i++) {
        auto vector = vectors[i];
        auto dataBuffer = getTuple(tupleIdx);
        if (isNull(dataBuffer + tableSchema.getNullMapOffset(), i)) {
            vector->setNull(valuePosInVec, true);
        } else {
            vector->setNull(valuePosInVec, false);
            vector->copyNonNullDataWithSameTypeIntoPos(
                valuePosInVec, dataBuffer + tableSchema.getColOffset(i));
        }
        vector->state->selectedSize = valuePosInVec + 1;
    }
}

void FactorizedTable::merge(FactorizedTable& other) {
    assert(tableSchema == other.tableSchema);
    move(begin(other.vectorOverflowBlocks), end(other.vectorOverflowBlocks),
        back_inserter(vectorOverflowBlocks));
    auto appendInfos = allocateTupleBlocks(other.numTuples);
    auto otherTupleIdx = 0u;
    for (auto& appendInfo : appendInfos) {
        for (auto i = 0u; i < appendInfo.numTuplesToAppend; i++) {
            memcpy(appendInfo.data + i * tableSchema.getNumBytesPerTuple(),
                other.getTuple(otherTupleIdx), tableSchema.getNumBytesPerTuple());
            otherTupleIdx++;
        }
    }
    this->stringBuffer->merge(*other.stringBuffer);
    this->numTuples += other.numTuples;
}

bool FactorizedTable::hasUnflatCol() const {
    vector<uint64_t> colIdxes(tableSchema.getNumColumns());
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

FlatTupleIterator FactorizedTable::getFlatTupleIterator() {
    return FlatTupleIterator(*this);
}

uint64_t FactorizedTable::getNumFlatTuples(uint64_t tupleIdx) const {
    unordered_map<uint32_t, bool> calculatedDataChunkPoses;
    uint64_t numFlatTuples = 1;
    auto tupleBuffer = getTuple(tupleIdx);
    for (auto i = 0u; i < tableSchema.getNumColumns(); i++) {
        auto column = tableSchema.getColumn(i);
        if (!calculatedDataChunkPoses.contains(column.getDataChunkPos())) {
            calculatedDataChunkPoses[column.getDataChunkPos()] = true;
            numFlatTuples *=
                (column.getIsUnflat() ? ((overflow_value_t*)tupleBuffer)->numElements : 1);
        }
        tupleBuffer += column.getNumBytes();
    }
    return numFlatTuples;
}

void FactorizedTable::updateFlatCell(uint64_t tupleIdx, uint64_t colIdx, ValueVector* valueVector) {
    auto pos = valueVector->state->getPositionOfCurrIdx();
    if (valueVector->isNull(pos)) {
        setNull(getTuple(tupleIdx) + tableSchema.getNullMapOffset(), colIdx);
    } else {
        valueVector->copyNonNullDataWithSameTypeOutFromPos(
            valueVector->state->getPositionOfCurrIdx(), getCell(tupleIdx, colIdx), *stringBuffer);
    }
}

void FactorizedTable::setNull(uint8_t* nullBuffer, uint64_t colIdx) {
    uint64_t nullMapIdx = colIdx >> 3;
    uint8_t nullMapMask = 0x1 << (colIdx & 7); // note: &7 is the same as %8
    nullBuffer[nullMapIdx] |= nullMapMask;
}

bool FactorizedTable::isNull(const uint8_t* nullBuffer, uint64_t colIdx) {
    uint32_t nullMapIdx = colIdx >> 3;
    uint8_t nullMapMask = 0x1 << (colIdx & 7); // note: &7 is the same as %8
    return nullBuffer[nullMapIdx] & nullMapMask;
}

uint64_t FactorizedTable::computeNumTuplesToAppend(
    const vector<shared_ptr<ValueVector>>& vectorsToAppend) const {
    auto unflatDataChunkPos = -1ul;
    auto numTuplesToAppend = 1ul;

    for (auto i = 0u; i < vectorsToAppend.size(); i++) {
        // If the caller tries to append an unflat column to a flat column in the factorizedTable,
        // the factorizedTable needs to flatten that vector.
        if (!tableSchema.getColumn(i).getIsUnflat() && !vectorsToAppend[i]->state->isFlat()) {
            // The caller is not allowed to append multiple unflat columns from different datachunks
            // to multiple flat columns in the factorizedTable.
            if (unflatDataChunkPos != -1 &&
                tableSchema.getColumn(i).getDataChunkPos() != unflatDataChunkPos) {
                assert(false);
            }
            unflatDataChunkPos = tableSchema.getColumn(i).getDataChunkPos();
            numTuplesToAppend = vectorsToAppend[i]->state->selectedSize;
        }
    }
    return numTuplesToAppend;
}

void FactorizedTable::assertTupleIdxColIdxAndValueIsFlat(uint64_t tupleIdx, uint64_t colIdx) const {
    assert(tupleIdx < numTuples);
    assert(colIdx < tableSchema.getNumColumns());
    assert(!tableSchema.getColumn(colIdx).getIsUnflat());
}

uint8_t* FactorizedTable::getCell(uint64_t tupleIdx, uint64_t colIdx) const {
    assertTupleIdxColIdxAndValueIsFlat(tupleIdx, colIdx);
    return getTuple(tupleIdx) + tableSchema.getColOffset(colIdx);
}

vector<BlockAppendingInfo> FactorizedTable::allocateTupleBlocks(uint64_t numTuplesToAppend) {
    auto numBytesPerTuple = tableSchema.getNumBytesPerTuple();
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

uint8_t* FactorizedTable::allocateOverflowBlocks(uint64_t numBytes) {
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

void FactorizedTable::copyVectorToDataBlock(const ValueVector& vector,
    const BlockAppendingInfo& blockAppendInfo, uint64_t numAppendedTuples, uint64_t colIdx) {
    auto colOffsetInDataBlock = tableSchema.getColOffset(colIdx);
    if (tableSchema.getColumn(colIdx).getIsUnflat()) {
        // For unflat column, we only store a overflow_value_t, which contains a pointer to the
        // overflow dataBlock, in the factorizedTable. The nullMasks are stored in the overflow
        // buffer.
        auto overflowValue = appendUnFlatVectorToOverflowBlocks(vector);
        for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
            memcpy(blockAppendInfo.data + colOffsetInDataBlock +
                       (i * tableSchema.getNumBytesPerTuple()),
                (uint8_t*)&overflowValue, sizeof(overflow_value_t));
        }
    } else {
        // Append a flat/unflat valueVector to a flat column in the factorizedTable.
        for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
            auto dstDataBuffer = blockAppendInfo.data + i * tableSchema.getNumBytesPerTuple();
            auto valuePositionInVectorToAppend =
                vector.state->isFlat() ? vector.state->getPositionOfCurrIdx() :
                                         vector.state->selectedPositions[numAppendedTuples + i];
            vector.copyNonNullDataWithSameTypeOutFromPos(
                valuePositionInVectorToAppend, dstDataBuffer + colOffsetInDataBlock, *stringBuffer);
            if (vector.isNull(valuePositionInVectorToAppend)) {
                setNull(dstDataBuffer + tableSchema.getNullMapOffset(), colIdx);
            }
        }
    }
}

overflow_value_t FactorizedTable::appendUnFlatVectorToOverflowBlocks(const ValueVector& vector) {
    assert(!vector.state->isFlat());
    auto numFlatTuplesInVector = vector.state->selectedSize;
    auto numBytesForData = vector.getNumBytesPerValue() * numFlatTuplesInVector;
    auto overflowBlockBuffer = allocateOverflowBlocks(
        numBytesForData + TableSchema::getNumBytesForNullBuffer(numFlatTuplesInVector));
    for (auto i = 0u; i < numFlatTuplesInVector; i++) {
        auto dstDataBuffer = overflowBlockBuffer + i * vector.getNumBytesPerValue();
        vector.copyNonNullDataWithSameTypeOutFromPos(
            vector.state->selectedPositions[i], dstDataBuffer, *stringBuffer);
        if (vector.isNull(vector.state->selectedPositions[i])) {
            setNull(overflowBlockBuffer + numBytesForData, i);
        }
    }
    return overflow_value_t{numFlatTuplesInVector, overflowBlockBuffer};
}

void FactorizedTable::readUnflatCol(
    uint8_t** tuplesToRead, uint64_t colIdx, ValueVector& vector) const {
    auto vectorOverflowValue =
        *(overflow_value_t*)(tuplesToRead[0] + tableSchema.getColOffset(colIdx));
    assert(vector.state->isUnfiltered());
    for (auto i = 0u; i < vectorOverflowValue.numElements; i++) {
        if (isNull(vectorOverflowValue.value +
                       vectorOverflowValue.numElements * vector.getNumBytesPerValue(),
                i)) {
            vector.setNull(i, true);
        } else {
            vector.setNull(i, false);
            vector.copyNonNullDataWithSameTypeIntoPos(
                i, vectorOverflowValue.value + i * vector.getNumBytesPerValue());
        }
    }
    vector.state->selectedSize = vectorOverflowValue.numElements;
}

void FactorizedTable::readFlatCol(
    uint8_t** tuplesToRead, uint64_t colIdx, ValueVector& vector, uint64_t numTuplesToRead) const {
    for (auto i = 0u; i < numTuplesToRead; i++) {
        auto positionInVectorToRead = vector.state->isFlat() ?
                                          vector.state->getPositionOfCurrIdx() :
                                          vector.state->selectedPositions[i];
        auto dataBuffer = tuplesToRead[i];
        if (isNull(dataBuffer + tableSchema.getNullMapOffset(), colIdx)) {
            vector.setNull(positionInVectorToRead, true);
        } else {
            vector.setNull(positionInVectorToRead, false);
            vector.copyNonNullDataWithSameTypeIntoPos(
                positionInVectorToRead, dataBuffer + tableSchema.getColOffset(colIdx));
        }
    }
    vector.state->selectedSize = vector.state->isFlat() ? 1 : numTuplesToRead;
}

uint8_t* FactorizedTable::getTuple(uint64_t tupleIdx) const {
    assert(tupleIdx < numTuples);
    auto blockIdxAndTupleIdxInBlock = getBlockIdxAndTupleIdxInBlock(tupleIdx);
    return tupleDataBlocks[blockIdxAndTupleIdxInBlock.first]->getData() +
           blockIdxAndTupleIdxInBlock.second * tableSchema.getNumBytesPerTuple();
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
    }
}

void FlatTupleIterator::getNextFlatTuple(FlatTuple& flatTuple) {
    // Go to the next tuple if we have iterated all the flat tuples of the current tuple.
    if (nextFlatTupleIdx >= numFlatTuples) {
        currentTupleBuffer = factorizedTable.getTuple(nextTupleIdx);
        numFlatTuples = factorizedTable.getNumFlatTuples(nextTupleIdx);
        nextFlatTupleIdx = 0;
        updateNumElementsInDataChunk();
        nextTupleIdx++;
    }
    auto colOffsetInTupleBuffer = 0ul;
    for (auto i = 0ul; i < factorizedTable.getTableSchema().getNumColumns(); i++) {
        auto column = factorizedTable.getTableSchema().getColumn(i);
        if (column.getIsUnflat()) {
            readUnflatColToFlatTuple(flatTuple, i, currentTupleBuffer);
        } else {
            readFlatColToFlatTuple(flatTuple, i, currentTupleBuffer);
        }
        colOffsetInTupleBuffer += column.getNumBytes();
    }
    updateFlatTuplePositionsInDataChunk();
    nextFlatTupleIdx++;
}

void FlatTupleIterator::readValueBufferToFlatTuple(
    FlatTuple& flatTuple, uint64_t flatTupleValIdx, const uint8_t* valueBuffer) {
    switch (flatTuple.getDataType(flatTupleValIdx)) {
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

void FlatTupleIterator::readUnflatColToFlatTuple(
    FlatTuple& flatTuple, uint64_t colIdx, uint8_t* valueBuffer) {
    auto overflowValue =
        (overflow_value_t*)(valueBuffer + factorizedTable.getTableSchema().getColOffset(colIdx));
    auto columnInFactorizedTable = factorizedTable.getTableSchema().getColumn(colIdx);
    auto tupleSizeInOverflowBuffer = TypeUtils::getDataTypeSize(flatTuple.getDataType(colIdx));
    valueBuffer =
        overflowValue->value +
        tupleSizeInOverflowBuffer *
            flatTuplePositionsInDataChunk[columnInFactorizedTable.getDataChunkPos()].first;
    flatTuple.nullMask[colIdx] = FactorizedTable::isNull(
        overflowValue->value + tupleSizeInOverflowBuffer * overflowValue->numElements,
        flatTuplePositionsInDataChunk[columnInFactorizedTable.getDataChunkPos()].first);
    if (!flatTuple.nullMask[colIdx]) {
        readValueBufferToFlatTuple(flatTuple, colIdx, valueBuffer);
    }
}

void FlatTupleIterator::readFlatColToFlatTuple(
    FlatTuple& flatTuple, uint64_t colIdx, uint8_t* valueBuffer) {
    flatTuple.nullMask[colIdx] = FactorizedTable::isNull(
        valueBuffer + factorizedTable.getTableSchema().getNullMapOffset(), colIdx);
    if (!flatTuple.nullMask[colIdx]) {
        readValueBufferToFlatTuple(
            flatTuple, colIdx, valueBuffer + factorizedTable.getTableSchema().getColOffset(colIdx));
    }
}

void FlatTupleIterator::updateInvalidEntriesInFlatTuplePositionsInDataChunk() {
    for (auto i = 0u; i < flatTuplePositionsInDataChunk.size(); i++) {
        bool isValidEntry = false;
        for (auto j = 0u; j < factorizedTable.getTableSchema().getNumColumns(); j++) {
            if (factorizedTable.getTableSchema().getColumn(j).getDataChunkPos() == i) {
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
    for (auto i = 0u; i < factorizedTable.getTableSchema().getNumColumns(); i++) {
        auto column = factorizedTable.getTableSchema().getColumn(i);
        // If this is an unflat column, the number of elements is stored in the
        // overflow_value_t struct. Otherwise the number of elements is 1.
        auto numElementsInDataChunk =
            column.getIsUnflat() ?
                ((overflow_value_t*)(currentTupleBuffer + colOffsetInTupleBuffer))->numElements :
                1;
        if (column.getDataChunkPos() >= flatTuplePositionsInDataChunk.size()) {
            flatTuplePositionsInDataChunk.resize(column.getDataChunkPos() + 1);
        }
        flatTuplePositionsInDataChunk[column.getDataChunkPos()] =
            make_pair(0 /* nextIdxToReadInDataChunk */, numElementsInDataChunk);
        colOffsetInTupleBuffer += column.getNumBytes();
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

} // namespace processor
} // namespace graphflow

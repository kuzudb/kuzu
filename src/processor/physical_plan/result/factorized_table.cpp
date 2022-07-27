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
    auto tuplesToWrite = make_unique<uint8_t*[]>(numTuplesToAppend);
    auto tupleIdx = 0ul;
    for (auto& blockAppendInfo : appendInfos) {
        for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
            tuplesToWrite[tupleIdx++] =
                blockAppendInfo.data + (i * tableSchema->getNumBytesPerTuple());
        }
    }
    assert(tupleIdx == numTuplesToAppend);
    for (auto i = 0u; i < vectors.size(); i++) {
        writeVectorToColumn(*vectors[i], tuplesToWrite.get(), i, numTuplesToAppend);
    }
    numTuples += numTuplesToAppend;
}

uint8_t* FactorizedTable::appendEmptyTuple() {
    if (tupleDataBlocks.empty() ||
        tupleDataBlocks.back()->freeSize < tableSchema->getNumBytesPerTuple()) {
        tupleDataBlocks.emplace_back(make_unique<DataBlock>(memoryManager));
    }
    auto& block = tupleDataBlocks.back();
    uint8_t* tuplePtr = block->getData() + LARGE_PAGE_SIZE - block->freeSize;
    block->freeSize -= tableSchema->getNumBytesPerTuple();
    block->numTuples++;
    numTuples++;
    return tuplePtr;
}

void FactorizedTable::scan(vector<shared_ptr<ValueVector>>& vectors, uint64_t tupleIdx,
    uint64_t numTuplesToScan, vector<uint32_t>& colIdxesToScan) const {
    assert(tupleIdx + numTuplesToScan <= numTuples && vectors.size() == colIdxesToScan.size());
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
        if (tableSchema->getColumn(colIdx)->isFlat()) {
            assert(!(vectors[i]->state->isFlat() && numTuplesToRead > 1));
            readFlatCol(tuplesToRead + startPos, colIdx, *vectors[i], numTuplesToRead);
        } else {
            // If the caller wants to read an unflat column from factorizedTable, the vector
            // must be unflat and the numTuplesToScan should be 1.
            assert(!vectors[i]->state->isFlat() && numTuplesToRead == 1);
            readUnflatCol(tuplesToRead + startPos, colIdx, *vectors[i]);
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
            numFlatTuples *= column->isFlat() ? 1 : ((overflow_value_t*)tupleBuffer)->numElements;
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

void FactorizedTable::setNonOverflowColNull(uint8_t* nullBuffer, uint32_t colIdx) {
    setNull(nullBuffer, colIdx);
    tableSchema->setMayContainsNullsToTrue(colIdx);
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

// TODO(Guodong): change this function to not use dataChunkPos in ColumnSchema.
uint64_t FactorizedTable::computeNumTuplesToAppend(
    const vector<shared_ptr<ValueVector>>& vectorsToAppend) const {
    auto unflatDataChunkPos = -1ul;
    auto numTuplesToAppend = 1ul;
    for (auto i = 0u; i < vectorsToAppend.size(); i++) {
        // If the caller tries to append an unflat vector to a flat column in the factorizedTable,
        // the factorizedTable needs to flatten that vector.
        if (tableSchema->getColumn(i)->isFlat() && !vectorsToAppend[i]->state->isFlat()) {
            // The caller is not allowed to append multiple unflat columns from different datachunks
            // to multiple flat columns in the factorizedTable.
            if (unflatDataChunkPos != -1 &&
                tableSchema->getColumn(i)->getDataChunkPos() != unflatDataChunkPos) {
                assert(false);
            }
            unflatDataChunkPos = tableSchema->getColumn(i)->getDataChunkPos();
            numTuplesToAppend = vectorsToAppend[i]->state->selVector->selectedSize;
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

template<typename T>
static constexpr bool isVarSizedType() {
    if constexpr (is_same<T, gf_string_t>::value || is_same<T, gf_list_t>::value ||
                  is_same<T, Value>::value) {
        return true;
    }
    return false;
}

template<typename T>
static void scatterFlatVectorToFlatColumn(const TableSchema& tableSchema, const ValueVector& vector,
    uint8_t** tuples, uint32_t colIdx, uint64_t numTuplesToWrite, OverflowBuffer& overflowBuffer) {
    auto posInVector = vector.state->getPositionOfCurrIdx();
    auto isValueNull = vector.isNull(posInVector);
    auto nullMaskOffsetInTuple = tableSchema.getNullMapOffset();
    if (isValueNull) {
        tableSchema.getColumn(colIdx)->setMayContainsNullsToTrue();
        for (auto i = 0u; i < numTuplesToWrite; i++) {
            FactorizedTable::setNull(tuples[i] + nullMaskOffsetInTuple, colIdx);
        }
    } else {
        auto valuesInVector = (T*)vector.values;
        auto offsetInTuple = tableSchema.getColOffset(colIdx);
        for (auto i = 0u; i < numTuplesToWrite; i++) {
            ValueVectorUtils::templateCopyValue((T*)&valuesInVector[posInVector],
                (T*)(tuples[i] + offsetInTuple), vector.dataType, overflowBuffer);
        }
    }
}

template<typename T>
static void scatterUnflatVectorToFlatColumn(const TableSchema& tableSchema,
    const ValueVector& vector, uint8_t** tuples, uint32_t colIdx, uint64_t numTuplesToWrite,
    OverflowBuffer& overflowBuffer) {
    auto valuesInVector = (T*)vector.values;
    auto offsetInTuple = tableSchema.getColOffset(colIdx);
    if (vector.hasNoNullsGuarantee()) {
        if (vector.state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < numTuplesToWrite; i++) {
                ValueVectorUtils::templateCopyValue((T*)&valuesInVector[i],
                    (T*)(tuples[i] + offsetInTuple), vector.dataType, overflowBuffer);
            }
        } else {
            for (auto i = 0u; i < numTuplesToWrite; i++) {
                auto pos = vector.state->selVector->selectedPositions[i];
                ValueVectorUtils::templateCopyValue((T*)&valuesInVector[pos],
                    (T*)(tuples[i] + offsetInTuple), vector.dataType, overflowBuffer);
            }
        }
    } else {
        auto nullMaskOffsetInTuple = tableSchema.getNullMapOffset();
        if (vector.state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < numTuplesToWrite; i++) {
                if (vector.isNull(i)) {
                    FactorizedTable::setNull(tuples[i] + nullMaskOffsetInTuple, colIdx);
                    tableSchema.getColumn(colIdx)->setMayContainsNullsToTrue();
                } else {
                    ValueVectorUtils::templateCopyValue((T*)&valuesInVector[i],
                        (T*)(tuples[i] + offsetInTuple), vector.dataType, overflowBuffer);
                }
            }
        } else {
            for (auto i = 0u; i < numTuplesToWrite; i++) {
                auto pos = vector.state->selVector->selectedPositions[i];
                if (vector.isNull(pos)) {
                    FactorizedTable::setNull(tuples[i] + nullMaskOffsetInTuple, colIdx);
                    tableSchema.getColumn(colIdx)->setMayContainsNullsToTrue();
                } else {
                    ValueVectorUtils::templateCopyValue((T*)&valuesInVector[pos],
                        (T*)(tuples[i] + offsetInTuple), vector.dataType, overflowBuffer);
                }
            }
        }
    }
}

template<typename T>
static void scatterToUnflatOverflow(const TableSchema& tableSchema, const ValueVector& vector,
    uint32_t colIdx, uint8_t* resultOverflow, OverflowBuffer& overflowBuffer) {
    assert(!vector.state->isFlat());
    auto valuesInVector = (T*)vector.values;
    auto valuesInOverflow = (T*)resultOverflow;
    if (vector.hasNoNullsGuarantee()) {
        if (vector.state->selVector->isUnfiltered()) {
            if constexpr (isVarSizedType<T>()) {
                for (auto i = 0u; i < vector.state->selVector->selectedSize; i++) {
                    ValueVectorUtils::templateCopyValue((T*)&valuesInVector[i],
                        (T*)&valuesInOverflow[i], vector.dataType, overflowBuffer);
                }
            } else {
                memcpy(resultOverflow, vector.values,
                    sizeof(T) * vector.state->selVector->selectedSize);
            }
        } else {
            for (auto i = 0u; i < vector.state->selVector->selectedSize; i++) {
                auto pos = vector.state->selVector->selectedPositions[i];
                ValueVectorUtils::templateCopyValue((T*)&valuesInVector[pos],
                    (T*)&valuesInOverflow[i], vector.dataType, overflowBuffer);
            }
        }
    } else {
        auto nullMaskInOverflow =
            resultOverflow + (vector.getNumBytesPerValue() * vector.state->selVector->selectedSize);
        if (vector.state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < vector.state->selVector->selectedSize; i++) {
                if (vector.isNull(i)) {
                    FactorizedTable::setNull(nullMaskInOverflow, i);
                    tableSchema.getColumn(colIdx)->setMayContainsNullsToTrue();
                } else {
                    ValueVectorUtils::templateCopyValue((T*)&valuesInVector[i],
                        (T*)&valuesInOverflow[i], vector.dataType, overflowBuffer);
                }
            }
        } else {
            for (auto i = 0u; i < vector.state->selVector->selectedSize; i++) {
                auto pos = vector.state->selVector->selectedPositions[i];
                if (vector.isNull(pos)) {
                    FactorizedTable::setNull(nullMaskInOverflow, i);
                    tableSchema.getColumn(colIdx)->setMayContainsNullsToTrue();
                } else {
                    ValueVectorUtils::templateCopyValue((T*)&valuesInVector[pos],
                        (T*)&valuesInOverflow[i], vector.dataType, overflowBuffer);
                }
            }
        }
    }
}

void FactorizedTable::writeFlatVectorToFlatColumn(
    const ValueVector& vector, uint8_t** tuples, uint32_t colIdx, uint64_t numTuplesToWrite) {
    assert(vector.state->isFlat());
    switch (vector.dataType.typeID) {
    case NODE_ID: {
        scatterFlatVectorToFlatColumn<nodeID_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case BOOL: {
        scatterFlatVectorToFlatColumn<bool>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case DATE: {
        scatterFlatVectorToFlatColumn<date_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case DOUBLE: {
        scatterFlatVectorToFlatColumn<double_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case INT64: {
        scatterFlatVectorToFlatColumn<int64_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case TIMESTAMP: {
        scatterFlatVectorToFlatColumn<timestamp_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case INTERVAL: {
        scatterFlatVectorToFlatColumn<interval_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case STRING: {
        scatterFlatVectorToFlatColumn<gf_string_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case LIST: {
        scatterFlatVectorToFlatColumn<gf_list_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case UNSTRUCTURED: {
        scatterFlatVectorToFlatColumn<Value>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    default:
        assert(false);
    }
}

void FactorizedTable::writeUnflatVectorToFlatColumn(
    const ValueVector& vector, uint8_t** tuples, uint32_t colIdx, uint64_t numTuplesToWrite) {
    assert(!vector.state->isFlat() && vector.state->selVector->selectedSize == numTuplesToWrite);
    switch (vector.dataType.typeID) {
    case NODE_ID: {
        scatterUnflatVectorToFlatColumn<nodeID_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case BOOL: {
        scatterUnflatVectorToFlatColumn<bool>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case DATE: {
        scatterUnflatVectorToFlatColumn<date_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case DOUBLE: {
        scatterUnflatVectorToFlatColumn<double_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case INT64: {
        scatterUnflatVectorToFlatColumn<int64_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case TIMESTAMP: {
        scatterUnflatVectorToFlatColumn<timestamp_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case INTERVAL: {
        scatterUnflatVectorToFlatColumn<interval_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case STRING: {
        scatterUnflatVectorToFlatColumn<gf_string_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case LIST: {
        scatterUnflatVectorToFlatColumn<gf_list_t>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    case UNSTRUCTURED: {
        scatterUnflatVectorToFlatColumn<Value>(
            *tableSchema, vector, tuples, colIdx, numTuplesToWrite, *overflowBuffer);
    } break;
    default:
        assert(false);
    }
}

// For an unflat column, only an unflat vector is allowed to copy from, for the column, we only
// store an overflow_value_t, which contains a pointer to the overflow dataBlock in the
// factorizedTable. NullMasks are stored inside the overflow buffer.
void FactorizedTable::writeVectorToUnflatColumn(
    const ValueVector& vector, uint8_t** tuples, uint32_t colIdx, uint64_t numTuplesToWrite) {
    assert(!vector.state->isFlat());
    auto unflatOverflowValue = appendUnFlatVectorToOverflowBlocks(vector, colIdx);
    auto offsetInTuple = tableSchema->getColOffset(colIdx);
    for (auto i = 0u; i < numTuplesToWrite; i++) {
        memcpy(tuples[i] + offsetInTuple, (uint8_t*)&unflatOverflowValue, sizeof(overflow_value_t));
    }
}

void FactorizedTable::writeVectorToColumn(
    const ValueVector& vector, uint8_t** tuples, uint32_t colIdx, uint64_t numTuplesToWrite) {
    if (tableSchema->getColumn(colIdx)->isFlat()) {
        writeVectorToFlatColumn(vector, tuples, colIdx, numTuplesToWrite);
    } else {
        writeVectorToUnflatColumn(vector, tuples, colIdx, numTuplesToWrite);
    }
}

overflow_value_t FactorizedTable::appendUnFlatVectorToOverflowBlocks(
    const ValueVector& vector, uint32_t colIdx) {
    assert(!vector.state->isFlat());
    auto numFlatTuplesInVector = vector.state->selVector->selectedSize;
    auto numBytesForData = vector.getNumBytesPerValue() * numFlatTuplesInVector;
    auto overflowBlock = allocateOverflowBlocks(
        numBytesForData + TableSchema::getNumBytesForNullBuffer(numFlatTuplesInVector));
    switch (vector.dataType.typeID) {
    case NODE_ID: {
        scatterToUnflatOverflow<nodeID_t>(
            *tableSchema, vector, colIdx, overflowBlock, *overflowBuffer);
    } break;
    case BOOL: {
        scatterToUnflatOverflow<bool>(*tableSchema, vector, colIdx, overflowBlock, *overflowBuffer);
    } break;
    case DATE: {
        scatterToUnflatOverflow<date_t>(
            *tableSchema, vector, colIdx, overflowBlock, *overflowBuffer);
    } break;
    case DOUBLE: {
        scatterToUnflatOverflow<double_t>(
            *tableSchema, vector, colIdx, overflowBlock, *overflowBuffer);
    } break;
    case INT64: {
        scatterToUnflatOverflow<int64_t>(
            *tableSchema, vector, colIdx, overflowBlock, *overflowBuffer);
    } break;
    case TIMESTAMP: {
        scatterToUnflatOverflow<timestamp_t>(
            *tableSchema, vector, colIdx, overflowBlock, *overflowBuffer);
    } break;
    case INTERVAL: {
        scatterToUnflatOverflow<interval_t>(
            *tableSchema, vector, colIdx, overflowBlock, *overflowBuffer);
    } break;
    case STRING: {
        scatterToUnflatOverflow<gf_string_t>(
            *tableSchema, vector, colIdx, overflowBlock, *overflowBuffer);
    } break;
    case LIST: {
        scatterToUnflatOverflow<gf_list_t>(
            *tableSchema, vector, colIdx, overflowBlock, *overflowBuffer);
    } break;
    case UNSTRUCTURED: {
        scatterToUnflatOverflow<Value>(
            *tableSchema, vector, colIdx, overflowBlock, *overflowBuffer);
    } break;
    default:
        assert(false);
    }
    return overflow_value_t{numFlatTuplesInVector, overflowBlock};
}

template<typename T>
static void gatherFromUnflatOverflow(
    const TableSchema& tableSchema, uint8_t** rows, uint32_t colIdx, ValueVector& result) {
    assert(!result.state->isFlat() && result.state->selVector->isUnfiltered());
    auto unflatOverflowValue = *(overflow_value_t*)(rows[0] + tableSchema.getColOffset(colIdx));
    result.state->selVector->selectedSize = unflatOverflowValue.numElements;
    auto valuesInRow = (T*)unflatOverflowValue.value;
    auto valuesInVector = (T*)result.values;
    if (tableSchema.getColumn(colIdx)->hasNoNullGuarantee()) {
        result.setAllNonNull();
        if constexpr (isVarSizedType<T>()) {
            for (auto i = 0u; i < result.state->selVector->selectedSize; i++) {
                ValueVectorUtils::templateCopyValue((T*)&valuesInRow[i], (T*)&valuesInVector[i],
                    result.dataType, result.getOverflowBuffer());
            }
        } else {
            memcpy(valuesInVector, valuesInRow, sizeof(T) * unflatOverflowValue.numElements);
        }
    } else {
        auto nullMask = unflatOverflowValue.value +
                        unflatOverflowValue.numElements * result.getNumBytesPerValue();
        for (auto i = 0u; i < result.state->selVector->selectedSize; i++) {
            auto isValueNull = FactorizedTable::isNull(nullMask, i);
            result.setNull(i, isValueNull);
        }
        if constexpr (isVarSizedType<T>()) {
            for (auto i = 0u; i < result.state->selVector->selectedSize; i++) {
                if (!result.isNull(i)) {
                    ValueVectorUtils::templateCopyValue((T*)&valuesInRow[i], (T*)&valuesInVector[i],
                        result.dataType, result.getOverflowBuffer());
                }
            }
        } else {
            memcpy(valuesInVector, valuesInRow, sizeof(T) * unflatOverflowValue.numElements);
        }
    }
}

template<typename T>
static void gatherFromFlatColToFlatVector(
    const TableSchema& tableSchema, uint8_t** rows, uint32_t colIdx, ValueVector& result) {
    assert(result.state->isFlat());
    auto values = (T*)result.values;
    auto posInVec = result.state->getPositionOfCurrIdx();
    auto isValueInRowNull = FactorizedTable::isNull(
        const_cast<const uint8_t*>(rows[0] + tableSchema.getNullMapOffset()), colIdx);
    result.setNull(posInVec, isValueInRowNull);
    if (!isValueInRowNull) {
        ValueVectorUtils::templateCopyValue((T*)(rows[0] + tableSchema.getColOffset(colIdx)),
            (T*)&values[posInVec], result.dataType, result.getOverflowBuffer());
    }
}

template<typename T>
static void gatherFromFlatColToUnflatVector(
    const TableSchema& tableSchema, uint8_t** rows, uint32_t colIdx, ValueVector& result) {
    assert(!result.state->isFlat());
    auto offsetInRowForCol = tableSchema.getColOffset(colIdx);
    auto values = (T*)result.values;
    if (tableSchema.getColumn(colIdx)->hasNoNullGuarantee()) {
        result.setAllNonNull();
        if (result.state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < result.state->selVector->selectedSize; i++) {
                ValueVectorUtils::templateCopyValue((T*)(rows[i] + offsetInRowForCol),
                    (T*)&values[i], result.dataType, result.getOverflowBuffer());
            }
        } else {
            for (auto i = 0u; i < result.state->selVector->selectedSize; i++) {
                auto pos = result.state->selVector->selectedPositions[i];
                ValueVectorUtils::templateCopyValue((T*)(rows[i] + offsetInRowForCol),
                    (T*)&values[pos], result.dataType, result.getOverflowBuffer());
            }
        }
    } else {
        auto nullMaskOffsetInRow = tableSchema.getNullMapOffset();
        if (result.state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < result.state->selVector->selectedSize; i++) {
                auto isValueInRowNull = FactorizedTable::isNull(
                    const_cast<const uint8_t*>(rows[i] + nullMaskOffsetInRow), colIdx);
                result.setNull(i, isValueInRowNull);
                if (!isValueInRowNull) {
                    ValueVectorUtils::templateCopyValue((T*)(rows[i] + offsetInRowForCol),
                        (T*)&values[i], result.dataType, result.getOverflowBuffer());
                }
            }
        } else {
            for (auto i = 0u; i < result.state->selVector->selectedSize; i++) {
                auto pos = result.state->selVector->selectedPositions[i];
                auto isValueInRowNull = FactorizedTable::isNull(
                    const_cast<const uint8_t*>(rows[i] + nullMaskOffsetInRow), colIdx);
                result.setNull(pos, isValueInRowNull);
                if (!isValueInRowNull) {
                    ValueVectorUtils::templateCopyValue((T*)(rows[i] + offsetInRowForCol),
                        (T*)&values[pos], result.dataType, result.getOverflowBuffer());
                }
            }
        }
    }
}

void FactorizedTable::readUnflatCol(uint8_t** tuples, uint32_t colIdx, ValueVector& result) const {
    assert(result.state->selVector->isUnfiltered());
    switch (result.dataType.typeID) {
    case NODE_ID: {
        gatherFromUnflatOverflow<nodeID_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case BOOL: {
        gatherFromUnflatOverflow<bool>(*tableSchema, tuples, colIdx, result);
    } break;
    case DATE: {
        gatherFromUnflatOverflow<date_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case DOUBLE: {
        gatherFromUnflatOverflow<double_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case INT64: {
        gatherFromUnflatOverflow<int64_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case TIMESTAMP: {
        gatherFromUnflatOverflow<timestamp_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case INTERVAL: {
        gatherFromUnflatOverflow<interval_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case STRING: {
        gatherFromUnflatOverflow<gf_string_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case LIST: {
        gatherFromUnflatOverflow<gf_list_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case UNSTRUCTURED: {
        gatherFromUnflatOverflow<Value>(*tableSchema, tuples, colIdx, result);
    } break;
    default:
        assert(false);
    }
}

void FactorizedTable::readFlatColToFlatVector(
    uint8_t** tuples, uint32_t colIdx, ValueVector& result) const {
    switch (result.dataType.typeID) {
    case NODE_ID: {
        gatherFromFlatColToFlatVector<nodeID_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case BOOL: {
        gatherFromFlatColToFlatVector<bool>(*tableSchema, tuples, colIdx, result);
    } break;
    case DATE: {
        gatherFromFlatColToFlatVector<date_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case DOUBLE: {
        gatherFromFlatColToFlatVector<double_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case INT64: {
        gatherFromFlatColToFlatVector<int64_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case TIMESTAMP: {
        gatherFromFlatColToFlatVector<timestamp_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case INTERVAL: {
        gatherFromFlatColToFlatVector<interval_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case STRING: {
        gatherFromFlatColToFlatVector<gf_string_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case LIST: {
        gatherFromFlatColToFlatVector<gf_list_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case UNSTRUCTURED: {
        gatherFromFlatColToFlatVector<Value>(*tableSchema, tuples, colIdx, result);
    } break;
    default:
        assert(false);
    }
}

void FactorizedTable::readFlatColToUnflatVector(
    uint8_t** tuples, uint32_t colIdx, ValueVector& result, uint64_t numTuplesToRead) const {
    result.state->selVector->selectedSize = numTuplesToRead;
    switch (result.dataType.typeID) {
    case NODE_ID: {
        gatherFromFlatColToUnflatVector<nodeID_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case BOOL: {
        gatherFromFlatColToUnflatVector<bool>(*tableSchema, tuples, colIdx, result);
    } break;
    case DATE: {
        gatherFromFlatColToUnflatVector<date_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case DOUBLE: {
        gatherFromFlatColToUnflatVector<double_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case INT64: {
        gatherFromFlatColToUnflatVector<int64_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case TIMESTAMP: {
        gatherFromFlatColToUnflatVector<timestamp_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case INTERVAL: {
        gatherFromFlatColToUnflatVector<interval_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case STRING: {
        gatherFromFlatColToUnflatVector<gf_string_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case LIST: {
        gatherFromFlatColToUnflatVector<gf_list_t>(*tableSchema, tuples, colIdx, result);
    } break;
    case UNSTRUCTURED: {
        gatherFromFlatColToUnflatVector<Value>(*tableSchema, tuples, colIdx, result);
    } break;
    default:
        assert(false);
    }
}

} // namespace processor
} // namespace graphflow

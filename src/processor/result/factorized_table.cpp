#include "processor/result/factorized_table.h"

#include "common/exception.h"
#include "common/vector/value_vector_utils.h"

using namespace kuzu::common;
using namespace std;

namespace kuzu {
namespace processor {

ColumnSchema::ColumnSchema(const ColumnSchema& other) {
    isUnflat = other.isUnflat;
    dataChunkPos = other.dataChunkPos;
    numBytes = other.numBytes;
    mayContainNulls = other.mayContainNulls;
}

FactorizedTableSchema::FactorizedTableSchema(const FactorizedTableSchema& other) {
    for (auto& column : other.columns) {
        appendColumn(make_unique<ColumnSchema>(*column));
    }
}

void FactorizedTableSchema::appendColumn(unique_ptr<ColumnSchema> column) {
    numBytesForDataPerTuple += column->getNumBytes();
    columns.push_back(std::move(column));
    colOffsets.push_back(
        colOffsets.empty() ? 0 : colOffsets.back() + getColumn(columns.size() - 2)->getNumBytes());
    numBytesForNullMapPerTuple = getNumBytesForNullBuffer(getNumColumns());
    numBytesPerTuple = numBytesForDataPerTuple + numBytesForNullMapPerTuple;
}

bool FactorizedTableSchema::operator==(const FactorizedTableSchema& other) const {
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

void DataBlock::copyTuples(DataBlock* blockToCopyFrom, uint32_t tupleIdxToCopyFrom,
    DataBlock* blockToCopyInto, uint32_t tupleIdxToCopyInfo, uint32_t numTuplesToCopy,
    uint32_t numBytesPerTuple) {
    for (auto i = 0u; i < numTuplesToCopy; i++) {
        memcpy(blockToCopyInto->getData() + (tupleIdxToCopyInfo * numBytesPerTuple),
            blockToCopyFrom->getData() + (tupleIdxToCopyFrom * numBytesPerTuple), numBytesPerTuple);
        tupleIdxToCopyFrom++;
        tupleIdxToCopyInfo++;
    }
    blockToCopyInto->numTuples += numTuplesToCopy;
    blockToCopyInto->freeSize -= (numTuplesToCopy * numBytesPerTuple);
}

void DataBlockCollection::merge(DataBlockCollection& other) {
    if (blocks.empty()) {
        append(std::move(other.blocks));
        return;
    }
    // Pop up the old last block first, and then push back blocks from other into the vector.
    auto oldLastBlock = std::move(blocks.back());
    blocks.pop_back();
    append(std::move(other.blocks));
    // Insert back tuples in the old last block to the new last block.
    auto newLastBlock = blocks.back().get();
    auto numTuplesToAppendIntoNewLastBlock =
        min(numTuplesPerBlock - newLastBlock->numTuples, oldLastBlock->numTuples);
    DataBlock::copyTuples(oldLastBlock.get(), 0, newLastBlock, newLastBlock->numTuples,
        numTuplesToAppendIntoNewLastBlock, numBytesPerTuple);
    // If any tuples left in the old last block, shift them to the beginning, and push the old last
    // block back.
    auto numTuplesLeftForNewBlock = oldLastBlock->numTuples - numTuplesToAppendIntoNewLastBlock;
    if (numTuplesLeftForNewBlock > 0) {
        auto tupleIdxInOldLastBlock = numTuplesToAppendIntoNewLastBlock;
        oldLastBlock->resetNumTuplesAndFreeSize();
        DataBlock::copyTuples(oldLastBlock.get(), tupleIdxInOldLastBlock, oldLastBlock.get(), 0,
            numTuplesLeftForNewBlock, numBytesPerTuple);
        blocks.push_back(std::move(oldLastBlock));
    }
}

FactorizedTable::FactorizedTable(
    MemoryManager* memoryManager, unique_ptr<FactorizedTableSchema> tableSchema)
    : memoryManager{memoryManager}, tableSchema{std::move(tableSchema)}, numTuples{0} {
    assert(this->tableSchema->getNumBytesPerTuple() <= LARGE_PAGE_SIZE);
    if (!this->tableSchema->isEmpty()) {
        inMemOverflowBuffer = make_unique<InMemOverflowBuffer>(memoryManager);
        numTuplesPerBlock = LARGE_PAGE_SIZE / this->tableSchema->getNumBytesPerTuple();
        flatTupleBlockCollection = make_unique<DataBlockCollection>(
            this->tableSchema->getNumBytesPerTuple(), numTuplesPerBlock);
        unflatTupleBlockCollection = make_unique<DataBlockCollection>();
    }
}

void FactorizedTable::append(const vector<shared_ptr<ValueVector>>& vectors) {
    auto numTuplesToAppend = computeNumTuplesToAppend(vectors);
    auto appendInfos = allocateFlatTupleBlocks(numTuplesToAppend);
    for (auto i = 0u; i < vectors.size(); i++) {
        auto numAppendedTuples = 0ul;
        for (auto& blockAppendInfo : appendInfos) {
            copyVectorToColumn(*vectors[i], blockAppendInfo, numAppendedTuples, i);
            numAppendedTuples += blockAppendInfo.numTuplesToAppend;
        }
        assert(numAppendedTuples == numTuplesToAppend);
    }
    numTuples += numTuplesToAppend;
}

uint8_t* FactorizedTable::appendEmptyTuple() {
    if (flatTupleBlockCollection->isEmpty() ||
        flatTupleBlockCollection->getBlocks().back()->freeSize <
            tableSchema->getNumBytesPerTuple()) {
        flatTupleBlockCollection->append(make_unique<DataBlock>(memoryManager));
    }
    auto& block = flatTupleBlockCollection->getBlocks().back();
    uint8_t* tuplePtr = block->getData() + LARGE_PAGE_SIZE - block->freeSize;
    block->freeSize -= tableSchema->getNumBytesPerTuple();
    block->numTuples++;
    numTuples++;
    return tuplePtr;
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

void FactorizedTable::lookup(vector<shared_ptr<ValueVector>>& vectors,
    const SelectionVector* selVector, vector<uint32_t>& colIdxesToScan,
    uint8_t* tupleToRead) const {
    assert(vectors.size() == colIdxesToScan.size());
    for (auto i = 0u; i < colIdxesToScan.size(); i++) {
        uint64_t colIdx = colIdxesToScan[i];
        if (tableSchema->getColumn(colIdx)->isFlat()) {
            readFlatCol(&tupleToRead, colIdx, *vectors[i], 1);
        } else {
            readUnflatCol(tupleToRead, selVector, colIdx, *vectors[i]);
        }
    }
}

void FactorizedTable::lookup(vector<shared_ptr<ValueVector>>& vectors,
    vector<uint32_t>& colIdxesToScan, vector<uint64_t>& tupleIdxesToRead, uint64_t startPos,
    uint64_t numTuplesToRead) const {
    assert(vectors.size() == colIdxesToScan.size());
    auto tuplesToRead = make_unique<uint8_t*[]>(tupleIdxesToRead.size());
    for (auto i = 0u; i < numTuplesToRead; i++) {
        tuplesToRead[i] = getTuple(tupleIdxesToRead[i + startPos]);
    }
    lookup(vectors, colIdxesToScan, tuplesToRead.get(), 0 /* startPos */, numTuplesToRead);
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
    if (other.numTuples == 0) {
        return;
    }
    mergeMayContainNulls(other);
    unflatTupleBlockCollection->append(std::move(other.unflatTupleBlockCollection));
    flatTupleBlockCollection->merge(*other.flatTupleBlockCollection);
    inMemOverflowBuffer->merge(*other.inMemOverflowBuffer);
    numTuples += other.numTuples;
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
    auto [blockIdx, tupleIdxInBlock] = getBlockIdxAndTupleIdxInBlock(tupleIdx);
    return flatTupleBlockCollection->getBlock(blockIdx)->getData() +
           tupleIdxInBlock * tableSchema->getNumBytesPerTuple();
}

void FactorizedTable::updateFlatCell(
    uint8_t* tuplePtr, uint32_t colIdx, ValueVector* valueVector, uint32_t pos) {
    if (valueVector->isNull(pos)) {
        setNonOverflowColNull(tuplePtr + tableSchema->getNullMapOffset(), colIdx);
    } else {
        ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(
            *valueVector, pos, tuplePtr + tableSchema->getColOffset(colIdx), *inMemOverflowBuffer);
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

void FactorizedTable::copyToInMemList(uint32_t colIdx, vector<uint64_t>& tupleIdxesToRead,
    uint8_t* data, NullMask* nullMask, uint64_t startElemPosInList,
    DiskOverflowFile* overflowFileOfInMemList, DataType type,
    NodeIDCompressionScheme* nodeIDCompressionScheme) const {
    auto column = tableSchema->getColumn(colIdx);
    assert(column->isFlat() == true);
    auto numBytesPerValue = nodeIDCompressionScheme == nullptr ?
                                Types::getDataTypeSize(type) :
                                nodeIDCompressionScheme->getNumBytesForNodeIDAfterCompression();
    auto colOffset = tableSchema->getColOffset(colIdx);
    auto listToFill = data + startElemPosInList * numBytesPerValue;
    for (auto i = 0u; i < tupleIdxesToRead.size(); i++) {
        auto tuple = getTuple(tupleIdxesToRead[i]);
        auto isNullInFT = isNonOverflowColNull(tuple + tableSchema->getNullMapOffset(), colIdx);
        if (nullMask != nullptr) {
            nullMask->setNull(startElemPosInList + i, isNullInFT);
        }
        if (!isNullInFT) {
            memcpy(listToFill, tuple + colOffset, numBytesPerValue);
            copyOverflowIfNecessary(listToFill, tuple + colOffset, type, overflowFileOfInMemList);
        }
        listToFill += numBytesPerValue;
    }
}

// This function can generalized to search a value with any dataType.
int64_t FactorizedTable::findValueInFlatColumn(uint64_t colIdx, int64_t value) const {
    assert(tableSchema->getColumn(colIdx)->isFlat());
    if (numTuples == 0) {
        return -1;
    }
    auto tupleIdx = 0u;
    auto numBlocks = flatTupleBlockCollection->getNumBlocks();
    auto numBytesForCol = tableSchema->getColumn(colIdx)->getNumBytes();
    for (auto blockIdx = 0u; blockIdx < numBlocks; blockIdx++) {
        // If this is not the last block, the numTuplesInCurBlock must be equal to the
        // numTuplesPerBlock. If this is the last block, the numTuplesInCurBLock equals to the
        // numTuples % numTuplesPerBlock.
        auto numTuplesInCurBlock =
            blockIdx == (numBlocks - 1) ? numTuples % numTuplesPerBlock : numTuplesPerBlock;
        auto tuplePtr = getTuple(tupleIdx);
        for (auto i = 0u; i < numTuplesInCurBlock; i++) {
            if (memcmp(tuplePtr + tableSchema->getColOffset(colIdx), &value, numBytesForCol) == 0) {
                return tupleIdx;
            }
            tuplePtr += tableSchema->getNumBytesPerTuple();
            tupleIdx++;
        }
    }
    return -1;
}

void FactorizedTable::clear() {
    numTuples = 0;
    flatTupleBlockCollection =
        make_unique<DataBlockCollection>(tableSchema->getNumBytesPerTuple(), numTuplesPerBlock);
    unflatTupleBlockCollection = make_unique<DataBlockCollection>();
    inMemOverflowBuffer->resetBuffer();
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

vector<BlockAppendingInfo> FactorizedTable::allocateFlatTupleBlocks(uint64_t numTuplesToAppend) {
    auto numBytesPerTuple = tableSchema->getNumBytesPerTuple();
    assert(numBytesPerTuple < LARGE_PAGE_SIZE);
    vector<BlockAppendingInfo> appendingInfos;
    while (numTuplesToAppend > 0) {
        if (flatTupleBlockCollection->isEmpty() ||
            flatTupleBlockCollection->getBlocks().back()->freeSize < numBytesPerTuple) {
            flatTupleBlockCollection->append(make_unique<DataBlock>(memoryManager));
        }
        auto& block = flatTupleBlockCollection->getBlocks().back();
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

uint8_t* FactorizedTable::allocateUnflatTupleBlock(uint32_t numBytes) {
    assert(numBytes < LARGE_PAGE_SIZE);
    if (unflatTupleBlockCollection->isEmpty()) {
        unflatTupleBlockCollection->append(make_unique<DataBlock>(memoryManager));
    }
    auto lastBlock = unflatTupleBlockCollection->getBlocks().back().get();
    if (lastBlock->freeSize > numBytes) {
        lastBlock->freeSize -= numBytes;
        return lastBlock->getData() + LARGE_PAGE_SIZE - lastBlock->freeSize - numBytes;
    }
    unflatTupleBlockCollection->append(make_unique<DataBlock>(memoryManager));
    lastBlock = unflatTupleBlockCollection->getBlocks().back().get();
    lastBlock->freeSize -= numBytes;
    return lastBlock->getData();
}

void FactorizedTable::copyFlatVectorToFlatColumn(
    const ValueVector& vector, const BlockAppendingInfo& blockAppendInfo, uint32_t colIdx) {
    auto valuePositionInVectorToAppend = vector.state->selVector->selectedPositions[0];
    auto colOffsetInDataBlock = tableSchema->getColOffset(colIdx);
    auto dstDataPtr = blockAppendInfo.data;
    for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
        if (vector.isNull(valuePositionInVectorToAppend)) {
            setNonOverflowColNull(dstDataPtr + tableSchema->getNullMapOffset(), colIdx);
        } else {
            ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(vector,
                valuePositionInVectorToAppend, dstDataPtr + colOffsetInDataBlock,
                *inMemOverflowBuffer);
        }
        dstDataPtr += tableSchema->getNumBytesPerTuple();
    }
}

void FactorizedTable::copyUnflatVectorToFlatColumn(const ValueVector& vector,
    const BlockAppendingInfo& blockAppendInfo, uint64_t numAppendedTuples, uint32_t colIdx) {
    auto byteOffsetOfColumnInTuple = tableSchema->getColOffset(colIdx);
    auto dstTuple = blockAppendInfo.data;
    if (vector.state->selVector->isUnfiltered()) {
        if (vector.hasNoNullsGuarantee()) {
            for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
                ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(vector,
                    numAppendedTuples + i, dstTuple + byteOffsetOfColumnInTuple,
                    *inMemOverflowBuffer);
                dstTuple += tableSchema->getNumBytesPerTuple();
            }
        } else {
            for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
                if (vector.isNull(numAppendedTuples + i)) {
                    setNonOverflowColNull(dstTuple + tableSchema->getNullMapOffset(), colIdx);
                } else {
                    ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(vector,
                        numAppendedTuples + i, dstTuple + byteOffsetOfColumnInTuple,
                        *inMemOverflowBuffer);
                }
                dstTuple += tableSchema->getNumBytesPerTuple();
            }
        }
    } else {
        if (vector.hasNoNullsGuarantee()) {
            for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
                ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(vector,
                    vector.state->selVector->selectedPositions[numAppendedTuples + i],
                    dstTuple + byteOffsetOfColumnInTuple, *inMemOverflowBuffer);
                dstTuple += tableSchema->getNumBytesPerTuple();
            }
        } else {
            for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
                auto pos = vector.state->selVector->selectedPositions[numAppendedTuples + i];
                if (vector.isNull(pos)) {
                    setNonOverflowColNull(dstTuple + tableSchema->getNullMapOffset(), colIdx);
                } else {
                    ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(
                        vector, pos, dstTuple + byteOffsetOfColumnInTuple, *inMemOverflowBuffer);
                }
                dstTuple += tableSchema->getNumBytesPerTuple();
            }
        }
    }
}

// For an unflat column, only an unflat vector is allowed to copy from, for the column, we only
// store an overflow_value_t, which contains a pointer to the overflow dataBlock in the
// factorizedTable. NullMasks are stored inside the overflow buffer.
void FactorizedTable::copyVectorToUnflatColumn(
    const ValueVector& vector, const BlockAppendingInfo& blockAppendInfo, uint32_t colIdx) {
    assert(!vector.state->isFlat());
    auto unflatTupleValue = appendVectorToUnflatTupleBlocks(vector, colIdx);
    auto blockPtr = blockAppendInfo.data + tableSchema->getColOffset(colIdx);
    for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
        memcpy(blockPtr, (uint8_t*)&unflatTupleValue, sizeof(overflow_value_t));
        blockPtr += tableSchema->getNumBytesPerTuple();
    }
}

void FactorizedTable::copyVectorToColumn(const ValueVector& vector,
    const BlockAppendingInfo& blockAppendInfo, uint64_t numAppendedTuples, uint32_t colIdx) {
    if (tableSchema->getColumn(colIdx)->isFlat()) {
        copyVectorToFlatColumn(vector, blockAppendInfo, numAppendedTuples, colIdx);
    } else {
        copyVectorToUnflatColumn(vector, blockAppendInfo, colIdx);
    }
}

overflow_value_t FactorizedTable::appendVectorToUnflatTupleBlocks(
    const ValueVector& vector, uint32_t colIdx) {
    assert(!vector.state->isFlat());
    auto numFlatTuplesInVector = vector.state->selVector->selectedSize;
    auto numBytesForData = vector.getNumBytesPerValue() * numFlatTuplesInVector;
    auto overflowBlockBuffer = allocateUnflatTupleBlock(
        numBytesForData + FactorizedTableSchema::getNumBytesForNullBuffer(numFlatTuplesInVector));
    if (vector.state->selVector->isUnfiltered()) {
        if (vector.hasNoNullsGuarantee()) {
            auto dstDataBuffer = overflowBlockBuffer;
            for (auto i = 0u; i < numFlatTuplesInVector; i++) {
                ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(
                    vector, i, dstDataBuffer, *inMemOverflowBuffer);
                dstDataBuffer += vector.getNumBytesPerValue();
            }
        } else {
            auto dstDataBuffer = overflowBlockBuffer;
            for (auto i = 0u; i < numFlatTuplesInVector; i++) {
                if (vector.isNull(i)) {
                    setOverflowColNull(overflowBlockBuffer + numBytesForData, colIdx, i);
                } else {
                    ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(
                        vector, i, dstDataBuffer, *inMemOverflowBuffer);
                }
                dstDataBuffer += vector.getNumBytesPerValue();
            }
        }
    } else {
        if (vector.hasNoNullsGuarantee()) {
            auto dstDataBuffer = overflowBlockBuffer;
            for (auto i = 0u; i < numFlatTuplesInVector; i++) {
                ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(vector,
                    vector.state->selVector->selectedPositions[i], dstDataBuffer,
                    *inMemOverflowBuffer);
                dstDataBuffer += vector.getNumBytesPerValue();
            }
        } else {
            auto dstDataBuffer = overflowBlockBuffer;
            for (auto i = 0u; i < numFlatTuplesInVector; i++) {
                auto pos = vector.state->selVector->selectedPositions[i];
                if (vector.isNull(pos)) {
                    setOverflowColNull(overflowBlockBuffer + numBytesForData, colIdx, i);
                } else {
                    ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(
                        vector, pos, dstDataBuffer, *inMemOverflowBuffer);
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
    assert(vector.state->selVector->isUnfiltered());
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
    vector.state->selVector->selectedSize = vectorOverflowValue.numElements;
}

void FactorizedTable::readUnflatCol(const uint8_t* tupleToRead, const SelectionVector* selVector,
    uint32_t colIdx, ValueVector& vector) const {
    auto vectorOverflowValue =
        *(overflow_value_t*)(tupleToRead + tableSchema->getColOffset(colIdx));
    assert(vector.state->selVector->isUnfiltered());
    if (hasNoNullGuarantee(colIdx)) {
        vector.setAllNonNull();
        auto val = vectorOverflowValue.value;
        for (auto i = 0u; i < vectorOverflowValue.numElements; i++) {
            auto pos = selVector->selectedPositions[i];
            ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(
                vector, i, val + (pos * vector.getNumBytesPerValue()));
        }
    } else {
        for (auto i = 0u; i < vectorOverflowValue.numElements; i++) {
            auto pos = selVector->selectedPositions[i];
            if (isOverflowColNull(vectorOverflowValue.value + vectorOverflowValue.numElements *
                                                                  vector.getNumBytesPerValue(),
                    pos, colIdx)) {
                vector.setNull(i, true);
            } else {
                vector.setNull(i, false);
                ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(
                    vector, i, vectorOverflowValue.value + pos * vector.getNumBytesPerValue());
            }
        }
    }
    vector.state->selVector->selectedSize = selVector->selectedSize;
}

void FactorizedTable::readFlatColToFlatVector(
    uint8_t** tuplesToRead, uint32_t colIdx, ValueVector& vector) const {
    assert(vector.state->isFlat());
    auto pos = vector.state->selVector->selectedPositions[0];
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
    vector.state->selVector->selectedSize = numTuplesToRead;
    if (hasNoNullGuarantee(colIdx)) {
        vector.setAllNonNull();
        for (auto i = 0u; i < numTuplesToRead; i++) {
            auto positionInVectorToWrite = vector.state->selVector->selectedPositions[i];
            auto srcData = tuplesToRead[i] + tableSchema->getColOffset(colIdx);
            ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(
                vector, positionInVectorToWrite, srcData);
        }
    } else {
        for (auto i = 0u; i < numTuplesToRead; i++) {
            auto positionInVectorToWrite = vector.state->selVector->selectedPositions[i];
            auto dataBuffer = tuplesToRead[i];
            if (isNonOverflowColNull(dataBuffer + tableSchema->getNullMapOffset(), colIdx)) {
                vector.setNull(positionInVectorToWrite, true);
            } else {
                vector.setNull(positionInVectorToWrite, false);
                ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(vector,
                    positionInVectorToWrite, dataBuffer + tableSchema->getColOffset(colIdx));
            }
        }
    }
}

void FactorizedTable::copyOverflowIfNecessary(
    uint8_t* dst, uint8_t* src, DataType type, DiskOverflowFile* diskOverflowFile) {
    switch (type.typeID) {
    case STRING: {
        ku_string_t* stringToWriteFrom = (ku_string_t*)src;
        if (!ku_string_t::isShortString(stringToWriteFrom->len)) {
            diskOverflowFile->writeStringOverflowAndUpdateOverflowPtr(
                *stringToWriteFrom, *(ku_string_t*)dst);
        }
    } break;
    case LIST: {
        diskOverflowFile->writeListOverflowAndUpdateOverflowPtr(
            *(ku_list_t*)src, *(ku_list_t*)dst, type);
    } break;
    default:
        return;
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
        if (column->isFlat()) {
            readFlatColToFlatTuple(i, currentTupleBuffer);
        } else {
            readUnflatColToFlatTuple(i, currentTupleBuffer);
        }
    }
    updateFlatTuplePositionsInDataChunk();
    nextFlatTupleIdx++;
    return iteratorFlatTuple;
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
    iteratorFlatTuple->getResultValue(colIdx)->setNull(factorizedTable.isOverflowColNull(
        overflowValue->value + tupleSizeInOverflowBuffer * overflowValue->numElements,
        flatTuplePositionsInDataChunk[columnInFactorizedTable->getDataChunkPos()].first, colIdx));
    if (!iteratorFlatTuple->getResultValue(colIdx)->isNullVal()) {
        readValueBufferToFlatTuple(colIdx, valueBuffer);
    }
}

void FlatTupleIterator::readFlatColToFlatTuple(uint32_t colIdx, uint8_t* valueBuffer) {
    iteratorFlatTuple->getResultValue(colIdx)->setNull(factorizedTable.isNonOverflowColNull(
        valueBuffer + factorizedTable.getTableSchema()->getNullMapOffset(), colIdx));
    if (!iteratorFlatTuple->getResultValue(colIdx)->isNullVal()) {
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
        // overflow_value_t struct. Otherwise, the number of elements is 1.
        auto numElementsInDataChunk =
            column->isFlat() ?
                1 :
                ((overflow_value_t*)(currentTupleBuffer + colOffsetInTupleBuffer))->numElements;
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
} // namespace kuzu

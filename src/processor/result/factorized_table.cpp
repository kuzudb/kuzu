#include "processor/result/factorized_table.h"

#include "common/assert.h"
#include "common/data_chunk/data_chunk_state.h"
#include "common/null_buffer.h"
#include "common/vector/value_vector.h"

using namespace kuzu::common;
using namespace kuzu::storage;

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
        appendColumn(std::make_unique<ColumnSchema>(*column));
    }
}

void FactorizedTableSchema::appendColumn(std::unique_ptr<ColumnSchema> column) {
    numBytesForDataPerTuple += column->getNumBytes();
    columns.push_back(std::move(column));
    colOffsets.push_back(
        colOffsets.empty() ? 0 : colOffsets.back() + getColumn(columns.size() - 2)->getNumBytes());
    numBytesForNullMapPerTuple = NullBuffer::getNumBytesForNullValues(getNumColumns());
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

uint64_t FactorizedTableSchema::getNumFlatColumns() const {
    auto numFlatColumns = 0u;
    for (auto& column : columns) {
        if (column->isFlat()) {
            numFlatColumns++;
        }
    }
    return numFlatColumns;
}

uint64_t FactorizedTableSchema::getNumUnflatColumns() const {
    auto numUnflatColumns = 0u;
    for (auto& column : columns) {
        if (!column->isFlat()) {
            numUnflatColumns++;
        }
    }
    return numUnflatColumns;
}

void DataBlock::copyTuples(DataBlock* blockToCopyFrom, ft_tuple_idx_t tupleIdxToCopyFrom,
    DataBlock* blockToCopyInto, ft_tuple_idx_t tupleIdxToCopyTo, uint32_t numTuplesToCopy,
    uint32_t numBytesPerTuple) {
    for (auto i = 0u; i < numTuplesToCopy; i++) {
        memcpy(blockToCopyInto->getData() + (tupleIdxToCopyTo * numBytesPerTuple),
            blockToCopyFrom->getData() + (tupleIdxToCopyFrom * numBytesPerTuple), numBytesPerTuple);
        tupleIdxToCopyFrom++;
        tupleIdxToCopyTo++;
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
        std::min(numTuplesPerBlock - newLastBlock->numTuples, oldLastBlock->numTuples);
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
    MemoryManager* memoryManager, std::unique_ptr<FactorizedTableSchema> tableSchema)
    : memoryManager{memoryManager}, tableSchema{std::move(tableSchema)}, numTuples{0} {
    KU_ASSERT(this->tableSchema->getNumBytesPerTuple() <= BufferPoolConstants::PAGE_256KB_SIZE);
    if (!this->tableSchema->isEmpty()) {
        inMemOverflowBuffer = std::make_unique<InMemOverflowBuffer>(memoryManager);
        numTuplesPerBlock =
            BufferPoolConstants::PAGE_256KB_SIZE / this->tableSchema->getNumBytesPerTuple();
        flatTupleBlockCollection = std::make_unique<DataBlockCollection>(
            this->tableSchema->getNumBytesPerTuple(), numTuplesPerBlock);
        unflatTupleBlockCollection = std::make_unique<DataBlockCollection>();
    }
}

void FactorizedTable::append(const std::vector<ValueVector*>& vectors) {
    auto numTuplesToAppend = computeNumTuplesToAppend(vectors);
    auto appendInfos = allocateFlatTupleBlocks(numTuplesToAppend);
    for (auto i = 0u; i < vectors.size(); i++) {
        auto numAppendedTuples = 0ul;
        for (auto& blockAppendInfo : appendInfos) {
            copyVectorToColumn(*vectors[i], blockAppendInfo, numAppendedTuples, i);
            numAppendedTuples += blockAppendInfo.numTuplesToAppend;
        }
        KU_ASSERT(numAppendedTuples == numTuplesToAppend);
    }
    numTuples += numTuplesToAppend;
}

uint8_t* FactorizedTable::appendEmptyTuple() {
    if (flatTupleBlockCollection->isEmpty() ||
        flatTupleBlockCollection->getBlocks().back()->freeSize <
            tableSchema->getNumBytesPerTuple()) {
        flatTupleBlockCollection->append(std::make_unique<DataBlock>(memoryManager));
    }
    auto& block = flatTupleBlockCollection->getBlocks().back();
    uint8_t* tuplePtr = block->getData() + BufferPoolConstants::PAGE_256KB_SIZE - block->freeSize;
    block->freeSize -= tableSchema->getNumBytesPerTuple();
    block->numTuples++;
    numTuples++;
    return tuplePtr;
}

void FactorizedTable::scan(std::vector<ValueVector*>& vectors, ft_tuple_idx_t tupleIdx,
    uint64_t numTuplesToScan, std::vector<ft_col_idx_t>& colIdxesToScan) const {
    KU_ASSERT(tupleIdx + numTuplesToScan <= numTuples);
    KU_ASSERT(vectors.size() == colIdxesToScan.size());
    std::unique_ptr<uint8_t*[]> tuplesToRead = std::make_unique<uint8_t*[]>(numTuplesToScan);
    for (auto i = 0u; i < numTuplesToScan; i++) {
        tuplesToRead[i] = getTuple(tupleIdx + i);
    }
    lookup(vectors, colIdxesToScan, tuplesToRead.get(), 0 /* startPos */, numTuplesToScan);
}

void FactorizedTable::lookup(std::vector<ValueVector*>& vectors,
    std::vector<ft_col_idx_t>& colIdxesToScan, uint8_t** tuplesToRead, uint64_t startPos,
    uint64_t numTuplesToRead) const {
    KU_ASSERT(vectors.size() == colIdxesToScan.size());
    for (auto i = 0u; i < colIdxesToScan.size(); i++) {
        auto vector = vectors[i];
        // TODO(Xiyang/Ziyi): we should set up a rule about when to reset. Should it be in operator?
        vector->resetAuxiliaryBuffer();
        ft_col_idx_t colIdx = colIdxesToScan[i];
        if (tableSchema->getColumn(colIdx)->isFlat()) {
            KU_ASSERT(!(vector->state->isFlat() && numTuplesToRead > 1));
            readFlatCol(tuplesToRead + startPos, colIdx, *vector, numTuplesToRead);
        } else {
            // If the caller wants to read an unflat column from factorizedTable, the vector
            // must be unflat and the numTuplesToScan should be 1.
            KU_ASSERT(!vector->state->isFlat() && numTuplesToRead == 1);
            readUnflatCol(tuplesToRead + startPos, colIdx, *vector);
        }
    }
}

void FactorizedTable::lookup(std::vector<ValueVector*>& vectors, const SelectionVector* selVector,
    std::vector<ft_col_idx_t>& colIdxesToScan, uint8_t* tupleToRead) const {
    KU_ASSERT(vectors.size() == colIdxesToScan.size());
    for (auto i = 0u; i < colIdxesToScan.size(); i++) {
        ft_col_idx_t colIdx = colIdxesToScan[i];
        if (tableSchema->getColumn(colIdx)->isFlat()) {
            readFlatCol(&tupleToRead, colIdx, *vectors[i], 1);
        } else {
            readUnflatCol(tupleToRead, selVector, colIdx, *vectors[i]);
        }
    }
}

void FactorizedTable::lookup(std::vector<ValueVector*>& vectors,
    std::vector<ft_col_idx_t>& colIdxesToScan, std::vector<ft_tuple_idx_t>& tupleIdxesToRead,
    uint64_t startPos, uint64_t numTuplesToRead) const {
    KU_ASSERT(vectors.size() == colIdxesToScan.size());
    auto tuplesToRead = std::make_unique<uint8_t*[]>(tupleIdxesToRead.size());
    KU_ASSERT(numTuplesToRead > 0);
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
    KU_ASSERT(*tableSchema == *other.tableSchema);
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
    std::vector<ft_col_idx_t> colIdxes(tableSchema->getNumColumns());
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

uint64_t FactorizedTable::getNumFlatTuples(ft_tuple_idx_t tupleIdx) const {
    std::unordered_map<uint32_t, bool> calculatedDataChunkPoses;
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

uint8_t* FactorizedTable::getTuple(ft_tuple_idx_t tupleIdx) const {
    KU_ASSERT(tupleIdx < numTuples);
    auto [blockIdx, tupleIdxInBlock] = getBlockIdxAndTupleIdxInBlock(tupleIdx);
    return flatTupleBlockCollection->getBlock(blockIdx)->getData() +
           tupleIdxInBlock * tableSchema->getNumBytesPerTuple();
}

void FactorizedTable::updateFlatCell(
    uint8_t* tuplePtr, ft_col_idx_t colIdx, ValueVector* valueVector, uint32_t pos) {
    auto nullBuffer = tuplePtr + tableSchema->getNullMapOffset();
    if (valueVector->isNull(pos)) {
        setNonOverflowColNull(nullBuffer, colIdx);
    } else {
        valueVector->copyToRowData(
            pos, tuplePtr + tableSchema->getColOffset(colIdx), inMemOverflowBuffer.get());
        NullBuffer::setNoNull(nullBuffer, colIdx);
    }
}

bool FactorizedTable::isOverflowColNull(
    const uint8_t* nullBuffer, ft_tuple_idx_t tupleIdx, ft_col_idx_t colIdx) const {
    KU_ASSERT(colIdx < tableSchema->getNumColumns());
    if (tableSchema->getColumn(colIdx)->hasNoNullGuarantee()) {
        return false;
    }
    return NullBuffer::isNull(nullBuffer, tupleIdx);
}

bool FactorizedTable::isNonOverflowColNull(const uint8_t* nullBuffer, ft_col_idx_t colIdx) const {
    KU_ASSERT(colIdx < tableSchema->getNumColumns());
    if (tableSchema->getColumn(colIdx)->hasNoNullGuarantee()) {
        return false;
    }
    return NullBuffer::isNull(nullBuffer, colIdx);
}

void FactorizedTable::setNonOverflowColNull(uint8_t* nullBuffer, ft_col_idx_t colIdx) {
    NullBuffer::setNull(nullBuffer, colIdx);
    tableSchema->setMayContainsNullsToTrue(colIdx);
}

void FactorizedTable::clear() {
    numTuples = 0;
    flatTupleBlockCollection = std::make_unique<DataBlockCollection>(
        tableSchema->getNumBytesPerTuple(), numTuplesPerBlock);
    unflatTupleBlockCollection = std::make_unique<DataBlockCollection>();
    inMemOverflowBuffer->resetBuffer();
}

void FactorizedTable::setOverflowColNull(
    uint8_t* nullBuffer, ft_col_idx_t colIdx, ft_tuple_idx_t tupleIdx) {
    NullBuffer::setNull(nullBuffer, tupleIdx);
    tableSchema->setMayContainsNullsToTrue(colIdx);
}

// TODO(Guodong): change this function to not use dataChunkPos in ColumnSchema.
uint64_t FactorizedTable::computeNumTuplesToAppend(
    const std::vector<ValueVector*>& vectorsToAppend) const {
    KU_ASSERT(!vectorsToAppend.empty());
    auto unflatDataChunkPos = -1ul;
    auto numTuplesToAppend = 1ul;
    for (auto i = 0u; i < vectorsToAppend.size(); i++) {
        // If the caller tries to append an unflat vector to a flat column in the
        // factorizedTable, the factorizedTable needs to flatten that vector.
        if (tableSchema->getColumn(i)->isFlat() && !vectorsToAppend[i]->state->isFlat()) {
            // The caller is not allowed to append multiple unflat columns from different
            // datachunks to multiple flat columns in the factorizedTable.
            if (!tableSchema->getColumn(i)->isFlat()) {
                if (unflatDataChunkPos != -1 &&
                    tableSchema->getColumn(i)->getDataChunkPos() != unflatDataChunkPos) {
                    KU_ASSERT(false);
                }
                unflatDataChunkPos = tableSchema->getColumn(i)->getDataChunkPos();
            }
            numTuplesToAppend = vectorsToAppend[i]->state->selVector->selectedSize;
        }
    }
    return numTuplesToAppend;
}

std::vector<BlockAppendingInfo> FactorizedTable::allocateFlatTupleBlocks(
    uint64_t numTuplesToAppend) {
    auto numBytesPerTuple = tableSchema->getNumBytesPerTuple();
    KU_ASSERT(numBytesPerTuple < BufferPoolConstants::PAGE_256KB_SIZE);
    std::vector<BlockAppendingInfo> appendingInfos;
    while (numTuplesToAppend > 0) {
        if (flatTupleBlockCollection->isEmpty() ||
            flatTupleBlockCollection->getBlocks().back()->freeSize < numBytesPerTuple) {
            flatTupleBlockCollection->append(std::make_unique<DataBlock>(memoryManager));
        }
        auto& block = flatTupleBlockCollection->getBlocks().back();
        auto numTuplesToAppendInCurBlock =
            std::min(numTuplesToAppend, block->freeSize / numBytesPerTuple);
        appendingInfos.emplace_back(
            block->getData() + BufferPoolConstants::PAGE_256KB_SIZE - block->freeSize,
            numTuplesToAppendInCurBlock);
        block->freeSize -= numTuplesToAppendInCurBlock * numBytesPerTuple;
        block->numTuples += numTuplesToAppendInCurBlock;
        numTuplesToAppend -= numTuplesToAppendInCurBlock;
    }
    return appendingInfos;
}

uint8_t* FactorizedTable::allocateUnflatTupleBlock(uint32_t numBytes) {
    KU_ASSERT(numBytes < BufferPoolConstants::PAGE_256KB_SIZE);
    if (unflatTupleBlockCollection->isEmpty()) {
        unflatTupleBlockCollection->append(std::make_unique<DataBlock>(memoryManager));
    }
    auto lastBlock = unflatTupleBlockCollection->getBlocks().back().get();
    if (lastBlock->freeSize > numBytes) {
        lastBlock->freeSize -= numBytes;
        return lastBlock->getData() + BufferPoolConstants::PAGE_256KB_SIZE - lastBlock->freeSize -
               numBytes;
    }
    unflatTupleBlockCollection->append(std::make_unique<DataBlock>(memoryManager));
    lastBlock = unflatTupleBlockCollection->getBlocks().back().get();
    lastBlock->freeSize -= numBytes;
    return lastBlock->getData();
}

void FactorizedTable::copyFlatVectorToFlatColumn(
    const ValueVector& vector, const BlockAppendingInfo& blockAppendInfo, ft_col_idx_t colIdx) {
    auto valuePositionInVectorToAppend = vector.state->selVector->selectedPositions[0];
    auto colOffsetInDataBlock = tableSchema->getColOffset(colIdx);
    auto dstDataPtr = blockAppendInfo.data;
    for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
        if (vector.isNull(valuePositionInVectorToAppend)) {
            setNonOverflowColNull(dstDataPtr + tableSchema->getNullMapOffset(), colIdx);
        } else {
            vector.copyToRowData(valuePositionInVectorToAppend, dstDataPtr + colOffsetInDataBlock,
                inMemOverflowBuffer.get());
        }
        dstDataPtr += tableSchema->getNumBytesPerTuple();
    }
}

void FactorizedTable::copyUnflatVectorToFlatColumn(const ValueVector& vector,
    const BlockAppendingInfo& blockAppendInfo, uint64_t numAppendedTuples, ft_col_idx_t colIdx) {
    auto byteOffsetOfColumnInTuple = tableSchema->getColOffset(colIdx);
    auto dstTuple = blockAppendInfo.data;
    if (vector.state->selVector->isUnfiltered()) {
        if (vector.hasNoNullsGuarantee()) {
            for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
                vector.copyToRowData(numAppendedTuples + i, dstTuple + byteOffsetOfColumnInTuple,
                    inMemOverflowBuffer.get());
                dstTuple += tableSchema->getNumBytesPerTuple();
            }
        } else {
            for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
                if (vector.isNull(numAppendedTuples + i)) {
                    setNonOverflowColNull(dstTuple + tableSchema->getNullMapOffset(), colIdx);
                } else {
                    vector.copyToRowData(numAppendedTuples + i,
                        dstTuple + byteOffsetOfColumnInTuple, inMemOverflowBuffer.get());
                }
                dstTuple += tableSchema->getNumBytesPerTuple();
            }
        }
    } else {
        if (vector.hasNoNullsGuarantee()) {
            for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
                vector.copyToRowData(
                    vector.state->selVector->selectedPositions[numAppendedTuples + i],
                    dstTuple + byteOffsetOfColumnInTuple, inMemOverflowBuffer.get());
                dstTuple += tableSchema->getNumBytesPerTuple();
            }
        } else {
            for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
                auto pos = vector.state->selVector->selectedPositions[numAppendedTuples + i];
                if (vector.isNull(pos)) {
                    setNonOverflowColNull(dstTuple + tableSchema->getNullMapOffset(), colIdx);
                } else {
                    vector.copyToRowData(
                        pos, dstTuple + byteOffsetOfColumnInTuple, inMemOverflowBuffer.get());
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
    const ValueVector& vector, const BlockAppendingInfo& blockAppendInfo, ft_col_idx_t colIdx) {
    KU_ASSERT(!vector.state->isFlat());
    auto unflatTupleValue = appendVectorToUnflatTupleBlocks(vector, colIdx);
    auto blockPtr = blockAppendInfo.data + tableSchema->getColOffset(colIdx);
    for (auto i = 0u; i < blockAppendInfo.numTuplesToAppend; i++) {
        memcpy(blockPtr, (uint8_t*)&unflatTupleValue, sizeof(overflow_value_t));
        blockPtr += tableSchema->getNumBytesPerTuple();
    }
}

void FactorizedTable::copyVectorToColumn(const ValueVector& vector,
    const BlockAppendingInfo& blockAppendInfo, uint64_t numAppendedTuples, ft_col_idx_t colIdx) {
    if (tableSchema->getColumn(colIdx)->isFlat()) {
        copyVectorToFlatColumn(vector, blockAppendInfo, numAppendedTuples, colIdx);
    } else {
        copyVectorToUnflatColumn(vector, blockAppendInfo, colIdx);
    }
}

overflow_value_t FactorizedTable::appendVectorToUnflatTupleBlocks(
    const ValueVector& vector, ft_col_idx_t colIdx) {
    KU_ASSERT(!vector.state->isFlat());
    auto numFlatTuplesInVector = vector.state->selVector->selectedSize;
    auto numBytesPerValue = LogicalTypeUtils::getRowLayoutSize(vector.dataType);
    auto numBytesForData = numBytesPerValue * numFlatTuplesInVector;
    auto overflowBlockBuffer = allocateUnflatTupleBlock(
        numBytesForData + NullBuffer::getNumBytesForNullValues(numFlatTuplesInVector));
    if (vector.state->selVector->isUnfiltered()) {
        if (vector.hasNoNullsGuarantee()) {
            auto dstDataBuffer = overflowBlockBuffer;
            for (auto i = 0u; i < numFlatTuplesInVector; i++) {
                vector.copyToRowData(i, dstDataBuffer, inMemOverflowBuffer.get());
                dstDataBuffer += numBytesPerValue;
            }
        } else {
            auto dstDataBuffer = overflowBlockBuffer;
            for (auto i = 0u; i < numFlatTuplesInVector; i++) {
                if (vector.isNull(i)) {
                    setOverflowColNull(overflowBlockBuffer + numBytesForData, colIdx, i);
                } else {
                    vector.copyToRowData(i, dstDataBuffer, inMemOverflowBuffer.get());
                }
                dstDataBuffer += numBytesPerValue;
            }
        }
    } else {
        if (vector.hasNoNullsGuarantee()) {
            auto dstDataBuffer = overflowBlockBuffer;
            for (auto i = 0u; i < numFlatTuplesInVector; i++) {
                vector.copyToRowData(vector.state->selVector->selectedPositions[i], dstDataBuffer,
                    inMemOverflowBuffer.get());
                dstDataBuffer += numBytesPerValue;
            }
        } else {
            auto dstDataBuffer = overflowBlockBuffer;
            for (auto i = 0u; i < numFlatTuplesInVector; i++) {
                auto pos = vector.state->selVector->selectedPositions[i];
                if (vector.isNull(pos)) {
                    setOverflowColNull(overflowBlockBuffer + numBytesForData, colIdx, i);
                } else {
                    vector.copyToRowData(pos, dstDataBuffer, inMemOverflowBuffer.get());
                }
                dstDataBuffer += numBytesPerValue;
            }
        }
    }
    return overflow_value_t{numFlatTuplesInVector, overflowBlockBuffer};
}

void FactorizedTable::readUnflatCol(
    uint8_t** tuplesToRead, ft_col_idx_t colIdx, ValueVector& vector) const {
    auto overflowColValue =
        *(overflow_value_t*)(tuplesToRead[0] + tableSchema->getColOffset(colIdx));
    KU_ASSERT(vector.state->selVector->isUnfiltered());
    auto numBytesPerValue = LogicalTypeUtils::getRowLayoutSize(vector.dataType);
    if (hasNoNullGuarantee(colIdx)) {
        vector.setAllNonNull();
        auto val = overflowColValue.value;
        for (auto i = 0u; i < overflowColValue.numElements; i++) {
            vector.copyFromRowData(i, val);
            val += numBytesPerValue;
        }
    } else {
        auto overflowColNullData =
            overflowColValue.value + overflowColValue.numElements * numBytesPerValue;
        auto overflowColData = overflowColValue.value;
        for (auto i = 0u; i < overflowColValue.numElements; i++) {
            if (isOverflowColNull(overflowColNullData, i, colIdx)) {
                vector.setNull(i, true);
            } else {
                vector.setNull(i, false);
                vector.copyFromRowData(i, overflowColData);
            }
            overflowColData += numBytesPerValue;
        }
    }
    vector.state->selVector->selectedSize = overflowColValue.numElements;
}

void FactorizedTable::readUnflatCol(const uint8_t* tupleToRead, const SelectionVector* selVector,
    ft_col_idx_t colIdx, ValueVector& vector) const {
    auto vectorOverflowValue =
        *(overflow_value_t*)(tupleToRead + tableSchema->getColOffset(colIdx));
    KU_ASSERT(vector.state->selVector->isUnfiltered());
    if (hasNoNullGuarantee(colIdx)) {
        vector.setAllNonNull();
        auto val = vectorOverflowValue.value;
        for (auto i = 0u; i < vectorOverflowValue.numElements; i++) {
            auto pos = selVector->selectedPositions[i];
            vector.copyFromRowData(i, val + (pos * vector.getNumBytesPerValue()));
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
                vector.copyFromRowData(
                    i, vectorOverflowValue.value + pos * vector.getNumBytesPerValue());
            }
        }
    }
    vector.state->selVector->selectedSize = selVector->selectedSize;
}

void FactorizedTable::readFlatColToFlatVector(
    uint8_t* tupleToRead, ft_col_idx_t colIdx, ValueVector& vector, sel_t pos) const {
    if (isNonOverflowColNull(tupleToRead + tableSchema->getNullMapOffset(), colIdx)) {
        vector.setNull(pos, true);
    } else {
        vector.setNull(pos, false);
        vector.copyFromRowData(pos, tupleToRead + tableSchema->getColOffset(colIdx));
    }
}

void FactorizedTable::readFlatCol(uint8_t** tuplesToRead, ft_col_idx_t colIdx, ValueVector& vector,
    uint64_t numTuplesToRead) const {
    if (vector.state->isFlat()) {
        auto pos = vector.state->selVector->selectedPositions[0];
        readFlatColToFlatVector(tuplesToRead[0], colIdx, vector, pos);
    } else {
        readFlatColToUnflatVector(tuplesToRead, colIdx, vector, numTuplesToRead);
    }
}

void FactorizedTable::readFlatColToUnflatVector(uint8_t** tuplesToRead, ft_col_idx_t colIdx,
    ValueVector& vector, uint64_t numTuplesToRead) const {
    vector.state->selVector->selectedSize = numTuplesToRead;
    if (hasNoNullGuarantee(colIdx)) {
        vector.setAllNonNull();
        for (auto i = 0u; i < numTuplesToRead; i++) {
            auto positionInVectorToWrite = vector.state->selVector->selectedPositions[i];
            auto srcData = tuplesToRead[i] + tableSchema->getColOffset(colIdx);
            vector.copyFromRowData(positionInVectorToWrite, srcData);
        }
    } else {
        for (auto i = 0u; i < numTuplesToRead; i++) {
            auto positionInVectorToWrite = vector.state->selVector->selectedPositions[i];
            auto dataBuffer = tuplesToRead[i];
            if (isNonOverflowColNull(dataBuffer + tableSchema->getNullMapOffset(), colIdx)) {
                vector.setNull(positionInVectorToWrite, true);
            } else {
                vector.setNull(positionInVectorToWrite, false);
                vector.copyFromRowData(
                    positionInVectorToWrite, dataBuffer + tableSchema->getColOffset(colIdx));
            }
        }
    }
}

void FactorizedTableUtils::appendStringToTable(
    FactorizedTable* factorizedTable, std::string& outputMsg, MemoryManager* memoryManager) {
    auto outputMsgVector = std::make_shared<ValueVector>(LogicalTypeID::STRING, memoryManager);
    outputMsgVector->state = DataChunkState::getSingleValueDataChunkState();
    auto outputKUStr = ku_string_t();
    outputKUStr.overflowPtr =
        reinterpret_cast<uint64_t>(StringVector::getInMemOverflowBuffer(outputMsgVector.get())
                                       ->allocateSpace(outputMsg.length()));
    outputKUStr.set(outputMsg);
    outputMsgVector->setValue(0, outputKUStr);
    factorizedTable->append(std::vector<ValueVector*>{outputMsgVector.get()});
}

std::shared_ptr<FactorizedTable> FactorizedTableUtils::getFactorizedTableForOutputMsg(
    std::string& outputMsg, storage::MemoryManager* memoryManager) {
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    ftTableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* flat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::STRING})));
    auto factorizedTable =
        std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
    appendStringToTable(factorizedTable.get(), outputMsg, memoryManager);
    return factorizedTable;
}

FlatTupleIterator::FlatTupleIterator(FactorizedTable& factorizedTable, std::vector<Value*> values)
    : factorizedTable{factorizedTable}, numFlatTuples{0}, nextFlatTupleIdx{0},
      nextTupleIdx{1}, values{std::move(values)} {
    resetState();
    KU_ASSERT(this->values.size() == factorizedTable.tableSchema->getNumColumns());
}

void FlatTupleIterator::getNextFlatTuple() {
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
}

void FlatTupleIterator::resetState() {
    numFlatTuples = 0;
    nextFlatTupleIdx = 0;
    nextTupleIdx = 1;
    if (factorizedTable.getNumTuples()) {
        currentTupleBuffer = factorizedTable.getTuple(0);
        numFlatTuples = factorizedTable.getNumFlatTuples(0);
        updateNumElementsInDataChunk();
        updateInvalidEntriesInFlatTuplePositionsInDataChunk();
    }
}

void FlatTupleIterator::readUnflatColToFlatTuple(ft_col_idx_t colIdx, uint8_t* valueBuffer) {
    auto overflowValue =
        (overflow_value_t*)(valueBuffer + factorizedTable.getTableSchema()->getColOffset(colIdx));
    auto columnInFactorizedTable = factorizedTable.getTableSchema()->getColumn(colIdx);
    auto tupleSizeInOverflowBuffer =
        LogicalTypeUtils::getRowLayoutSize(*values[colIdx]->getDataType());
    valueBuffer =
        overflowValue->value +
        tupleSizeInOverflowBuffer *
            flatTuplePositionsInDataChunk[columnInFactorizedTable->getDataChunkPos()].first;
    auto isNull = factorizedTable.isOverflowColNull(
        overflowValue->value + tupleSizeInOverflowBuffer * overflowValue->numElements,
        flatTuplePositionsInDataChunk[columnInFactorizedTable->getDataChunkPos()].first, colIdx);
    values[colIdx]->setNull(isNull);
    if (!isNull) {
        readValueBufferToValue(colIdx, valueBuffer);
    }
}

void FlatTupleIterator::readFlatColToFlatTuple(ft_col_idx_t colIdx, uint8_t* valueBuffer) {
    auto isNull = factorizedTable.isNonOverflowColNull(
        valueBuffer + factorizedTable.getTableSchema()->getNullMapOffset(), colIdx);
    values[colIdx]->setNull(isNull);
    if (!isNull) {
        readValueBufferToValue(
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
            flatTuplePositionsInDataChunk[i] = std::make_pair(UINT64_MAX, UINT64_MAX);
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
            std::make_pair(0 /* nextIdxToReadInDataChunk */, numElementsInDataChunk);
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

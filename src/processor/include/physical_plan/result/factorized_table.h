#pragma once

#include <numeric>
#include <unordered_map>

#include "src/common/include/memory_manager.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/overflow_buffer.h"
#include "src/processor/include/physical_plan/result/flat_tuple.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using namespace graphflow::common;
using namespace graphflow::processor;

namespace graphflow {
namespace processor {

struct BlockAppendingInfo {
    BlockAppendingInfo(uint8_t* data, uint64_t numTuplesToAppend)
        : data{data}, numTuplesToAppend{numTuplesToAppend} {}

    uint8_t* data;
    uint64_t numTuplesToAppend;
};

// This struct allocates and holds one bmBackedBlock when constructed. The bmBackedBlock will be
// released when this struct goes out of scope.
struct DataBlock {
public:
    DataBlock(MemoryManager* memoryManager) : numTuples{0}, memoryManager{memoryManager} {
        block = memoryManager->allocateBMBackedBlock(true);
        freeSize = block->size;
    }

    DataBlock(DataBlock&& other) = default;

    ~DataBlock() { memoryManager->freeBMBackedBlock(block->pageIdx); }

    uint8_t* getData() const { return block->data; }

public:
    uint64_t freeSize;
    uint64_t numTuples;
    MemoryManager* memoryManager;

private:
    unique_ptr<BMBackedMemoryBlock> block;
};

class ColumnSchema {
public:
    ColumnSchema(bool isUnflat, uint32_t dataChunksPos, uint64_t numBytes)
        : isUnflat{isUnflat}, dataChunkPos{dataChunksPos}, numBytes{numBytes} {}

    inline bool getIsUnflat() const { return isUnflat; }

    inline uint32_t getDataChunkPos() const { return dataChunkPos; }

    inline uint64_t getNumBytes() const { return numBytes; }

    inline bool operator==(const ColumnSchema& other) const {
        return isUnflat == other.isUnflat && dataChunkPos == other.dataChunkPos &&
               numBytes == other.numBytes;
    }
    inline bool operator!=(const ColumnSchema& other) const { return !(*this == other); }

private:
    // We need isUnflat, dataChunkPos to know the factorization structure in the factorizedTable.
    bool isUnflat;
    uint32_t dataChunkPos;
    uint64_t numBytes;
};

class TableSchema {
public:
    TableSchema() = default;

    explicit TableSchema(vector<ColumnSchema> columns);

    void appendColumn(const ColumnSchema& column);

    inline ColumnSchema getColumn(uint32_t idx) const { return columns[idx]; }

    inline uint32_t getNumColumns() const { return columns.size(); }

    inline uint64_t getNullMapOffset() const { return numBytesForDataPerTuple; }

    inline uint64_t getNumBytesForNullMap() const { return numBytesForNullMapPerTuple; }

    inline uint64_t getNumBytesPerTuple() const {
        return numBytesForDataPerTuple + numBytesForNullMapPerTuple;
    }

    inline uint64_t getColOffset(uint64_t idx) const { return colOffsets[idx]; }

    static inline uint64_t getNumBytesForNullBuffer(uint64_t numColumns) {
        return (numColumns >> 3) + ((numColumns & 7) != 0); // &7 is the same as %8;
    }

    bool operator==(const TableSchema& other) const;
    inline bool operator!=(const TableSchema& other) const { return !(*this == other); }

private:
    vector<ColumnSchema> columns;
    uint64_t numBytesForDataPerTuple = 0;
    uint64_t numBytesForNullMapPerTuple = 0;
    vector<uint64_t> colOffsets;
};

class FlatTupleIterator;

class FactorizedTable {
    friend FlatTupleIterator;

public:
    FactorizedTable(MemoryManager* memoryManager, const TableSchema& tableSchema);

    void append(const vector<shared_ptr<ValueVector>>& vectors);

    //! This function appends an empty tuple to the factorizedTable and returns a pointer to that
    //! tuple.
    uint8_t* appendEmptyTuple();

    // This function scans numTuplesToScan of rows to vectors starting at tupleIdx. Callers are
    // responsible for making sure all the parameters are valid.
    void scan(vector<shared_ptr<ValueVector>>& vectors, uint64_t tupleIdx,
        uint64_t numTuplesToScan) const {
        vector<uint64_t> colIdxes(tableSchema.getNumColumns());
        iota(colIdxes.begin(), colIdxes.end(), 0);
        scan(vectors, tupleIdx, numTuplesToScan, colIdxes);
    }

    void scan(vector<shared_ptr<ValueVector>>& vectors, uint64_t tupleIdx, uint64_t numTuplesToScan,
        vector<uint64_t>& colIdxToScan) const;

    void lookup(vector<shared_ptr<ValueVector>>& vectors, vector<uint64_t>& colIdxesToScan,
        uint8_t** tuplesToRead, uint64_t startPos, uint64_t numTuplesToRead) const;

    // This is a specialized scan function to read one flat tuple in factorizedTable to an
    // unflat valueVector.
    void scanTupleToVectorPos(
        vector<shared_ptr<ValueVector>> vectors, uint64_t tupleIdx, uint64_t valuePosInVec) const;

    void merge(FactorizedTable& other);

    OverflowBuffer* getOverflowBuffer() { return overflowBuffer.get(); }

    bool hasUnflatCol() const;

    inline bool hasUnflatCol(vector<uint64_t>& colIdxes) const {
        return any_of(colIdxes.begin(), colIdxes.end(),
            [this](uint64_t colIdx) { return tableSchema.getColumn(colIdx).getIsUnflat(); });
    }

    inline uint64_t getNumTuples() const { return numTuples; }

    uint64_t getTotalNumFlatTuples() const;

    inline vector<unique_ptr<DataBlock>>& getTupleDataBlocks() { return tupleDataBlocks; }
    inline const TableSchema& getTableSchema() const { return tableSchema; }

    FlatTupleIterator getFlatTupleIterator();

    uint64_t getNumFlatTuples(uint64_t tupleIdx) const;

    // This function returns a gf_string_t stored at the [tupleIdx, colIdx]. It causes an
    // assertion failure if either the [tupleIdx, colIdx] is invalid or the cell is unflat.
    inline gf_string_t getString(uint64_t tupleIdx, uint64_t colIdx) const {
        return *((gf_string_t*)getCell(tupleIdx, colIdx));
    }

    // This function returns a unstructured value stored at the [tupleIdx, colIdx]. It causes
    // an assertion failure if either the [tupleIdx, colIdx] is invalid or the cell is unflat.
    inline Value getUnstrValue(uint64_t tupleIdx, uint64_t colIdx) const {
        return *((Value*)getCell(tupleIdx, colIdx));
    }

    uint8_t* getTuple(uint64_t tupleIdx) const;

    inline void updateFlatCell(uint64_t tupleIdx, uint64_t colIdx, void* dataBuf) {
        memcpy(getCell(tupleIdx, colIdx), dataBuf, tableSchema.getColumn(colIdx).getNumBytes());
    }

    void updateFlatCell(uint64_t tupleIdx, uint64_t colIdx, ValueVector* valueVector);

    static bool isNull(const uint8_t* nullMapBuffer, uint64_t colIdx);

private:
    static void setNull(uint8_t* nullBuffer, uint64_t colIdx);

    uint64_t computeNumTuplesToAppend(const vector<shared_ptr<ValueVector>>& vectorsToAppend) const;

    void assertTupleIdxColIdxAndValueIsFlat(uint64_t tupleIdx, uint64_t colIdx) const;

    uint8_t* getCell(uint64_t tupleIdx, uint64_t colIdx) const;

    inline pair<uint64_t, uint64_t> getBlockIdxAndTupleIdxInBlock(uint64_t tupleIdx) const {
        return make_pair(tupleIdx / numTuplesPerBlock, tupleIdx % numTuplesPerBlock);
    }

    vector<BlockAppendingInfo> allocateTupleBlocks(uint64_t numTuplesToAppend);

    uint8_t* allocateOverflowBlocks(uint64_t numBytes);

    void copyVectorToDataBlock(const ValueVector& vector, const BlockAppendingInfo& blockAppendInfo,
        uint64_t numAppendedTuples, uint64_t colIdx);
    overflow_value_t appendUnFlatVectorToOverflowBlocks(const ValueVector& vector);

    void readUnflatCol(uint8_t** tuplesToRead, uint64_t colIdx, ValueVector& vector) const;

    void readFlatCol(uint8_t** tuplesToRead, uint64_t colIdx, ValueVector& vector,
        uint64_t numTuplesToRead) const;

    MemoryManager* memoryManager;
    TableSchema tableSchema;
    uint64_t numTuples;
    uint64_t numTuplesPerBlock;
    vector<unique_ptr<DataBlock>> tupleDataBlocks;
    vector<unique_ptr<DataBlock>> vectorOverflowBlocks;
    unique_ptr<OverflowBuffer> overflowBuffer;
};

class FlatTupleIterator {
public:
    explicit FlatTupleIterator(FactorizedTable& factorizedTable);

    inline bool hasNextFlatTuple() {
        return nextTupleIdx < factorizedTable.getNumTuples() || nextFlatTupleIdx < numFlatTuples;
    }

    void getNextFlatTuple(FlatTuple& flatTuple);

private:
    static void readValueBufferToFlatTuple(
        FlatTuple& flatTuple, uint64_t flatTupleValIdx, const uint8_t* valueBuffer);

    void readUnflatColToFlatTuple(
        FlatTuple& flatTuple, uint64_t flatTupleValIdx, uint8_t* valueBuffer);

    void readFlatColToFlatTuple(FlatTuple& flatTuple, uint64_t colIdx, uint8_t* valueBuffer);

    // The dataChunkPos may be not consecutive, which means some entries in the
    // flatTuplePositionsInDataChunk is invalid. We put pair(UINT64_MAX, UINT64_MAX) in the
    // invalid entries.
    inline bool isValidDataChunkPos(uint32_t dataChunkPos) const {
        return flatTuplePositionsInDataChunk[dataChunkPos].first != UINT64_MAX;
    }

    // We put pair(UINT64_MAX, UINT64_MAX) in all invalid entries in
    // FlatTuplePositionsInDataChunk.
    void updateInvalidEntriesInFlatTuplePositionsInDataChunk();

    // This function is used to update the number of elements in the dataChunk when we want
    // to iterate a new tuple.
    void updateNumElementsInDataChunk();

    // This function updates the flatTuplePositionsInDataChunk, so that getNextFlatTuple() can
    // correctly outputs the next flat tuple in the current tuple. For example, we want to read
    // two unflat columns, which are on different dataChunks A,B and both have 100 columns. The
    // flatTuplePositionsInDataChunk after the first call to getNextFlatTuple() looks like:
    // {dataChunkA : [0, 100]}, {dataChunkB : [0, 100]} This function updates the
    // flatTuplePositionsInDataChunk to: {dataChunkA: [1, 100]}, {dataChunkB: [0, 100]}. Meaning
    // that the getNextFlatTuple() should read the second element in the first unflat column and
    // the first element in the second unflat column.
    void updateFlatTuplePositionsInDataChunk();

    FactorizedTable& factorizedTable;
    uint8_t* currentTupleBuffer;
    uint64_t numFlatTuples;
    uint64_t nextFlatTupleIdx;
    uint64_t nextTupleIdx;
    // This field stores the (nextIdxToReadInDataChunk, numElementsInDataChunk) of each dataChunk.
    vector<pair<uint64_t, uint64_t>> flatTuplePositionsInDataChunk;
};

} // namespace processor
} // namespace graphflow

#pragma once

#include <numeric>
#include <unordered_map>

#include "src/common/include/overflow_buffer.h"
#include "src/processor/include/physical_plan/result/flat_tuple.h"
#include "src/processor/include/physical_plan/result/result_set.h"
#include "src/storage/buffer_manager/include/memory_manager.h"

using namespace graphflow::common;

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
    explicit DataBlock(MemoryManager* memoryManager) : numTuples{0}, memoryManager{memoryManager} {
        block = memoryManager->allocateBlock(true);
        freeSize = block->size;
    }

    DataBlock(DataBlock&& other) = default;

    ~DataBlock() { memoryManager->freeBlock(block->pageIdx); }

    uint8_t* getData() const { return block->data; }

    void resetToZero() { memset(block->data, 0, LARGE_PAGE_SIZE); }

public:
    uint64_t freeSize;
    uint32_t numTuples;
    MemoryManager* memoryManager;

private:
    unique_ptr<MemoryBlock> block;
};

class ColumnSchema {
public:
    ColumnSchema(bool isUnflat, uint32_t dataChunksPos, uint32_t numBytes)
        : isUnflat{isUnflat}, dataChunkPos{dataChunksPos}, numBytes{numBytes}, mayContainNulls{
                                                                                   false} {}

    ColumnSchema(const ColumnSchema& other);

    inline bool isFlat() const { return !isUnflat; }

    inline uint32_t getDataChunkPos() const { return dataChunkPos; }

    inline uint32_t getNumBytes() const { return numBytes; }

    inline bool operator==(const ColumnSchema& other) const {
        return isUnflat == other.isUnflat && dataChunkPos == other.dataChunkPos &&
               numBytes == other.numBytes;
    }
    inline bool operator!=(const ColumnSchema& other) const { return !(*this == other); }

    inline void setMayContainsNullsToTrue() { mayContainNulls = true; }

    inline bool hasNoNullGuarantee() const { return !mayContainNulls; }

private:
    // We need isUnflat, dataChunkPos to know the factorization structure in the factorizedTable.
    bool isUnflat;
    uint32_t dataChunkPos;
    uint32_t numBytes;
    bool mayContainNulls;
};

class TableSchema {
public:
    TableSchema() = default;

    TableSchema(const TableSchema& other);

    explicit TableSchema(vector<unique_ptr<ColumnSchema>> columns) : columns{move(columns)} {}

    void appendColumn(unique_ptr<ColumnSchema> column);

    inline ColumnSchema* getColumn(uint32_t idx) const { return columns[idx].get(); }

    inline uint32_t getNumColumns() const { return columns.size(); }

    inline uint32_t getNullMapOffset() const { return numBytesForDataPerTuple; }

    inline uint32_t getNumBytesPerTuple() const { return numBytesPerTuple; }

    inline uint32_t getColOffset(uint32_t idx) const { return colOffsets[idx]; }

    inline void setMayContainsNullsToTrue(uint32_t idx) {
        assert(idx < columns.size());
        columns[idx]->setMayContainsNullsToTrue();
    }

    static inline uint32_t getNumBytesForNullBuffer(uint32_t numColumns) {
        return (numColumns >> 3) + ((numColumns & 7) != 0); // &7 is the same as %8;
    }

    inline bool isEmpty() const { return columns.empty(); }

    bool operator==(const TableSchema& other) const;
    inline bool operator!=(const TableSchema& other) const { return !(*this == other); }

private:
    vector<unique_ptr<ColumnSchema>> columns;
    uint32_t numBytesForDataPerTuple = 0;
    uint32_t numBytesForNullMapPerTuple = 0;
    uint32_t numBytesPerTuple = 0;
    vector<uint32_t> colOffsets;
};

class FlatTupleIterator;

class FactorizedTable {
    friend FlatTupleIterator;
    friend class JoinHashTable;

public:
    FactorizedTable(MemoryManager* memoryManager, unique_ptr<TableSchema> tableSchema);

    void append(const vector<shared_ptr<ValueVector>>& vectors);

    //! This function appends an empty tuple to the factorizedTable and returns a pointer to that
    //! tuple.
    uint8_t* appendEmptyTuple();

    // This function scans numTuplesToScan of rows to vectors starting at tupleIdx. Callers are
    // responsible for making sure all the parameters are valid.
    inline void scan(vector<shared_ptr<ValueVector>>& vectors, uint64_t tupleIdx,
        uint64_t numTuplesToScan) const {
        vector<uint32_t> colIdxes(tableSchema->getNumColumns());
        iota(colIdxes.begin(), colIdxes.end(), 0);
        scan(vectors, tupleIdx, numTuplesToScan, colIdxes);
    }
    void scan(vector<shared_ptr<ValueVector>>& vectors, uint64_t tupleIdx, uint64_t numTuplesToScan,
        vector<uint32_t>& colIdxToScan) const;
    void lookup(vector<shared_ptr<ValueVector>>& vectors, vector<uint32_t>& colIdxesToScan,
        uint8_t** tuplesToRead, uint64_t startPos, uint64_t numTuplesToRead) const;

    // When we merge two factorizedTables, we need to update the hasNoNullGuarantee based on
    // other factorizedTable.
    void mergeMayContainNulls(FactorizedTable& other);
    void merge(FactorizedTable& other);

    inline OverflowBuffer* getOverflowBuffer() const { return overflowBuffer.get(); }

    bool hasUnflatCol() const;
    inline bool hasUnflatCol(vector<uint32_t>& colIdxes) const {
        return any_of(colIdxes.begin(), colIdxes.end(),
            [this](uint64_t colIdx) { return !tableSchema->getColumn(colIdx)->isFlat(); });
    }

    inline uint64_t getNumTuples() const { return numTuples; }
    uint64_t getTotalNumFlatTuples() const;
    uint64_t getNumFlatTuples(uint64_t tupleIdx) const;

    inline vector<unique_ptr<DataBlock>>& getTupleDataBlocks() { return tupleDataBlocks; }
    inline const TableSchema* getTableSchema() const { return tableSchema.get(); }

    template<typename TYPE>
    inline TYPE getData(uint32_t blockIdx, uint32_t blockOffset, uint32_t colOffset) const {
        return *((TYPE*)getCell(blockIdx, blockOffset, colOffset));
    }

    uint8_t* getTuple(uint64_t tupleIdx) const;

    void updateFlatCell(uint8_t* tuplePtr, uint32_t colIdx, ValueVector* valueVector, uint32_t pos);
    inline void updateFlatCellNoNull(uint8_t* ftTuplePtr, uint32_t colIdx, void* dataBuf) {
        memcpy(ftTuplePtr + tableSchema->getColOffset(colIdx), dataBuf,
            tableSchema->getColumn(colIdx)->getNumBytes());
    }

    inline uint64_t getNumTuplesPerBlock() const { return numTuplesPerBlock; }

    inline bool hasNoNullGuarantee(uint32_t colIdx) const {
        return tableSchema->getColumn(colIdx)->hasNoNullGuarantee();
    }

    bool isOverflowColNull(const uint8_t* nullBuffer, uint32_t tupleIdx, uint32_t colIdx) const;
    bool isNonOverflowColNull(const uint8_t* nullBuffer, uint32_t colIdx) const;
    void setNonOverflowColNull(uint8_t* nullBuffer, uint32_t colIdx);

    static bool isNull(const uint8_t* nullMapBuffer, uint32_t idx);
    static void setNull(uint8_t* nullBuffer, uint32_t idx);

private:
    void setOverflowColNull(uint8_t* nullBuffer, uint32_t colIdx, uint32_t tupleIdx);

    uint64_t computeNumTuplesToAppend(const vector<shared_ptr<ValueVector>>& vectorsToAppend) const;

    inline uint8_t* getCell(uint32_t blockIdx, uint32_t blockOffset, uint32_t colOffset) const {
        return tupleDataBlocks[blockIdx]->getData() +
               blockOffset * tableSchema->getNumBytesPerTuple() + colOffset;
    }
    inline pair<uint64_t, uint64_t> getBlockIdxAndTupleIdxInBlock(uint64_t tupleIdx) const {
        return make_pair(tupleIdx / numTuplesPerBlock, tupleIdx % numTuplesPerBlock);
    }

    vector<BlockAppendingInfo> allocateTupleBlocks(uint64_t numTuplesToAppend);
    uint8_t* allocateOverflowBlocks(uint32_t numBytes);
    void writeFlatVectorToFlatColumn(
        const ValueVector& vector, uint8_t** tuples, uint32_t colIdx, uint64_t numTuplesToWrite);
    void writeUnflatVectorToFlatColumn(
        const ValueVector& vector, uint8_t** tuples, uint32_t colIdx, uint64_t numTuplesToWrite);
    inline void writeVectorToFlatColumn(
        const ValueVector& vector, uint8_t** tuples, uint32_t colIdx, uint64_t numTuplesToWrite) {
        vector.state->isFlat() ?
            writeFlatVectorToFlatColumn(vector, tuples, colIdx, numTuplesToWrite) :
            writeUnflatVectorToFlatColumn(vector, tuples, colIdx, numTuplesToWrite);
    }
    void writeVectorToUnflatColumn(
        const ValueVector& vector, uint8_t** tuples, uint32_t colIdx, uint64_t numTuplesToWrite);
    void writeVectorToColumn(
        const ValueVector& vector, uint8_t** tuples, uint32_t colIdx, uint64_t numTuplesToWrite);
    overflow_value_t appendUnFlatVectorToOverflowBlocks(const ValueVector& vector, uint32_t colIdx);

    void readUnflatCol(uint8_t** tuples, uint32_t colIdx, ValueVector& result) const;
    void readFlatColToFlatVector(uint8_t** tuples, uint32_t colIdx, ValueVector& result) const;
    void readFlatColToUnflatVector(
        uint8_t** tuples, uint32_t colIdx, ValueVector& result, uint64_t numTuplesToRead) const;
    inline void readFlatCol(
        uint8_t** tuples, uint32_t colIdx, ValueVector& result, uint64_t numTuplesToRead) const {
        result.state->isFlat() ? readFlatColToFlatVector(tuples, colIdx, result) :
                                 readFlatColToUnflatVector(tuples, colIdx, result, numTuplesToRead);
    }

    MemoryManager* memoryManager;
    unique_ptr<TableSchema> tableSchema;
    uint64_t numTuples;
    uint32_t numTuplesPerBlock;
    vector<unique_ptr<DataBlock>> tupleDataBlocks;
    vector<unique_ptr<DataBlock>> vectorOverflowBlocks;
    unique_ptr<OverflowBuffer> overflowBuffer;
};

} // namespace processor
} // namespace graphflow

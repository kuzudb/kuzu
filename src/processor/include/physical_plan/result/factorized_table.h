#pragma once

#include <unordered_map>

#include "src/common/include/memory_manager.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/string_buffer.h"
#include "src/processor/include/physical_plan/result/flat_tuple.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using namespace graphflow::common;
using namespace graphflow::processor;

namespace graphflow {
namespace processor {

struct BlockAppendingInfo {
    BlockAppendingInfo(uint8_t* data, uint64_t numEntriesToAppend)
        : data{data}, numEntriesToAppend{numEntriesToAppend} {}

    uint8_t* data;
    uint64_t numEntriesToAppend;
};

struct DataBlock {
public:
    explicit DataBlock(unique_ptr<MemoryBlock> block)
        : data{block->data}, freeSize{block->size}, numEntries{0}, block{move(block)} {}

public:
    uint8_t* data;
    uint64_t freeSize;
    uint64_t numEntries;

private:
    unique_ptr<MemoryBlock> block;
};

struct FieldInTupleSchema {
    FieldInTupleSchema(DataType dataType, bool isUnflat, uint32_t dataChunksPos)
        : dataType{dataType}, isUnflat{isUnflat}, dataChunkPos{dataChunksPos} {}

    inline bool operator==(const FieldInTupleSchema& other) const {
        return dataType == other.dataType && isUnflat == other.isUnflat;
    }
    inline bool operator!=(const FieldInTupleSchema& other) const { return !(*this == other); }

    inline uint64_t getFieldSize() const {
        return isUnflat ? sizeof(overflow_value_t) : TypeUtils::getDataTypeSize(dataType);
    }

    DataType dataType;
    // We need isUnflat, dataChunkPos to know the factorization structure in the factorizedTable.
    bool isUnflat;
    uint32_t dataChunkPos;
};

struct TupleSchema {
    TupleSchema() : numBytesPerTuple{0} {}

    explicit TupleSchema(vector<FieldInTupleSchema> fields)
        : fields{move(fields)}, numBytesPerTuple{0} {
        for (auto& field : this->fields) {
            numBytesPerTuple += field.getFieldSize();
        }
        initialize();
    }

    TupleSchema(const TupleSchema& tupleSchema) = default;

    inline void appendField(const FieldInTupleSchema& field) {
        assert(!isInitialized);
        fields.push_back(field);
        numBytesPerTuple += field.getFieldSize();
    }

    inline uint64_t getNullMapOffset() const { return numBytesPerTuple - getNullMapAlignedSize(); }

    inline uint64_t getNullMapAlignedSize() const {
        // 4 bytes alignment for nullMap
        return ((nullMapSizeInBytes >> 2) + ((nullMapSizeInBytes & 3) != 0))
               << 2; // &3 is the same as %4
    }

    inline void initialize() {
        assert(!isInitialized);
        // we utilize the bitmap to represent the nullMask for each column.
        // 1 byte nullMap can represent the nullMasks for 8 columns
        nullMapSizeInBytes =
            (this->fields.size() >> 3) + ((this->fields.size() & 7) != 0); // &7 is the same as %8
        numBytesPerTuple += getNullMapAlignedSize();
        isInitialized = true;
    }

    bool operator==(const TupleSchema& other) const;
    inline bool operator!=(const TupleSchema& other) const { return !(*this == other); }
    bool isInitialized = false;
    vector<FieldInTupleSchema> fields;
    uint64_t numBytesPerTuple;
    uint64_t nullMapSizeInBytes;
};

class FlatTupleIterator;

// To represent the null values in FactorizedTable, we use a bitmap to represent the null fields in
// each tuple
// 1. For unflat columns, we use a large bitmap to represent the nulls in the whole unflat
// columns and stores it at the end of the unflat column memory.
// 2. For all other columns, we just store a bitmap at the end of each tuple.
// For example: we have 3 columns: a1   a2(unflat)      a3
//                                  1   [null,4,6,7]      null
// Since the 3rd column is a null value, we set the 3rd bit(from right to left) of the first byte
// to 1.
// The memory of the factorizedTable looks like: 1st tuple: 1  overflowPtrToA2  0
// nullMap:4(00000100). Since the 1st element in the unflat column is a null value, we set the 1st
// bit(from right to left) of the first byte to 1.
// The unflat column memory: 3 4 6 0 nullMap:0(00000001).
class FactorizedTable {
public:
    FactorizedTable(MemoryManager& memoryManager, const TupleSchema& tupleSchema);
    void append(const vector<shared_ptr<ValueVector>>& vectors, uint64_t numTuplesToAppend);
    // Actual number of tuples scanned is returned. If it's 0, the scan already hits the end.
    uint64_t scan(const vector<uint64_t>& fieldsToScan, const vector<DataPos>& resultDataPos,
        ResultSet& resultSet, uint64_t startTupleIdx, uint64_t numTuplesToScan) const;
    uint64_t lookup(const vector<uint64_t>& fieldsToRead, const vector<DataPos>& resultDataPos,
        ResultSet& resultSet, uint8_t** tuplesToRead, uint64_t startPos,
        uint64_t numTuplesToRead) const;
    // This is a specialized scan function to read one flat tuple in factorizedTable to an
    // unflat valueVector.
    void readFlatTupleToUnflatVector(const vector<uint64_t>& fieldsToScan,
        const vector<DataPos>& resultDataPos, ResultSet& resultSet, uint64_t tupleIdx,
        uint64_t valuePosInVec) const;
    void merge(unique_ptr<FactorizedTable> other);
    uint64_t getFieldOffsetInTuple(uint64_t fieldId) const;
    bool hasUnflatColToRead(const vector<uint64_t>& fieldsToRead) const;

    inline uint64_t getNumTuples() const { return numTuples; }
    inline uint8_t* getTuple(uint64_t tupleIdx) const {
        assert(tupleIdx < numTuples);
        auto blockIdx = tupleIdx / numTuplesPerBlock;
        auto tupleIdxInBlock = tupleIdx % numTuplesPerBlock;
        return tupleDataBlocks[blockIdx].data + tupleIdxInBlock * tupleSchema.numBytesPerTuple;
    }

    inline uint64_t getNumFlatTuples(uint64_t tupleIdx) const {
        unordered_map<uint32_t, bool> calculatedDataChunkPoses;
        uint64_t numFlatTuples = 1;
        auto tupleBuffer = getTuple(tupleIdx);
        for (auto field : tupleSchema.fields) {
            if (!calculatedDataChunkPoses.contains(field.dataChunkPos)) {
                calculatedDataChunkPoses[field.dataChunkPos] = true;
                numFlatTuples *=
                    (field.isUnflat ? ((overflow_value_t*)tupleBuffer)->numElements : 1);
            }
            tupleBuffer += field.getFieldSize();
        }
        return numFlatTuples;
    }

    inline vector<DataBlock>& getTupleDataBlocks() { return tupleDataBlocks; }
    inline const TupleSchema& getTupleSchema() const { return tupleSchema; }
    inline static bool isNull(uint8_t* nullMapBuffer, uint64_t colIdx) {
        uint32_t nullMapIdx = colIdx >> 3;
        uint8_t nullMapMask = 0x1 << (colIdx & 7); // note: &7 is the same as %8
        return nullMapBuffer[nullMapIdx] & nullMapMask;
    }
    inline void setNullMap(uint8_t* nullMapBuffer, uint32_t colIdx) {
        uint32_t nullMapIdx = colIdx >> 3;
        uint8_t nullMapMask = 0x1 << (colIdx & 7); // note: &7 is the same as %8
        nullMapBuffer[nullMapIdx] |= nullMapMask;
    }

    FlatTupleIterator getFlatTuples();

    // This function returns a gf_string_t stored at the [tupleIdx, colIdx]. It throws an
    // exception if either the [tupleIdx, colIdx] is invalid or the cell is unflat.
    inline gf_string_t getString(uint64_t tupleIdx, uint64_t colIdx) const {
        assertTupleIdxColIdxAndValueIsFlat(tupleIdx, colIdx);
        return *((gf_string_t*)(getTuple(tupleIdx) + getFieldOffsetInTuple(colIdx)));
    }

    // This function returns a unstructured value stored at the [tupleIdx, colIdx]. It throws an
    // exception if either the [tupleIdx, colIdx] is invalid or the cell is unflat.
    inline Value getUnstrValue(uint64_t tupleIdx, uint64_t colIdx) const {
        assertTupleIdxColIdxAndValueIsFlat(tupleIdx, colIdx);
        return *((Value*)(getTuple(tupleIdx) + getFieldOffsetInTuple(colIdx)));
    }

private:
    inline void assertTupleIdxColIdxAndValueIsFlat(uint64_t tupleIdx, uint64_t colIdx) const {
        assert(tupleIdx < numTuples);
        assert(colIdx < tupleSchema.fields.size());
        assert(!tupleSchema.fields[colIdx].isUnflat);
    }

    vector<BlockAppendingInfo> allocateDataBlocks(vector<DataBlock>& dataBlocks,
        uint64_t numBytesPerEntry, uint64_t numEntriesToAppend, bool allocateOnlyFromLastBlock);

    void copyVectorToBlock(ValueVector& vector, const BlockAppendingInfo& blockAppendInfo,
        const FieldInTupleSchema& field, uint64_t posInVector, uint64_t offsetInTuple,
        uint64_t colIdx);
    overflow_value_t appendUnFlatVectorToOverflowBlocks(ValueVector& vector, uint64_t colIdx);

    void appendVector(ValueVector& valueVector, const vector<BlockAppendingInfo>& blockAppendInfos,
        const FieldInTupleSchema& field, uint64_t numTuples, uint64_t offsetInTuple,
        uint64_t colIdx);
    void readUnflatVector(uint8_t** tuples, uint64_t offsetInTuple, uint64_t startTuplePos,
        ValueVector& vector) const;
    void readFlatVector(uint8_t** tuples, uint64_t offsetInTuple, ValueVector& vector,
        uint64_t numTuplesToRead, uint64_t colIdx, uint64_t startPos, uint64_t valuePosInVec) const;
    // If the given vector is flat valuePosInVecIfUnflat will be ignored.
    void copyVectorDataToBuffer(ValueVector& vector, uint64_t valuePosInVecIfUnflat,
        uint8_t* buffer, uint64_t offsetInBuffer, uint64_t offsetStride, uint64_t numValues,
        uint64_t colIdx, bool isUnflat);

    vector<uint64_t> computeFieldOffsets(const vector<uint64_t>& fieldsToRead) const;

    MemoryManager& memoryManager;
    TupleSchema tupleSchema;
    uint64_t numTuples;
    uint64_t numTuplesPerBlock;
    vector<DataBlock> tupleDataBlocks;
    vector<DataBlock> vectorOverflowBlocks;
    unique_ptr<StringBuffer> stringBuffer;
};

class FlatTupleIterator {
public:
    explicit FlatTupleIterator(FactorizedTable& factorizedTable);

    inline bool hasNextFlatTuple() {
        return nextTupleIdx < factorizedTable.getNumTuples() || nextFlatTupleIdx < numFlatTuples;
    }

    FlatTuple getNextFlatTuple();

private:
    void readValueBufferToFlatTuple(
        DataType dataType, FlatTuple& flatTuple, uint64_t flatTupleValIdx, uint8_t* valueBuffer);

    void readUnflatColToFlatTuple(FlatTuple& flatTuple, uint64_t flatTupleValIdx,
        const FieldInTupleSchema& field, uint8_t* valueBuffer);

    void readFlatColToFlatTuple(FlatTuple& flatTuple, uint64_t flatTupleValIdx,
        const FieldInTupleSchema& field, uint8_t* valueBuffer);

    // The dataChunkPos may be not consecutive, which means some entries in the
    // flatTuplePositionsInDataChunk is invalid. We put pair(UINT64_MAX, UINT64_MAX) in the invalid
    // entries.
    inline bool isValidDataChunkPos(uint32_t dataChunkPos) const {
        return flatTuplePositionsInDataChunk[dataChunkPos].first != UINT64_MAX;
    }

    // We put pair(UINT64_MAX, UINT64_MAX) in all invalid entries in FlatTuplePositionsInDataChunk.
    void updateInvalidEntriesInFlatTuplePositionsInDataChunk();

    // This function is used to update the number of elements in the dataChunk when we want
    // to iterate a new tuple.
    void updateNumElementsInDataChunk();

    // This function updates the flatTuplePositionsInDataChunk, so that getNextFlatTuple() can
    // correctly outputs the next flat tuple in the current tuple. For example, we want to read two
    // unflat columns, which are on different dataChunks A,B and both have 100 columns. The
    // flatTuplePositionsInDataChunk after the first call to getNextFlatTuple() looks like:
    // {dataChunkA : [0, 100]}, {dataChunkB : [0, 100]} This function updates the
    // flatTuplePositionsInDataChunk to: {dataChunkA: [1, 100]}, {dataChunkB: [0, 100]}. Meaning
    // that the getNextFlatTuple() should read the second element in the first unflat column and the
    // first element in the second unflat column.
    void updateFlatTuplePositionsInDataChunk();

    FactorizedTable& factorizedTable;
    // This field is used to construct the result flat tuple.
    vector<DataType> dataTypes;
    uint8_t* currentTupleBuffer;
    uint64_t numFlatTuples;
    uint64_t nextFlatTupleIdx;
    uint64_t nextTupleIdx;
    // This field stores the (nextIdxToReadInDataChunk, numElementsInDataChunk) of each dataChunk.
    vector<pair<uint64_t, uint64_t>> flatTuplePositionsInDataChunk;
};

} // namespace processor
} // namespace graphflow

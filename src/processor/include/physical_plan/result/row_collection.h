#pragma once

#include "src/common/include/memory_manager.h"
#include "src/common/include/types.h"
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

struct FieldInLayout {
    FieldInLayout(DataType dataType, uint64_t size, bool isVectorOverflow)
        : dataType{dataType}, size{size}, isVectorOverflow{isVectorOverflow} {}

    inline bool operator==(const FieldInLayout& other) const {
        return dataType == other.dataType && size == other.size &&
               isVectorOverflow == other.isVectorOverflow;
    }
    inline bool operator!=(const FieldInLayout& other) const { return !(*this == other); }

    DataType dataType;
    uint64_t size;
    bool isVectorOverflow;
};

struct RowLayout {
    RowLayout() : numBytesPerRow{0} {}

    explicit RowLayout(vector<FieldInLayout> fields) : fields{move(fields)}, numBytesPerRow{0} {
        for (auto& field : this->fields) {
            numBytesPerRow += field.size;
        }
        initialize();
    }

    RowLayout(const RowLayout& layout) = default;

    inline void appendField(const FieldInLayout& field) {
        assert(!isInitialized);
        fields.push_back(field);
        numBytesPerRow += field.size;
    }

    inline uint64_t getNullMapOffset() const { return numBytesPerRow - getNullMapAlignedSize(); }

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
        numBytesPerRow += getNullMapAlignedSize();
        isInitialized = true;
    }

    bool operator==(const RowLayout& other) const;
    inline bool operator!=(const RowLayout& other) const { return !(*this == other); }
    bool isInitialized = false;
    vector<FieldInLayout> fields;
    uint64_t numBytesPerRow;
    uint64_t nullMapSizeInBytes;
};

// To represent the null values in RowCollection, we use a bitmap to represent the null fields in
// each row
// 1. For overflow columns, we use a large bitmap to represent the nulls in the whole overflow
// columns and stores it at the end of the overflow memory.
// 2. For all other columns, we just store a bitmap at the end of each row.
// For example: we have 3 columns: a1   a2(overflow)      a3
//                                  1   [null,4,6,7]      null
// Since the 3rd column is a null value, we set the 3rd bit(from right to left) of the first byte
// to 1.
// The memory of the row Collection looks like: 1st row: 1  overflowPtrToA2  0 nullMap:4(00000100).
// Since the 1st element in the overflow column is a null value, we set the 1st
// bit(from right to left) of the first byte to 1.
// The overflow column memory: 3 4 6 0 nullMap:0(00000001).
class RowCollection {
public:
    RowCollection(MemoryManager& memoryManager, const RowLayout& layout);
    void append(const vector<shared_ptr<ValueVector>>& vectors, uint64_t numRowsToAppend);
    // Actual number of rows scanned is returned. If it's 0, the scan already hits the end.
    uint64_t scan(const vector<uint64_t>& fieldsToScan, const vector<DataPos>& resultDataPos,
        ResultSet& resultSet, uint64_t startRowId, uint64_t numRowsToScan) const;
    uint64_t lookup(const vector<uint64_t>& fieldsToRead, const vector<DataPos>& resultDataPos,
        ResultSet& resultSet, uint8_t** rowsToRead, uint64_t startPos,
        uint64_t numRowsToRead) const;
    void merge(unique_ptr<RowCollection> other);
    uint64_t getFieldOffsetInRow(uint64_t fieldId) const;

    inline uint64_t getNumRows() const { return numRows; }
    inline uint8_t* getRow(uint64_t rowId) const {
        assert(rowId < numRows);
        auto blockId = rowId / numRowsPerBlock;
        auto rowIdInBlock = rowId % numRowsPerBlock;
        return rowDataBlocks[blockId].data + rowIdInBlock * layout.numBytesPerRow;
    }
    inline vector<DataBlock>& getRowDataBlocks() { return rowDataBlocks; }
    inline const RowLayout& getLayout() const { return layout; }
    inline bool isNull(uint8_t* nullMapBuffer, uint64_t colIdx) const {
        uint32_t nullMapIdx = colIdx >> 3;
        uint8_t nullMapMask = 0x1 << (colIdx & 7); // note: &7 is the same as %8
        return nullMapBuffer[nullMapIdx] & nullMapMask;
    }
    inline void setNullMap(uint8_t* nullMapBuffer, uint32_t colIdx) {
        uint32_t nullMapIdx = colIdx >> 3;
        uint8_t nullMapMask = 0x1 << (colIdx & 7); // note: &7 is the same as %8
        nullMapBuffer[nullMapIdx] |= nullMapMask;
    }

private:
    vector<BlockAppendingInfo> allocateDataBlocks(vector<DataBlock>& dataBlocks,
        uint64_t numBytesPerEntry, uint64_t numEntriesToAppend, bool allocateOnlyFromLastBlock);

    void copyVectorToBlock(const ValueVector& vector, const BlockAppendingInfo& blockAppendInfo,
        const FieldInLayout& field, uint64_t posInVector, uint64_t offsetInRow, uint64_t colIdx);
    overflow_value_t appendUnFlatVectorToOverflowBlocks(const ValueVector& vector, uint64_t colIdx);

    void appendVector(ValueVector& valueVector, const vector<BlockAppendingInfo>& blockAppendInfos,
        const FieldInLayout& field, uint64_t numRows, uint64_t offsetInRow, uint64_t colIdx);
    void readOverflowVector(
        uint8_t** rows, uint64_t offsetInRow, uint64_t startRowPos, ValueVector& vector) const;
    void readNonOverflowVector(uint8_t** rows, uint64_t offsetInRow, ValueVector& vector,
        uint64_t numRowsToRead, uint64_t colIdx) const;
    void copyVectorDataToBuffer(const ValueVector& vector, uint64_t valuePosInVec,
        uint64_t posStride, uint8_t* buffer, uint64_t offsetInBuffer, uint64_t offsetStride,
        uint64_t numValues, uint64_t colIdx, bool isVectorOverflow);

    MemoryManager& memoryManager;
    RowLayout layout;
    uint64_t numRows;
    uint64_t numRowsPerBlock;
    vector<DataBlock> rowDataBlocks;
    vector<DataBlock> vectorOverflowBlocks;
};
} // namespace processor
} // namespace graphflow

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
    FieldInLayout(uint64_t size, bool isVectorOverflow)
        : size{size}, isVectorOverflow{isVectorOverflow} {}

    inline bool operator==(const FieldInLayout& other) const {
        return size == other.size && isVectorOverflow == other.isVectorOverflow;
    }
    inline bool operator!=(const FieldInLayout& other) const { return !(*this == other); }

    uint64_t size;
    bool isVectorOverflow;
};

struct RowLayout {
    RowLayout() : numBytesPerRow{0} {}

    explicit RowLayout(vector<FieldInLayout> fields) : fields{move(fields)}, numBytesPerRow{0} {
        for (auto& field : this->fields) {
            numBytesPerRow += field.size;
        }
    }

    RowLayout(const RowLayout& layout) = default;

    inline void appendField(const FieldInLayout& field) {
        fields.push_back(field);
        numBytesPerRow += field.size;
    }

    bool operator==(const RowLayout& other) const;
    inline bool operator!=(const RowLayout& other) const { return !(*this == other); }

    vector<FieldInLayout> fields;
    uint64_t numBytesPerRow;
};

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

    inline uint64_t getNumRows() const { return numRows; }
    inline uint8_t* getRow(uint64_t rowId) const {
        assert(rowId < numRows);
        auto blockId = rowId / numRowsPerBlock;
        auto rowIdInBlock = rowId % numRowsPerBlock;
        return rowDataBlocks[blockId].data + rowIdInBlock * layout.numBytesPerRow;
    }
    inline vector<DataBlock>& getRowDataBlocks() { return rowDataBlocks; }
    inline const RowLayout& getLayout() { return layout; }

    uint64_t getFieldOffsetInRow(uint64_t fieldId) const;

private:
    vector<BlockAppendingInfo> allocateDataBlocks(vector<DataBlock>& dataBlocks,
        uint64_t numBytesPerEntry, uint64_t numEntriesToAppend, bool allocateOnlyFromLastBlock);

    void copyVectorToBlock(const ValueVector& vector, const BlockAppendingInfo& blockAppendInfo,
        const FieldInLayout& field, uint64_t posInVector, uint64_t offsetInRow);
    overflow_value_t appendUnFlatVectorToOverflowBlocks(const ValueVector& vector);

    void appendVector(ValueVector& valueVector, const vector<BlockAppendingInfo>& blockAppendInfos,
        const FieldInLayout& field, uint64_t numRows, uint64_t offsetInRow);
    void readVector(const FieldInLayout& field, uint8_t** rows, uint64_t offsetInRow,
        uint64_t startRowPos, ValueVector& vector, uint64_t numValues) const;

    static void copyVectorDataToBuffer(const ValueVector& vector, uint64_t valuePosInVec,
        uint64_t posStride, uint8_t* buffer, uint64_t offsetInBuffer, uint64_t offsetStride,
        uint64_t numValues);
    static void copyBufferDataToVector(uint8_t** locations, uint64_t offset, uint64_t length,
        ValueVector& vector, uint64_t valuePosInVec, uint64_t numValues);

    MemoryManager& memoryManager;
    RowLayout layout;
    uint64_t numRows;
    uint64_t numRowsPerBlock;
    vector<DataBlock> rowDataBlocks;
    vector<DataBlock> vectorOverflowBlocks;
};
} // namespace processor
} // namespace graphflow

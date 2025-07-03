#pragma once

#include "storage/table/column_reader_writer.h"

namespace kuzu {
namespace storage {
class MemoryManager;

class NullColumn;
class StructColumn;
class RelTableData;
struct ColumnCheckpointState;
class PageAllocator;

class Column {
    friend class StringColumn;
    friend class StructColumn;
    friend class ListColumn;
    friend class RelTableData;

public:
    Column(std::string name, common::LogicalType dataType, FileHandle* dataFH, MemoryManager* mm,
        ShadowFile* shadowFile, bool enableCompression, bool requireNullColumn = true);
    Column(std::string name, common::PhysicalTypeID physicalType, FileHandle* dataFH,
        MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression,
        bool requireNullColumn = true);

    virtual ~Column();

    void populateExtraChunkState(ChunkState& state) const;

    static std::unique_ptr<ColumnChunkData> flushChunkData(const ColumnChunkData& chunkData,
        PageAllocator& pageAllocator);
    static std::unique_ptr<ColumnChunkData> flushNonNestedChunkData(
        const ColumnChunkData& chunkData, PageAllocator& pageAllocator);
    static ColumnChunkMetadata flushData(const ColumnChunkData& chunkData,
        PageAllocator& pageAllocator);

    virtual void scan(const ChunkState& state, common::offset_t startOffsetInChunk,
        common::row_idx_t numValuesToScan, common::ValueVector* resultVector) const;
    virtual void lookupValue(const ChunkState& state, common::offset_t nodeOffset,
        common::ValueVector* resultVector, uint32_t posInVector) const;

    // Scan from [startOffsetInGroup, endOffsetInGroup).
    virtual void scan(const ChunkState& state, common::offset_t startOffsetInGroup,
        common::offset_t endOffsetInGroup, common::ValueVector* resultVector,
        uint64_t offsetInVector) const;
    // Scan from [startOffsetInGroup, endOffsetInGroup).
    virtual void scan(const ChunkState& state, ColumnChunkData* columnChunk,
        common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) const;

    common::LogicalType& getDataType() { return dataType; }
    const common::LogicalType& getDataType() const { return dataType; }

    Column* getNullColumn() const;

    std::string getName() const { return name; }

    virtual void scan(const ChunkState& state, common::offset_t startOffsetInGroup,
        common::offset_t endOffsetInGroup, uint8_t* result);

    // Batch write to a set of sequential pages.
    virtual void write(ColumnChunkData& persistentChunk, ChunkState& state,
        common::offset_t dstOffset, ColumnChunkData* data, common::offset_t srcOffset,
        common::length_t numValues);

    // Append values to the end of the node group, resizing it if necessary
    common::offset_t appendValues(ColumnChunkData& persistentChunk, ChunkState& state,
        const uint8_t* data, const common::NullMask* nullChunkData, common::offset_t numValues);

    virtual void checkpointColumnChunk(ColumnCheckpointState& checkpointState,
        PageAllocator& pageAllocator);

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET& cast() const {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }

protected:
    virtual void scanInternal(const ChunkState& state, common::offset_t startOffsetInChunk,
        common::row_idx_t numValuesToScan, common::ValueVector* resultVector) const;

    virtual void lookupInternal(const ChunkState& state, common::offset_t nodeOffset,
        common::ValueVector* resultVector, uint32_t posInVector) const;

    void writeValues(ChunkState& state, common::offset_t dstOffset, const uint8_t* data,
        const common::NullMask* nullChunkData, common::offset_t srcOffset = 0,
        common::offset_t numValues = 1) const;

    void updateStatistics(ColumnChunkMetadata& metadata, common::offset_t maxIndex,
        const std::optional<StorageValue>& min, const std::optional<StorageValue>& max) const;

protected:
    bool isEndOffsetOutOfPagesCapacity(const ColumnChunkMetadata& metadata,
        common::offset_t endOffset) const;

    virtual bool canCheckpointInPlace(const ChunkState& state,
        const ColumnCheckpointState& checkpointState);

    void checkpointColumnChunkInPlace(ChunkState& state,
        const ColumnCheckpointState& checkpointState, PageAllocator& pageAllocator);
    void checkpointNullData(const ColumnCheckpointState& checkpointState,
        PageAllocator& pageAllocator) const;

    void checkpointColumnChunkOutOfPlace(const ChunkState& state,
        const ColumnCheckpointState& checkpointState, PageAllocator& pageAllocator) const;

    // check if val is in range [start, end)
    static bool isInRange(uint64_t val, uint64_t start, uint64_t end) {
        return val >= start && val < end;
    }

protected:
    std::string name;
    common::LogicalType dataType;
    MemoryManager* mm;
    FileHandle* dataFH;
    ShadowFile* shadowFile;
    std::unique_ptr<NullColumn> nullColumn;
    read_values_to_vector_func_t readToVectorFunc;
    write_values_func_t writeFunc;
    read_values_to_page_func_t readToPageFunc;
    bool enableCompression;

    std::unique_ptr<ColumnReadWriter> columnReadWriter;
};

class InternalIDColumn final : public Column {
public:
    InternalIDColumn(std::string name, FileHandle* dataFH, MemoryManager* mm,
        ShadowFile* shadowFile, bool enableCompression);

    void scan(const ChunkState& state, common::offset_t startOffsetInChunk,
        common::row_idx_t numValuesToScan, common::ValueVector* resultVector) const override {
        Column::scan(state, startOffsetInChunk, numValuesToScan, resultVector);
        populateCommonTableID(resultVector);
    }

    void scan(const ChunkState& state, common::offset_t startOffsetInGroup,
        common::offset_t endOffsetInGroup, common::ValueVector* resultVector,
        uint64_t offsetInVector) const override {
        Column::scan(state, startOffsetInGroup, endOffsetInGroup, resultVector, offsetInVector);
        populateCommonTableID(resultVector);
    }

    void lookupInternal(const ChunkState& state, common::offset_t nodeOffset,
        common::ValueVector* resultVector, uint32_t posInVector) const override {
        Column::lookupInternal(state, nodeOffset, resultVector, posInVector);
        populateCommonTableID(resultVector);
    }

    common::table_id_t getCommonTableID() const { return commonTableID; }
    // TODO(Guodong): This function should be removed through rewriting INTERNAL_ID as STRUCT.
    void setCommonTableID(common::table_id_t tableID) { commonTableID = tableID; }

private:
    void populateCommonTableID(const common::ValueVector* resultVector) const;

private:
    common::table_id_t commonTableID;
};

struct ColumnFactory {
    static std::unique_ptr<Column> createColumn(std::string name, common::LogicalType dataType,
        FileHandle* dataFH, MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression);
    static std::unique_ptr<Column> createColumn(std::string name,
        common::PhysicalTypeID physicalType, FileHandle* dataFH, MemoryManager* mm,
        ShadowFile* shadowFile, bool enableCompression);
};

} // namespace storage
} // namespace kuzu

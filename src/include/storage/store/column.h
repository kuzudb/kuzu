#pragma once

#include "catalog/catalog.h"
#include "common/null_mask.h"
#include "common/types/types.h"
#include "storage/db_file_id.h"
#include "storage/store/column_chunk.h"
#include "storage/store/column_chunk_data.h"
#include "storage/store/column_reader_writer.h"

namespace kuzu {
namespace evaluator {
class ExpressionEvaluator;
} // namespace evaluator
namespace storage {
class MemoryManager;

struct CompressionMetadata;

// This is a special usage for the `batchLookup` interface.
using batch_lookup_func_t = read_values_to_page_func_t;

class NullColumn;
class StructColumn;
class RelTableData;
struct ColumnCheckpointState;
class ColumnChunk;
class BufferManager;
class Column {
    friend class StringColumn;
    friend class StructColumn;
    friend class ListColumn;
    friend class RelTableData;

public:
    // TODO(Guodong): Remove transaction from interface of Column. There is no need to be aware
    // of transaction when reading/writing from/to disk pages.
    Column(std::string name, common::LogicalType dataType, FileHandle* dataFH, MemoryManager* mm,
        ShadowFile* shadowFile, bool enableCompression, bool requireNullColumn = true);
    Column(std::string name, common::PhysicalTypeID physicalType, FileHandle* dataFH,
        MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression,
        bool requireNullColumn = true);

    virtual ~Column();

    void populateExtraChunkState(SegmentState& state) const;

    static std::unique_ptr<ColumnChunkData> flushChunkData(const ColumnChunkData& chunkData,
        FileHandle& dataFH);
    static std::unique_ptr<ColumnChunkData> flushNonNestedChunkData(
        const ColumnChunkData& chunkData, FileHandle& dataFH);
    static ColumnChunkMetadata flushData(const ColumnChunkData& chunkData, FileHandle& dataFH);

    // Use lookupInternal to specialize
    void lookupValue(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, common::ValueVector* resultVector, uint32_t posInVector) const;

    // Scan from [offsetInChunk, offsetInChunk + length) (use scanInternal to specialize).
    // FIXME(bmwinger): this function does not take into account the resultVector's SelectionVector
    // However its specializations in StringColumn and ListColumn do
    virtual void scan(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t length,
        common::ValueVector* resultVector, uint64_t offsetInVector) const;
    // Scan from [offsetInChunk, offsetInChunk + length) (use scanInternal to specialize).
    // Appends to the end of the columnChunk
    virtual void scan(const transaction::Transaction* transaction, const ChunkState& state,
        ColumnChunkData* columnChunk, common::offset_t offsetInChunk = 0,
        common::offset_t numValues = UINT64_MAX) const;
    // Scan from [offsetInChunk, offsetInChunk + length) (use scanInternal to specialize).
    // Appends to the end of the columnChunk
    virtual void scanSegment(const transaction::Transaction* transaction, const SegmentState& state,
        ColumnChunkData* columnChunk, common::offset_t offsetInSegment,
        common::offset_t numValue) const;
    // Scan to raw data (does not scan any nested data and should only be used on primitive columns)
    void scanSegment(const transaction::Transaction* transaction, const SegmentState& state,
        common::offset_t startOffsetInSegment, common::offset_t length, uint8_t* result);
    // Scan to raw data (does not scan any nested data and should only be used on primitive columns)
    void scan(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t length, uint8_t* result);

    common::LogicalType& getDataType() { return dataType; }
    const common::LogicalType& getDataType() const { return dataType; }

    Column* getNullColumn() const;

    std::string_view getName() const { return name; }

    // Batch write to a set of sequential pages.
    void write(ColumnChunkData& persistentChunk, ChunkState& state, common::offset_t dstOffset,
        const ColumnChunkData& data, common::offset_t srcOffset, common::length_t numValues) const;

    virtual void writeInternal(ColumnChunkData& persistentChunk, SegmentState& state,
        common::offset_t dstOffsetInSegment, const ColumnChunkData& data,
        common::offset_t srcOffset, common::length_t numValues) const;

    // Append values to the end of the node group, resizing it if necessary
    common::offset_t appendValues(ColumnChunkData& persistentChunk, SegmentState& state,
        const uint8_t* data, const common::NullMask* nullChunkData,
        common::offset_t numValues) const;

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET& cast() const {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }

    virtual void checkpointSegment(ColumnCheckpointState&& checkpointState) const;

protected:
    virtual void scanInternal(const transaction::Transaction* transaction,
        const SegmentState& state, common::offset_t startOffsetInSegment,
        common::row_idx_t numValuesToScan, common::ValueVector* resultVector,
        common::offset_t startOffsetInVector) const;

    virtual void scanSegment(const transaction::Transaction* transaction, const SegmentState& state,
        common::offset_t startOffsetInSegment, common::row_idx_t numValuesToScan,
        common::ValueVector* resultVector, common::offset_t startOffsetInVector) const;

    // Writes to the end of the result chunk
    virtual void scanInternal(const transaction::Transaction* transaction,
        const SegmentState& state, common::offset_t startOffsetInSegment,
        common::row_idx_t numValuesToScan, ColumnChunkData* resultChunk) const;

    virtual void lookupInternal(const transaction::Transaction* transaction,
        const SegmentState& state, common::offset_t offsetInSegment,
        common::ValueVector* resultVector, uint32_t posInVector) const;

    void writeValues(ChunkState& state, common::offset_t dstOffset, const uint8_t* data,
        const common::NullMask* nullChunkData, common::offset_t srcOffset = 0,
        common::offset_t numValues = 1) const;

    void writeValuesInternal(SegmentState& state, common::offset_t dstOffsetInSegment,
        const uint8_t* data, const common::NullMask* nullChunkData, common::offset_t srcOffset = 0,
        common::offset_t numValues = 1) const;

    // Produces a page cursor for the offset relative to the given node group
    static PageCursor getPageCursorForOffsetInGroup(common::offset_t offsetInChunk,
        const ChunkState& state);
    void updatePageWithCursor(PageCursor cursor,
        const std::function<void(uint8_t*, common::offset_t)>& writeOp) const;

    void updateStatistics(ColumnChunkMetadata& metadata, common::offset_t maxIndex,
        const std::optional<StorageValue>& min, const std::optional<StorageValue>& max) const;

protected:
    bool isEndOffsetOutOfPagesCapacity(const ColumnChunkMetadata& metadata,
        common::offset_t maxOffset) const;

    virtual bool canCheckpointInPlace(const SegmentState& state,
        const ColumnCheckpointState& checkpointState) const;

    void checkpointColumnChunkInPlace(SegmentState& state,
        const ColumnCheckpointState& checkpointState) const;
    void checkpointNullData(const ColumnCheckpointState& checkpointState) const;

    void checkpointColumnChunkOutOfPlace(const SegmentState& state,
        const ColumnCheckpointState& checkpointState) const;

    // check if val is in range [start, end)
    static bool isInRange(uint64_t val, uint64_t start, uint64_t end) {
        return val >= start && val < end;
    }

protected:
    std::string name;
    DBFileID dbFileID;
    common::LogicalType dataType;
    FileHandle* dataFH;
    MemoryManager* mm;
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

    void scan(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t length,
        common::ValueVector* resultVector, uint64_t offsetInVector) const override {
        Column::scan(transaction, state, startOffsetInGroup, length, resultVector, offsetInVector);
        populateCommonTableID(resultVector);
    }

    void lookupInternal(const transaction::Transaction* transaction, const SegmentState& state,
        common::offset_t offsetInSegment, common::ValueVector* resultVector,
        uint32_t posInVector) const override {
        Column::lookupInternal(transaction, state, offsetInSegment, resultVector, posInVector);
        populateCommonTableID(resultVector);
    }

    common::table_id_t getCommonTableID() const { return commonTableID; }
    // TODO(Guodong): This function should be removed through rewritting INTERNAL_ID as STRUCT.
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
